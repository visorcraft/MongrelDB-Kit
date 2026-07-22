import { RemoteDatabase as NativeRemoteDatabase } from '@visorcraft/mongreldb/native.js';
import { tableFromIPC, tableFromJSON, type Table as ArrowTable } from 'apache-arrow';
import { randomBytes } from 'node:crypto';
import type {
	CancelOutcome,
	SqlCommitHlc,
	SqlOptions,
	SqlQuery,
	SqlQueryStatus
} from './db.js';
import {
	CommitOutcomeError,
	CapabilityUnsupportedError,
	KitError,
	QueryCancelledError,
	QueryIdConflictError,
	QueryExecutionError,
	QueryOutcomeUnknownError,
	RemoteProtocolError,
	ResultLimitExceededError,
	SerializationError,
	QueryTimeoutError,
	TransactionAbortedError
} from './errors.js';
import type { SqlErrorMetadata } from './errors.js';
import { procedureJson, type ProcedureCallOptions, type ProcedureSpec } from './procedure.js';
import { triggerJson, type TriggerSpec } from './trigger.js';
import {
	createVirtualTableSql,
	dropVirtualTableSql,
	type VirtualTableSpec
} from './external.js';

const SQL_RECOVERY_WINDOW_MS = 2_000;
const SQL_RECOVERY_REQUEST_TIMEOUT_MS = 250;
const SQL_RECOVERY_POLL_INTERVAL_MS = 25;
const MAX_U64 = 18_446_744_073_709_551_615n;
const MAX_CONTROL_JSON_RESPONSE_BYTES = 1024 * 1024;
const MAX_PAGE_JSON_RESPONSE_BYTES = 64 * 1024 * 1024;
const TERMINAL_SQL_STATES = new Set(['completed', 'failed', 'cancelled', 'pre_cancelled']);

function containsHttpHeaderControl(value: string): boolean {
	return /[\u0000-\u001f\u007f]/u.test(value);
}

function parseJsonStrict(text: string): unknown {
	let offset = 0;
	const fail = (message: string): never => {
		throw new SyntaxError(`${message} at JSON offset ${offset}`);
	};
	const whitespace = () => {
		while (offset < text.length
			&& [' ', '\t', '\r', '\n'].includes(text[offset]!)) offset += 1;
	};
	const string = (): string => {
		if (text[offset] !== '"') fail('expected string');
		const start = offset++;
		while (offset < text.length) {
			const char = text[offset++]!;
			if (char === '"') return JSON.parse(text.slice(start, offset)) as string;
			if (char.charCodeAt(0) < 0x20) fail('unescaped control character');
			if (char !== '\\') continue;
			if (offset >= text.length) fail('unterminated escape');
			const escape = text[offset++]!;
			if (escape === 'u') {
				if (!/^[0-9a-fA-F]{4}$/.test(text.slice(offset, offset + 4))) {
					fail('invalid unicode escape');
				}
				offset += 4;
			} else if (!'"\\/bfnrt'.includes(escape)) {
				fail('invalid string escape');
			}
		}
		return fail('unterminated string');
	};
	const value = (): unknown => {
		whitespace();
		const char = text[offset];
		if (char === '"') return string();
		if (char === '{') {
			offset += 1;
			const object: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
			const keys = new Set<string>();
			whitespace();
			if (text[offset] === '}') {
				offset += 1;
				return object;
			}
			while (true) {
				whitespace();
				const key = string();
				if (keys.has(key)) fail(`duplicate JSON object key ${JSON.stringify(key)}`);
				keys.add(key);
				whitespace();
				if (text[offset++] !== ':') fail('expected colon');
				object[key] = value();
				whitespace();
				const separator = text[offset++];
				if (separator === '}') return object;
				if (separator !== ',') fail('expected comma or object end');
			}
		}
		if (char === '[') {
			offset += 1;
			const array: unknown[] = [];
			whitespace();
			if (text[offset] === ']') {
				offset += 1;
				return array;
			}
			while (true) {
				array.push(value());
				whitespace();
				const separator = text[offset++];
				if (separator === ']') return array;
				if (separator !== ',') fail('expected comma or array end');
			}
		}
		for (const [literal, parsed] of [['true', true], ['false', false], ['null', null]] as const) {
			if (text.startsWith(literal, offset)) {
				offset += literal.length;
				return parsed;
			}
		}
		const number = text.slice(offset).match(/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/u)?.[0];
		if (!number) return fail('expected JSON value');
		offset += number.length;
		const parsed = Number(number);
		if (!Number.isFinite(parsed)) return fail('non-finite JSON number');
		return parsed;
	};
	const parsed = value();
	whitespace();
	if (offset !== text.length) fail('trailing JSON data');
	return parsed;
}

async function responseBytesBounded(response: Response, limit: number): Promise<Uint8Array> {
	const declared = response.headers.get('content-length');
	if (declared !== null) {
		if (!/^\d+$/u.test(declared) || Number(declared) > limit) {
			throw new SyntaxError(`HTTP response exceeded ${limit} bytes`);
		}
	}
	if (!response.body) return new Uint8Array();
	const reader = response.body.getReader();
	const chunks: Uint8Array[] = [];
	let length = 0;
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			length += value.byteLength;
			if (length > limit) {
				await reader.cancel().catch(() => undefined);
				throw new SyntaxError(`HTTP response exceeded ${limit} bytes`);
			}
			chunks.push(value);
		}
	} finally {
		reader.releaseLock();
	}
	const bytes = new Uint8Array(length);
	let offset = 0;
	for (const chunk of chunks) {
		bytes.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return bytes;
}

async function responseTextBounded(response: Response, limit: number): Promise<string> {
	try {
		return new TextDecoder('utf-8', { fatal: true }).decode(
			await responseBytesBounded(response, limit)
		);
	} catch {
		throw new SyntaxError('HTTP response was not valid UTF-8');
	}
}

async function responseJsonStrict(
	response: Response,
	limit = MAX_CONTROL_JSON_RESPONSE_BYTES
): Promise<unknown> {
	return parseJsonStrict(await responseTextBounded(response, limit));
}

function preExecutionCancellationError(
	queryId: string,
	outcome?: CancelOutcome
): KitError {
	const metadata: SqlErrorMetadata = {
		cancelOutcome: outcome,
		cancellationReason: 'client_request',
		retryable: false,
		serverState: 'pre_cancelled'
	};
	if (outcome === 'not_found') {
		return new KitError(
			'SQL pre-registration cancellation was not found by the server',
			'QUERY_NOT_FOUND'
		);
	}
	if (outcome === 'too_late') {
		return new KitError('SQL cancellation arrived after the commit fence', 'CANCEL_TOO_LATE');
	}
	if (outcome === 'already_finished') {
		return new KitError('SQL query ID already belongs to a finished query', 'QUERY_ALREADY_FINISHED');
	}
	return new QueryCancelledError(
		queryId,
		'SQL query cancelled before execution',
		{ committed: false, committedStatements: 0, completedStatements: 0, statementIndex: 0 },
		metadata
	);
}

type RemoteRetention = {
	setHistoryRetentionEpochs(epochs: bigint): void;
	historyRetentionEpochs(): bigint;
	earliestRetainedEpoch(): bigint;
};

export type RemoteAuth =
	| { bearerToken: string }
	| { username: string; password: string };

export type RemoteOptions = {
	auth?: RemoteAuth;
};

export type RemoteSqlPaginationOptions = {
	queryId?: string;
	timeoutMs?: number;
	projection: string[];
	pageSizeRows: number;
	maxPageBytes?: number;
	maxPageTokens?: number;
	maxOutputRows?: number;
	maxOutputBytes?: number;
};

export type RemoteSqlControlOptions = {
	queryId?: string;
	timeoutMs?: number;
	signal?: AbortSignal;
};

export type RemoteSqlPage = {
	status: string;
	rows: Record<string, unknown>[];
	nextCursor?: string;
	page: {
		offset: number;
		rowCount: number;
		totalRows: number;
		byteCount: number;
		estimatedTokens: number;
		limits: { rows: number; bytes: number; tokens: number };
		projection: string[];
		expiresAtMs: number;
		snapshot: string;
		tokenEstimate: string;
	};
};

export type RemoteIdempotentSqlOptions = {
	queryId?: string;
	timeoutMs?: number;
	idempotencyKey: string;
	maxOutputRows?: number;
	maxOutputBytes?: number;
};

export type RemoteSqlWriteReceipt = {
	queryId: string;
	originalQueryId: string;
	status: string;
	terminalState?: string;
	serverState?: string;
	cancelOutcome?: CancelOutcome;
	cancellationReason?: string;
	committed: boolean;
	committedStatements: number;
	lastCommitEpoch?: bigint;
	firstCommitStatementIndex?: number;
	lastCommitStatementIndex?: number;
	completedStatements: number;
	statementIndex: number;
	retryable: boolean;
	idempotencyReplayed: boolean;
	idempotencyPersisted: boolean;
	idempotencyExpiresAtMs: number;
	terminalError?: { code: string; category: string };
};

/** Structural HLC from server durable recovery (0.64+). */
export type RemoteCommitHlc = {
	physical_micros: number;
	logical?: number;
	node_tiebreaker?: number;
};

export type RemoteDurableOutcome = {
	committed: boolean | null;
	committed_statements?: number | null;
	last_commit_epoch?: number | null;
	last_commit_epoch_text?: string | null;
	last_commit_hlc?: RemoteCommitHlc | null;
	first_commit_statement_index?: number | null;
	last_commit_statement_index?: number | null;
	completed_statements?: number | null;
	statement_index?: number | null;
	serialization?: string;
	serialization_state?: string | null;
	terminal_state?: string | null;
};

type RawRemoteSqlWriteReceipt = {
	query_id: string;
	original_query_id: string;
	status: string;
	terminal_state?: string | null;
	server_state?: string | null;
	cancel_outcome?: CancelOutcome | null;
	cancellation_reason?: string | null;
	committed: boolean;
	committed_statements: number;
	last_commit_epoch?: number | null;
	last_commit_epoch_text?: string | null;
	first_commit_statement_index?: number | null;
	last_commit_statement_index?: number | null;
	completed_statements: number;
	statement_index: number;
	retryable: boolean;
	idempotency_replayed: boolean;
	idempotency_persisted: boolean;
	idempotency_expires_at_ms: number;
	outcome: RemoteDurableOutcome;
	terminal_error?: { code: string; category: string } | null;
};

function isNonNegativeInteger(value: unknown): value is number {
	return Number.isSafeInteger(value) && (value as number) >= 0;
}

function isOptionalNonNegativeInteger(value: unknown): boolean {
	return value === undefined || value === null || isNonNegativeInteger(value);
}

function hasOnlyKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
	return Object.keys(value).every((key) => allowed.includes(key));
}

function hasAllKeys(value: Record<string, unknown>, required: readonly string[]): boolean {
	return required.every((key) => Object.prototype.hasOwnProperty.call(value, key));
}

function normalizeQueryId(value: string): string {
	if (typeof value !== 'string' || !/^[0-9a-f]{32}$/i.test(value)) {
		throw new TypeError('queryId must be exactly 32 hexadecimal characters');
	}
	return value.toLowerCase();
}

function isOptionalEpoch(value: unknown, exactText: unknown): boolean {
	if (exactText !== undefined && exactText !== null
		&& (typeof exactText !== 'string' || !/^\d+$/.test(exactText))) return false;
	if (typeof exactText === 'string'
		&& (BigInt(exactText).toString() !== exactText || BigInt(exactText) > MAX_U64)) return false;
	if (value === undefined || value === null) return true;
	if (!Number.isSafeInteger(value) || (value as number) < 0) return false;
	if (typeof exactText === 'string') return BigInt(value as number) === BigInt(exactText);
	return true;
}

function receiptEpoch(value: unknown, exactText: unknown): bigint | undefined {
	if (typeof exactText === 'string') return BigInt(exactText);
	return typeof value === 'number' ? BigInt(value) : undefined;
}

function isQueryNotFoundResponse(value: unknown, queryId: string): boolean {
	if (value === null || typeof value !== 'object') return false;
	const body = value as Record<string, unknown>;
	const outcome = body.outcome as Record<string, unknown> | null;
	const error = body.error as Record<string, unknown> | null;
	const nullable = [
		'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
		'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
		'statement_index'
	] as const;
	return hasOnlyKeys(body, [
		'query_id', 'status', 'terminal_state', ...nullable, 'cancel_outcome',
		'cancellation_reason', 'retryable', 'server_state', 'outcome', 'error'
	]) && hasAllKeys(body, [
		'query_id', 'status', 'terminal_state', ...nullable, 'cancel_outcome',
		'cancellation_reason', 'retryable', 'server_state', 'outcome', 'error'
	])
		&& outcome !== null && typeof outcome === 'object'
		&& hasOnlyKeys(outcome, [...nullable, 'serialization'])
		&& hasAllKeys(outcome, [...nullable, 'serialization'])
		&& error !== null && typeof error === 'object'
		&& hasOnlyKeys(error, ['code', 'message', 'query_id', 'committed', 'retryable'])
		&& hasAllKeys(error, ['code', 'message', 'query_id', 'committed', 'retryable'])
		&& body.query_id === queryId && body.status === 'unknown'
		&& body.terminal_state === null && body.cancel_outcome === 'not_found'
		&& body.cancellation_reason === null && body.retryable === false
		&& body.server_state === 'not_found'
		&& nullable.every((field) => body[field] === null && outcome[field] === null)
		&& outcome.serialization === 'unknown'
		&& error.code === 'QUERY_NOT_FOUND'
		&& typeof error.message === 'string' && error.message.length > 0
		&& error.query_id === queryId && error.committed === null && error.retryable === false;
}

function isRemoteSqlWriteReceipt(
	value: unknown,
	queryId: string,
	expectedOriginalQueryId?: string
): value is RawRemoteSqlWriteReceipt {
	if (value === null || typeof value !== 'object') return false;
	const body = value as Record<string, unknown>;
	const outcome = body.outcome;
	if (outcome === null || typeof outcome !== 'object') return false;
	const durable = outcome as Record<string, unknown>;
	const terminal = body.terminal_error;
	if (!hasOnlyKeys(body, [
		'query_id', 'original_query_id', 'status', 'terminal_state', 'server_state',
		'cancel_outcome', 'cancellation_reason', 'committed', 'committed_statements',
		'last_commit_epoch', 'last_commit_epoch_text', 'first_commit_statement_index',
		'last_commit_statement_index', 'completed_statements', 'statement_index', 'retryable',
		'idempotency_replayed', 'idempotency_persisted', 'idempotency_expires_at_ms',
		'outcome', 'terminal_error'
	]) || !hasOnlyKeys(durable, [
		'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
		'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
		'statement_index', 'serialization'
	]) || !hasAllKeys(durable, [
		'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
		'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
		'statement_index', 'serialization'
	])) return false;
	if (terminal !== undefined && terminal !== null
		&& (typeof terminal !== 'object'
			|| !hasOnlyKeys(terminal as Record<string, unknown>, ['code', 'category']))) return false;
	const statusCommitted = body.status === 'completed'
		? false
		: ['committed', 'committed_with_error', 'partially_committed',
			'cancelled_after_commit', 'deadline_after_commit'].includes(body.status as string)
			? true
			: undefined;
	const shapeValid = body.query_id === queryId
		&& typeof body.original_query_id === 'string'
		&& /^[0-9a-f]{32}$/i.test(body.original_query_id)
		&& typeof body.status === 'string'
		&& body.status.trim().length > 0
		&& (body.terminal_state === undefined || body.terminal_state === null
			|| body.terminal_state === body.status)
		&& (body.server_state === undefined || body.server_state === null
			|| ['completed', 'failed', 'cancelled'].includes(body.server_state as string))
		&& (body.cancel_outcome === undefined || body.cancel_outcome === null
			|| body.cancel_outcome === 'already_finished')
		&& (body.cancellation_reason === undefined || body.cancellation_reason === null
			|| ['none', 'client_request', 'deadline', 'client_disconnected',
				'session_closed', 'server_shutdown'].includes(body.cancellation_reason as string))
		&& typeof body.committed === 'boolean'
		&& isNonNegativeInteger(body.committed_statements)
		&& body.committed === durable.committed
		&& body.committed_statements === durable.committed_statements
		&& typeof body.retryable === 'boolean'
		&& typeof body.idempotency_replayed === 'boolean'
		&& typeof body.idempotency_persisted === 'boolean'
		&& isNonNegativeInteger(body.idempotency_expires_at_ms)
		&& typeof durable.committed === 'boolean'
		&& isNonNegativeInteger(durable.committed_statements)
		&& ['not_started', 'in_progress', 'succeeded', 'failed', 'unknown']
			.includes(durable.serialization as string)
		&& isOptionalEpoch(body.last_commit_epoch, body.last_commit_epoch_text)
		&& isOptionalNonNegativeInteger(body.first_commit_statement_index)
		&& isOptionalNonNegativeInteger(body.last_commit_statement_index)
		&& isNonNegativeInteger(body.completed_statements)
		&& isNonNegativeInteger(body.statement_index)
		&& isOptionalEpoch(durable.last_commit_epoch, durable.last_commit_epoch_text)
		&& isOptionalNonNegativeInteger(durable.first_commit_statement_index)
		&& isOptionalNonNegativeInteger(durable.last_commit_statement_index)
		&& (terminal === undefined || terminal === null
			|| typeof terminal === 'object'
				&& typeof (terminal as Record<string, unknown>).code === 'string'
				&& ((terminal as Record<string, unknown>).code as string).trim().length > 0
				&& ['cancellation', 'deadline', 'result_limit', 'serialization', 'execution']
					.includes((terminal as Record<string, unknown>).category as string));
	if (!shapeValid) return false;
	if (statusCommitted === undefined || body.committed !== statusCommitted) return false;
	if (body.server_state != null) {
		const expectedState = ['completed', 'committed'].includes(body.status as string)
			? 'completed'
			: ['committed_with_error', 'partially_committed'].includes(body.status as string)
				? 'failed' : 'cancelled';
		if (body.server_state !== expectedState) return false;
	}
	if (body.cancellation_reason != null) {
		const reasonMatches = body.status === 'cancelled_after_commit'
			? !['none', 'deadline'].includes(body.cancellation_reason as string)
			: body.status === 'deadline_after_commit'
				? body.cancellation_reason === 'deadline'
				: body.cancellation_reason === 'none';
		if (!reasonMatches) return false;
	}
	const terminalRecord = terminal as Record<string, unknown> | null | undefined;
	if (terminalRecord != null
		&& (((terminalRecord.category === 'cancellation')
			!== ['QUERY_CANCELLED', 'QUERY_CANCELLED_AFTER_COMMIT'].includes(terminalRecord.code as string))
			|| ((terminalRecord.category === 'deadline')
				!== ['DEADLINE_EXCEEDED', 'DEADLINE_AFTER_COMMIT'].includes(terminalRecord.code as string))
			|| ((terminalRecord.category === 'result_limit')
				!== (terminalRecord.code === 'RESULT_LIMIT_EXCEEDED'))
			|| ((terminalRecord.category === 'serialization')
				!== ['SERIALIZATION_FAILED', 'SERIALIZATION_FAILED_AFTER_COMMIT']
					.includes(terminalRecord.code as string)))) return false;
	const terminalMatches = body.status === 'completed' || body.status === 'committed'
		? terminal == null
		: body.status === 'cancelled_after_commit'
			? terminalRecord?.code === 'QUERY_CANCELLED_AFTER_COMMIT'
				&& terminalRecord.category === 'cancellation'
			: body.status === 'deadline_after_commit'
				? terminalRecord?.code === 'DEADLINE_AFTER_COMMIT'
					&& terminalRecord.category === 'deadline'
				: ['committed_with_error', 'partially_committed'].includes(body.status as string)
					&& terminal != null;
	if (!terminalMatches) return false;
	const topEpoch = receiptEpoch(body.last_commit_epoch, body.last_commit_epoch_text);
	const outcomeEpoch = receiptEpoch(durable.last_commit_epoch, durable.last_commit_epoch_text);
	if (topEpoch !== outcomeEpoch) return false;
	const topFirst = body.first_commit_statement_index as number | null | undefined;
	const outcomeFirst = durable.first_commit_statement_index as number | null | undefined;
	const topLast = body.last_commit_statement_index as number | null | undefined;
	const outcomeLast = durable.last_commit_statement_index as number | null | undefined;
	if ((topFirst ?? null) !== (outcomeFirst ?? null) || (topLast ?? null) !== (outcomeLast ?? null)) {
		return false;
	}
	if (body.completed_statements !== durable.completed_statements
		|| body.statement_index !== durable.statement_index) return false;
	if (topFirst != null && topLast != null && topFirst > topLast) return false;
	if (body.committed) {
		if (body.committed_statements === 0 || topEpoch === undefined
			|| body.last_commit_epoch_text == null || durable.last_commit_epoch_text == null
			|| topFirst == null || topLast == null) return false;
	} else if (body.committed_statements !== 0 || topEpoch !== undefined
		|| topFirst != null || topLast != null) return false;
	if (topFirst != null && topLast != null
		&& ((body.committed_statements as number) > topLast - topFirst + 1
			|| topLast > (body.statement_index as number))) return false;
	if ((body.statement_index as number) > (body.completed_statements as number)
		|| (body.completed_statements as number) > (body.statement_index as number) + 1) return false;
	const originalQueryIdMatches = expectedOriginalQueryId === undefined
		? body.idempotency_replayed === true || body.original_query_id === queryId
		: body.idempotency_replayed === true
			? body.original_query_id === expectedOriginalQueryId
			: body.original_query_id === queryId;
	return body.idempotency_persisted === true
		&& (body.idempotency_expires_at_ms as number) > 0
		&& body.retryable === false
		&& originalQueryIdMatches;
}

type RawRemoteQueryStatus = {
	query_id: string;
	status: string;
	state: string;
	server_state?: string;
	terminal_state?: string | null;
	operation?: string;
	started_ms_ago?: number;
	deadline_ms_remaining?: number | null;
	session_id?: string | null;
	committed: boolean | null;
	committed_statements?: number | null;
	last_commit_epoch?: number | null;
	last_commit_epoch_text?: string | null;
	last_commit_hlc?: RemoteCommitHlc | null;
	first_commit_statement_index?: number | null;
	last_commit_statement_index?: number | null;
	completed_statements?: number | null;
	statement_index?: number | null;
	cancel_outcome?: CancelOutcome | null;
	retryable: boolean;
	terminal_error?: { code?: string; category?: string } | null;
	cancellation_reason: string;
	outcome: RemoteDurableOutcome;
	/** Nested durable recovery object (mirrors outcome; 0.64+). */
	durable?: RemoteDurableOutcome | null;
	trace?: {
		queue_duration_us?: number;
		planning_duration_us?: number;
		execution_duration_us?: number;
		serialization_duration_us?: number;
		cancel_requested_phase?: string | null;
		cancel_observed_phase?: string | null;
		commit_fence_outcome?: string;
	};
};

const QUERY_STATES = new Set([
	'queued', 'planning', 'executing', 'streaming', 'serializing', 'commit_critical',
	'cancelling', 'completed', 'failed', 'cancelled', 'pre_cancelled', 'finished'
]);
const QUERY_STATUSES = new Set([
	'running', 'outcome_unknown', 'completed', 'failed_before_commit',
	'cancelled_before_commit', 'deadline_before_commit', 'cancelled_before_start',
	'committed', 'committed_with_error', 'partially_committed',
	'cancelled_after_commit', 'deadline_after_commit', 'finished'
]);
const COMMITTED_QUERY_STATUSES = new Set([
	'committed', 'committed_with_error', 'partially_committed',
	'cancelled_after_commit', 'deadline_after_commit'
]);

function sameOptional(left: unknown, right: unknown): boolean {
	return (left ?? null) === (right ?? null);
}

const OUTCOME_ALLOWED_KEYS = [
	'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
	'last_commit_hlc', 'first_commit_statement_index', 'last_commit_statement_index',
	'completed_statements', 'statement_index', 'serialization', 'serialization_state',
	'terminal_state'
] as const;

const OUTCOME_REQUIRED_KEYS = [
	'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
	'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
	'statement_index', 'serialization'
] as const;

function isRemoteCommitHlc(value: unknown): value is RemoteCommitHlc {
	if (value === null || value === undefined) return true;
	if (value === null || typeof value !== 'object') return false;
	const hlc = value as Record<string, unknown>;
	if (!hasOnlyKeys(hlc, ['physical_micros', 'logical', 'node_tiebreaker'])) return false;
	if (typeof hlc.physical_micros !== 'number'
		|| !Number.isSafeInteger(hlc.physical_micros)
		|| hlc.physical_micros < 0) return false;
	if (hlc.logical !== undefined
		&& (typeof hlc.logical !== 'number' || !Number.isSafeInteger(hlc.logical) || hlc.logical < 0)) {
		return false;
	}
	if (hlc.node_tiebreaker !== undefined
		&& (typeof hlc.node_tiebreaker !== 'number'
			|| !Number.isSafeInteger(hlc.node_tiebreaker)
			|| hlc.node_tiebreaker < 0)) {
		return false;
	}
	return true;
}

function parseRemoteCommitHlc(value: unknown): SqlCommitHlc | undefined {
	if (value === null || value === undefined || typeof value !== 'object') return undefined;
	const hlc = value as RemoteCommitHlc;
	return {
		physicalMicros: hlc.physical_micros,
		logical: hlc.logical ?? 0,
		nodeTiebreaker: hlc.node_tiebreaker ?? 0
	};
}

function pickCommitHlc(
	...sources: Array<RemoteDurableOutcome | null | undefined | { last_commit_hlc?: RemoteCommitHlc | null }>
): SqlCommitHlc | undefined {
	for (const source of sources) {
		if (!source) continue;
		const parsed = parseRemoteCommitHlc(
			'last_commit_hlc' in source ? source.last_commit_hlc : undefined
		);
		if (parsed !== undefined) return parsed;
	}
	return undefined;
}

function pickSerializationState(
	outcome?: RemoteDurableOutcome | null,
	durable?: RemoteDurableOutcome | null
): string | undefined {
	const state = durable?.serialization_state
		?? outcome?.serialization_state
		?? durable?.serialization
		?? outcome?.serialization;
	return state === undefined || state === null ? undefined : state;
}

function isRemoteQueryStatus(value: unknown, queryId: string): value is RawRemoteQueryStatus {
	if (value === null || typeof value !== 'object') return false;
	const body = value as Record<string, unknown>;
	const outcome = body.outcome !== null && typeof body.outcome === 'object'
			? body.outcome as Record<string, unknown>
			: undefined;
	if (outcome === undefined) return false;
	const durable = body.durable === undefined || body.durable === null
		? undefined
		: typeof body.durable === 'object'
			? body.durable as Record<string, unknown>
			: undefined;
	if (body.durable !== undefined && body.durable !== null && durable === undefined) return false;
	const serverState = body.server_state ?? '';
	const terminal = body.terminal_error;
	if (!hasOnlyKeys(body, [
		'query_id', 'status', 'state', 'server_state', 'terminal_state', 'detail', 'operation',
		'started_ms_ago', 'deadline_ms_remaining', 'session_id',
		'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
		'last_commit_hlc',
		'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
		'statement_index', 'cancel_outcome', 'retryable', 'terminal_error',
		'cancellation_reason', 'outcome', 'durable', 'trace'
	]) || !hasOnlyKeys(outcome, OUTCOME_ALLOWED_KEYS)
		|| !hasAllKeys(outcome, OUTCOME_REQUIRED_KEYS)
		|| (durable !== undefined && (!hasOnlyKeys(durable, OUTCOME_ALLOWED_KEYS)
			|| !hasAllKeys(durable, OUTCOME_REQUIRED_KEYS)))
	) return false;
	if (!isRemoteCommitHlc(body.last_commit_hlc)
		|| !isRemoteCommitHlc(outcome.last_commit_hlc)
		|| (durable !== undefined && !isRemoteCommitHlc(durable.last_commit_hlc))) return false;
	if (terminal !== undefined && terminal !== null
		&& (typeof terminal !== 'object'
			|| !hasOnlyKeys(terminal as Record<string, unknown>, ['code', 'category']))) return false;
	const trace = body.trace;
	if (trace !== undefined && (trace === null || typeof trace !== 'object'
		|| !hasOnlyKeys(trace as Record<string, unknown>, [
			'queue_duration_us', 'planning_duration_us', 'execution_duration_us',
			'serialization_duration_us', 'cancel_requested_phase', 'cancel_observed_phase',
			'commit_fence_outcome'
		]))) return false;
	const traceRecord = trace as Record<string, unknown> | undefined;
	const tracePhaseValid = (phase: unknown): boolean => phase === undefined || phase === null
		|| typeof phase === 'string' && QUERY_STATES.has(phase);
	if (traceRecord !== undefined
		&& (traceRecord.queue_duration_us !== undefined
			&& !isNonNegativeInteger(traceRecord.queue_duration_us)
			|| traceRecord.planning_duration_us !== undefined
			&& !isNonNegativeInteger(traceRecord.planning_duration_us)
			|| traceRecord.execution_duration_us !== undefined
			&& !isNonNegativeInteger(traceRecord.execution_duration_us)
			|| traceRecord.serialization_duration_us !== undefined
			&& !isNonNegativeInteger(traceRecord.serialization_duration_us)
			|| !tracePhaseValid(traceRecord.cancel_requested_phase)
			|| !tracePhaseValid(traceRecord.cancel_observed_phase)
			|| traceRecord.commit_fence_outcome !== undefined
			&& !['not_reached', 'cancel_won', 'commit_won']
				.includes(traceRecord.commit_fence_outcome as string))) return false;
	if (body.query_id !== queryId
		|| body.detail !== undefined && body.detail !== 'compact'
		|| typeof body.status !== 'string' || !QUERY_STATUSES.has(body.status)
		|| typeof body.state !== 'string' || !QUERY_STATES.has(body.state)
		|| typeof serverState !== 'string'
		|| serverState !== '' && (!QUERY_STATES.has(serverState) || serverState !== body.state)
		|| body.terminal_state != null && body.terminal_state !== body.status
		|| body.started_ms_ago !== undefined && !isNonNegativeInteger(body.started_ms_ago)
		|| body.deadline_ms_remaining !== undefined && body.deadline_ms_remaining !== null
			&& !isNonNegativeInteger(body.deadline_ms_remaining)
		|| body.session_id !== undefined && body.session_id !== null
			&& (typeof body.session_id !== 'string' || body.session_id.length > 256)
		|| ![undefined, null, true, false].includes(body.committed as undefined | null | boolean)
		|| ![undefined, null, true, false].includes(outcome.committed as undefined | null | boolean)
		|| typeof body.retryable !== 'boolean'
		|| typeof body.cancellation_reason !== 'string'
		|| !['not_started', 'in_progress', 'succeeded', 'failed', 'unknown']
			.includes(outcome.serialization as string)
		|| !isOptionalNonNegativeInteger(body.committed_statements)
		|| !isOptionalNonNegativeInteger(body.first_commit_statement_index)
		|| !isOptionalNonNegativeInteger(body.last_commit_statement_index)
		|| !isOptionalNonNegativeInteger(body.completed_statements)
		|| !isOptionalNonNegativeInteger(body.statement_index)
		|| !isOptionalNonNegativeInteger(outcome.committed_statements)
		|| !isOptionalNonNegativeInteger(outcome.first_commit_statement_index)
		|| !isOptionalNonNegativeInteger(outcome.last_commit_statement_index)
		|| !isOptionalNonNegativeInteger(outcome.completed_statements)
		|| !isOptionalNonNegativeInteger(outcome.statement_index)
		|| !isOptionalEpoch(body.last_commit_epoch, body.last_commit_epoch_text)
		|| !isOptionalEpoch(outcome.last_commit_epoch, outcome.last_commit_epoch_text)
		|| terminal != null && (typeof terminal !== 'object'
			|| typeof (terminal as Record<string, unknown>).code !== 'string'
			|| ((terminal as Record<string, unknown>).code as string).trim().length === 0
			|| !['cancellation', 'deadline', 'result_limit', 'serialization', 'execution']
				.includes((terminal as Record<string, unknown>).category as string))) {
		return false;
	}
	const topEpoch = receiptEpoch(body.last_commit_epoch, body.last_commit_epoch_text);
	const outcomeEpoch = receiptEpoch(outcome.last_commit_epoch, outcome.last_commit_epoch_text);
	if (topEpoch !== outcomeEpoch
		|| !sameOptional(body.committed, outcome.committed)
		|| !sameOptional(body.committed_statements, outcome.committed_statements)
		|| !sameOptional(body.first_commit_statement_index, outcome.first_commit_statement_index)
		|| !sameOptional(body.last_commit_statement_index, outcome.last_commit_statement_index)
		|| !sameOptional(body.completed_statements, outcome.completed_statements)
		|| !sameOptional(body.statement_index, outcome.statement_index)) return false;

	const committed = body.committed ?? null;
	const committedStatements = body.committed_statements as number | null | undefined;
	const first = body.first_commit_statement_index as number | null | undefined;
	const last = body.last_commit_statement_index as number | null | undefined;
	const completed = body.completed_statements as number | null | undefined;
	const statement = body.statement_index as number | null | undefined;
	const stateMatchesStatus = body.status === 'running'
		? ['queued', 'planning', 'executing', 'streaming', 'serializing',
			'commit_critical', 'cancelling'].includes(body.state as string)
		: body.status === 'committed'
			? ['planning', 'executing', 'streaming', 'serializing',
				'commit_critical', 'cancelling', 'completed'].includes(body.state as string)
			: body.status === 'completed' ? body.state === 'completed'
				: ['failed_before_commit', 'committed_with_error', 'partially_committed',
					'outcome_unknown'].includes(body.status as string) ? body.state === 'failed'
					: ['cancelled_before_commit', 'deadline_before_commit',
						'cancelled_after_commit', 'deadline_after_commit'].includes(body.status as string)
						? body.state === 'cancelled'
						: body.status === 'cancelled_before_start' ? body.state === 'pre_cancelled'
							: body.status === 'finished' && body.state === 'finished';
	if (!stateMatchesStatus) return false;
	const expectedTerminal = body.status === 'running' || body.status === 'finished'
		|| body.status === 'committed' && body.state !== 'completed'
		? null : body.status;
	if ((body.terminal_state ?? null) !== expectedTerminal) return false;
	if (committed === true) {
		if (!COMMITTED_QUERY_STATUSES.has(body.status as string)
			|| committedStatements == null || committedStatements === 0
			|| topEpoch === undefined || body.last_commit_epoch_text == null
			|| outcome.last_commit_epoch_text == null || first == null || last == null
			|| completed == null || statement == null) return false;
	} else if (committed === false) {
		if (COMMITTED_QUERY_STATUSES.has(body.status as string)
			|| body.status === 'outcome_unknown' || body.status === 'finished'
			|| committedStatements !== 0 || topEpoch !== undefined
			|| first != null || last != null || completed == null || statement == null) return false;
	} else if (body.status !== 'outcome_unknown' && body.status !== 'finished'
		|| committedStatements != null || topEpoch !== undefined
		|| first != null || last != null || completed != null || statement != null) return false;

	if (first != null && last != null && committedStatements != null && statement != null
		&& (first > last || committedStatements > last - first + 1 || last > statement)) return false;
	if (completed != null && statement != null
		&& (statement > completed || completed > statement + 1)) return false;
	const expectedCancel = body.state === 'cancelling' ? 'accepted'
		: body.state === 'commit_critical' ? 'too_late'
			: ['completed', 'failed', 'cancelled', 'finished'].includes(body.state as string)
				? 'already_finished'
				: body.state === 'pre_cancelled' ? 'pre_cancelled' : null;
	if ((body.cancel_outcome ?? null) !== expectedCancel) return false;
	const terminalError = terminal as Record<string, unknown> | null | undefined;
	const terminalCode = terminalError?.code;
	const terminalCategory = terminalError?.category;
	const terminalMatches = ['running', 'completed', 'committed', 'finished']
		.includes(body.status as string) ? terminal == null
		: body.status === 'outcome_unknown'
			? terminalCode === 'QUERY_OUTCOME_UNKNOWN' && terminalCategory === 'execution'
			: ['cancelled_before_commit', 'cancelled_before_start'].includes(body.status as string)
				? terminalCode === 'QUERY_CANCELLED' && terminalCategory === 'cancellation'
				: body.status === 'cancelled_after_commit'
					? terminalCode === 'QUERY_CANCELLED_AFTER_COMMIT'
						&& terminalCategory === 'cancellation'
					: body.status === 'deadline_before_commit'
						? terminalCode === 'DEADLINE_EXCEEDED' && terminalCategory === 'deadline'
						: body.status === 'deadline_after_commit'
							? terminalCode === 'DEADLINE_AFTER_COMMIT'
								&& terminalCategory === 'deadline'
							: terminal != null;
	if (!terminalMatches) return false;
	if (terminalError != null
		&& ((terminalCategory === 'cancellation')
			!== ['QUERY_CANCELLED', 'QUERY_CANCELLED_AFTER_COMMIT'].includes(terminalCode as string)
			|| (terminalCategory === 'deadline')
			!== ['DEADLINE_EXCEEDED', 'DEADLINE_AFTER_COMMIT'].includes(terminalCode as string))) {
		return false;
	}
	const retryable = ['IDEMPOTENCY_STORE_FULL', 'IDEMPOTENCY_STORE_UNAVAILABLE']
		.includes(terminalCode as string);
	if (body.retryable !== retryable) return false;
	const reason = body.cancellation_reason;
	if (!['none', 'client_request', 'deadline', 'client_disconnected',
		'session_closed', 'server_shutdown'].includes(reason as string)) return false;
	if (['deadline_before_commit', 'deadline_after_commit'].includes(body.status as string)) {
		return reason === 'deadline';
	}
	if (['cancelled_before_commit', 'cancelled_before_start', 'cancelled_after_commit']
		.includes(body.status as string)
		|| ['running', 'committed'].includes(body.status as string) && body.state === 'cancelling') {
		return reason !== 'none';
	}
	return reason === 'none' || body.state === 'commit_critical';
}

type RawRemoteSqlPage = {
	status: string;
	rows: Record<string, unknown>[];
	next_cursor?: string | null;
	page: {
		offset: number;
		row_count: number;
		total_rows: number;
		byte_count: number;
		estimated_tokens: number;
		limits: { rows: number; bytes: number; tokens: number };
		projection: string[];
		expires_at_ms: number;
		snapshot: string;
		token_estimate: string;
	};
};

function isRemoteSqlPage(
	value: unknown,
	initialOptions?: RemoteSqlPaginationOptions
): value is RawRemoteSqlPage {
	if (value === null || typeof value !== 'object') return false;
	const body = value as Record<string, unknown>;
	if (body.page === null || typeof body.page !== 'object' || !Array.isArray(body.rows)) return false;
	const page = body.page as Record<string, unknown>;
	if (page.limits === null || typeof page.limits !== 'object') return false;
	const limits = page.limits as Record<string, unknown>;
	if (!hasOnlyKeys(body, ['status', 'rows', 'next_cursor', 'page'])
		|| !hasOnlyKeys(page, [
			'offset', 'row_count', 'total_rows', 'byte_count', 'estimated_tokens', 'limits',
			'projection', 'expires_at_ms', 'snapshot', 'token_estimate'
		])
		|| !hasOnlyKeys(limits, ['rows', 'bytes', 'tokens'])) return false;
	const shapeValid = body.status === 'completed'
		&& body.rows.every((row) => row !== null && typeof row === 'object' && !Array.isArray(row))
		&& (body.next_cursor === undefined || body.next_cursor === null
			|| typeof body.next_cursor === 'string' && body.next_cursor.length > 0)
		&& isNonNegativeInteger(page.offset)
		&& isNonNegativeInteger(page.row_count)
		&& page.row_count === body.rows.length
		&& isNonNegativeInteger(page.total_rows)
		&& (page.offset as number) <= (page.total_rows as number)
		&& (page.row_count as number) <= (page.total_rows as number) - (page.offset as number)
		&& isNonNegativeInteger(page.byte_count)
		&& isNonNegativeInteger(page.estimated_tokens)
		&& isNonNegativeInteger(limits.rows)
		&& isNonNegativeInteger(limits.bytes)
		&& isNonNegativeInteger(limits.tokens)
		&& Array.isArray(page.projection)
		&& page.projection.length > 0 && page.projection.length <= 128
		&& page.projection.every((column) => typeof column === 'string'
			&& column.length > 0 && column !== '*'
			&& Buffer.byteLength(column, 'utf8') <= 256)
		&& page.projection.reduce(
			(bytes, column) => bytes + Buffer.byteLength(column as string, 'utf8'),
			0
		) <= 16 * 1024
		&& isNonNegativeInteger(page.expires_at_ms)
		&& page.snapshot === 'retained_result'
		&& page.token_estimate === 'ceil(projected_json_bytes/4)';
	if (!shapeValid) return false;
	const projection = page.projection as string[];
	if (new Set(projection).size !== projection.length
		|| body.rows.some((row) => {
			const keys = Object.keys(row);
			return keys.length !== projection.length
				|| projection.some((column) => !Object.prototype.hasOwnProperty.call(row, column));
		})) return false;
	const byteCount = body.rows.reduce(
		(bytes, row, index) => bytes + (index === 0 ? 0 : 1)
			+ Buffer.byteLength(JSON.stringify(row), 'utf8'),
		2
	);
	if (page.byte_count !== byteCount
		|| page.estimated_tokens !== Math.ceil(byteCount / 4)) return false;
	if ((limits.rows as number) === 0 || (limits.bytes as number) === 0
		|| (limits.tokens as number) === 0
		|| (limits.bytes as number) > MAX_PAGE_JSON_RESPONSE_BYTES
		|| (page.row_count as number) > (limits.rows as number)
		|| (page.byte_count as number) > (limits.bytes as number)
		|| (page.estimated_tokens as number) > (limits.tokens as number)) return false;
	const hasMore = (page.offset as number) + (page.row_count as number) < (page.total_rows as number);
	if (hasMore && page.row_count === 0
		|| hasMore !== (typeof body.next_cursor === 'string')) return false;
	if (typeof body.next_cursor === 'string'
		&& Buffer.byteLength(body.next_cursor, 'utf8') > 2_048) return false;
	if (page.expires_at_ms === 0) return false;
	if (initialOptions === undefined) return true;
	return page.offset === 0
		&& JSON.stringify(page.projection) === JSON.stringify(initialOptions.projection)
		&& (limits.rows as number) <= initialOptions.pageSizeRows
		&& (initialOptions.maxPageBytes === undefined
			|| (limits.bytes as number) <= initialOptions.maxPageBytes)
		&& (initialOptions.maxPageTokens === undefined
			|| (limits.tokens as number) <= initialOptions.maxPageTokens)
		&& (initialOptions.maxOutputRows === undefined
			|| (page.total_rows as number) <= initialOptions.maxOutputRows)
		&& (initialOptions.maxOutputBytes === undefined
			|| (page.byte_count as number) <= initialOptions.maxOutputBytes);
}

function remoteSqlPage(page: RawRemoteSqlPage): RemoteSqlPage {
	return {
		status: page.status,
		rows: page.rows,
		nextCursor: page.next_cursor ?? undefined,
		page: {
			offset: page.page.offset,
			rowCount: page.page.row_count,
			totalRows: page.page.total_rows,
			byteCount: page.page.byte_count,
			estimatedTokens: page.page.estimated_tokens,
			limits: page.page.limits,
			projection: page.page.projection,
			expiresAtMs: page.page.expires_at_ms,
			snapshot: page.page.snapshot,
			tokenEstimate: page.page.token_estimate
		}
	};
}

export type RemoteQueryStatus = SqlQueryStatus;

type NativeRemoteOptions = {
	bearerToken?: string;
	username?: string;
	password?: string;
};

type NativeRemoteConstructor = new (
	url: string,
	options?: NativeRemoteOptions
) => NativeRemoteDatabase;

class RetryIdempotentSqlError extends Error {
	constructor(readonly outcome: QueryOutcomeUnknownError) {
		super(outcome.message);
	}
}

class QueryNotRetainedError extends QueryOutcomeUnknownError {
	constructor(queryId: string, message: string) {
		super(queryId, message, { retryable: false, serverState: 'not_found' });
	}
}

class InvalidQueryStatusError extends QueryOutcomeUnknownError {
	constructor(queryId: string, message: string) {
		super(queryId, `invalid query status response: ${message}`, {
			retryable: false,
			serverState: 'invalid_status'
		});
	}
}

function remoteEpoch(text: string | null | undefined, value: number | null | undefined): bigint | undefined {
	if (text !== undefined && text !== null) {
		if (!/^\d+$/.test(text) || BigInt(text) > MAX_U64) {
			throw new KitError('invalid last_commit_epoch_text from server');
		}
		return BigInt(text);
	}
	if (value === undefined || value === null) return undefined;
	if (!Number.isSafeInteger(value) || value < 0) {
		throw new KitError('server returned an unsafe numeric commit epoch', 'QUERY_OUTCOME_UNKNOWN');
	}
	return BigInt(value);
}

function mergedCommitState(
	nested: boolean | null | undefined,
	top: boolean | null | undefined
): boolean | null {
	if (nested === true || top === true) return true;
	return nested ?? top ?? null;
}

function recoveryStatusIsDecisive(status: RawRemoteQueryStatus): boolean {
	return TERMINAL_SQL_STATES.has(status.state)
		|| mergedCommitState(status.outcome?.committed, status.committed) === true;
}

function maxKnown(
	left: number | null | undefined,
	right: number | null | undefined
): number | undefined {
	if (left === undefined || left === null) return right ?? undefined;
	if (right === undefined || right === null) return left;
	return Math.max(left, right);
}

function cancelOutcome(value: unknown): CancelOutcome | undefined {
	return ({
		accepted: 'accepted',
		cancellation_requested: 'accepted',
		already_cancelling: 'already_cancelling',
		cancelling: 'already_cancelling',
		too_late: 'too_late',
		commit_critical: 'too_late',
		already_finished: 'already_finished',
		finished: 'already_finished',
		not_found: 'not_found',
		pre_cancelled: 'pre_cancelled'
	} as Record<string, CancelOutcome>)[String(value)];
}

function validateCancelResponse(
	value: unknown,
	queryId: string,
	httpStatus: number
): CancelOutcome {
	if (value === null || typeof value !== 'object') {
		throw new KitError('SQL cancellation response was not an object', 'REMOTE_PROTOCOL');
	}
	const body = value as Record<string, unknown>;
	if (!hasOnlyKeys(body, [
		'query_id', 'state', 'cancel_outcome', 'status', 'terminal_state', 'code',
		'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
		'last_commit_hlc',
		'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
		'statement_index', 'cancellation_reason', 'retryable', 'server_state', 'outcome', 'durable',
		'error', 'terminal_error'
	])) {
		throw new KitError('SQL cancellation response has unknown fields', 'REMOTE_PROTOCOL');
	}
	if (body.outcome !== undefined && body.outcome !== null
		&& (typeof body.outcome !== 'object'
			|| !hasOnlyKeys(body.outcome as Record<string, unknown>, OUTCOME_ALLOWED_KEYS))) {
		throw new KitError('SQL cancellation outcome has unknown fields', 'REMOTE_PROTOCOL');
	}
	if (body.error !== undefined && body.error !== null
		&& (typeof body.error !== 'object'
			|| !hasOnlyKeys(body.error as Record<string, unknown>, [
				'code', 'message', 'query_id', 'committed', 'retryable'
			]))) {
		throw new KitError('SQL cancellation error has unknown fields', 'REMOTE_PROTOCOL');
	}
	if (body.terminal_error !== undefined && body.terminal_error !== null
		&& (typeof body.terminal_error !== 'object'
			|| !hasOnlyKeys(body.terminal_error as Record<string, unknown>, [
				'code', 'category'
			])
			|| typeof (body.terminal_error as Record<string, unknown>).code !== 'string'
			|| ((body.terminal_error as Record<string, unknown>).code as string).length === 0
			|| typeof (body.terminal_error as Record<string, unknown>).category !== 'string'
			|| ((body.terminal_error as Record<string, unknown>).category as string).length === 0)) {
		throw new KitError('SQL cancellation terminal error has invalid fields', 'REMOTE_PROTOCOL');
	}
	const durable = body.outcome as Record<string, unknown> | null | undefined;
	if (durable != null) {
		const required = [
			'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
			'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
			'statement_index', 'serialization'
		];
		if (!required.every((field) => Object.prototype.hasOwnProperty.call(durable, field))
			|| ![null, true, false].includes(durable.committed as null | boolean)
			|| ['committed_statements', 'first_commit_statement_index',
				'last_commit_statement_index', 'completed_statements', 'statement_index']
				.some((field) => !isOptionalNonNegativeInteger(durable[field]))
			|| !isOptionalEpoch(durable.last_commit_epoch, durable.last_commit_epoch_text)
			|| !['not_started', 'in_progress', 'succeeded', 'failed', 'unknown']
				.includes(durable.serialization as string)) {
			throw new KitError('SQL cancellation outcome fields are invalid', 'REMOTE_PROTOCOL');
		}
	}
	if (body.committed !== undefined && ![null, true, false]
		.includes(body.committed as null | boolean)) {
		throw new KitError('SQL cancellation committed field is invalid', 'REMOTE_PROTOCOL');
	}
	for (const field of ['committed_statements', 'first_commit_statement_index',
		'last_commit_statement_index', 'completed_statements', 'statement_index'] as const) {
		if (body[field] !== undefined && !isOptionalNonNegativeInteger(body[field])) {
			throw new KitError(`SQL cancellation ${field} is invalid`, 'REMOTE_PROTOCOL');
		}
		if (durable != null && body[field] !== undefined
			&& !sameOptional(body[field], durable[field])) {
			throw new KitError(`SQL cancellation ${field} disagrees with outcome`, 'REMOTE_PROTOCOL');
		}
	}
	if ((body.last_commit_epoch !== undefined || body.last_commit_epoch_text !== undefined)
		&& !isOptionalEpoch(body.last_commit_epoch, body.last_commit_epoch_text)) {
		throw new KitError('SQL cancellation commit epoch is invalid', 'REMOTE_PROTOCOL');
	}
	if (durable != null
		&& (body.committed !== undefined && !sameOptional(body.committed, durable.committed)
			|| (body.last_commit_epoch !== undefined || body.last_commit_epoch_text !== undefined)
				&& receiptEpoch(body.last_commit_epoch, body.last_commit_epoch_text)
					!== receiptEpoch(durable.last_commit_epoch, durable.last_commit_epoch_text))) {
		throw new KitError('SQL cancellation outcome metadata disagrees', 'REMOTE_PROTOCOL');
	}
	if (body.status !== undefined && (typeof body.status !== 'string'
		|| !QUERY_STATUSES.has(body.status) && body.status !== 'unknown')) {
		throw new KitError('SQL cancellation status is invalid', 'REMOTE_PROTOCOL');
	}
	if (body.terminal_state !== undefined && body.terminal_state !== null
		&& (typeof body.terminal_state !== 'string'
			|| body.terminal_state !== body.status)) {
		throw new KitError('SQL cancellation terminal state is invalid', 'REMOTE_PROTOCOL');
	}
	if (body.retryable !== undefined && typeof body.retryable !== 'boolean') {
		throw new KitError('SQL cancellation retryable field is invalid', 'REMOTE_PROTOCOL');
	}
	if (body.server_state !== undefined && (typeof body.server_state !== 'string'
		|| !QUERY_STATES.has(body.server_state) && body.server_state !== 'not_found')) {
		throw new KitError('SQL cancellation server state is invalid', 'REMOTE_PROTOCOL');
	}
	if (body.cancellation_reason !== undefined && body.cancellation_reason !== null
		&& (typeof body.cancellation_reason !== 'string'
			|| !['none', 'client_request', 'deadline', 'client_disconnected',
				'session_closed', 'server_shutdown'].includes(body.cancellation_reason))) {
		throw new KitError('SQL cancellation reason is invalid', 'REMOTE_PROTOCOL');
	}
	const detail = body.error as Record<string, unknown> | null | undefined;
	if (detail != null
		&& (typeof detail.code !== 'string' || detail.code.length === 0
			|| typeof detail.message !== 'string' || detail.message.length === 0
			|| detail.query_id !== undefined && detail.query_id !== null
				&& typeof detail.query_id !== 'string'
			|| detail.committed !== undefined
				&& ![null, true, false].includes(detail.committed as null | boolean)
			|| detail.retryable !== undefined && typeof detail.retryable !== 'boolean')) {
		throw new KitError('SQL cancellation error fields are invalid', 'REMOTE_PROTOCOL');
	}
	if (body.query_id !== queryId) {
		throw new KitError('SQL cancellation query ID does not match the request', 'REMOTE_PROTOCOL');
	}
	const decodeField = (name: 'cancel_outcome' | 'state'): CancelOutcome | undefined => {
		const raw = body[name];
		if (raw === undefined || raw === null) return undefined;
		const decoded = cancelOutcome(raw);
		if (decoded === undefined) {
			throw new KitError(`SQL cancellation ${name} is invalid`, 'REMOTE_PROTOCOL');
		}
		return decoded;
	};
	const outcome = decodeField('cancel_outcome');
	let state = decodeField('state');
	if (outcome === 'not_found' && state === undefined && body.server_state === 'not_found') {
		state = 'not_found';
	}
	if (outcome === undefined || state === undefined) {
		throw new KitError('SQL cancellation state and outcome are required', 'REMOTE_PROTOCOL');
	}
	if (outcome !== state) {
		throw new KitError('SQL cancellation state and outcome disagree', 'REMOTE_PROTOCOL');
	}
	const result = outcome;
	const compatible = httpStatus === 202 && ['accepted', 'pre_cancelled'].includes(result ?? '')
		|| httpStatus === 200 && ['already_cancelling', 'already_finished'].includes(result ?? '')
		|| httpStatus === 409 && result === 'too_late'
		|| httpStatus === 404 && result === 'not_found';
	if (!compatible) {
		throw new KitError('SQL cancellation HTTP status and outcome disagree', 'REMOTE_PROTOCOL');
	}
	const terminal = body.terminal_error as Record<string, unknown> | null | undefined;
	if (result === 'pre_cancelled') {
		if (terminal != null
			&& (terminal.code !== 'QUERY_CANCELLED' || terminal.category !== 'cancellation')) {
			throw new KitError(
				'SQL cancellation terminal error disagrees with outcome',
				'REMOTE_PROTOCOL'
			);
		}
	} else if (terminal != null) {
		throw new KitError(
			'SQL cancellation terminal error disagrees with outcome',
			'REMOTE_PROTOCOL'
		);
	}
	return result;
}

type NativeRemoteWriteError = {
	code?: unknown;
	message?: unknown;
	queryId?: unknown;
	query_id?: unknown;
	committed?: unknown;
	outcomeKnown?: unknown;
	epoch?: unknown;
	epochText?: unknown;
	lastCommitEpoch?: unknown;
	lastCommitEpochText?: unknown;
	committedStatements?: unknown;
	firstCommitStatementIndex?: unknown;
	lastCommitStatementIndex?: unknown;
	completedStatements?: unknown;
	statementIndex?: unknown;
	cancelOutcome?: unknown;
	cancellationReason?: unknown;
	retryable?: unknown;
	serverState?: unknown;
	status?: unknown;
	terminalState?: unknown;
	remoteQueryError?: unknown;
};

function exactNativeValue(
	values: unknown[],
	valid: (value: unknown) => boolean
): unknown {
	let exact: unknown = undefined;
	for (const value of values) {
		if (value === undefined) continue;
		if (!valid(value) || exact !== undefined && !Object.is(exact, value)) {
			throw new Error('conflicting native response fields');
		}
		exact = value;
	}
	return exact;
}

function nativeRemoteWriteEpoch(...sources: NativeRemoteWriteError[]): bigint | undefined {
	let exact: bigint | undefined;
	const merge = (value: unknown, text: boolean) => {
		if (value === undefined || value === null) return;
		let parsed: bigint;
		if (text) {
			if (typeof value !== 'string' || !/^\d+$/u.test(value)
				|| BigInt(value).toString() !== value) throw new Error('invalid exact commit epoch');
			parsed = BigInt(value);
		} else if (typeof value === 'bigint') parsed = value;
		else if (typeof value === 'number' && Number.isSafeInteger(value)) parsed = BigInt(value);
		else if (typeof value === 'string' && /^\d+$/u.test(value)
			&& BigInt(value).toString() === value) parsed = BigInt(value);
		else throw new Error('invalid commit epoch');
		if (parsed < 0n || parsed > MAX_U64) throw new Error('commit epoch outside u64 range');
		if (exact !== undefined && exact !== parsed) throw new Error('conflicting commit epoch fields');
		exact = parsed;
	};
	for (const source of sources) {
		merge(source.lastCommitEpoch, false);
		merge(source.epoch, false);
		merge(source.lastCommitEpochText, true);
		merge(source.epochText, true);
	}
	return exact;
}

function nativeRemoteWriteError(error: unknown): Error {
	const original = error instanceof Error ? error : new Error(String(error));
	if (error instanceof KitError) return error;
	if (error === null || typeof error !== 'object') return original;
	const top = error as NativeRemoteWriteError;
	const details = top.remoteQueryError !== null && typeof top.remoteQueryError === 'object'
		? top.remoteQueryError as NativeRemoteWriteError
		: top;
	const candidateCodes = [top.code, details.code].filter((value) => typeof value === 'string');
	if (!candidateCodes.some((code) => code === 'COMMIT_OUTCOME'
		|| code === 'QUERY_OUTCOME_UNKNOWN')) return original;
	try {
		if (details !== top && !hasOnlyKeys(details as Record<string, unknown>, [
			'code', 'message', 'queryId', 'status', 'httpStatus', 'outcomeKnown', 'committed',
			'epoch', 'epochText', 'committedStatements', 'lastCommitEpoch',
			'lastCommitEpochText', 'firstCommitStatementIndex', 'lastCommitStatementIndex',
			'completedStatements', 'statementIndex', 'cancelOutcome', 'cancellationReason',
			'retryable', 'serverState', 'terminalState'
		])) throw new Error('unknown native response field');
		const code = exactNativeValue(
			[top.code, details.code],
			(value) => value === 'COMMIT_OUTCOME' || value === 'QUERY_OUTCOME_UNKNOWN'
		) as 'COMMIT_OUTCOME' | 'QUERY_OUTCOME_UNKNOWN';
		const queryId = (exactNativeValue(
			[top.queryId, top.query_id, details.queryId, details.query_id],
			(value) => typeof value === 'string'
				&& (value === 'unknown' || /^[0-9a-f]{32}$/u.test(value))
		) as string | undefined) ?? 'unknown';
		const message = (exactNativeValue(
			[top.message, details.message],
			(value) => typeof value === 'string'
		) as string | undefined) ?? original.message;
		const retryable = exactNativeValue(
			[top.retryable, details.retryable],
			(value) => typeof value === 'boolean'
		) as boolean | undefined;
		const serverState = exactNativeValue(
			[top.serverState, details.serverState],
			(value) => typeof value === 'string'
		) as string | undefined;
		const status = exactNativeValue(
			[top.status, details.status],
			(value) => typeof value === 'string'
		) as string | undefined;
		const committed = exactNativeValue(
			[top.committed, details.committed],
			(value) => value === null || typeof value === 'boolean'
		) as boolean | null | undefined;
		const outcomeKnown = exactNativeValue(
			[top.outcomeKnown, details.outcomeKnown],
			(value) => typeof value === 'boolean'
		) as boolean | undefined;
		for (const field of [
			'committedStatements', 'firstCommitStatementIndex', 'lastCommitStatementIndex',
			'completedStatements', 'statementIndex'
		] as const) {
			exactNativeValue(
				[top[field], details[field]],
				(value) => value === null || isNonNegativeInteger(value)
			);
		}
		for (const field of [
			'cancelOutcome', 'cancellationReason', 'terminalState'
		] as const) {
			exactNativeValue(
				[top[field], details[field]],
				(value) => value === null || typeof value === 'string'
			);
		}
		const lastCommitEpoch = nativeRemoteWriteEpoch(details, top);
		const metadata: SqlErrorMetadata = {
			retryable,
			serverState: serverState ?? status
		};
		if (code === 'QUERY_OUTCOME_UNKNOWN') {
			if (committed !== null || outcomeKnown !== false || retryable !== false) {
				throw new Error('invalid unknown outcome metadata');
			}
			return new QueryOutcomeUnknownError(queryId, message, metadata);
		}
		if (committed !== true || outcomeKnown !== true || retryable !== false
			|| lastCommitEpoch === undefined) {
			throw new Error('invalid committed outcome metadata');
		}
		return new CommitOutcomeError(
			queryId,
			message || 'remote write committed but response failed',
			{ committed: true, lastCommitEpoch },
			metadata
		);
	} catch {
		return new QueryOutcomeUnknownError(
			'unknown',
			'invalid committed outcome metadata from server',
			{ retryable: false, serverState: 'invalid_outcome' }
		);
	}
}

function parseRemoteWriteJson(
	response: string,
	operation: string,
	commitProven = true
): unknown {
	try {
		return parseJsonStrict(response);
	} catch {
		if (!commitProven) {
			throw new QueryOutcomeUnknownError(
				'unknown',
				`remote ${operation} finished but its response was invalid`,
				{ retryable: false, serverState: 'invalid_response' }
			);
		}
		throw new CommitOutcomeError(
			'unknown',
			`remote ${operation} committed but its response was invalid`,
			{ committed: true },
			{ retryable: false, serverState: 'invalid_response' }
		);
	}
}

function parseProcedureCallResponse(response: string): unknown {
	const value = parseRemoteWriteJson(response, 'procedure call', false);
	const invalid = (): never => {
		throw new QueryOutcomeUnknownError(
			'unknown',
			'remote procedure call returned invalid commit metadata',
			{ retryable: false, serverState: 'invalid_response' }
		);
	};
	if (value === null || typeof value !== 'object') return invalid();
	const body = value as Record<string, unknown>;
	const fields = ['status', 'committed', 'epoch', 'epoch_text', 'result'] as const;
	if (!hasOnlyKeys(body, fields) || !hasAllKeys(body, fields)
		|| body.status !== 'ok' || typeof body.committed !== 'boolean') return invalid();
	if (body.committed) {
		if (!isNonNegativeInteger(body.epoch) || typeof body.epoch_text !== 'string'
			|| !isOptionalEpoch(body.epoch, body.epoch_text)) return invalid();
	} else if (body.epoch !== null || body.epoch_text !== null) return invalid();
	return value;
}

/**
 * A thin Kit client for a running `mongreldb-server` daemon.
 *
 * Unlike the embedded {@link KitDatabase}, the daemon speaks SQL + native query
 * over HTTP, not the Kit's typed/validated object model — so this surface is
 * SQL-oriented: run SQL (returning Arrow), read table names / counts, force a
 * commit, and health-check. Schema, validation, and constraints are the local
 * KitDatabase's job; connect a remote for cross-process reads/queries.
 *
 * TypeScript-only: the addon exposes the daemon client; the Rust/Python kit
 * would need the `mongreldb-client` crate for an equivalent.
 */
export class RemoteDatabase {
	private readonly inner: NativeRemoteDatabase & RemoteRetention;
	private readonly url: string;
	readonly #authorization?: string;
	private capabilities?: Promise<RemoteCapabilities | null>;

	/** Connect (lazily) to a daemon at `url`, e.g. `http://127.0.0.1:8453`. */
	constructor(url: string, options: RemoteOptions = {}) {
		let parsed: URL;
		try {
			parsed = new URL(url);
		} catch {
			throw new TypeError('remote URL must be a valid http:// or https:// URL');
		}
		if (!['http:', 'https:'].includes(parsed.protocol) || !parsed.hostname) {
			throw new TypeError('remote URL must use http:// or https:// and include a host');
		}
		if (parsed.username || parsed.password) {
			throw new TypeError('remote credentials must use the auth option, not the URL');
		}
		if (parsed.search || parsed.hash) {
			throw new TypeError('remote URL must not include a query or fragment');
		}
		if (options.auth && 'bearerToken' in options.auth
			&& (typeof options.auth.bearerToken !== 'string'
				|| options.auth.bearerToken.trim().length === 0
				|| containsHttpHeaderControl(options.auth.bearerToken))) {
			throw new TypeError('bearer token must not be empty');
		}
		if (options.auth && !('bearerToken' in options.auth)
			&& (typeof options.auth.username !== 'string'
				|| options.auth.username.length === 0
				|| options.auth.username.includes(':')
				|| containsHttpHeaderControl(options.auth.username)
				|| typeof options.auth.password !== 'string'
				|| containsHttpHeaderControl(options.auth.password))) {
			throw new TypeError('basic-auth username must be non-empty and contain no colon');
		}
		this.url = `${parsed.origin}${parsed.pathname}`.replace(/\/$/, '');
		const nativeOptions: NativeRemoteOptions | undefined = options.auth && 'bearerToken' in options.auth
			? { bearerToken: options.auth.bearerToken }
			: options.auth
				? { username: options.auth.username, password: options.auth.password }
				: undefined;
		this.#authorization = options.auth && 'bearerToken' in options.auth
			? `Bearer ${options.auth.bearerToken}`
			: options.auth
				? `Basic ${Buffer.from(`${options.auth.username}:${options.auth.password}`, 'utf8').toString('base64')}`
				: undefined;
		const NativeRemote = NativeRemoteDatabase as unknown as NativeRemoteConstructor;
		this.inner = new NativeRemote(this.url, nativeOptions) as NativeRemoteDatabase & RemoteRetention;
	}

	private request(path: string, init: RequestInit = {}): Promise<Response> {
		const headers = new Headers(init.headers);
		if (this.#authorization) headers.set('authorization', this.#authorization);
		return fetch(`${this.url}${path}`, { ...init, headers });
	}

	private async recoveryRequest(
		path: string,
		init: RequestInit,
		timeoutMs: number
	): Promise<Response> {
		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), timeoutMs);
		try {
			return await this.request(path, { ...init, signal: controller.signal });
		} finally {
			clearTimeout(timer);
		}
	}

	/** Liveness check; returns the server's health string (throws if down). */
	health(): string {
		return this.inner.health();
	}

	/** Live table names on the server. */
	tableNames(): string[] {
		return this.inner.tableNames();
	}

	/** Row count of `table`. */
	count(table: string): bigint {
		return this.inner.count(table);
	}

	setHistoryRetentionEpochs(epochs: bigint): void {
		this.inner.setHistoryRetentionEpochs(epochs);
	}

	historyRetentionEpochs(): bigint {
		return this.inner.historyRetentionEpochs();
	}

	earliestRetainedEpoch(): bigint {
		return this.inner.earliestRetainedEpoch();
	}

	/** Run a SQL query without blocking the Node event loop. */
	async sql(sql: string, options?: SqlOptions): Promise<ArrowTable> {
		return this.startSql(sql, options).result;
	}

	async sqlRows(sql: string, options?: SqlOptions): Promise<Record<string, unknown>[]> {
		return [...(await this.sql(sql, options))].map((row) => ({ ...row }));
	}

	startSql(sql: string, options: SqlOptions = {}): SqlQuery<ArrowTable> {
		const queryId = normalizeQueryId(
			options.queryId ?? randomBytes(16).toString('hex')
		);
		if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0)) {
			throw new RangeError('timeoutMs must be a positive safe integer');
		}
		for (const [name, value] of [['maxOutputRows', options.maxOutputRows], ['maxOutputBytes', options.maxOutputBytes]] as const) {
			if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
				throw new RangeError(`${name} must be a positive safe integer`);
			}
		}
		if (options.signal?.aborted) {
			return {
				id: queryId,
				result: Promise.reject(preExecutionCancellationError(queryId, 'pre_cancelled')),
				cancel: async (): Promise<CancelOutcome> => 'pre_cancelled',
				status: async () => ({
					queryId,
					phase: 'pre_cancelled',
					terminalState: 'cancelled_before_start',
					serverState: 'pre_cancelled',
					operation: 'sql',
					committed: false,
					durableOutcome: { committed: false, committedStatements: 0 },
					terminalErrorCode: 'QUERY_CANCELLED',
					completedStatements: 0,
					statementIndex: 0,
					cancellationReason: 'client_request',
					cancelOutcome: 'pre_cancelled',
					retryable: false
				})
			};
		}

		const controller = new AbortController();
		let executionStarted = false;
		let cancelPromise: Promise<CancelOutcome> | undefined;
		let signalCancelPromise: Promise<CancelOutcome> | undefined;
		const applyCancelOutcome = (outcome: CancelOutcome): CancelOutcome => {
			if (outcome === 'accepted' || outcome === 'already_cancelling' || outcome === 'pre_cancelled') {
				controller.abort();
			}
			return outcome;
		};
		const cancel = (): Promise<CancelOutcome> => {
			cancelPromise ??= this.cancelSql(queryId).then(applyCancelOutcome);
			return cancelPromise;
		};
		const cancelForSignal = (): Promise<CancelOutcome> => {
			signalCancelPromise ??= this.cancelSqlForSignal(queryId).then(applyCancelOutcome);
			return signalCancelPromise;
		};
		const onAbort = () => {
			void cancelForSignal().catch(() => controller.abort());
		};
		options.signal?.addEventListener('abort', onAbort, { once: true });
		const result = this.requireSqlCancellation()
			.then(async () => {
				if (options.signal?.aborted) {
					const outcome = await cancelForSignal();
					throw preExecutionCancellationError(queryId, outcome);
				}
				executionStarted = true;
				return this.executeSql(
					sql,
					queryId,
					options.timeoutMs,
					options.maxOutputRows,
					options.maxOutputBytes,
					controller.signal
				);
			})
			.catch(async (error: unknown) => {
				const outcome = await (signalCancelPromise ?? cancelPromise)?.catch(() => undefined);
				if (!executionStarted && options.signal?.aborted) {
					if (error instanceof CapabilityUnsupportedError) throw error;
					if (error instanceof KitError && outcome === undefined) throw error;
					throw preExecutionCancellationError(queryId, outcome);
				}
				if (outcome === 'pre_cancelled') {
					throw new QueryCancelledError(queryId);
				}
				if (outcome === 'accepted' || outcome === 'already_cancelling'
					|| outcome === 'too_late' || outcome === 'already_finished') {
					return this.resolveLostQueryOutcome(queryId, error);
				}
				if (error instanceof KitError) throw error;
				return this.resolveLostQueryOutcome(queryId, error);
			})
			.finally(() => options.signal?.removeEventListener('abort', onAbort));
		return { id: queryId, result, cancel, status: () => this.queryStatus(queryId) };
	}

	async cancelSql(queryId: string): Promise<CancelOutcome> {
		queryId = normalizeQueryId(queryId);
		await this.requireSqlCancellation();
		return this.sendCancelSql(queryId);
	}

	private async cancelSqlForSignal(queryId: string): Promise<CancelOutcome> {
		await this.requireSqlCancellation();
		return this.sendCancelSql(queryId, SQL_RECOVERY_REQUEST_TIMEOUT_MS);
	}

	private async sendCancelSql(queryId: string, timeoutMs?: number): Promise<CancelOutcome> {
		const path = `/queries/${queryId}/cancel`;
		const response = timeoutMs === undefined
			? await this.request(path, { method: 'POST' })
			: await this.recoveryRequest(path, { method: 'POST' }, timeoutMs);
		if ([200, 202, 404, 409].includes(response.status)) {
			const body: unknown = await responseJsonStrict(response).catch(() => {
				throw new KitError('SQL cancellation response was not valid JSON', 'REMOTE_PROTOCOL');
			});
			return validateCancelResponse(body, queryId, response.status);
		}
		throw new KitError(`SQL cancellation failed with HTTP ${response.status}`);
	}

	async queryStatus(queryId: string): Promise<RemoteQueryStatus> {
		queryId = normalizeQueryId(queryId);
		const status = await this.rawQueryStatus(queryId);
		const committed = mergedCommitState(status.outcome?.committed, status.committed);
		const durable = status.durable ?? undefined;
		return {
			queryId: status.query_id,
			phase: status.server_state || status.state,
			terminalState: status.terminal_state ?? undefined,
			serverState: status.server_state || status.state,
			operation: status.operation ?? 'sql',
			committed,
			durableOutcome: {
				committed,
				committedStatements: maxKnown(
					status.outcome?.committed_statements,
					status.committed_statements
				) ?? null,
				lastCommitEpoch: remoteEpoch(
					status.outcome?.last_commit_epoch_text ?? status.last_commit_epoch_text,
					status.outcome?.last_commit_epoch ?? status.last_commit_epoch
				),
				lastCommitHlc: pickCommitHlc(durable, status.outcome, status),
				firstCommitStatementIndex: status.outcome?.first_commit_statement_index
					?? status.first_commit_statement_index ?? undefined,
				lastCommitStatementIndex: status.outcome?.last_commit_statement_index
					?? status.last_commit_statement_index ?? undefined,
				serializationState: pickSerializationState(status.outcome, durable)
			},
			terminalErrorCode: status.terminal_error?.code,
			terminalErrorCategory: status.terminal_error?.category,
			completedStatements: status.outcome?.completed_statements ?? status.completed_statements ?? null,
			statementIndex: status.outcome?.statement_index ?? status.statement_index ?? null,
			cancellationReason: status.cancellation_reason ?? 'none',
			cancelOutcome: status.cancel_outcome ?? undefined,
			retryable: status.retryable
		};
	}

	/**
	 * Text → embed under the active semantic identity → ANN retrieve
	 * (`POST /kit/retrieve_text`, 0.64+).
	 */
	async retrieveText(
		table: string,
		embeddingColumn: number,
		text: string,
		options: { k?: number; deadlineMs?: number; maxWork?: number } = {}
	): Promise<{
		hits: Array<{ rowId: string; rank: number; score: unknown }>;
		provenance: {
			embeddingColumn: number;
			providerRegistryGeneration: number;
			querySourceFingerprint: string;
			semanticIdentity: Record<string, unknown>;
		};
	}> {
		if (!Number.isInteger(embeddingColumn) || embeddingColumn < 0 || embeddingColumn > 0xffff) {
			throw new RangeError('embeddingColumn must be a u16 column id');
		}
		if (options.k !== undefined && (!Number.isSafeInteger(options.k) || options.k <= 0)) {
			throw new RangeError('k must be a positive safe integer');
		}
		const body: Record<string, unknown> = {
			table,
			embedding_column: embeddingColumn,
			text
		};
		if (options.k !== undefined) body.k = options.k;
		if (options.deadlineMs !== undefined) body.deadline_ms = options.deadlineMs;
		if (options.maxWork !== undefined) body.max_work = options.maxWork;
		const response = await this.request('/kit/retrieve_text', {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify(body)
		});
		if (!response.ok) {
			throw new KitError(
				`retrieve_text failed with HTTP ${response.status}`,
				'REMOTE_PROTOCOL'
			);
		}
		const payload: unknown = await responseJsonStrict(response);
		if (payload === null || typeof payload !== 'object') {
			throw new KitError('retrieve_text response was not an object', 'REMOTE_PROTOCOL');
		}
		const env = payload as Record<string, unknown>;
		if (!Array.isArray(env.hits) || env.provenance === null || typeof env.provenance !== 'object') {
			throw new KitError('retrieve_text response missing hits/provenance', 'REMOTE_PROTOCOL');
		}
		const provenance = env.provenance as Record<string, unknown>;
		return {
			hits: (env.hits as Array<Record<string, unknown>>).map((hit) => ({
				rowId: String(hit.row_id),
				rank: hit.rank as number,
				score: hit.score
			})),
			provenance: {
				embeddingColumn: provenance.embedding_column as number,
				providerRegistryGeneration: provenance.provider_registry_generation as number,
				querySourceFingerprint: String(provenance.query_source_fingerprint ?? ''),
				semanticIdentity: (provenance.semantic_identity ?? {}) as Record<string, unknown>
			}
		};
	}

	/**
	 * Build a multi-retriever hybrid search body for `POST /kit/search`.
	 * Exposed for wire tests and callers that need the payload without executing.
	 */
	static buildKitSearchBody(input: {
		table: string;
		retrievers: Array<Record<string, unknown>>;
		fusionConstant?: number;
		limit?: number;
		must?: Array<Record<string, unknown>>;
		rerank?: Record<string, unknown>;
		projection?: number[];
	}): Record<string, unknown> {
		if (!input.table) throw new TypeError('table is required');
		if (!input.retrievers || input.retrievers.length === 0) {
			throw new RangeError('search requires at least one retriever');
		}
		const limit = input.limit ?? 10;
		if (!Number.isSafeInteger(limit) || limit <= 0) {
			throw new RangeError('limit must be a positive safe integer');
		}
		const body: Record<string, unknown> = {
			table: input.table,
			retrievers: input.retrievers,
			fusion: { reciprocal_rank: { constant: input.fusionConstant ?? 60 } },
			limit
		};
		if (input.must && input.must.length > 0) body.must = input.must;
		if (input.rerank) body.rerank = input.rerank;
		if (input.projection) body.projection = input.projection;
		return body;
	}

	/** Multi-retriever hybrid search (`POST /kit/search`). */
	async kitSearch(body: Record<string, unknown>): Promise<{
		hits: Array<Record<string, unknown>>;
		trace?: unknown;
		nextCursor?: string;
	}> {
		const response = await this.request('/kit/search', {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify(body)
		});
		if (!response.ok) {
			throw new KitError(`kit search failed with HTTP ${response.status}`, 'REMOTE_PROTOCOL');
		}
		const payload: unknown = await responseJsonStrict(response);
		if (payload === null || typeof payload !== 'object') {
			throw new KitError('kit search response was not an object', 'REMOTE_PROTOCOL');
		}
		const env = payload as Record<string, unknown>;
		return {
			hits: Array.isArray(env.hits) ? env.hits as Array<Record<string, unknown>> : [],
			trace: env.trace,
			nextCursor: typeof env.next_cursor === 'string' ? env.next_cursor : undefined
		};
	}

	async sqlPage(sql: string, options: RemoteSqlPaginationOptions): Promise<RemoteSqlPage> {
		await this.requireSqlPagination();
		if (options.projection.length === 0 || options.projection.length > 128
			|| options.projection.some((column) => typeof column !== 'string'
				|| column.length === 0 || column === '*'
				|| Buffer.byteLength(column, 'utf8') > 256)
			|| new Set(options.projection).size !== options.projection.length
			|| options.projection.reduce(
				(bytes, column) => bytes + Buffer.byteLength(column, 'utf8'),
				0
			) > 16 * 1024) {
			throw new RangeError('projection must contain 1 to 128 unique explicit columns');
		}
		for (const [name, value] of [
			['pageSizeRows', options.pageSizeRows],
			['timeoutMs', options.timeoutMs],
			['maxPageBytes', options.maxPageBytes],
			['maxPageTokens', options.maxPageTokens],
			['maxOutputRows', options.maxOutputRows],
			['maxOutputBytes', options.maxOutputBytes]
		] as const) {
			if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
				throw new RangeError(`${name} must be a positive safe integer`);
			}
		}
		const queryId = normalizeQueryId(
			options.queryId ?? randomBytes(16).toString('hex')
		);
		let response: Response;
		try {
			response = await this.request('/sql', {
				method: 'POST',
				headers: { 'content-type': 'application/json' },
				body: JSON.stringify({
					sql,
					format: 'json',
					query_id: queryId,
					timeout_ms: options.timeoutMs,
					max_output_rows: options.maxOutputRows,
					max_output_bytes: options.maxOutputBytes,
					pagination: {
						page_size_rows: options.pageSizeRows,
						projection: options.projection,
						max_page_bytes: options.maxPageBytes,
						max_page_tokens: options.maxPageTokens
					}
				})
			});
		} catch (error) {
			return this.resolveLostQueryOutcome(queryId, error);
		}
		if (!response.ok) {
			const error = await remoteSqlError(response, queryId);
			if (error instanceof QueryOutcomeUnknownError) {
				return this.resolveLostQueryOutcome(queryId, error);
			}
			throw error;
		}
		if (response.headers.get('x-mongreldb-query-id') !== queryId) {
			return this.resolveLostQueryOutcome(
				queryId,
				new Error('SQL response x-mongreldb-query-id does not match the request')
			);
		}
		const body = await responseJsonStrict(response, MAX_PAGE_JSON_RESPONSE_BYTES).catch((error: unknown) =>
			this.resolveLostQueryOutcome(queryId, error));
		if (!isRemoteSqlPage(body, options)) {
			return this.resolveLostQueryOutcome(
				queryId,
				new Error('SQL pagination response was not a valid retained page')
			);
		}
		return remoteSqlPage(body);
	}

	async continueSqlPage(
		cursor: string,
		options: RemoteSqlControlOptions = {}
	): Promise<RemoteSqlPage> {
		await this.requireSqlPagination();
		if (cursor.length === 0 || Buffer.byteLength(cursor, 'utf8') > 2_048) {
			throw new RangeError('cursor must contain 1 to 2048 UTF-8 bytes');
		}
		if (options.timeoutMs !== undefined
			&& (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0)) {
			throw new RangeError('timeoutMs must be a positive safe integer');
		}
		const queryId = normalizeQueryId(options.queryId ?? randomBytes(16).toString('hex'));
		let response: Response;
		try {
			response = await this.request('/sql/continue', {
				method: 'POST',
				headers: { 'content-type': 'application/json' },
				body: JSON.stringify({
					cursor,
					operation_id: queryId,
					timeout_ms: options.timeoutMs
				}),
				signal: options.signal
			});
		} catch (error) {
			if (options.signal?.aborted) await this.cancelSql(queryId).catch(() => undefined);
			throw error;
		}
		if (!response.ok) throw await remoteSqlError(response, queryId);
		if (response.headers.get('x-mongreldb-query-id') !== queryId) {
			throw new SerializationError(
				queryId,
				'SQL continuation response x-mongreldb-query-id does not match the request',
				{ committed: false }
			);
		}
		const body = await responseJsonStrict(response, MAX_PAGE_JSON_RESPONSE_BYTES).catch((error: unknown) => {
			throw new SerializationError(
				queryId,
				error instanceof Error ? error.message : 'SQL continuation response was not valid JSON',
				{ committed: false }
			);
		});
		if (!isRemoteSqlPage(body)) {
			throw new SerializationError(
				queryId,
				'SQL continuation response was not a valid retained page',
				{ committed: false }
			);
		}
		return remoteSqlPage(body);
	}

	async executeIdempotentSql(
		sql: string,
		options: RemoteIdempotentSqlOptions
	): Promise<RemoteSqlWriteReceipt> {
		await this.requireSqlIdempotency();
		if (options.idempotencyKey.length === 0
			|| Buffer.byteLength(options.idempotencyKey, 'utf8') > 256) {
			throw new RangeError('idempotencyKey must contain 1 to 256 bytes');
		}
		for (const [name, value] of [
			['timeoutMs', options.timeoutMs],
			['maxOutputRows', options.maxOutputRows],
			['maxOutputBytes', options.maxOutputBytes]
		] as const) {
			if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
				throw new RangeError(`${name} must be a positive safe integer`);
			}
		}
		const queryId = normalizeQueryId(
			options.queryId ?? randomBytes(16).toString('hex')
		);
		try {
			return await this.executeIdempotentSqlOnce(sql, options, queryId);
		} catch (error) {
			if (!(error instanceof RetryIdempotentSqlError)) throw error;
			await this.requireFreshSqlIdempotency();
			let replayQueryId = randomBytes(16).toString('hex');
			while (replayQueryId === queryId) replayQueryId = randomBytes(16).toString('hex');
			try {
				return await this.executeIdempotentSqlOnce(sql, options, replayQueryId, queryId);
			} catch (replayError) {
				if (replayError instanceof RetryIdempotentSqlError) throw replayError.outcome;
				throw replayError;
			}
		}
	}

	private async executeIdempotentSqlOnce(
		sql: string,
		options: RemoteIdempotentSqlOptions,
		queryId: string,
		expectedOriginalQueryId?: string
	): Promise<RemoteSqlWriteReceipt> {
		let response: Response;
		try {
			response = await this.request('/sql', {
				method: 'POST',
				headers: { 'content-type': 'application/json' },
				body: JSON.stringify({
					sql,
					format: 'json',
					query_id: queryId,
					timeout_ms: options.timeoutMs,
					max_output_rows: options.maxOutputRows,
					max_output_bytes: options.maxOutputBytes,
					idempotency_key: options.idempotencyKey
				})
			});
		} catch (error) {
			return this.resolveIdempotentSqlLoss(queryId, error);
		}
		if (!response.ok) {
			const error = await remoteSqlError(response, queryId);
			if (error instanceof QueryOutcomeUnknownError) {
				return this.resolveIdempotentSqlLoss(queryId, error);
			}
			throw error;
		}
		const body: unknown = await responseJsonStrict(response).catch((error: unknown) =>
			this.resolveIdempotentSqlLoss(queryId, error));
		if (!isRemoteSqlWriteReceipt(body, queryId, expectedOriginalQueryId)) {
			return this.resolveIdempotentSqlLoss(
				queryId,
				new Error('SQL idempotency response was not a valid durable receipt')
			);
		}
		const receipt: RemoteSqlWriteReceipt = {
			queryId: body.query_id,
			originalQueryId: body.original_query_id,
			status: body.status,
			terminalState: body.terminal_state ?? undefined,
			serverState: body.server_state ?? undefined,
			cancelOutcome: body.cancel_outcome ?? undefined,
			cancellationReason: body.cancellation_reason ?? undefined,
			committed: body.committed,
			committedStatements: body.committed_statements,
			lastCommitEpoch: remoteEpoch(
				body.outcome.last_commit_epoch_text ?? body.last_commit_epoch_text,
				body.outcome.last_commit_epoch ?? body.last_commit_epoch
			),
			firstCommitStatementIndex: body.first_commit_statement_index
				?? body.outcome.first_commit_statement_index
				?? undefined,
			lastCommitStatementIndex: body.last_commit_statement_index
				?? body.outcome.last_commit_statement_index
				?? undefined,
			completedStatements: body.completed_statements,
			statementIndex: body.statement_index,
			retryable: body.retryable,
			idempotencyReplayed: body.idempotency_replayed,
			idempotencyPersisted: body.idempotency_persisted,
			idempotencyExpiresAtMs: body.idempotency_expires_at_ms,
			terminalError: body.terminal_error ?? undefined
		};
		if (response.headers.get('x-mongreldb-query-id') !== queryId) {
			if (receipt.committed) {
				throw new CommitOutcomeError(
					queryId,
					'SQL committed but x-mongreldb-query-id did not match the request',
					{
						committed: true,
						committedStatements: receipt.committedStatements,
						lastCommitEpoch: receipt.lastCommitEpoch,
						firstCommitStatementIndex: receipt.firstCommitStatementIndex,
						lastCommitStatementIndex: receipt.lastCommitStatementIndex,
						completedStatements: receipt.completedStatements,
						statementIndex: receipt.statementIndex
					},
					{
						cancelOutcome: receipt.cancelOutcome,
						cancellationReason: receipt.cancellationReason,
						retryable: false,
						serverState: receipt.serverState
					}
				);
			}
			return this.resolveIdempotentSqlLoss(
				queryId,
				new Error('SQL response x-mongreldb-query-id does not match the request')
			);
		}
		return receipt;
	}

	private async resolveIdempotentSqlLoss(queryId: string, cause: unknown): Promise<never> {
		let status: RawRemoteQueryStatus;
		try {
			status = await this.rawQueryStatus(queryId, SQL_RECOVERY_REQUEST_TIMEOUT_MS);
		} catch (error) {
			if (error instanceof QueryNotRetainedError) {
				throw new RetryIdempotentSqlError(error);
			}
			if (!(error instanceof QueryOutcomeUnknownError)) {
				return this.resolveLostQueryOutcome(queryId, cause);
			}
			throw error;
		}
		return this.resolveLostQueryOutcome(queryId, cause, status);
	}

	private async rawQueryStatus(
		queryId: string,
		recoveryTimeoutMs?: number
	): Promise<RawRemoteQueryStatus> {
		await this.requireSqlCancellation();
		const path = `/queries/${queryId}`;
		const response = recoveryTimeoutMs === undefined
			? await this.request(path)
			: await this.recoveryRequest(path, {}, recoveryTimeoutMs);
		if (response.status === 404) {
			const body = await responseJsonStrict(response).catch((error: unknown) => {
				throw new InvalidQueryStatusError(
					queryId,
					error instanceof Error ? error.message : 'query-not-found response was not valid JSON'
				);
			});
			if (!isQueryNotFoundResponse(body, queryId)) {
				throw new InvalidQueryStatusError(
					queryId,
					'query-not-found response fields were inconsistent'
				);
			}
			throw new QueryNotRetainedError(queryId, `query ${queryId} is not retained`);
		}
		if (!response.ok) throw new KitError(`query status failed with HTTP ${response.status}`);
		const body: unknown = await responseJsonStrict(response).catch((error: unknown) => {
			throw new InvalidQueryStatusError(
				queryId,
				error instanceof Error ? error.message : 'response was not valid JSON'
			);
		});
		if (!isRemoteQueryStatus(body, queryId)) {
			throw new InvalidQueryStatusError(queryId, 'response fields were inconsistent');
		}
		return body;
	}

	private async resolveLostQueryOutcome(
		queryId: string,
		cause: unknown,
		initialStatus?: RawRemoteQueryStatus
	): Promise<never> {
		const deadline = Date.now() + SQL_RECOVERY_WINDOW_MS;
		let status = initialStatus ?? await this.queryStatusForRecovery(queryId, deadline);
		if (!status || !recoveryStatusIsDecisive(status)) {
			await this.cancelSqlForRecovery(queryId, deadline);
		}
		while (Date.now() < deadline) {
			if (status && recoveryStatusIsDecisive(status)) break;
			status = await this.queryStatusForRecovery(queryId, deadline) ?? status;
			if (status && recoveryStatusIsDecisive(status)) break;
			const remaining = deadline - Date.now();
			if (remaining > 0) {
				await new Promise((resolve) => setTimeout(
					resolve,
					Math.min(SQL_RECOVERY_POLL_INTERVAL_MS, remaining)
				));
			}
		}
		if (!status || !recoveryStatusIsDecisive(status)) {
			throw new QueryOutcomeUnknownError(
				queryId,
				cause instanceof Error ? cause.message : 'SQL transport failed before terminal status was retained'
			);
		}
		const outcome = status?.outcome;
		const message = cause instanceof Error ? cause.message : 'SQL response was lost';
		const committed = mergedCommitState(outcome?.committed, status.committed);
		const metadata: SqlErrorMetadata = {
			cancelOutcome: status.cancel_outcome ?? undefined,
			cancellationReason: status.cancellation_reason,
			retryable: status.retryable,
			serverState: status.server_state || status.state
		};
		if (committed === null) {
			throw new QueryOutcomeUnknownError(queryId, message, metadata);
		}
		const durable = status.durable ?? undefined;
		const durableOutcome = {
			committed,
			committedStatements: maxKnown(
				outcome?.committed_statements,
				status.committed_statements
			),
			lastCommitEpoch: remoteEpoch(
				outcome?.last_commit_epoch_text ?? status.last_commit_epoch_text,
				outcome?.last_commit_epoch ?? status.last_commit_epoch
			),
			lastCommitHlc: pickCommitHlc(durable, outcome, status),
			firstCommitStatementIndex: outcome?.first_commit_statement_index
				?? status.first_commit_statement_index ?? undefined,
			lastCommitStatementIndex: outcome?.last_commit_statement_index
				?? status.last_commit_statement_index ?? undefined,
			completedStatements: outcome?.completed_statements ?? status.completed_statements ?? undefined,
			statementIndex: outcome?.statement_index ?? status.statement_index ?? undefined,
			serializationState: pickSerializationState(outcome, durable)
		};
		switch (status.terminal_error?.code) {
			case 'QUERY_CANCELLED':
			case 'QUERY_CANCELLED_AFTER_COMMIT':
				throw new QueryCancelledError(queryId, message, durableOutcome, metadata);
			case 'DEADLINE_EXCEEDED':
			case 'DEADLINE_AFTER_COMMIT':
				throw new QueryTimeoutError(queryId, message, durableOutcome, metadata);
			case 'RESULT_LIMIT_EXCEEDED':
				throw new ResultLimitExceededError(queryId, message, durableOutcome, metadata);
			case 'SERIALIZATION_FAILED':
			case 'SERIALIZATION_FAILED_AFTER_COMMIT':
				throw new SerializationError(queryId, message, durableOutcome, metadata);
			default:
				if (durableOutcome.committed) {
					throw new CommitOutcomeError(queryId, message, durableOutcome, metadata);
				}
				if (status.state === 'completed') {
					throw new SerializationError(queryId, message, durableOutcome, metadata);
				}
				throw new QueryExecutionError(
					queryId,
					status.terminal_error?.code ?? 'QUERY_FAILED',
					message,
					durableOutcome,
					metadata
				);
		}
	}

	private recoveryTimeout(deadline: number): number | undefined {
		const remaining = deadline - Date.now();
		return remaining > 0
			? Math.min(SQL_RECOVERY_REQUEST_TIMEOUT_MS, remaining)
			: undefined;
	}

	private async queryStatusForRecovery(
		queryId: string,
		deadline: number
	): Promise<RawRemoteQueryStatus | undefined> {
		const timeout = this.recoveryTimeout(deadline);
		if (timeout === undefined) return undefined;
		return this.rawQueryStatus(queryId, timeout).catch(() => undefined);
	}

	private async cancelSqlForRecovery(queryId: string, deadline: number): Promise<void> {
		const timeout = this.recoveryTimeout(deadline);
		if (timeout === undefined) return;
		const response = await this.recoveryRequest(
			`/queries/${queryId}/cancel`,
			{ method: 'POST' },
			timeout
		).catch(() => undefined);
		if (response && [200, 202, 404, 409].includes(response.status)) {
			const body = await responseJsonStrict(response).catch(() => undefined);
			if (body !== undefined) {
				try {
					validateCancelResponse(body, queryId, response.status);
				} catch {
					// Recovery remains outcome-unknown when control metadata is malformed.
				}
			}
		}
	}

	private async executeSql(
		sql: string,
		queryId: string,
		timeoutMs?: number,
		maxOutputRows?: number,
		maxOutputBytes?: number,
		signal?: AbortSignal
	): Promise<ArrowTable> {
		const response = await this.request('/sql', {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({
				sql,
				format: 'arrow',
				query_id: queryId,
				timeout_ms: timeoutMs,
				max_output_rows: maxOutputRows,
				max_output_bytes: maxOutputBytes
			}),
			signal
		});
		if (!response.ok) {
			const error = await remoteSqlError(response, queryId);
			if (error instanceof QueryOutcomeUnknownError) {
				return this.resolveLostQueryOutcome(queryId, error);
			}
			throw error;
		}
		if (response.headers.get('x-mongreldb-query-id') !== queryId) {
			return this.resolveLostQueryOutcome(
				queryId,
				new Error('SQL response x-mongreldb-query-id does not match the request')
			);
		}
		const bytes = await responseBytesBounded(
			response,
			Math.min(maxOutputBytes ?? MAX_PAGE_JSON_RESPONSE_BYTES, MAX_PAGE_JSON_RESPONSE_BYTES)
		);
		if (bytes.length === 0) {
			return tableFromJSON([]) as unknown as ArrowTable;
		}
		return tableFromIPC(bytes);
	}

	private async loadCapabilities(): Promise<RemoteCapabilities | null> {
		const response = await this.request('/capabilities');
		if (response.status === 404) return null;
		if (!response.ok) throw new KitError(`capability request failed with HTTP ${response.status}`);
		const capabilities = await responseJsonStrict(response).catch(() => {
			throw new KitError('capability response was not valid bounded JSON', 'REMOTE_PROTOCOL');
		});
		if (!isRemoteCapabilities(capabilities)) {
			throw new KitError('capability response had unknown or invalid fields', 'REMOTE_PROTOCOL');
		}
		return capabilities;
	}

	private validateSqlCancellationCapability(
		capability: SqlCancellationCapabilities | undefined
	): SqlCancellationCapabilities {
		if (capability?.version !== 2 || !capability.client_query_ids || !capability.cancel_endpoint
			|| !capability.query_status || !capability.pre_registration_cancel) {
			throw new CapabilityUnsupportedError('server does not support SQL cancellation capability version 2');
		}
		return capability;
	}

	private validateSqlIdempotencyCapability(
		capability: SqlIdempotencyCapabilities | undefined
	): SqlIdempotencyCapabilities {
		if (capability?.version !== 1
			|| !capability.durable_pre_execution_intent
			|| !capability.replay_committed_receipt
			|| !capability.indeterminate_never_reexecutes) {
			throw new CapabilityUnsupportedError('server does not support durable SQL idempotency capability version 1');
		}
		return capability;
	}

	private async requireSqlCancellation(): Promise<SqlCancellationCapabilities> {
		this.capabilities ??= this.loadCapabilities();
		const capability = (await this.capabilities)?.sql_cancellation;
		return this.validateSqlCancellationCapability(capability);
	}

	private async requireSqlPagination(): Promise<SqlPaginationCapabilities> {
		await this.requireSqlCancellation();
		const capability = (await this.capabilities)?.sql_pagination;
		if (capability?.version !== 1
			|| capability.continuation_endpoint !== '/sql/continue'
			|| !capability.retained_snapshot
			|| !capability.projection_required
			|| !capability.byte_and_token_hints) {
			throw new CapabilityUnsupportedError('server does not support SQL pagination capability version 1');
		}
		return capability;
	}

	private async requireSqlIdempotency(): Promise<SqlIdempotencyCapabilities> {
		await this.requireSqlCancellation();
		const capability = (await this.capabilities)?.sql_idempotency;
		return this.validateSqlIdempotencyCapability(capability);
	}

	private async requireFreshSqlIdempotency(): Promise<void> {
		const capabilities = await this.loadCapabilities();
		this.validateSqlCancellationCapability(capabilities?.sql_cancellation);
		this.validateSqlIdempotencyCapability(capabilities?.sql_idempotency);
	}

	/** Flush/commit `table` on the server; returns the new epoch. */
	commit(table: string): bigint {
		try {
			return this.inner.commit(table);
		} catch (error) {
			throw nativeRemoteWriteError(error);
		}
	}

	createProcedure(spec: ProcedureSpec): unknown {
		try {
			return parseRemoteWriteJson(
				this.inner.createProcedure({ json: procedureJson(spec) }),
				'procedure creation'
			);
		} catch (error) {
			throw nativeRemoteWriteError(error);
		}
	}

	dropProcedure(name: string): void {
		try {
			this.inner.dropProcedure(name);
		} catch (error) {
			throw nativeRemoteWriteError(error);
		}
	}

	callProcedure(name: string, opts: ProcedureCallOptions = {}): unknown {
		try {
			return parseProcedureCallResponse(
				this.inner.callProcedure(name, {
					argsJson: JSON.stringify(opts.args ?? {}),
					idempotencyKey: opts.idempotencyKey
				})
			);
		} catch (error) {
			throw nativeRemoteWriteError(error);
		}
	}

	createTrigger(spec: TriggerSpec): unknown {
		try {
			return parseRemoteWriteJson(
				this.inner.createTrigger({ json: triggerJson(spec) }),
				'trigger creation'
			);
		} catch (error) {
			throw nativeRemoteWriteError(error);
		}
	}

	replaceTrigger(name: string, spec: TriggerSpec): unknown {
		try {
			return parseRemoteWriteJson(
				this.inner.replaceTrigger(name, { json: triggerJson(spec) }),
				'trigger replacement'
			);
		} catch (error) {
			throw nativeRemoteWriteError(error);
		}
	}

	dropTrigger(name: string): void {
		try {
			this.inner.dropTrigger(name);
		} catch (error) {
			throw nativeRemoteWriteError(error);
		}
	}

	triggers(): unknown {
		return parseJsonStrict(this.inner.triggers());
	}

	trigger(name: string): unknown {
		return parseJsonStrict(this.inner.trigger(name));
	}

	createVirtualTable(spec: VirtualTableSpec): Promise<ArrowTable> {
		return this.sql(createVirtualTableSql(spec));
	}

	dropVirtualTable(name: string): Promise<ArrowTable> {
		return this.sql(dropVirtualTableSql(name));
	}

	/** Compact every table on the daemon (POST /compact). Returns
	 * `{ compacted, skipped }`. */
	compact(): { compacted: number; skipped: number } {
		return this.inner.compact();
	}

	/** Compact a single table on the daemon (POST /tables/{name}/compact).
	 * Returns `true` if compacted, `false` if skipped. */
	compactTable(table: string): boolean {
		return this.inner.compactTable(table);
	}
}

type SqlCancellationCapabilities = {
	version: number;
	client_query_ids: boolean;
	cancel_endpoint: boolean;
	query_status: boolean;
	stream_disconnect_cancels: boolean;
	pre_registration_cancel: boolean;
};

type SqlIdempotencyCapabilities = {
	version: number;
	durable_pre_execution_intent: boolean;
	replay_committed_receipt: boolean;
	indeterminate_never_reexecutes: boolean;
};

type SqlPaginationCapabilities = {
	version: number;
	continuation_endpoint: string;
	retained_snapshot: boolean;
	projection_required: boolean;
	byte_and_token_hints: boolean;
};

type RemoteCapabilities = {
	sql_cancellation?: SqlCancellationCapabilities;
	sql_idempotency?: SqlIdempotencyCapabilities;
	sql_pagination?: SqlPaginationCapabilities;
};

function isRemoteCapabilities(value: unknown): value is RemoteCapabilities {
	if (value === null || typeof value !== 'object') return false;
	const body = value as Record<string, unknown>;
	if (!hasOnlyKeys(body, ['sql_cancellation', 'sql_idempotency', 'sql_pagination'])) return false;
	const cancellation = body.sql_cancellation as Record<string, unknown> | undefined;
	if (cancellation !== undefined && (cancellation === null || typeof cancellation !== 'object'
		|| !hasOnlyKeys(cancellation, [
			'version', 'client_query_ids', 'cancel_endpoint', 'query_status',
			'stream_disconnect_cancels', 'pre_registration_cancel'
		])
		|| !isNonNegativeInteger(cancellation.version) || cancellation.version > 255
		|| ['client_query_ids', 'cancel_endpoint', 'query_status',
			'stream_disconnect_cancels', 'pre_registration_cancel']
			.some((field) => typeof cancellation[field] !== 'boolean'))) return false;
	const idempotency = body.sql_idempotency as Record<string, unknown> | undefined;
	if (idempotency !== undefined && (idempotency === null || typeof idempotency !== 'object'
		|| !hasOnlyKeys(idempotency, [
			'version', 'durable_pre_execution_intent', 'replay_committed_receipt',
			'indeterminate_never_reexecutes'
		])
		|| !isNonNegativeInteger(idempotency.version) || idempotency.version > 255
		|| ['durable_pre_execution_intent', 'replay_committed_receipt',
			'indeterminate_never_reexecutes']
			.some((field) => typeof idempotency[field] !== 'boolean'))) return false;
	const pagination = body.sql_pagination as Record<string, unknown> | undefined;
	return pagination === undefined || pagination !== null && typeof pagination === 'object'
		&& hasOnlyKeys(pagination, [
			'version', 'continuation_endpoint', 'retained_snapshot', 'projection_required',
			'byte_and_token_hints'
		])
		&& isNonNegativeInteger(pagination.version) && pagination.version <= 255
		&& typeof pagination.continuation_endpoint === 'string'
		&& ['retained_snapshot', 'projection_required', 'byte_and_token_hints']
			.every((field) => typeof pagination[field] === 'boolean');
}

function isRemoteSqlErrorEnvelope(value: unknown, queryId: string): boolean {
	if (value === null || typeof value !== 'object') return false;
	const body = value as Record<string, unknown>;
	if (body.outcome === null || typeof body.outcome !== 'object'
		|| body.error === null || typeof body.error !== 'object') return false;
	const outcome = body.outcome as Record<string, unknown>;
	const error = body.error as Record<string, unknown>;
	if (!hasOnlyKeys(body, [
		'query_id', 'status', 'terminal_state', 'committed', 'committed_statements',
		'last_commit_epoch', 'last_commit_epoch_text', 'first_commit_statement_index',
		'last_commit_statement_index', 'completed_statements', 'statement_index',
		'cancel_outcome', 'cancellation_reason', 'retryable', 'server_state', 'outcome',
		'error', 'max_rows', 'max_bytes'
	]) || !hasOnlyKeys(outcome, [
		'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
		'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
		'statement_index', 'serialization'
	]) || !hasOnlyKeys(error, [
		'code', 'message', 'query_id', 'committed', 'retryable', 'max_rows', 'max_bytes'
	]) || !hasAllKeys(outcome, [
		'committed', 'committed_statements', 'last_commit_epoch', 'last_commit_epoch_text',
		'first_commit_statement_index', 'last_commit_statement_index', 'completed_statements',
		'statement_index', 'serialization'
	])) return false;
	const code = error.code;
	const status = body.status;
	if (body.query_id !== queryId || error.query_id !== queryId
		|| typeof code !== 'string' || code.trim().length === 0
		|| typeof error.message !== 'string' || error.message.trim().length === 0
		|| typeof status !== 'string' || body.terminal_state !== status
		|| !['not_started', 'in_progress', 'succeeded', 'failed', 'unknown']
			.includes(outcome.serialization as string)
		|| typeof body.retryable !== 'boolean' || error.retryable !== body.retryable
		|| ![null, true, false].includes(body.committed as null | boolean)
		|| body.committed !== outcome.committed || body.committed !== error.committed) return false;
	const names = ['committed_statements', 'first_commit_statement_index',
		'last_commit_statement_index', 'completed_statements', 'statement_index'] as const;
	if (names.some((name) => !isOptionalNonNegativeInteger(body[name])
		|| !sameOptional(body[name], outcome[name]))) return false;
	for (const name of ['max_rows', 'max_bytes'] as const) {
		const topLimit = body[name];
		const errorLimit = error[name];
		if (topLimit != null && (!isNonNegativeInteger(topLimit) || topLimit === 0)
			|| errorLimit != null && (!isNonNegativeInteger(errorLimit) || errorLimit === 0)
			|| topLimit != null && errorLimit != null && topLimit !== errorLimit) return false;
	}
	if (!isOptionalEpoch(body.last_commit_epoch, body.last_commit_epoch_text)
		|| !isOptionalEpoch(outcome.last_commit_epoch, outcome.last_commit_epoch_text)) return false;
	const topEpoch = receiptEpoch(body.last_commit_epoch, body.last_commit_epoch_text);
	const outcomeEpoch = receiptEpoch(outcome.last_commit_epoch, outcome.last_commit_epoch_text);
	if (topEpoch !== outcomeEpoch) return false;
	const committed = body.committed as boolean | null;
	const committedStatements = body.committed_statements as number | null | undefined;
	const first = body.first_commit_statement_index as number | null | undefined;
	const last = body.last_commit_statement_index as number | null | undefined;
	const completed = body.completed_statements as number | null | undefined;
	const statement = body.statement_index as number | null | undefined;
	const unknown = code === 'QUERY_OUTCOME_UNKNOWN';
	if (committed === true) {
		if (unknown || !COMMITTED_QUERY_STATUSES.has(status)
			|| committedStatements == null || committedStatements === 0
			|| topEpoch === undefined || body.last_commit_epoch_text == null
			|| outcome.last_commit_epoch_text == null || first == null || last == null
			|| completed == null || statement == null) return false;
	} else if (committed === false) {
		if (unknown || !['failed_before_commit', 'cancelled_before_commit',
			'deadline_before_commit', 'cancelled_before_start'].includes(status)
			|| committedStatements !== 0 || topEpoch !== undefined
			|| first != null || last != null || completed == null || statement == null) return false;
	} else if (!unknown || status !== 'outcome_unknown'
		|| names.some((name) => body[name] != null) || topEpoch !== undefined
		|| body.retryable !== false) return false;
	if (first != null && last != null && committedStatements != null && statement != null
		&& (first > last || committedStatements > last - first + 1 || last > statement)) return false;
	if (completed != null && statement != null
		&& (statement > completed || completed > statement + 1)) return false;
	const expectedRetryable = ['QUERY_REGISTRY_FULL', 'IDEMPOTENCY_STORE_FULL',
		'IDEMPOTENCY_STORE_UNAVAILABLE'].includes(code);
	if (body.retryable !== expectedRetryable) return false;
	const codeMatchesStatus = ({
		QUERY_OUTCOME_UNKNOWN: status === 'outcome_unknown',
		QUERY_CANCELLED_AFTER_COMMIT: status === 'cancelled_after_commit' && committed,
		DEADLINE_AFTER_COMMIT: status === 'deadline_after_commit' && committed,
		QUERY_CANCELLED: ['cancelled_before_commit', 'cancelled_before_start'].includes(status),
		DEADLINE_EXCEEDED: status === 'deadline_before_commit',
		COMMIT_OUTCOME: committed === true,
		SERIALIZATION_FAILED_AFTER_COMMIT: committed === true
	} as Record<string, boolean>)[code] ?? true;
	const statusMatchesCode = ({
		outcome_unknown: code === 'QUERY_OUTCOME_UNKNOWN',
		cancelled_after_commit: code === 'QUERY_CANCELLED_AFTER_COMMIT',
		deadline_after_commit: code === 'DEADLINE_AFTER_COMMIT',
		cancelled_before_commit: code === 'QUERY_CANCELLED',
		cancelled_before_start: code === 'QUERY_CANCELLED',
		deadline_before_commit: code === 'DEADLINE_EXCEEDED'
	} as Record<string, boolean>)[status] ?? true;
	return codeMatchesStatus && statusMatchesCode;
}

function isSqlCursorErrorEnvelope(value: unknown): boolean {
	if (value === null || typeof value !== 'object') return false;
	const body = value as Record<string, unknown>;
	const outcome = body.outcome as Record<string, unknown> | null;
	const error = body.error as Record<string, unknown> | null;
	const countFields = [
		'committed_statements', 'first_commit_statement_index',
		'last_commit_statement_index', 'completed_statements', 'statement_index'
	] as const;
	return hasOnlyKeys(body, [
		'status', 'terminal_state', 'server_state', 'committed', ...countFields,
		'last_commit_epoch', 'last_commit_epoch_text', 'cancel_outcome',
		'cancellation_reason', 'retryable', 'outcome', 'error'
	]) && hasAllKeys(body, [
		'status', 'terminal_state', 'server_state', 'committed', ...countFields,
		'last_commit_epoch', 'last_commit_epoch_text', 'cancel_outcome',
		'cancellation_reason', 'retryable', 'outcome', 'error'
	])
		&& outcome !== null && typeof outcome === 'object'
		&& hasOnlyKeys(outcome, [
			'committed', ...countFields, 'last_commit_epoch', 'last_commit_epoch_text',
			'serialization'
		]) && hasAllKeys(outcome, [
			'committed', ...countFields, 'last_commit_epoch', 'last_commit_epoch_text',
			'serialization'
		])
		&& error !== null && typeof error === 'object'
		&& hasOnlyKeys(error, ['code', 'message', 'committed', 'retryable'])
		&& hasAllKeys(error, ['code', 'message', 'committed', 'retryable'])
		&& body.status === 'failed_before_commit'
		&& body.terminal_state === 'failed_before_commit' && body.server_state === 'failed'
		&& body.committed === false && outcome.committed === false
		&& body.committed_statements === 0 && outcome.committed_statements === 0
		&& body.last_commit_epoch === null && outcome.last_commit_epoch === null
		&& body.last_commit_epoch_text === null && outcome.last_commit_epoch_text === null
		&& body.first_commit_statement_index === null
		&& outcome.first_commit_statement_index === null
		&& body.last_commit_statement_index === null
		&& outcome.last_commit_statement_index === null
		&& body.completed_statements === 0 && outcome.completed_statements === 0
		&& body.statement_index === 0 && outcome.statement_index === 0
		&& body.cancel_outcome === null && body.cancellation_reason === null
		&& body.retryable === false && outcome.serialization === 'not_started'
		&& typeof error.code === 'string' && error.code.length > 0
		&& typeof error.message === 'string' && error.message.length > 0
		&& error.committed === false && error.retryable === false;
}

async function remoteSqlError(response: Response, fallbackQueryId: string): Promise<Error> {
	let body: {
		query_id?: string;
		status?: string;
		terminal_state?: string;
		error?: { code?: string; message?: string; query_id?: string };
		outcome?: RemoteDurableOutcome;
		committed?: boolean | null;
		committed_statements?: number | null;
		last_commit_epoch?: number;
		last_commit_epoch_text?: string;
		first_commit_statement_index?: number;
		last_commit_statement_index?: number;
		completed_statements?: number | null;
		statement_index?: number | null;
		cancel_outcome?: CancelOutcome;
		cancellation_reason?: string;
		retryable?: boolean;
		server_state?: string;
	} = {};
	try {
		const value: unknown = await responseJsonStrict(response);
		if (fallbackQueryId === 'unknown' && isSqlCursorErrorEnvelope(value)) {
			body = value as typeof body;
		} else {
			const candidateQueryId = value !== null && typeof value === 'object'
				&& typeof (value as Record<string, unknown>).query_id === 'string'
				&& /^[0-9a-f]{32}$/u.test((value as Record<string, unknown>).query_id as string)
				? (value as Record<string, unknown>).query_id as string
				: undefined;
			const expectedQueryId = fallbackQueryId === 'unknown' ? candidateQueryId : fallbackQueryId;
			if (expectedQueryId === undefined || !isRemoteSqlErrorEnvelope(value, expectedQueryId)) {
				return fallbackQueryId === 'unknown'
					? new SerializationError(
						'unknown',
						`SQL error response was malformed (HTTP ${response.status})`,
						{ committed: false }
					)
					: new QueryOutcomeUnknownError(
						fallbackQueryId,
						`SQL error response was malformed (HTTP ${response.status})`,
						{ retryable: false, serverState: 'invalid_outcome' }
					);
			}
			body = value as typeof body;
		}
	} catch {
		return fallbackQueryId === 'unknown'
			? new SerializationError('unknown', `SQL error response was not valid JSON (HTTP ${response.status})`, { committed: false })
			: new QueryOutcomeUnknownError(fallbackQueryId, `SQL error response was not valid JSON (HTTP ${response.status})`);
	}
	const queryId = body.error?.query_id ?? fallbackQueryId;
	const message = body.error?.message;
	const outcome = {
		committed: mergedCommitState(body.outcome?.committed, body.committed),
		committedStatements: maxKnown(
			body.outcome?.committed_statements,
			body.committed_statements
		),
		lastCommitEpoch: remoteEpoch(
			body.outcome?.last_commit_epoch_text ?? body.last_commit_epoch_text,
			body.outcome?.last_commit_epoch ?? body.last_commit_epoch
		),
		firstCommitStatementIndex: body.outcome?.first_commit_statement_index
			?? body.first_commit_statement_index,
		lastCommitStatementIndex: body.outcome?.last_commit_statement_index
			?? body.last_commit_statement_index,
		completedStatements: maxKnown(
			body.outcome?.completed_statements,
			body.completed_statements
		),
		statementIndex: maxKnown(body.outcome?.statement_index, body.statement_index)
	};
	const metadata: SqlErrorMetadata = {
		cancelOutcome: body.cancel_outcome,
		cancellationReason: body.cancellation_reason,
		retryable: body.retryable,
		serverState: body.server_state
	};
	const serverCode = body.error?.code;
	switch (serverCode) {
		case 'QUERY_CANCELLED':
		case 'QUERY_CANCELLED_AFTER_COMMIT':
			return new QueryCancelledError(queryId, message, outcome, metadata);
		case 'DEADLINE_EXCEEDED':
		case 'DEADLINE_AFTER_COMMIT':
			return new QueryTimeoutError(queryId, message, outcome, metadata);
		case 'QUERY_ID_CONFLICT':
			return new QueryIdConflictError(queryId, message);
		case 'TRANSACTION_ABORTED':
			return new TransactionAbortedError(message);
		case 'COMMIT_OUTCOME':
			return new CommitOutcomeError(queryId, message ?? 'SQL committed but response failed', { ...outcome, committed: true }, metadata);
		case 'SERIALIZATION_FAILED_AFTER_COMMIT':
			return new SerializationError(queryId, message ?? 'SQL committed but response serialization failed', { ...outcome, committed: true }, metadata);
		case 'QUERY_OUTCOME_UNKNOWN':
			return new QueryOutcomeUnknownError(queryId, message, metadata);
		case 'QUERY_REGISTRY_FULL':
		case 'CANCEL_TOO_LATE':
		case 'QUERY_ALREADY_FINISHED':
		case 'QUERY_NOT_FOUND':
			return new KitError(
				message ?? `SQL request failed with HTTP ${response.status}`,
				serverCode
			);
		case 'RESULT_LIMIT_EXCEEDED':
			return new ResultLimitExceededError(queryId, message ?? 'SQL result limit exceeded', outcome, metadata);
		case 'SERIALIZATION_FAILED':
			return new SerializationError(queryId, message ?? 'SQL response serialization failed', outcome, metadata);
		case 'CAPABILITY_UNSUPPORTED':
			return new CapabilityUnsupportedError(message ?? 'server capability is unsupported');
		default:
			if (serverCode) {
				return new RemoteProtocolError(
					serverCode,
					response.status,
					message ?? `SQL request failed with HTTP ${response.status}`,
					queryId === 'unknown' ? undefined : queryId,
					outcome,
					metadata
				);
			}
			return new KitError(message ?? `SQL request failed with HTTP ${response.status}`);
	}
}
