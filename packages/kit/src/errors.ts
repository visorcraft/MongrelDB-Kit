export type KitErrorCode =
	| 'STORAGE'
	| 'VALIDATION'
	| 'NOT_FOUND'
	| 'DUPLICATE'
	| 'FOREIGN_KEY'
	| 'RESTRICT'
	| 'CONFLICT'
	| 'TRIGGER_VALIDATION'
	| 'MIGRATION'
	| 'SCHEMA_DRIFT'
	| 'TIMEOUT'
	| 'UNSUPPORTED'
	| 'INTEGRITY'
	| 'AUTH_REQUIRED'
	| 'AUTH_NOT_REQUIRED'
	| 'INVALID_CREDENTIALS'
	| 'PERMISSION_DENIED'
	| 'QUERY_CANCELLED'
	| 'DEADLINE_EXCEEDED'
	| 'QUERY_ID_CONFLICT'
	| 'QUERY_REGISTRY_FULL'
	| 'QUERY_FAILED'
	| 'REMOTE_PROTOCOL'
	| 'CANCEL_TOO_LATE'
	| 'QUERY_ALREADY_FINISHED'
	| 'QUERY_NOT_FOUND'
	| 'TRANSACTION_ABORTED'
	| 'COMMIT_OUTCOME'
	| 'QUERY_CANCELLED_AFTER_COMMIT'
	| 'DEADLINE_AFTER_COMMIT'
	| 'RESULT_LIMIT_EXCEEDED'
	| 'SERIALIZATION_FAILED'
	| 'SERIALIZATION_FAILED_AFTER_COMMIT'
	| 'CAPABILITY_UNSUPPORTED'
	| 'QUERY_OUTCOME_UNKNOWN';

export interface DurableQueryOutcome {
	committed: boolean | null;
	committedStatements?: number;
	lastCommitEpoch?: bigint;
	firstCommitStatementIndex?: number;
	lastCommitStatementIndex?: number;
	completedStatements?: number;
	statementIndex?: number;
}

export interface SqlErrorMetadata {
	cancelOutcome?: string;
	cancellationReason?: string;
	retryable?: boolean;
	serverState?: string;
}

export class KitError extends Error {
	readonly code: KitErrorCode;

	constructor(message: string, code: KitErrorCode = 'STORAGE') {
		super(message);
		this.name = 'KitError';
		this.code = code;
	}
}

export class KitValidationError extends KitError {
	table?: string;
	column?: string;

	constructor(message: string, table?: string, column?: string) {
		super(message, 'VALIDATION');
		this.name = 'KitValidationError';
		this.table = table;
		this.column = column;
	}
}

export class KitNotFoundError extends KitError {
	table: string;
	pk: unknown;

	constructor(table: string, pk: unknown) {
		super(`${table}(${String(pk)}) not found`, 'NOT_FOUND');
		this.name = 'KitNotFoundError';
		this.table = table;
		this.pk = pk;
	}
}

export class KitDuplicateError extends KitError {
	table: string;
	constraint: string;

	constructor(table: string, constraint: string) {
		super(`Duplicate in ${table} for ${constraint}`, 'DUPLICATE');
		this.name = 'KitDuplicateError';
		this.table = table;
		this.constraint = constraint;
	}
}

export class KitForeignKeyError extends KitError {
	table: string;
	constraint: string;

	constructor(table: string, constraint: string) {
		super(`Foreign key violation in ${table} for ${constraint}`, 'FOREIGN_KEY');
		this.name = 'KitForeignKeyError';
		this.table = table;
		this.constraint = constraint;
	}
}

export class KitRestrictError extends KitError {
	table: string;
	constraint: string;

	constructor(table: string, constraint: string) {
		super(`Restrict violation in ${table} for ${constraint}`, 'RESTRICT');
		this.name = 'KitRestrictError';
		this.table = table;
		this.constraint = constraint;
	}
}

export class KitConflictError extends KitError {
	retryable = true;

	constructor(message = 'Conflict') {
		super(message, 'CONFLICT');
		this.name = 'KitConflictError';
	}
}

export class KitTriggerValidationError extends KitError {
	constructor(message: string) {
		super(message, 'TRIGGER_VALIDATION');
		this.name = 'KitTriggerValidationError';
	}
}

export class KitMigrationError extends KitError {
	constructor(message: string) {
		super(message, 'MIGRATION');
		this.name = 'KitMigrationError';
	}
}

export class KitSchemaDriftError extends KitError {
	constructor(message: string) {
		super(message, 'SCHEMA_DRIFT');
		this.name = 'KitSchemaDriftError';
	}
}

export class KitTimeoutError extends KitError {
	constructor(message = 'Transaction timed out') {
		super(message, 'TIMEOUT');
		this.name = 'KitTimeoutError';
	}
}

export class KitUnsupportedError extends KitError {
	constructor(message: string, code: 'UNSUPPORTED' | 'CAPABILITY_UNSUPPORTED' = 'UNSUPPORTED') {
		super(message, code);
		this.name = 'KitUnsupportedError';
	}
}

export class CapabilityUnsupportedError extends KitUnsupportedError {
	constructor(message: string) {
		super(message, 'CAPABILITY_UNSUPPORTED');
		this.name = 'CapabilityUnsupportedError';
	}
}

export class QueryCancelledError extends KitError {
	readonly queryId: string;
	readonly committed: boolean | null;
	readonly committedStatements?: number;
	readonly lastCommitEpoch?: bigint;
	readonly firstCommitStatementIndex?: number;
	readonly lastCommitStatementIndex?: number;
	readonly completedStatements?: number;
	readonly statementIndex?: number;
	readonly cancelOutcome?: string;
	readonly cancellationReason?: string;
	readonly retryable?: boolean;
	readonly serverState?: string;

	constructor(queryId: string, message = 'SQL query cancelled', outcome: DurableQueryOutcome = { committed: false }, metadata: SqlErrorMetadata = {}) {
		super(message, outcome.committed ? 'QUERY_CANCELLED_AFTER_COMMIT' : 'QUERY_CANCELLED');
		this.name = 'QueryCancelledError';
		this.queryId = queryId;
		this.committed = outcome.committed;
		this.committedStatements = outcome.committedStatements;
		this.lastCommitEpoch = outcome.lastCommitEpoch;
		this.firstCommitStatementIndex = outcome.firstCommitStatementIndex;
		this.lastCommitStatementIndex = outcome.lastCommitStatementIndex;
		this.completedStatements = outcome.completedStatements;
		this.statementIndex = outcome.statementIndex;
		this.cancelOutcome = metadata.cancelOutcome;
		this.cancellationReason = metadata.cancellationReason;
		this.retryable = metadata.retryable;
		this.serverState = metadata.serverState;
	}
}

export class QueryTimeoutError extends KitError {
	readonly queryId: string;
	readonly committed: boolean | null;
	readonly committedStatements?: number;
	readonly lastCommitEpoch?: bigint;
	readonly firstCommitStatementIndex?: number;
	readonly lastCommitStatementIndex?: number;
	readonly completedStatements?: number;
	readonly statementIndex?: number;
	readonly cancelOutcome?: string;
	readonly cancellationReason?: string;
	readonly retryable?: boolean;
	readonly serverState?: string;

	constructor(queryId: string, message = 'SQL query deadline exceeded', outcome: DurableQueryOutcome = { committed: false }, metadata: SqlErrorMetadata = {}) {
		super(message, outcome.committed ? 'DEADLINE_AFTER_COMMIT' : 'DEADLINE_EXCEEDED');
		this.name = 'QueryTimeoutError';
		this.queryId = queryId;
		this.committed = outcome.committed;
		this.committedStatements = outcome.committedStatements;
		this.lastCommitEpoch = outcome.lastCommitEpoch;
		this.firstCommitStatementIndex = outcome.firstCommitStatementIndex;
		this.lastCommitStatementIndex = outcome.lastCommitStatementIndex;
		this.completedStatements = outcome.completedStatements;
		this.statementIndex = outcome.statementIndex;
		this.cancelOutcome = metadata.cancelOutcome;
		this.cancellationReason = metadata.cancellationReason;
		this.retryable = metadata.retryable;
		this.serverState = metadata.serverState;
	}
}

export class CommitOutcomeError extends KitError {
	readonly queryId: string;
	readonly committed: boolean | null;
	readonly committedStatements?: number;
	readonly lastCommitEpoch?: bigint;
	readonly firstCommitStatementIndex?: number;
	readonly lastCommitStatementIndex?: number;
	readonly completedStatements?: number;
	readonly statementIndex?: number;
	readonly cancelOutcome?: string;
	readonly cancellationReason?: string;
	readonly retryable?: boolean;
	readonly serverState?: string;

	constructor(queryId: string, message: string, outcome: DurableQueryOutcome, metadata: SqlErrorMetadata = {}) {
		super(message, 'COMMIT_OUTCOME');
		this.name = 'CommitOutcomeError';
		this.queryId = queryId;
		this.committed = outcome.committed;
		this.committedStatements = outcome.committedStatements;
		this.lastCommitEpoch = outcome.lastCommitEpoch;
		this.firstCommitStatementIndex = outcome.firstCommitStatementIndex;
		this.lastCommitStatementIndex = outcome.lastCommitStatementIndex;
		this.completedStatements = outcome.completedStatements;
		this.statementIndex = outcome.statementIndex;
		this.cancelOutcome = metadata.cancelOutcome;
		this.cancellationReason = metadata.cancellationReason;
		this.retryable = metadata.retryable;
		this.serverState = metadata.serverState;
	}
}

export class QueryExecutionError extends KitError {
	readonly queryId: string;
	readonly terminalCode: string;
	readonly committed: boolean | null;
	readonly committedStatements?: number;
	readonly lastCommitEpoch?: bigint;
	readonly firstCommitStatementIndex?: number;
	readonly lastCommitStatementIndex?: number;
	readonly completedStatements?: number;
	readonly statementIndex?: number;
	readonly cancelOutcome?: string;
	readonly cancellationReason?: string;
	readonly retryable?: boolean;
	readonly serverState?: string;

	constructor(queryId: string, terminalCode: string, message: string, outcome: DurableQueryOutcome, metadata: SqlErrorMetadata = {}) {
		super(message, 'QUERY_FAILED');
		this.name = 'QueryExecutionError';
		this.queryId = queryId;
		this.terminalCode = terminalCode;
		this.committed = outcome.committed;
		this.committedStatements = outcome.committedStatements;
		this.lastCommitEpoch = outcome.lastCommitEpoch;
		this.firstCommitStatementIndex = outcome.firstCommitStatementIndex;
		this.lastCommitStatementIndex = outcome.lastCommitStatementIndex;
		this.completedStatements = outcome.completedStatements;
		this.statementIndex = outcome.statementIndex;
		this.cancelOutcome = metadata.cancelOutcome;
		this.cancellationReason = metadata.cancellationReason;
		this.retryable = metadata.retryable;
		this.serverState = metadata.serverState;
	}
}

export class RemoteProtocolError extends KitError {
	readonly serverCode: string;
	readonly httpStatus: number;
	readonly queryId?: string;
	readonly durableOutcome: DurableQueryOutcome;
	readonly metadata: SqlErrorMetadata;

	constructor(serverCode: string, httpStatus: number, message: string, queryId: string | undefined, outcome: DurableQueryOutcome, metadata: SqlErrorMetadata = {}) {
		super(message, 'REMOTE_PROTOCOL');
		this.name = 'RemoteProtocolError';
		this.serverCode = serverCode;
		this.httpStatus = httpStatus;
		this.queryId = queryId;
		this.durableOutcome = outcome;
		this.metadata = metadata;
	}
}

export class ResultLimitExceededError extends KitError {
	readonly queryId: string;
	readonly committed: boolean | null;
	readonly committedStatements?: number;
	readonly lastCommitEpoch?: bigint;
	readonly firstCommitStatementIndex?: number;
	readonly lastCommitStatementIndex?: number;
	readonly completedStatements?: number;
	readonly statementIndex?: number;
	readonly cancelOutcome?: string;
	readonly cancellationReason?: string;
	readonly retryable?: boolean;
	readonly serverState?: string;

	constructor(queryId: string, message: string, outcome: DurableQueryOutcome, metadata: SqlErrorMetadata = {}) {
		super(message, 'RESULT_LIMIT_EXCEEDED');
		this.name = 'ResultLimitExceededError';
		this.queryId = queryId;
		this.committed = outcome.committed;
		this.committedStatements = outcome.committedStatements;
		this.lastCommitEpoch = outcome.lastCommitEpoch;
		this.firstCommitStatementIndex = outcome.firstCommitStatementIndex;
		this.lastCommitStatementIndex = outcome.lastCommitStatementIndex;
		this.completedStatements = outcome.completedStatements;
		this.statementIndex = outcome.statementIndex;
		this.cancelOutcome = metadata.cancelOutcome;
		this.cancellationReason = metadata.cancellationReason;
		this.retryable = metadata.retryable;
		this.serverState = metadata.serverState;
	}
}

export class SerializationError extends KitError {
	readonly queryId: string;
	readonly committed: boolean | null;
	readonly committedStatements?: number;
	readonly lastCommitEpoch?: bigint;
	readonly firstCommitStatementIndex?: number;
	readonly lastCommitStatementIndex?: number;
	readonly completedStatements?: number;
	readonly statementIndex?: number;
	readonly cancelOutcome?: string;
	readonly cancellationReason?: string;
	readonly retryable?: boolean;
	readonly serverState?: string;

	constructor(queryId: string, message: string, outcome: DurableQueryOutcome, metadata: SqlErrorMetadata = {}) {
		super(message, outcome.committed ? 'SERIALIZATION_FAILED_AFTER_COMMIT' : 'SERIALIZATION_FAILED');
		this.name = 'SerializationError';
		this.queryId = queryId;
		this.committed = outcome.committed;
		this.committedStatements = outcome.committedStatements;
		this.lastCommitEpoch = outcome.lastCommitEpoch;
		this.firstCommitStatementIndex = outcome.firstCommitStatementIndex;
		this.lastCommitStatementIndex = outcome.lastCommitStatementIndex;
		this.completedStatements = outcome.completedStatements;
		this.statementIndex = outcome.statementIndex;
		this.cancelOutcome = metadata.cancelOutcome;
		this.cancellationReason = metadata.cancellationReason;
		this.retryable = metadata.retryable;
		this.serverState = metadata.serverState;
	}
}

export class QueryOutcomeUnknownError extends KitError {
	readonly queryId: string;
	readonly committed = null;
	readonly committedStatements = null;
	readonly lastCommitEpoch = null;
	readonly firstCommitStatementIndex = null;
	readonly lastCommitStatementIndex = null;
	readonly completedStatements = null;
	readonly statementIndex = null;
	readonly cancelOutcome?: string;
	readonly cancellationReason?: string;
	readonly retryable?: boolean;
	readonly serverState?: string;

	constructor(queryId: string, message = 'SQL query outcome is unknown', metadata: SqlErrorMetadata = {}) {
		super(message, 'QUERY_OUTCOME_UNKNOWN');
		this.name = 'QueryOutcomeUnknownError';
		this.queryId = queryId;
		this.cancelOutcome = metadata.cancelOutcome;
		this.cancellationReason = metadata.cancellationReason;
		this.retryable = metadata.retryable;
		this.serverState = metadata.serverState;
	}
}

export class QueryIdConflictError extends KitError {
	readonly queryId: string;

	constructor(queryId: string, message = 'SQL query ID is already active') {
		super(message, 'QUERY_ID_CONFLICT');
		this.name = 'QueryIdConflictError';
		this.queryId = queryId;
	}
}

export class TransactionAbortedError extends KitError {
	constructor(message = 'SQL transaction is aborted; only ROLLBACK is allowed') {
		super(message, 'TRANSACTION_ABORTED');
		this.name = 'TransactionAbortedError';
	}
}

/** Thrown when a `require_auth` database is opened without credentials, or an
 * operation runs on a handle with no cached principal. HTTP 401 equivalent. */
export class KitAuthRequiredError extends KitError {
	constructor(message: string) {
		super(message, 'AUTH_REQUIRED');
		this.name = 'KitAuthRequiredError';
	}
}

/** Thrown when a credentialed constructor is used on a credentialless database
 * (the caller picked the wrong constructor). */
export class KitAuthNotRequiredError extends KitError {
	constructor(message: string) {
		super(message, 'AUTH_NOT_REQUIRED');
		this.name = 'KitAuthNotRequiredError';
	}
}

/** Thrown when `openWithCredentials` verification fails (bad username/password).
 * HTTP 401 equivalent. */
export class KitInvalidCredentialsError extends KitError {
	readonly username: string;

	constructor(username: string) {
		super(`invalid credentials for user "${username}"`, 'INVALID_CREDENTIALS');
		this.name = 'KitInvalidCredentialsError';
		this.username = username;
	}
}

/** Thrown when an operation's required permission is not satisfied by the
 * cached principal. HTTP 403 equivalent. */
export class KitPermissionDeniedError extends KitError {
	constructor(message: string) {
		super(message, 'PERMISSION_DENIED');
		this.name = 'KitPermissionDeniedError';
	}
}

/**
 * Returns true when the underlying error is a retryable MongrelDB write-write
 * conflict. The native addon prefixes commit-time conflict errors with
 * `__CONFLICT__:` so callers can detect them without instanceof checks against
 * addon-owned classes that may not cross the require boundary cleanly.
 */
export function isRetryableConflict(err: unknown): boolean {
	if (err instanceof KitConflictError) return true;
	if (err == null || typeof err !== 'object' || !('message' in err)) return false;
	const msg = (err as { message?: unknown }).message;
	return typeof msg === 'string' && msg.startsWith('__CONFLICT__:');
}
