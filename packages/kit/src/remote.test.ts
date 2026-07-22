import { describe, it, expect } from 'vitest';
import { Worker } from 'node:worker_threads';
import type { IncomingHttpHeaders } from 'node:http';
import { inspect } from 'node:util';
import { RemoteDatabase } from './remote.js';
import {
	CommitOutcomeError,
	KitUnsupportedError,
	QueryCancelledError,
	QueryExecutionError,
	QueryOutcomeUnknownError,
	QueryTimeoutError,
	RemoteProtocolError,
	ResultLimitExceededError,
	SerializationError
} from './errors.js';

type RequestRecord = {
	method?: string;
	path?: string;
	headers: IncomingHttpHeaders;
	body: string;
};

const WORKER_SCRIPT = `
const { parentPort, workerData } = require('node:worker_threads');
const http = require('http');

const mode = workerData || 'success';
const requests = [];
let statusCalls = 0;
let sqlCalls = 0;
let capabilityCalls = 0;
let originalQueryId;
const cancelledQueries = new Set();

const queryNotFound = (queryId) => {
	const nullable = {
		committed: null,
		committed_statements: null,
		last_commit_epoch: null,
		last_commit_epoch_text: null,
		first_commit_statement_index: null,
		last_commit_statement_index: null,
		completed_statements: null,
		statement_index: null
	};
	return {
		query_id: queryId,
		status: 'unknown',
		terminal_state: null,
		...nullable,
		cancel_outcome: 'not_found',
		cancellation_reason: null,
		retryable: false,
		server_state: 'not_found',
		outcome: { ...nullable, serialization: 'unknown' },
		error: {
			code: 'QUERY_NOT_FOUND',
			message: 'query not found',
			query_id: queryId,
			committed: null,
			retryable: false
		}
	};
};

const server = http.createServer((req, res) => {
	let body = '';
	req.setEncoding('utf8');
	req.on('data', (chunk) => {
		body += chunk;
	});
	req.on('end', () => {
		requests.push({ method: req.method, path: req.url, headers: req.headers, body });
		if (req.url === '/sql') {
			const request = JSON.parse(body);
			if (!['missing_query_header', 'page_missing_query_header',
				'idempotency_missing_query_header'].includes(mode)) {
				res.setHeader(
					'x-mongreldb-query-id',
					mode === 'wrong_query_header'
						? '99990000111122223333444455556666'
						: request.query_id
				);
			}
		}
		const expectedAuth = mode === 'basic_auth'
			? 'Basic YWxpY2U6c2VjcmV0'
			: 'Bearer secret';
		if ((mode === 'auth' || mode === 'basic_auth')
			&& req.headers.authorization !== expectedAuth) {
			res.writeHead(401, { 'content-type': 'application/json' });
			res.end(JSON.stringify({ error: { code: 'UNAUTHORIZED', message: 'missing auth' } }));
			return;
		}
		if (mode === 'error') {
			res.writeHead(503, { 'content-type': 'application/json' });
			res.end(JSON.stringify({ error: 'service unavailable' }));
			return;
		}
		if ((mode === 'recovery_hangs' || mode === 'cancel_hangs')
			&& req.url.startsWith('/queries/')) {
			return;
		} else if ((mode === 'outcome_unknown' || mode === 'invalid_receipt'
			|| mode === 'crossed_sql_error')
			&& req.url.startsWith('/queries/')
			&& !req.url.endsWith('/cancel')) {
			const queryId = req.url.split('/')[2];
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: queryId,
				status: 'outcome_unknown',
				terminal_state: 'outcome_unknown',
				state: 'failed',
				server_state: 'failed',
				operation: 'sql',
				committed: null,
				committed_statements: null,
				last_commit_epoch: null,
				last_commit_epoch_text: null,
				first_commit_statement_index: null,
				last_commit_statement_index: null,
				completed_statements: null,
				statement_index: null,
				cancel_outcome: 'already_finished',
				cancellation_reason: 'none',
				retryable: false,
				outcome: {
					committed: null,
					committed_statements: null,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: null,
					statement_index: null,
					serialization: 'unknown'
				},
				terminal_error: { code: 'QUERY_OUTCOME_UNKNOWN', category: 'execution' }
			}));
		} else if ((mode === 'invalid_status_query_id' || mode === 'invalid_status_commit'
			|| mode === 'invalid_status_unknown' || mode === 'invalid_status_trace')
			&& req.url.startsWith('/queries/') && !req.url.endsWith('/cancel')) {
			const queryId = req.url.split('/')[2];
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: mode === 'invalid_status_query_id'
					? '11111111111111111111111111111111'
					: queryId,
				status: mode === 'invalid_status_commit' ? 'completed' : 'committed',
				terminal_state: 'committed',
				state: 'completed',
				server_state: 'completed',
				...(mode === 'invalid_status_unknown' ? { unexpected: true } : {}),
				trace: mode === 'invalid_status_trace' ? {
					queue_duration_us: 1,
					planning_duration_us: 2,
					execution_duration_us: 9007199254740992,
					serialization_duration_us: 4,
					cancel_requested_phase: null,
					cancel_observed_phase: null,
					commit_fence_outcome: 'commit_won'
				} : undefined,
				committed: true,
				committed_statements: 1,
				last_commit_epoch: 17,
				last_commit_epoch_text: '17',
				first_commit_statement_index: 0,
				last_commit_statement_index: 0,
				completed_statements: 1,
				statement_index: 0,
				cancel_outcome: 'already_finished',
				cancellation_reason: 'none',
				retryable: false,
				outcome: {
					committed: true,
					committed_statements: 1,
					last_commit_epoch: 17,
					last_commit_epoch_text: '17',
					first_commit_statement_index: 0,
					last_commit_statement_index: 0,
					completed_statements: 1,
					statement_index: 0,
					serialization: 'succeeded'
				},
				terminal_error: null
			}));
		} else if (mode === 'cancelling_deadline'
			&& req.url.startsWith('/queries/') && !req.url.endsWith('/cancel')) {
			const queryId = req.url.split('/')[2];
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: queryId,
				status: 'running',
				terminal_state: null,
				state: 'cancelling',
				server_state: 'cancelling',
				operation: 'SELECT',
				committed: false,
				committed_statements: 0,
				last_commit_epoch: null,
				last_commit_epoch_text: null,
				first_commit_statement_index: null,
				last_commit_statement_index: null,
				completed_statements: 0,
				statement_index: 0,
				cancel_outcome: 'accepted',
				cancellation_reason: 'deadline',
				retryable: false,
				outcome: {
					committed: false,
					committed_statements: 0,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: 0,
					statement_index: 0,
					serialization: 'in_progress'
				},
				terminal_error: null
			}));
		} else if (mode === 'compact_status'
			&& req.url.startsWith('/queries/') && !req.url.endsWith('/cancel')) {
			const queryId = req.url.split('/')[2];
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: queryId,
				detail: 'compact',
				status: 'finished',
				terminal_state: null,
				state: 'finished',
				server_state: 'finished',
				operation: '',
				committed: null,
				committed_statements: null,
				last_commit_epoch: null,
				last_commit_epoch_text: null,
				first_commit_statement_index: null,
				last_commit_statement_index: null,
				completed_statements: null,
				statement_index: null,
				cancel_outcome: 'already_finished',
				cancellation_reason: 'none',
				retryable: false,
				outcome: {
					committed: null,
					committed_statements: null,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: null,
					statement_index: null,
					serialization: 'unknown'
				},
				terminal_error: null
			}));
		} else if ((mode === 'malformed_page' || mode === 'invalid_page'
			|| mode === 'missing_query_header' || mode === 'wrong_query_header'
			|| mode === 'page_missing_query_header')
			&& req.url.startsWith('/queries/')
			&& !req.url.endsWith('/cancel')) {
			const queryId = req.url.split('/')[2];
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: queryId,
				status: 'completed',
				terminal_state: 'completed',
				state: 'completed',
				server_state: 'completed',
				committed: false,
				committed_statements: 0,
				last_commit_epoch: null,
				last_commit_epoch_text: null,
				first_commit_statement_index: null,
				last_commit_statement_index: null,
				completed_statements: 1,
				statement_index: 0,
				cancel_outcome: 'already_finished',
				cancellation_reason: 'none',
				retryable: false,
				outcome: {
					committed: false,
					committed_statements: 0,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: 1,
					statement_index: 0,
					serialization: 'succeeded'
				},
				terminal_error: null
			}));
		} else if (mode === 'committed_serializing'
			&& req.url.startsWith('/queries/') && !req.url.endsWith('/cancel')) {
			const queryId = req.url.split('/')[2];
			const hlc = { physical_micros: 1_700_000_000_000_000, logical: 3, node_tiebreaker: 7 };
			const outcome = {
				committed: true, committed_statements: 1,
				last_commit_epoch: 17, last_commit_epoch_text: '17',
				last_commit_hlc: hlc,
				first_commit_statement_index: 0, last_commit_statement_index: 0,
				completed_statements: 1, statement_index: 0,
				serialization: 'in_progress',
				serialization_state: 'in_progress'
			};
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: queryId,
				status: 'committed', terminal_state: null,
				state: 'serializing', server_state: 'serializing', operation: 'INSERT',
				committed: true, committed_statements: 1,
				last_commit_epoch: 17, last_commit_epoch_text: '17',
				last_commit_hlc: hlc,
				first_commit_statement_index: 0, last_commit_statement_index: 0,
				completed_statements: 1, statement_index: 0,
				cancel_outcome: null, cancellation_reason: 'none', retryable: false,
				outcome,
				durable: outcome,
				terminal_error: null, trace: {}
			}));
		} else if (req.url.startsWith('/queries/') && !req.url.endsWith('/cancel')
			&& cancelledQueries.has(req.url.split('/')[2])) {
			const queryId = req.url.split('/')[2];
			const committed = mode === 'cancel_partial_commit';
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: queryId,
				status: committed ? 'cancelled_after_commit' : 'cancelled_before_commit',
				terminal_state: committed ? 'cancelled_after_commit' : 'cancelled_before_commit',
				state: 'cancelled',
				server_state: 'cancelled',
				operation: 'sql',
				committed,
				committed_statements: committed ? 1 : 0,
				last_commit_epoch: committed ? 17 : null,
				last_commit_epoch_text: committed ? '17' : null,
				first_commit_statement_index: committed ? 0 : null,
				last_commit_statement_index: committed ? 0 : null,
				completed_statements: committed ? 1 : 0,
				statement_index: committed ? 1 : 0,
				cancel_outcome: 'already_finished',
				cancellation_reason: 'client_request',
				retryable: false,
				outcome: {
					committed,
					committed_statements: committed ? 1 : 0,
					last_commit_epoch: committed ? 17 : null,
					last_commit_epoch_text: committed ? '17' : null,
					first_commit_statement_index: committed ? 0 : null,
					last_commit_statement_index: committed ? 0 : null,
					completed_statements: committed ? 1 : 0,
					statement_index: committed ? 1 : 0,
					serialization: 'failed'
				},
				terminal_error: {
					code: committed ? 'QUERY_CANCELLED_AFTER_COMMIT' : 'QUERY_CANCELLED',
					category: 'cancellation'
				}
			}));
		} else if (req.url.startsWith('/queries/') && !req.url.endsWith('/cancel')
			&& (mode === 'transport_failed' || mode === 'transport_pre_cancelled')) {
			const queryId = req.url.split('/')[2];
			const preCancelled = mode === 'transport_pre_cancelled';
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: queryId,
				status: preCancelled ? 'cancelled_before_start' : 'failed_before_commit',
				terminal_state: preCancelled ? 'cancelled_before_start' : 'failed_before_commit',
				state: preCancelled ? 'pre_cancelled' : 'failed',
				server_state: preCancelled ? 'pre_cancelled' : 'failed',
				committed: false,
				committed_statements: 0,
				last_commit_epoch: null,
				last_commit_epoch_text: null,
				first_commit_statement_index: null,
				last_commit_statement_index: null,
				completed_statements: 0,
				statement_index: 0,
				cancel_outcome: preCancelled ? 'pre_cancelled' : 'already_finished',
				cancellation_reason: preCancelled ? 'client_request' : 'none',
				retryable: false,
				outcome: {
					committed: false,
					committed_statements: 0,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: 0,
					statement_index: 0,
					serialization: preCancelled ? 'not_started' : 'failed'
				},
				terminal_error: {
					code: preCancelled ? 'QUERY_CANCELLED' : 'QUERY_FAILED',
					category: preCancelled ? 'cancellation' : 'execution'
				}
			}));
		} else if (req.url.startsWith('/queries/') && !req.url.endsWith('/cancel')
			&& (mode === 'transport_committed' || mode === 'invalid_body_committed'
				|| mode === 'ordinary_body_committed')) {
			statusCalls += 1;
			const queryId = req.url.split('/')[2];
			const terminal = statusCalls > 1;
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({
				query_id: queryId,
				status: terminal ? 'committed' : 'running',
				terminal_state: terminal ? 'committed' : null,
				state: terminal ? 'completed' : 'executing',
				server_state: terminal ? 'completed' : 'executing',
				operation: 'INSERT',
				started_ms_ago: 12,
				deadline_ms_remaining: null,
				session_id: null,
				committed: terminal,
				committed_statements: terminal ? 1 : 0,
				last_commit_epoch: terminal ? 17 : null,
				last_commit_epoch_text: terminal ? '17' : null,
				first_commit_statement_index: terminal ? 0 : null,
				last_commit_statement_index: terminal ? 0 : null,
				completed_statements: terminal ? 1 : 0,
				statement_index: 0,
				cancel_outcome: terminal ? 'already_finished' : null,
				cancellation_reason: 'none',
				retryable: false,
				outcome: terminal
					? { committed: true, committed_statements: 1, last_commit_epoch: 17, last_commit_epoch_text: '17', first_commit_statement_index: 0, last_commit_statement_index: 0, completed_statements: 1, statement_index: 0, serialization: 'succeeded' }
					: { committed: false, committed_statements: 0, last_commit_epoch: null, last_commit_epoch_text: null, first_commit_statement_index: null, last_commit_statement_index: null, completed_statements: 0, statement_index: 0, serialization: 'in_progress' },
				terminal_error: null,
				trace: {
					queue_duration_us: 1,
					planning_duration_us: 2,
					execution_duration_us: 3,
					serialization_duration_us: 4,
					cancel_requested_phase: null,
					cancel_observed_phase: null,
					commit_fence_outcome: terminal ? 'commit_won' : 'not_reached'
				}
			}));
		} else if (req.url.startsWith('/queries/') && !req.url.endsWith('/cancel')
			&& (mode === 'idempotency_restart_replay'
				|| mode === 'idempotency_restart_fresh_execution'
				|| mode === 'idempotency_restart_downgrade'
				|| mode === 'idempotency_wrong_original'
				|| mode === 'idempotency_malformed_not_found')) {
			const queryId = req.url.split('/')[2];
			res.writeHead(404, { 'content-type': 'application/json' });
			res.end(JSON.stringify(mode === 'idempotency_malformed_not_found'
				? {}
				: queryNotFound(queryId)));
		} else if (req.url === '/history/retention') {
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({ history_retention_epochs: 42, earliest_retained_epoch: 5 }));
		} else if (req.url === '/capabilities' && mode !== 'old') {
			capabilityCalls += 1;
			const capabilities = {
				sql_cancellation: { version: 2, client_query_ids: true, cancel_endpoint: true, query_status: true, stream_disconnect_cancels: true, pre_registration_cancel: true },
				sql_pagination: { version: 1, continuation_endpoint: '/sql/continue', retained_snapshot: true, projection_required: true, byte_and_token_hints: true },
				sql_idempotency: { version: 1, durable_pre_execution_intent: true, replay_committed_receipt: true, indeterminate_never_reexecutes: true }
			};
			if (mode === 'capability_unknown') capabilities.sql_cancellation.unexpected = true;
			if (mode === 'capability_unsafe') capabilities.sql_cancellation.version = 9007199254740992;
			if (mode === 'idempotency_restart_downgrade' && capabilityCalls > 1) {
				delete capabilities.sql_idempotency;
			}
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(mode === 'capability_oversized'
				? JSON.stringify({ ...capabilities, padding: 'x'.repeat(1024 * 1024) })
				: JSON.stringify(capabilities));
		} else if (req.url === '/sql/continue'
			&& (mode === 'cursor_error' || mode === 'cursor_error_conflict')) {
			const operationId = JSON.parse(body).operation_id;
			const error = {
				query_id: operationId,
				status: 'failed_before_commit',
				terminal_state: 'failed_before_commit',
				server_state: 'failed',
				committed: false,
				committed_statements: 0,
				last_commit_epoch: null,
				last_commit_epoch_text: null,
				first_commit_statement_index: null,
				last_commit_statement_index: null,
				completed_statements: 0,
				statement_index: 0,
				cancel_outcome: 'already_finished',
				cancellation_reason: 'none',
				retryable: false,
				outcome: {
					committed: false,
					committed_statements: 0,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: 0,
					statement_index: 0,
					serialization: 'not_started'
				},
				error: {
					code: 'SQL_CURSOR_NOT_FOUND',
					message: 'cursor missing',
					query_id: operationId,
					committed: mode === 'cursor_error_conflict',
					retryable: false
				}
			};
			res.writeHead(404, {
				'content-type': 'application/json',
				'x-mongreldb-query-id': operationId
			});
			res.end(JSON.stringify(error));
		} else if (req.url === '/sql/continue' && mode === 'malformed_page') {
			const operationId = JSON.parse(body).operation_id;
			res.writeHead(200, {
				'content-type': 'application/json',
				'x-mongreldb-query-id': operationId
			});
			res.end(JSON.stringify({ status: 'completed', rows: [] }));
		} else if (req.url === '/sql/continue') {
			const operationId = JSON.parse(body).operation_id;
			res.writeHead(200, {
				'content-type': 'application/json',
				'x-mongreldb-query-id': operationId
			});
			res.end(JSON.stringify({
				status: 'completed', rows: [{ id: 2 }], next_cursor: null,
				page: { offset: 1, row_count: 1, total_rows: 2, byte_count: 10, estimated_tokens: 3, limits: { rows: 1, bytes: 1024, tokens: 256 }, projection: ['id'], expires_at_ms: 999, snapshot: 'retained_result', token_estimate: 'ceil(projected_json_bytes/4)' }
			}));
		} else if (req.url === '/sql') {
			const request = JSON.parse(body);
			if (mode === 'idempotency_restart_replay'
				|| mode === 'idempotency_restart_fresh_execution'
				|| mode === 'idempotency_restart_downgrade'
				|| mode === 'idempotency_wrong_original'
				|| mode === 'idempotency_malformed_not_found') {
				sqlCalls += 1;
				if (sqlCalls === 1) {
					originalQueryId = request.query_id;
					res.destroy();
				} else {
					res.writeHead(200, { 'content-type': 'application/json' });
					res.end(JSON.stringify({
						query_id: request.query_id,
						original_query_id: mode === 'idempotency_wrong_original'
							? '99990000111122223333444455556666'
							: mode === 'idempotency_restart_fresh_execution'
								? request.query_id
								: originalQueryId,
						status: 'committed',
						committed: true,
						committed_statements: 1,
						last_commit_epoch: 29,
						last_commit_epoch_text: '29',
						first_commit_statement_index: 0,
						last_commit_statement_index: 0,
						completed_statements: 1,
						statement_index: 0,
						retryable: false,
						idempotency_replayed: mode !== 'idempotency_restart_fresh_execution',
						idempotency_persisted: true,
						idempotency_expires_at_ms: 999,
						outcome: {
							committed: true,
							committed_statements: 1,
							last_commit_epoch: 29,
							last_commit_epoch_text: '29',
							first_commit_statement_index: 0,
							last_commit_statement_index: 0,
							completed_statements: 1,
							statement_index: 0,
							serialization: 'succeeded'
						},
						terminal_error: null
					}));
				}
			} else if (mode === 'transport_failed' || mode === 'transport_pre_cancelled') {
				res.destroy();
			} else if (request.idempotency_key && mode === 'invalid_receipt') {
				const receipt = {
					query_id: request.query_id,
					original_query_id: request.query_id,
					status: 'committed',
					committed: true,
					committed_statements: 1,
					last_commit_epoch: 17,
					last_commit_epoch_text: '17',
					first_commit_statement_index: 0,
					last_commit_statement_index: 0,
					completed_statements: 1,
					statement_index: 0,
					retryable: false,
					idempotency_replayed: false,
					idempotency_persisted: true,
					idempotency_expires_at_ms: 999,
					outcome: {
						committed: true,
						committed_statements: 1,
						last_commit_epoch: 17,
						last_commit_epoch_text: '17',
						first_commit_statement_index: 0,
						last_commit_statement_index: 0,
						completed_statements: 1,
						statement_index: 0,
						serialization: 'succeeded'
					},
					terminal_error: null
				};
				switch (request.sql) {
					case 'numeric-exact': receipt.last_commit_epoch_text = '18'; break;
					case 'epoch-disagreement':
						receipt.outcome.last_commit_epoch = 18;
						receipt.outcome.last_commit_epoch_text = '18';
						break;
					case 'unsafe-numeric':
						receipt.last_commit_epoch = 9007199254740992;
						receipt.last_commit_epoch_text = '9007199254740992';
						receipt.outcome.last_commit_epoch = 9007199254740992;
						receipt.outcome.last_commit_epoch_text = '9007199254740992';
						break;
					case 'overflow-epoch':
						receipt.last_commit_epoch = null;
						receipt.last_commit_epoch_text = '18446744073709551616';
						receipt.outcome.last_commit_epoch = null;
						receipt.outcome.last_commit_epoch_text = '18446744073709551616';
						break;
					case 'missing-outcome-field':
						delete receipt.outcome.last_commit_epoch;
						break;
					case 'reversed-index':
						receipt.first_commit_statement_index = 1;
						receipt.last_commit_statement_index = 0;
						receipt.outcome.first_commit_statement_index = 1;
						break;
					case 'count-disagreement': receipt.outcome.completed_statements = 0; break;
					case 'empty-serialization': receipt.outcome.serialization = ''; break;
					case 'invalid-serialization': receipt.outcome.serialization = 'completed'; break;
					case 'empty-terminal': receipt.terminal_error = { code: '', category: 'execution' }; break;
					case 'unexpected-terminal': receipt.terminal_error = { code: 'QUERY_FAILED', category: 'execution' }; break;
				}
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end(JSON.stringify(receipt));
			} else if (request.idempotency_key
				&& (mode === 'invalid_status_query_id' || mode === 'invalid_status_commit'
					|| mode === 'invalid_status_unknown' || mode === 'invalid_status_trace')) {
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end('{}');
			} else if (request.idempotency_key
				&& (mode === 'transport_committed' || mode === 'committed_serializing'
					|| mode === 'recovery_hangs')) {
				res.destroy();
			} else if (request.idempotency_key && mode === 'invalid_body_committed') {
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end('not-json');
			} else if (request.idempotency_key && mode === 'ordinary_body_committed') {
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end(JSON.stringify({ status: 'completed', rows: [] }));
			} else if (request.idempotency_key && mode === 'outcome_unknown') {
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end('not-json');
			} else if (request.pagination && mode === 'malformed_page') {
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end(JSON.stringify({ status: 'completed', rows: [] }));
			} else if (request.pagination && mode === 'invalid_page') {
				const page = {
					status: 'completed',
					rows: [{ id: 1 }],
					next_cursor: null,
					page: {
						offset: 0,
						row_count: 1,
						total_rows: 1,
						byte_count: 10,
						estimated_tokens: 3,
						limits: { rows: 1, bytes: 1024, tokens: 256 },
						projection: ['id'],
						expires_at_ms: 999,
						snapshot: 'retained_result',
						token_estimate: 'ceil(projected_json_bytes/4)'
					}
				};
				switch (request.sql) {
					case 'row-count': page.page.row_count = 2; break;
					case 'range': page.page.offset = 1; break;
					case 'zero-limit': page.page.limits.rows = 0; break;
					case 'exceeded-limit': page.page.byte_count = 1025; break;
					case 'projection': page.page.projection = ['other']; break;
					case 'row-keys': page.rows = [{ other: 1 }]; break;
					case 'prototype-key':
						page.rows = [{ other: 1 }];
						page.page.projection = ['toString'];
						page.page.byte_count = 13;
						page.page.estimated_tokens = 4;
						break;
					case 'duplicate-projection': page.page.projection = ['id', 'id']; break;
					case 'projection-bound': page.page.projection = ['x'.repeat(257)]; break;
					case 'page-byte-cap': page.page.limits.bytes = 64 * 1024 * 1024 + 1; break;
					case 'cursor-byte-cap':
						page.page.total_rows = 2;
						page.next_cursor = '😀'.repeat(600);
						break;
					case 'byte-count': page.page.byte_count += 1; break;
					case 'token-count': page.page.estimated_tokens += 1; break;
					case 'zero-row':
						page.rows = [];
						page.page.row_count = 0;
						page.page.total_rows = 1;
						page.page.byte_count = 2;
						page.page.estimated_tokens = 1;
						page.next_cursor = 'cursor-1';
						break;
					case 'snapshot': page.page.snapshot = 'live'; break;
					case 'cursor': page.next_cursor = 'unexpected'; break;
					case 'request-limit': page.page.limits.rows = 2; break;
					case 'output-limit':
						page.page.total_rows = 2;
						page.next_cursor = 'cursor-1';
						break;
				}
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end(JSON.stringify(page));
			} else if (request.pagination) {
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					status: 'completed', rows: [{ id: 1 }], next_cursor: 'cursor-1',
					page: { offset: 0, row_count: 1, total_rows: 2, byte_count: 10, estimated_tokens: 3, limits: { rows: 1, bytes: 1024, tokens: 256 }, projection: ['id'], expires_at_ms: 999, snapshot: 'retained_result', token_estimate: 'ceil(projected_json_bytes/4)' }
				}));
			} else if (request.idempotency_key) {
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: request.query_id, original_query_id: request.query_id, status: 'committed', terminal_state: 'committed', server_state: 'completed', cancel_outcome: 'already_finished', cancellation_reason: 'none', committed: true, committed_statements: 1, last_commit_epoch: null, last_commit_epoch_text: '9007199254740993', first_commit_statement_index: 0, last_commit_statement_index: 0, completed_statements: 1, statement_index: 0, retryable: false, idempotency_replayed: false, idempotency_persisted: true, idempotency_expires_at_ms: 999,
					outcome: { committed: true, committed_statements: 1, last_commit_epoch: null, last_commit_epoch_text: '9007199254740993', first_commit_statement_index: 0, last_commit_statement_index: 0, completed_statements: 1, statement_index: 0, serialization: 'succeeded' }, terminal_error: null
				}));
			} else if (request.sql === 'CROSSED') {
				res.writeHead(409, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: request.query_id,
					status: 'cancelled_before_commit',
					terminal_state: 'cancelled_before_commit',
					committed: false,
					committed_statements: 0,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: 0,
					statement_index: 0,
					retryable: false,
					outcome: {
						committed: false,
						committed_statements: 0,
						last_commit_epoch: null,
						last_commit_epoch_text: null,
						first_commit_statement_index: null,
						last_commit_statement_index: null,
						completed_statements: 0,
						statement_index: 0,
						serialization: 'failed'
					},
					error: {
						code: 'RESULT_LIMIT_EXCEEDED',
						message: 'crossed metadata',
						query_id: request.query_id,
						committed: false,
						retryable: false
					}
				}));
			} else if (request.sql === 'LIMIT') {
				res.writeHead(413, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: request.query_id,
					status: 'committed_with_error',
					terminal_state: 'committed_with_error',
					committed: true,
					committed_statements: 2,
					last_commit_epoch: null,
					last_commit_epoch_text: '9007199254740993',
					first_commit_statement_index: 0,
					last_commit_statement_index: 2,
					completed_statements: 3,
					statement_index: 3,
					error: { code: 'RESULT_LIMIT_EXCEEDED', message: 'too large', query_id: request.query_id, committed: true, retryable: false },
					outcome: { committed: true, committed_statements: 2, last_commit_epoch: null, last_commit_epoch_text: '9007199254740993', first_commit_statement_index: 0, last_commit_statement_index: 2, completed_statements: 3, statement_index: 3, serialization: 'failed' },
					cancel_outcome: 'already_finished',
					cancellation_reason: 'none',
					retryable: false,
					server_state: 'failed'
				}));
			} else if (request.sql === 'TIMEOUT') {
				res.writeHead(504, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: request.query_id,
					status: 'deadline_before_commit',
					terminal_state: 'deadline_before_commit',
					committed: false,
					committed_statements: 0,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: 0,
					statement_index: 0,
					cancel_outcome: 'accepted',
					cancellation_reason: 'deadline',
					retryable: false,
					server_state: 'cancelled',
					outcome: { committed: false, committed_statements: 0, last_commit_epoch: null, last_commit_epoch_text: null, first_commit_statement_index: null, last_commit_statement_index: null, completed_statements: 0, statement_index: 0, serialization: 'not_started' },
					error: { code: 'DEADLINE_EXCEEDED', message: 'timed out', query_id: request.query_id, committed: false, retryable: false }
				}));
			} else if (request.sql === 'SLOW') {
				setTimeout(() => {
					if (!res.writableEnded) {
						res.writeHead(200, { 'content-type': 'application/octet-stream' });
						res.end();
					}
				}, 1000);
			} else {
				res.writeHead(200, { 'content-type': 'application/octet-stream' });
				res.end();
			}
		} else if (req.url.endsWith('/cancel')) {
			if (mode === 'idempotency_restart_replay') {
				const queryId = req.url.split('/')[2];
				cancelledQueries.add(req.url.split('/')[2]);
				res.writeHead(202, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: queryId,
					state: 'pre_cancelled',
					cancel_outcome: 'pre_cancelled',
					terminal_error: { code: 'QUERY_CANCELLED', category: 'cancellation' }
				}));
				return;
			}
			if (mode === 'cancel_not_found') {
				const queryId = req.url.split('/')[2];
				res.writeHead(404, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: queryId,
					status: 'unknown',
					terminal_state: null,
					committed: null,
					committed_statements: null,
					last_commit_epoch: null,
					last_commit_epoch_text: null,
					first_commit_statement_index: null,
					last_commit_statement_index: null,
					completed_statements: null,
					statement_index: null,
					cancel_outcome: 'not_found',
					cancellation_reason: null,
					retryable: false,
					server_state: 'not_found',
					outcome: {
						committed: null,
						committed_statements: null,
						last_commit_epoch: null,
						last_commit_epoch_text: null,
						first_commit_statement_index: null,
						last_commit_statement_index: null,
						completed_statements: null,
						statement_index: null,
						serialization: 'unknown'
					},
					error: {
						code: 'QUERY_NOT_FOUND',
						message: 'query not found',
						query_id: queryId,
						committed: null,
						retryable: false
					}
				}));
				return;
			}
			if (mode === 'cancel_wrong_query_id') {
				res.writeHead(202, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: '11112222333344445555666677778888',
					state: 'cancellation_requested',
					cancel_outcome: 'accepted'
				}));
				return;
			}
			if (mode === 'cancel_conflict') {
				const queryId = req.url.split('/')[2];
				res.writeHead(409, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: queryId,
					state: 'cancellation_requested',
					cancel_outcome: 'too_late'
				}));
				return;
			}
			if (mode === 'cancel_wrong_http') {
				const queryId = req.url.split('/')[2];
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: queryId,
					state: 'cancellation_requested',
					cancel_outcome: 'accepted'
				}));
				return;
			}
			if (mode === 'cancel_invalid_field') {
				const queryId = req.url.split('/')[2];
				res.writeHead(202, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: queryId,
					state: 'cancellation_requested',
					cancel_outcome: 'mystery'
				}));
				return;
			}
			if (mode === 'cancel_missing_field') {
				const queryId = req.url.split('/')[2];
				res.writeHead(202, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: queryId,
					state: 'cancellation_requested'
				}));
				return;
			}
			if (mode === 'cancel_duplicate_field') {
				const queryId = req.url.split('/')[2];
				res.writeHead(202, { 'content-type': 'application/json' });
				res.end('{"query_id":"' + queryId
					+ '","state":"cancellation_requested","state":"cancellation_requested",'
					+ '"cancel_outcome":"accepted"}');
				return;
			}
			if (mode === 'cancel_unknown_field') {
				const queryId = req.url.split('/')[2];
				res.writeHead(202, { 'content-type': 'application/json' });
				res.end(JSON.stringify({
					query_id: queryId,
					state: 'cancellation_requested',
					cancel_outcome: 'accepted',
					unexpected: true
				}));
				return;
			}
			if (mode === 'already_finished') {
				const queryId = req.url.split('/')[2];
				res.writeHead(200, { 'content-type': 'application/json' });
				res.end(JSON.stringify({ query_id: queryId, state: 'finished', cancel_outcome: 'already_finished' }));
				return;
			}
			if (mode === 'too_late' || mode === 'transport_committed'
				|| mode === 'invalid_body_committed' || mode === 'ordinary_body_committed') {
				const queryId = req.url.split('/')[2];
				res.writeHead(409, { 'content-type': 'application/json' });
				res.end(JSON.stringify({ query_id: queryId, state: 'commit_critical', cancel_outcome: 'too_late' }));
			} else {
				const queryId = req.url.split('/')[2];
				cancelledQueries.add(queryId);
				res.writeHead(202, { 'content-type': 'application/json' });
				res.end(JSON.stringify({ query_id: queryId, state: 'cancellation_requested', cancel_outcome: 'accepted' }));
			}
		} else {
			res.writeHead(404, { 'content-type': 'text/plain' });
			res.end('not found');
		}
	});
});

server.listen(0, '127.0.0.1', () => {
	const { port } = server.address();
	parentPort.postMessage({ type: 'ready', port });
});

parentPort.on('message', (msg) => {
	if (msg.type === 'getRequests') {
		parentPort.postMessage({ type: 'requests', requests });
	}
});
`;

function startMockServer(
	mode: 'success' | 'error' | 'old' | 'too_late' | 'already_finished' | 'auth'
		| 'basic_auth'
		| 'transport_committed' | 'invalid_body_committed' | 'ordinary_body_committed'
		| 'committed_serializing'
		| 'recovery_hangs'
		| 'transport_failed'
		| 'transport_pre_cancelled'
		| 'missing_query_header' | 'wrong_query_header' | 'page_missing_query_header'
		| 'idempotency_missing_query_header'
		| 'malformed_page' | 'invalid_page'
		| 'cursor_error' | 'cursor_error_conflict'
		| 'idempotency_restart_replay' | 'idempotency_restart_fresh_execution'
		| 'idempotency_restart_downgrade' | 'idempotency_wrong_original'
		| 'idempotency_malformed_not_found'
		| 'invalid_receipt' | 'invalid_status_query_id' | 'invalid_status_commit'
		| 'invalid_status_unknown' | 'invalid_status_trace'
		| 'capability_unknown' | 'capability_unsafe' | 'capability_oversized'
		| 'cancel_partial_commit' | 'cancel_hangs' | 'cancel_not_found'
		| 'cancel_wrong_query_id' | 'cancel_conflict' | 'cancel_wrong_http'
		| 'cancel_invalid_field' | 'cancel_missing_field' | 'cancel_duplicate_field'
		| 'cancel_unknown_field'
		| 'outcome_unknown' | 'crossed_sql_error' | 'cancelling_deadline' | 'compact_status' = 'success'
): Promise<{ url: string; worker: Worker }> {
	const worker = new Worker(WORKER_SCRIPT, { eval: true, workerData: mode });
	return new Promise((resolve, reject) => {
		worker.once('message', (msg: { type: string; port?: number }) => {
			if (msg.type === 'ready' && msg.port !== undefined) {
				resolve({ url: `http://127.0.0.1:${msg.port}`, worker });
			} else {
				reject(new Error(`unexpected worker message: ${JSON.stringify(msg)}`));
			}
		});
		worker.once('error', reject);
	});
}

function getRequests(worker: Worker): Promise<RequestRecord[]> {
	return new Promise((resolve) => {
		worker.once('message', (msg: { type: string; requests?: RequestRecord[] }) => {
			if (msg.type === 'requests') {
				resolve(msg.requests ?? []);
			}
		});
		worker.postMessage({ type: 'getRequests' });
	});
}

function stopMockServer(worker: Worker): Promise<void> {
	return worker.terminate().then(() => {});
}

describe('RemoteDatabase', () => {
	it('buildKitSearchBody accepts multi-retriever fusion wire', () => {
		const body = RemoteDatabase.buildKitSearchBody({
			table: 'docs',
			retrievers: [
				{ name: 'ann', weight: 1, ann: { column_id: 3, query: [0.1, 0.2], k: 10 } },
				{ name: 'sparse', weight: 0.5, sparse: { column_id: 4, query: [[1, 0.5]], k: 10 } }
			],
			fusionConstant: 60,
			limit: 5
		});
		expect(body.table).toBe('docs');
		expect(body.retrievers).toHaveLength(2);
		expect(body.fusion).toEqual({ reciprocal_rank: { constant: 60 } });
		expect(body.limit).toBe(5);
		// Round-trip through JSON like the HTTP client encoder.
		const wire = JSON.parse(JSON.stringify(body));
		expect(wire.retrievers).toHaveLength(2);
	});

	it('preserves unknown durable outcome as null', async () => {
		const { url, worker } = await startMockServer('outcome_unknown');
		try {
			const remote = new RemoteDatabase(url);
			const status = await remote.queryStatus('11112222333344445555666677778888');
			expect(status.committed).toBeNull();
			expect(status.durableOutcome.committed).toBeNull();
			expect(status.durableOutcome.committedStatements).toBeNull();
			expect(status.completedStatements).toBeNull();
			expect(status.statementIndex).toBeNull();
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each([
		['cancelling_deadline', 'cancelling', 'deadline'],
		['compact_status', 'finished', 'none']
	] as const)('keeps null terminal state for %s status', async (mode, phase, reason) => {
		const { url, worker } = await startMockServer(mode);
		try {
			const remote = new RemoteDatabase(url);
			const status = await remote.queryStatus('11112222333344445555666677778888');
			expect(status.phase).toBe(phase);
			expect(status.terminalState).toBeUndefined();
			expect(status.cancellationReason).toBe(reason);
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each(['capability_unknown', 'capability_unsafe', 'capability_oversized'] as const)(
		'fails closed on %s capability metadata',
		async (mode) => {
			const { url, worker } = await startMockServer(mode);
			try {
				const remote = new RemoteDatabase(url);
				await expect(remote.cancelSql('12344321123443211234432112344321'))
					.rejects.toMatchObject({ code: 'REMOTE_PROTOCOL' });
			} finally {
				await stopMockServer(worker);
			}
		}
	);

	it('setHistoryRetentionEpochs sends PUT /history/retention with a number body', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			remote.setHistoryRetentionEpochs(42n);
			const requests = await getRequests(worker);
			expect(requests).toHaveLength(1);
			expect(requests[0].method).toBe('PUT');
			expect(requests[0].path).toBe('/history/retention');
			expect(requests[0].headers['content-type']).toBe('application/json');
			expect(JSON.parse(requests[0].body)).toEqual({ history_retention_epochs: 42 });
		} finally {
			await stopMockServer(worker);
		}
	});

	it('historyRetentionEpochs sends GET /history/retention and returns the value', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			expect(remote.historyRetentionEpochs()).toBe(42n);
			const requests = await getRequests(worker);
			expect(requests).toHaveLength(1);
			expect(requests[0].method).toBe('GET');
			expect(requests[0].path).toBe('/history/retention');
		} finally {
			await stopMockServer(worker);
		}
	});

	it('earliestRetainedEpoch sends GET /history/retention and returns the value', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			expect(remote.earliestRetainedEpoch()).toBe(5n);
			const requests = await getRequests(worker);
			expect(requests).toHaveLength(1);
			expect(requests[0].method).toBe('GET');
			expect(requests[0].path).toBe('/history/retention');
		} finally {
			await stopMockServer(worker);
		}
	});

	it('propagates 503 errors from the daemon', async () => {
		const { url, worker } = await startMockServer('error');
		try {
			const remote = new RemoteDatabase(url);
			expect(() => remote.historyRetentionEpochs()).toThrow();
			expect(() => remote.earliestRetainedEpoch()).toThrow();
			expect(() => remote.setHistoryRetentionEpochs(7n)).toThrow();
		} finally {
			await stopMockServer(worker);
		}
	});

	it('sends query ID and server timeout and maps timeout error', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.sql('TIMEOUT', {
				queryId: '11112222333344445555666677778888',
				timeoutMs: 250
			})).rejects.toBeInstanceOf(QueryTimeoutError);
			const requests = await getRequests(worker);
			const sqlRequest = requests.find((request) => request.path === '/sql');
			expect(JSON.parse(sqlRequest!.body)).toMatchObject({
				format: 'arrow',
				query_id: '11112222333344445555666677778888',
				timeout_ms: 250
			});
		} finally {
			await stopMockServer(worker);
		}
	});

	it('maps remote output limits with exact durable outcome', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			const queryId = 'fedcbafedcbafedcbafedcbafedcbafe';
			const error = await remote.sql('LIMIT', { queryId, maxOutputRows: 1 })
				.catch((caught: unknown) => caught);
			expect(error).toBeInstanceOf(ResultLimitExceededError);
			expect(error).toMatchObject({
				code: 'RESULT_LIMIT_EXCEEDED',
				queryId,
				committed: true,
				committedStatements: 2,
				lastCommitEpoch: 9007199254740993n,
				firstCommitStatementIndex: 0,
				lastCommitStatementIndex: 2,
				completedStatements: 3,
				statementIndex: 3,
				cancelOutcome: 'already_finished',
				cancellationReason: 'none',
				retryable: false,
				serverState: 'failed'
			});
		} finally {
			await stopMockServer(worker);
		}
	});

	it('rejects a SQL error whose terminal status and code cross', async () => {
		const { url, worker } = await startMockServer('crossed_sql_error');
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.sql('CROSSED', {
				queryId: 'abcdefabcdefabcdefabcdefabcdefab'
			})).rejects.toBeInstanceOf(QueryOutcomeUnknownError);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('AbortSignal sends remote cancellation and rejects typed', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			const controller = new AbortController();
			const result = remote.sql('SLOW', {
				queryId: 'aaaabbbbccccddddeeeeffff00001111',
				signal: controller.signal
			});
			await new Promise((resolve) => setTimeout(resolve, 20));
			controller.abort();
			await expect(result).rejects.toBeInstanceOf(QueryCancelledError);
			await new Promise((resolve) => setTimeout(resolve, 20));
			const requests = await getRequests(worker);
			expect(requests.some((request) => request.path === '/queries/aaaabbbbccccddddeeeeffff00001111/cancel')).toBe(true);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('AbortSignal cancellation preserves earlier committed statements', async () => {
		const { url, worker } = await startMockServer('cancel_partial_commit');
		try {
			const remote = new RemoteDatabase(url);
			const controller = new AbortController();
			const queryId = 'ddddccccbbbbaaaa9999888877776666';
			const result = remote.sql('SLOW', { queryId, signal: controller.signal });
			await new Promise((resolve) => setTimeout(resolve, 20));
			controller.abort();
			const error = await result.catch((caught: unknown) => caught);
			expect(error).toBeInstanceOf(QueryCancelledError);
				expect(error).toMatchObject({
					queryId,
					committed: true,
					committedStatements: 1,
					lastCommitEpoch: 17n,
					completedStatements: 1,
					statementIndex: 1,
					cancelOutcome: 'already_finished',
					cancellationReason: 'client_request',
					retryable: false,
					serverState: 'cancelled'
				});
		} finally {
			await stopMockServer(worker);
		}
	});

	it('AbortSignal remains bounded when the cancel endpoint never responds', async () => {
		const { url, worker } = await startMockServer('cancel_hangs');
		try {
			const remote = new RemoteDatabase(url);
			const controller = new AbortController();
			const queryId = 'eeee1111222233334444555566667777';
			const started = Date.now();
			const result = remote.sql('SLOW', { queryId, signal: controller.signal });
			await new Promise((resolve) => setTimeout(resolve, 20));
			controller.abort();
			const error = await result.catch((caught: unknown) => caught);
			expect(error).toBeInstanceOf(QueryOutcomeUnknownError);
			expect(error).toMatchObject({
				queryId,
				committed: null,
				committedStatements: null,
				lastCommitEpoch: null,
				completedStatements: null,
				statementIndex: null
			});
			const elapsed = Date.now() - started;
			expect(elapsed).toBeGreaterThanOrEqual(1_500);
			expect(elapsed).toBeLessThan(3_500);
			const requests = await getRequests(worker);
			expect(requests.some((request) => request.path === `/queries/${queryId}/cancel`)).toBe(true);
			expect(requests.some((request) => request.path === `/queries/${queryId}`)).toBe(true);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('does not abort the SQL response when commit reports too late', async () => {
		const { url, worker } = await startMockServer('too_late');
		try {
			const remote = new RemoteDatabase(url);
			const query = remote.startSql('SLOW', {
				queryId: 'bbbbaaaaccccddddeeeeffff00001111'
			});
			await new Promise((resolve) => setTimeout(resolve, 20));
			await expect(query.cancel()).resolves.toBe('too_late');
			await expect(query.result).resolves.toBeDefined();
		} finally {
			await stopMockServer(worker);
		}
	});

	it('preserves already-finished cancellation conflicts', async () => {
		const { url, worker } = await startMockServer('already_finished');
		try {
			const remote = new RemoteDatabase(url);
			const query = remote.startSql('SLOW', {
				queryId: 'ffffeeeeddddccccbbbbaaaa99998888'
			});
			await new Promise((resolve) => setTimeout(resolve, 20));
			await expect(query.cancel()).resolves.toBe('already_finished');
			await expect(query.result).resolves.toBeDefined();
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each([
		'cancel_wrong_query_id',
		'cancel_conflict',
		'cancel_wrong_http',
		'cancel_invalid_field',
		'cancel_missing_field',
		'cancel_duplicate_field',
		'cancel_unknown_field'
	] as const)(
		'rejects malformed cancellation metadata: %s',
		async (mode) => {
			const { url, worker } = await startMockServer(mode);
			try {
				const remote = new RemoteDatabase(url);
				await expect(remote.cancelSql('aaaabbbbccccddddeeeeffff00001111'))
					.rejects.toThrow(/cancellation/);
			} finally {
				await stopMockServer(worker);
			}
		}
	);

	it('does not send SQL for an already-aborted signal', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			const controller = new AbortController();
			controller.abort();
			const query = remote.startSql('SELECT 1', { signal: controller.signal });
			await expect(query.result).rejects.toMatchObject({
				name: 'QueryCancelledError',
				committed: false,
				committedStatements: 0,
				completedStatements: 0,
				statementIndex: 0,
				cancelOutcome: 'pre_cancelled',
				cancellationReason: 'client_request',
				retryable: false,
				serverState: 'pre_cancelled'
			});
			await expect(query.status()).resolves.toMatchObject({
				phase: 'pre_cancelled',
				serverState: 'pre_cancelled',
				terminalState: 'cancelled_before_start',
				cancelOutcome: 'pre_cancelled'
			});
			const requests = await getRequests(worker);
			expect(requests.some((request) => request.path === '/sql')).toBe(false);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('never sends SQL when an abort races capability discovery and pre-cancel is not found', async () => {
		const { url, worker } = await startMockServer('cancel_not_found');
		try {
			const remote = new RemoteDatabase(url);
			const controller = new AbortController();
			const queryId = '12344321123443211234432112344321';
			const query = remote.startSql('SELECT 1', { queryId, signal: controller.signal });
			controller.abort();
			await expect(query.result).rejects.toMatchObject({ code: 'QUERY_NOT_FOUND' });
			const requests = await getRequests(worker);
			expect(requests.some((request) => request.path === `/queries/${queryId}/cancel`)).toBe(true);
			expect(requests.some((request) => request.path === '/sql')).toBe(false);
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each([
		['auth', { bearerToken: 'secret' }, 'Bearer secret'],
		['basic_auth', { username: 'alice', password: 'secret' }, 'Basic YWxpY2U6c2VjcmV0']
	] as const)('sends %s credentials on native and fetch routes', async (mode, auth, expected) => {
		const { url, worker } = await startMockServer(mode);
		try {
			const remote = new RemoteDatabase(url, { auth });
			expect(remote.historyRetentionEpochs()).toBe(42n);
			await expect(remote.sql('SELECT 1', { timeoutMs: 100 })).resolves.toBeDefined();
			const requests = await getRequests(worker);
			expect(requests.length).toBeGreaterThan(1);
			expect(requests.every((request) => request.headers.authorization === expected)).toBe(true);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('paginates with advertised limits and authenticates continuation', async () => {
		const { url, worker } = await startMockServer('auth');
		try {
			const remote = new RemoteDatabase(url, { auth: { bearerToken: 'secret' } });
			const first = await remote.sqlPage('SELECT id FROM items', {
				queryId: '1234567890abcdef1234567890abcdef',
				timeoutMs: 250,
				projection: ['id'],
				pageSizeRows: 1,
				maxPageBytes: 1024,
				maxPageTokens: 256
			});
			expect(first.rows).toEqual([{ id: 1 }]);
			expect(first.nextCursor).toBe('cursor-1');
			expect(first.page).toMatchObject({ rowCount: 1, totalRows: 2, projection: ['id'] });
			const second = await remote.continueSqlPage(first.nextCursor!);
			expect(second.rows).toEqual([{ id: 2 }]);
			expect(second.nextCursor).toBeUndefined();
			const requests = await getRequests(worker);
			const sqlRequest = requests.find((request) => request.path === '/sql');
			expect(JSON.parse(sqlRequest!.body)).toMatchObject({
				query_id: '1234567890abcdef1234567890abcdef',
				timeout_ms: 250,
				pagination: {
					page_size_rows: 1,
					projection: ['id'],
					max_page_bytes: 1024,
					max_page_tokens: 256
				}
			});
			expect(requests.find((request) => request.path === '/sql/continue')?.headers.authorization)
				.toBe('Bearer secret');
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each(['missing_query_header', 'wrong_query_header'] as const)(
		'rejects an Arrow response with %s',
		async (mode) => {
			const { url, worker } = await startMockServer(mode);
			try {
				await expect(new RemoteDatabase(url).sql('SELECT 1', {
					queryId: '1234567890abcdef1234567890abcdef'
				})).rejects.toBeInstanceOf(SerializationError);
			} finally {
				await stopMockServer(worker);
			}
		}
	);

	it('rejects an initial page without its query ID header', async () => {
		const { url, worker } = await startMockServer('page_missing_query_header');
		try {
			await expect(new RemoteDatabase(url).sqlPage('SELECT id FROM items', {
				queryId: '1234567890abcdef1234567890abcdef',
				projection: ['id'],
				pageSizeRows: 1
			})).rejects.toBeInstanceOf(SerializationError);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('keeps exact committed receipt proof when its query ID header is absent', async () => {
		const { url, worker } = await startMockServer('idempotency_missing_query_header');
		try {
			await expect(new RemoteDatabase(url).executeIdempotentSql(
				'INSERT INTO items VALUES (1)',
				{
					queryId: 'abcdefabcdefabcdefabcdefabcdefab',
					idempotencyKey: 'insert-one'
				}
			)).rejects.toMatchObject({
				name: 'CommitOutcomeError',
				queryId: 'abcdefabcdefabcdefabcdefabcdefab',
				committed: true,
				committedStatements: 1,
				lastCommitEpoch: 9007199254740993n
			});
		} finally {
			await stopMockServer(worker);
		}
	});

	it('maps malformed pagination responses to serialization failures', async () => {
		for (const continuation of [false, true]) {
			const { url, worker } = await startMockServer('malformed_page');
			try {
				const remote = new RemoteDatabase(url);
				const error = await (continuation
					? remote.continueSqlPage('cursor-1')
					: remote.sqlPage('SELECT id FROM items', {
						queryId: '1234567890abcdef1234567890abcdef',
						projection: ['id'],
						pageSizeRows: 1
					})).catch((caught: unknown) => caught);
				expect(error).toBeInstanceOf(SerializationError);
				expect(error).toMatchObject({ committed: false });
			} finally {
				await stopMockServer(worker);
			}
		}
	});

	it('requires exact durable metadata on cursor error responses', async () => {
		for (const [mode, expected] of [
			['cursor_error', RemoteProtocolError],
			['cursor_error_conflict', QueryOutcomeUnknownError]
		] as const) {
			const { url, worker } = await startMockServer(mode);
			try {
				const remote = new RemoteDatabase(url);
				await expect(remote.continueSqlPage('cursor-1')).rejects.toBeInstanceOf(expected);
			} finally {
				await stopMockServer(worker);
			}
		}
	});

	it('rejects oversized UTF-8 continuation cursors before the route', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.continueSqlPage('😀'.repeat(600))).rejects.toThrow(
				'1 to 2048 UTF-8 bytes'
			);
			const requests = await getRequests(worker);
			expect(requests.some((request) => request.path === '/sql/continue')).toBe(false);
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each([
		'row-count',
		'range',
		'zero-limit',
		'exceeded-limit',
		'projection',
		'row-keys',
		'prototype-key',
		'duplicate-projection',
		'projection-bound',
		'page-byte-cap',
		'cursor-byte-cap',
		'byte-count',
		'token-count',
		'zero-row',
		'snapshot',
		'cursor',
		'request-limit',
		'output-limit'
	])('rejects inconsistent retained page metadata: %s', async (sql) => {
		const { url, worker } = await startMockServer('invalid_page');
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.sqlPage(sql, {
				queryId: '1234567890abcdef1234567890abcdef',
				projection: sql === 'prototype-key' ? ['toString'] : ['id'],
				pageSizeRows: 1,
				maxPageBytes: 1024,
				maxPageTokens: 256,
				maxOutputRows: 1,
				maxOutputBytes: 1024
			})).rejects.toBeInstanceOf(SerializationError);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('refuses the second idempotent POST after capability downgrade', async () => {
		const { url, worker } = await startMockServer('idempotency_restart_downgrade');
		try {
			await expect(new RemoteDatabase(url).executeIdempotentSql(
				'INSERT INTO items VALUES (1)',
				{
					queryId: 'abcdefabcdefabcdefabcdefabcdefab',
					idempotencyKey: 'insert-one'
				}
			)).rejects.toBeInstanceOf(KitUnsupportedError);
			const requests = await getRequests(worker);
			expect(requests.filter((request) => request.path === '/capabilities')).toHaveLength(2);
			expect(requests.filter((request) => request.path === '/sql')).toHaveLength(1);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('preserves an exact epoch beyond the JavaScript safe integer range', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			const receipt = await remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				queryId: 'abcdefabcdefabcdefabcdefabcdefab',
				idempotencyKey: 'insert-one',
				maxOutputRows: 1,
				maxOutputBytes: 1024
			});
			expect(receipt).toMatchObject({
				terminalState: 'committed',
				serverState: 'completed',
				cancelOutcome: 'already_finished',
				cancellationReason: 'none',
				committed: true,
				committedStatements: 1,
				lastCommitEpoch: 9007199254740993n,
				firstCommitStatementIndex: 0,
				lastCommitStatementIndex: 0,
				idempotencyPersisted: true,
				retryable: false
			});
			const requests = await getRequests(worker);
			const sqlRequest = requests.find((request) => request.path === '/sql');
			expect(JSON.parse(sqlRequest!.body)).toMatchObject({
				idempotency_key: 'insert-one',
				format: 'json',
				max_output_rows: 1,
				max_output_bytes: 1024
			});
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each([
		'numeric-exact',
		'epoch-disagreement',
		'unsafe-numeric',
		'overflow-epoch',
		'missing-outcome-field',
		'reversed-index',
		'count-disagreement',
		'empty-serialization',
		'invalid-serialization',
		'empty-terminal',
		'unexpected-terminal'
	])('rejects inconsistent durable receipt field: %s', async (sql) => {
		const { url, worker } = await startMockServer('invalid_receipt');
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.executeIdempotentSql(sql, {
				queryId: 'abcdefabcdefabcdefabcdefabcdefab',
				idempotencyKey: `invalid-${sql}`
			})).rejects.toBeInstanceOf(QueryOutcomeUnknownError);
			const requests = await getRequests(worker);
			expect(requests.filter((request) => request.path === '/sql')).toHaveLength(1);
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each([
		'invalid_status_query_id',
		'invalid_status_commit',
		'invalid_status_unknown',
		'invalid_status_trace'
	] as const)(
		'fails closed on %s during idempotent recovery',
		async (mode) => {
			const { url, worker } = await startMockServer(mode);
			try {
				const remote = new RemoteDatabase(url);
				const error = await remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
					queryId: 'abcdefabcdefabcdefabcdefabcdefab',
					idempotencyKey: 'status-conflict'
				}).catch((caught: unknown) => caught);
				expect(error).toBeInstanceOf(QueryOutcomeUnknownError);
				expect(error).toMatchObject({ serverState: 'invalid_status' });
				const requests = await getRequests(worker);
				expect(requests.filter((request) => request.path === '/sql')).toHaveLength(1);
				expect(requests.some((request) => request.path?.endsWith('/cancel'))).toBe(false);
			} finally {
				await stopMockServer(worker);
			}
		}
	);

	it.each(['transport_committed', 'invalid_body_committed', 'ordinary_body_committed'] as const)(
		'recovers idempotent SQL after %s response loss',
		async (mode) => {
			const { url, worker } = await startMockServer(mode);
			try {
				const remote = new RemoteDatabase(url);
				const queryId = 'abcdefabcdefabcdefabcdefabcdefab';
				const error = await remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
					queryId,
					idempotencyKey: 'insert-one'
				}).catch((caught: unknown) => caught);
				expect(error).toBeInstanceOf(CommitOutcomeError);
				expect(error).toMatchObject({
					queryId,
					committed: true,
					committedStatements: 1,
					lastCommitEpoch: 17n
				});
				const requests = await getRequests(worker);
				expect(requests.some((request) => request.path === `/queries/${queryId}/cancel`)).toBe(true);
				expect(requests.filter((request) => request.path === `/queries/${queryId}`)).toHaveLength(2);
			} finally {
				await stopMockServer(worker);
			}
		}
	);

	it('treats committed serializing status as immediately decisive', async () => {
		const { url, worker } = await startMockServer('committed_serializing');
		try {
			const remote = new RemoteDatabase(url);
			const queryId = 'abcdefabcdefabcdefabcdefabcdefab';
			await expect(remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				queryId,
				idempotencyKey: 'insert-one'
			})).rejects.toMatchObject({
				name: 'CommitOutcomeError',
				queryId,
				committed: true,
				lastCommitEpoch: 17n,
				lastCommitHlc: {
					physicalMicros: 1_700_000_000_000_000,
					logical: 3,
					nodeTiebreaker: 7
				},
				serializationState: 'in_progress'
			});
			const status = await remote.queryStatus(queryId);
			expect(status.durableOutcome.lastCommitHlc).toEqual({
				physicalMicros: 1_700_000_000_000_000,
				logical: 3,
				nodeTiebreaker: 7
			});
			expect(status.durableOutcome.serializationState).toBe('in_progress');
			const requests = await getRequests(worker);
			expect(requests.filter((request) => request.path === `/queries/${queryId}`).length)
				.toBeGreaterThanOrEqual(1);
			expect(requests.some((request) => request.path?.endsWith('/cancel'))).toBe(false);
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each([
		['idempotency_restart_replay', true],
		['idempotency_restart_fresh_execution', false]
	] as const)('retries a durable idempotency key safely after restart: %s', async (mode, replayed) => {
		const { url, worker } = await startMockServer(mode);
		try {
			const remote = new RemoteDatabase(url);
			const originalQueryId = 'abcdefabcdefabcdefabcdefabcdefab';
			const receipt = await remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				queryId: originalQueryId,
				idempotencyKey: 'insert-one',
				timeoutMs: 250,
				maxOutputRows: 1,
				maxOutputBytes: 1024
			});
			expect(receipt).toMatchObject({
				committed: true,
				lastCommitEpoch: 29n,
				idempotencyReplayed: replayed
			});
			expect(receipt.queryId).not.toBe(originalQueryId);
			expect(receipt.originalQueryId).toBe(replayed ? originalQueryId : receipt.queryId);
			const requests = await getRequests(worker);
			const sqlRequests = requests
				.filter((request) => request.path === '/sql')
				.map((request) => JSON.parse(request.body));
			expect(sqlRequests).toHaveLength(2);
			expect(sqlRequests[0]).toMatchObject({
				sql: 'INSERT INTO items VALUES (1)',
				idempotency_key: 'insert-one',
				timeout_ms: 250,
				max_output_rows: 1,
				max_output_bytes: 1024
			});
			expect(sqlRequests[1]).toMatchObject({
				...sqlRequests[0],
				query_id: receipt.queryId
			});
			expect(requests.some((request) => request.path?.endsWith('/cancel'))).toBe(false);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('does not replay after a malformed query-not-found response', async () => {
		const { url, worker } = await startMockServer('idempotency_malformed_not_found');
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				queryId: 'abcdefabcdefabcdefabcdefabcdefab',
				idempotencyKey: 'insert-one'
			})).rejects.toMatchObject({ serverState: 'invalid_status' });
			const requests = await getRequests(worker);
			expect(requests.filter((request) => request.path === '/sql')).toHaveLength(1);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('rejects replay receipts bound to a different original query ID', async () => {
		const { url, worker } = await startMockServer('idempotency_wrong_original');
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				queryId: 'abcdefabcdefabcdefabcdefabcdefab',
				idempotencyKey: 'insert-one'
			})).rejects.toBeInstanceOf(QueryOutcomeUnknownError);
			const requests = await getRequests(worker);
			expect(requests.filter((request) => request.path === '/sql')).toHaveLength(2);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('does not replay a durable idempotency key after an indeterminate terminal status', async () => {
		const { url, worker } = await startMockServer('outcome_unknown');
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				queryId: 'abcdefabcdefabcdefabcdefabcdefab',
				idempotencyKey: 'insert-one'
			})).rejects.toBeInstanceOf(QueryOutcomeUnknownError);
			const requests = await getRequests(worker);
			expect(requests.filter((request) => request.path === '/sql')).toHaveLength(1);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('returns a known execution failure after SQL response loss', async () => {
		const { url, worker } = await startMockServer('transport_failed');
		try {
			const remote = new RemoteDatabase(url);
			const queryId = 'abcdefabcdefabcdefabcdefabcdefab';
			const error = await remote.sql('BROKEN SQL', { queryId }).catch((caught: unknown) => caught);
			expect(error).toBeInstanceOf(QueryExecutionError);
			expect(error).toMatchObject({
				queryId,
				terminalCode: 'QUERY_FAILED',
				committed: false,
				committedStatements: 0,
				retryable: false,
				serverState: 'failed'
			});
		} finally {
			await stopMockServer(worker);
		}
	});

	it('treats pre-cancelled recovery status as terminal cancellation', async () => {
		const { url, worker } = await startMockServer('transport_pre_cancelled');
		try {
			const remote = new RemoteDatabase(url);
			const queryId = 'abcdefabcdefabcdefabcdefabcdefab';
			const error = await remote.sql('SELECT 1', { queryId }).catch((caught: unknown) => caught);
			expect(error).toBeInstanceOf(QueryCancelledError);
			expect(error).toMatchObject({
				queryId,
				committed: false,
				committedStatements: 0,
				cancelOutcome: 'pre_cancelled',
				cancellationReason: 'client_request',
				serverState: 'pre_cancelled'
			});
			const requests = await getRequests(worker);
			expect(requests.some((request) => request.path === `/queries/${queryId}/cancel`)).toBe(false);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('does not replay without an explicit not-retained status', async () => {
		const { url, worker } = await startMockServer('recovery_hangs');
		try {
			const remote = new RemoteDatabase(url);
			const started = Date.now();
			await expect(remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				queryId: 'abcdefabcdefabcdefabcdefabcdefab',
				idempotencyKey: 'insert-one'
			})).rejects.toBeInstanceOf(QueryOutcomeUnknownError);
			const elapsed = Date.now() - started;
			expect(elapsed).toBeGreaterThanOrEqual(1_500);
			expect(elapsed).toBeLessThan(3_500);
			const requests = await getRequests(worker);
			expect(requests.filter((request) => request.path === '/sql')).toHaveLength(1);
			expect(requests.some((request) => request.path?.endsWith('/cancel'))).toBe(true);
			expect(requests.some((request) => request.path?.startsWith('/queries/'))).toBe(true);
		} finally {
			await stopMockServer(worker);
		}
	});

	it.each([
		'ftp://example.com',
		'http:///',
		'https://alice:secret@example.test',
		'https://example.test?token=secret',
		'https://example.test#token'
	])(
		'rejects unsafe remote URL %s',
		(url) => expect(() => new RemoteDatabase(url)).toThrow(TypeError)
	);

	it('rejects non-canonical query IDs before building a route', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.cancelSql('../../capabilities')).rejects.toThrow(
				'32 hexadecimal characters'
			);
			expect(() => remote.startSql('SELECT 1', { queryId: 'not-a-query-id' })).toThrow(
				'32 hexadecimal characters'
			);
			expect(await getRequests(worker)).toHaveLength(0);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('does not expose credentials through object inspection', () => {
		const bearer = new RemoteDatabase('https://example.test', {
			auth: { bearerToken: 'bearer-inspection-secret' }
		});
		const basic = new RemoteDatabase('https://example.test', {
			auth: { username: 'alice', password: 'basic-inspection-secret' }
		});

		for (const remote of [bearer, basic]) {
			const rendered = `${inspect(remote)} ${JSON.stringify(remote)}`;
			expect(rendered).not.toContain('bearer-inspection-secret');
			expect(rendered).not.toContain('basic-inspection-secret');
			expect(rendered).not.toContain('authorization');
			expect(Object.keys(remote)).not.toContain('authorization');
		}
	});

	it.each([
		{ bearerToken: '' },
		{ bearerToken: 'secret\r\ninjected' },
		{ username: '', password: 'secret' },
		{ username: 'alice:admin', password: 'secret' },
		{ username: 'alice', password: 'secret\ninjected' }
	] as const)('rejects ambiguous auth options before connecting', (auth) => {
		expect(() => new RemoteDatabase('https://example.test', { auth })).toThrow(TypeError);
	});

	it('rejects unadvertised or invalid pagination and idempotency before SQL', async () => {
		const old = await startMockServer('old');
		try {
			const remote = new RemoteDatabase(old.url);
			await expect(remote.sqlPage('SELECT id FROM items', {
				projection: ['id'],
				pageSizeRows: 1
			})).rejects.toBeInstanceOf(KitUnsupportedError);
			await expect(remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				idempotencyKey: 'one'
			})).rejects.toBeInstanceOf(KitUnsupportedError);
			const requests = await getRequests(old.worker);
			expect(requests.some((request) => request.path === '/sql')).toBe(false);
		} finally {
			await stopMockServer(old.worker);
		}

		const current = await startMockServer();
		try {
			const remote = new RemoteDatabase(current.url);
			await expect(remote.sqlPage('SELECT id FROM items', {
				projection: [],
				pageSizeRows: 0
			})).rejects.toBeInstanceOf(RangeError);
			await expect(remote.executeIdempotentSql('INSERT INTO items VALUES (1)', {
				idempotencyKey: ''
			})).rejects.toBeInstanceOf(RangeError);
			const requests = await getRequests(current.worker);
			expect(requests.some((request) => request.path === '/sql')).toBe(false);
		} finally {
			await stopMockServer(current.worker);
		}
	});

	it('rejects SQL when durable status recovery is unavailable', async () => {
		const { url, worker } = await startMockServer('old');
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.sql('SELECT 1')).rejects.toBeInstanceOf(KitUnsupportedError);
			await expect(remote.sql('SELECT 1', { timeoutMs: 10 })).rejects.toBeInstanceOf(KitUnsupportedError);
		} finally {
			await stopMockServer(worker);
		}
	});
});
