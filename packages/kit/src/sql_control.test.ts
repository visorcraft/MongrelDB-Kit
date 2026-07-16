import { describe, expect, it, vi } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema } from './schema.js';
import {
	CommitOutcomeError,
	QueryCancelledError,
	QueryIdConflictError,
	QueryTimeoutError,
	ResultLimitExceededError,
	SerializationError
} from './errors.js';

function database() {
	const directory = mkdtempSync(join(tmpdir(), 'kit-sql-control-'));
	return { directory, db: KitDatabase.openSync(directory, new Schema([])) };
}

describe('controlled embedded SQL', () => {
	it('returns a client query ID before async completion', async () => {
		const { directory, db } = database();
		try {
			const query = db.startSql('SELECT 1 AS value');
			expect(query.id).toMatch(/^[0-9a-f]{32}$/);
			await expect(query.result).resolves.toBeDefined();
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it('already-aborted signal does not start native work', async () => {
		const { directory, db } = database();
		try {
			const start = vi.spyOn(db.nativeDb, 'startSql');
			const controller = new AbortController();
			controller.abort();
			const query = db.startSql('SELECT 1', { signal: controller.signal });
			await expect(query.cancel()).resolves.toBe('pre_cancelled');
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
			expect(start).not.toHaveBeenCalled();
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it('abort calls native cancel, removes listener, and maps typed error', async () => {
		const { directory, db } = database();
		try {
			const queryId = 'aaaabbbbccccddddeeeeffff00001111';
			let reject!: (error: Error) => void;
			const result = new Promise<Buffer>((_resolve, rejectPromise) => {
				reject = rejectPromise;
			});
			const cancel = vi.fn(() => {
				reject(new Error('query cancelled'));
				return 'Accepted';
			});
			vi.spyOn(db.nativeDb, 'startSql').mockReturnValue({
				id: queryId,
				cancel,
				status: () => ({
					queryId,
					phase: 'cancelled',
					serverState: 'cancelled',
					terminalState: 'cancelled_before_commit',
					operation: 'SELECT',
					committed: false,
					durableOutcome: { committed: false, committedStatements: 0 },
					terminalErrorCode: 'QUERY_CANCELLED',
					completedStatements: 0,
					statementIndex: 0,
					cancellationReason: 'client_request'
				}),
				result: () => result
			} as never);
			const controller = new AbortController();
			const remove = vi.spyOn(controller.signal, 'removeEventListener');
			const pending = db.sqlRows('SELECT 1', { queryId, signal: controller.signal });
			controller.abort();
			await expect(pending).rejects.toBeInstanceOf(QueryCancelledError);
			expect(cancel).toHaveBeenCalledOnce();
			expect(remove).toHaveBeenCalledWith('abort', expect.any(Function));
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it('maps native deadline and keeps session reusable', async () => {
		const { directory, db } = database();
		try {
			const queryId = '11112222333344445555666677778888';
			const start = vi.spyOn(db.nativeDb, 'startSql');
			start.mockReturnValueOnce({
				id: queryId,
				cancel: () => 'AlreadyFinished',
				status: () => ({
					queryId,
					phase: 'cancelled',
					serverState: 'cancelled',
					terminalState: 'deadline_before_commit',
					operation: 'SELECT',
					committed: false,
					durableOutcome: { committed: false, committedStatements: 0 },
					terminalErrorCode: 'DEADLINE_EXCEEDED',
					completedStatements: 0,
					statementIndex: 0,
					cancellationReason: 'deadline'
				}),
				result: async () => {
					throw new Error('query deadline exceeded');
				}
			} as never);
			await expect(db.sql('SELECT 1', { queryId, timeoutMs: 1 })).rejects.toBeInstanceOf(QueryTimeoutError);
			start.mockRestore();
			const rows = await db.sqlRows('SELECT 2 AS value');
			expect(rows[0].value).toBe(2n);
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it('maps structured native start errors without parsing messages', () => {
		const { directory, db } = database();
		try {
			const error = Object.assign(new Error('opaque'), { code: 'QUERY_ID_CONFLICT' });
			vi.spyOn(db.nativeDb, 'startSql').mockImplementation(() => {
				throw error;
			});
			expect(() => db.startSql('SELECT 1', {
				queryId: 'abcdefabcdefabcdefabcdefabcdefab'
			})).toThrow(QueryIdConflictError);
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it('maps output-limit status to a stable error code', async () => {
		const { directory, db } = database();
		try {
			const queryId = '12341234123412341234123412341234';
			vi.spyOn(db.nativeDb, 'startSql').mockReturnValue({
				id: queryId,
				cancel: () => 'AlreadyFinished',
				status: () => ({
					queryId,
					phase: 'failed',
					serverState: 'failed',
					terminalState: 'failed_before_commit',
					operation: 'SELECT',
					committed: true,
					durableOutcome: { committed: true, committedStatements: 2, lastCommitEpoch: 9007199254740993n },
					terminalErrorCode: 'RESULT_LIMIT_EXCEEDED',
					completedStatements: 3,
					statementIndex: 3,
					cancellationReason: 'none'
				}),
				result: async () => {
					throw new Error('opaque');
				}
			} as never);
			const promise = db.sql('SELECT 1', { queryId, maxOutputRows: 1 });
			await expect(promise).rejects.toMatchObject({
				name: 'ResultLimitExceededError',
				code: 'RESULT_LIMIT_EXCEEDED',
				queryId,
				committed: true,
				committedStatements: 2,
				lastCommitEpoch: 9007199254740993n,
				completedStatements: 3,
				statementIndex: 3
			});
			await promise.catch((error: unknown) => expect(error).toBeInstanceOf(ResultLimitExceededError));
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it('maps serialization status with durable outcome', async () => {
		const { directory, db } = database();
		try {
			const queryId = '43214321432143214321432143214321';
			vi.spyOn(db.nativeDb, 'startSql').mockReturnValue({
				id: queryId,
				cancel: () => 'AlreadyFinished',
				status: () => ({
					queryId,
					phase: 'failed',
					serverState: 'failed',
					terminalState: 'committed_with_error',
					operation: 'INSERT',
					committed: true,
					durableOutcome: { committed: true, committedStatements: 1, lastCommitEpoch: 77n },
					terminalErrorCode: 'SERIALIZATION_FAILED_AFTER_COMMIT',
					completedStatements: 1,
					statementIndex: 0,
					cancellationReason: 'none'
				}),
				result: async () => {
					throw new Error('opaque');
				}
			} as never);
			const promise = db.sql('INSERT INTO t VALUES (1)', { queryId });
			await expect(promise).rejects.toMatchObject({
				name: 'SerializationError',
				code: 'SERIALIZATION_FAILED_AFTER_COMMIT',
				committed: true,
				committedStatements: 1,
				lastCommitEpoch: 77n
			});
			await promise.catch((error: unknown) => expect(error).toBeInstanceOf(SerializationError));
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it('never drops a committed outcome for an unknown terminal error', async () => {
		const { directory, db } = database();
		try {
			const queryId = '98769876987698769876987698769876';
			vi.spyOn(db.nativeDb, 'startSql').mockReturnValue({
				id: queryId,
				cancel: () => 'AlreadyFinished',
				status: () => ({
					queryId,
					phase: 'failed',
					serverState: 'failed',
					terminalState: 'committed_with_error',
					operation: 'DDL',
					committed: true,
					durableOutcome: { committed: true, committedStatements: 1, lastCommitEpoch: 91n },
					terminalErrorCode: 'SQL_EXECUTION_FAILED',
					completedStatements: 1,
					statementIndex: 0,
					cancellationReason: 'none'
				}),
				result: async () => {
					throw new Error('post-commit refresh failed');
				}
			} as never);
			const promise = db.sql('CREATE TABLE t (id BIGINT)', { queryId });
			await expect(promise).rejects.toMatchObject({
				name: 'CommitOutcomeError',
				committed: true,
				committedStatements: 1,
				lastCommitEpoch: 91n
			});
			await promise.catch((error: unknown) => expect(error).toBeInstanceOf(CommitOutcomeError));
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});
});
