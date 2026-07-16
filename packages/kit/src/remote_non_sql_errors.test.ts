import { beforeEach, describe, expect, it, vi } from 'vitest';

const native = vi.hoisted(() => ({ error: undefined as unknown, response: '{}' }));

vi.mock('@visorcraft/mongreldb/native.js', () => ({
	RemoteDatabase: class {
		commit(): never {
			throw native.error;
		}

		dropProcedure(): never {
			throw native.error;
		}

		dropTrigger(): never {
			throw native.error;
		}

		createProcedure(): string {
			return native.response;
		}

		callProcedure(): string {
			return native.response;
		}
	}
}));

import { CommitOutcomeError, QueryOutcomeUnknownError } from './errors.js';
import { RemoteDatabase } from './remote.js';

describe('remote non-SQL outcome errors', () => {
	beforeEach(() => {
		native.error = undefined;
		native.response = '{}';
	});

	it('keeps committed state, exact epoch, and retryability', () => {
		native.error = Object.assign(new Error('commit completed'), {
			code: 'COMMIT_OUTCOME',
			outcomeKnown: true,
			committed: true,
			epoch: 42n,
			retryable: false,
			status: 'committed'
		});
		const remote = new RemoteDatabase('http://127.0.0.1:8453');
		try {
			remote.commit('users');
			expect.fail('commit should fail with its durable outcome');
		} catch (error) {
			expect(error).toBeInstanceOf(CommitOutcomeError);
			const outcome = error as CommitOutcomeError;
			expect(outcome.code).toBe('COMMIT_OUTCOME');
			expect(outcome.committed).toBe(true);
			expect(outcome.lastCommitEpoch).toBe(42n);
			expect(outcome.retryable).toBe(false);
			expect(outcome.serverState).toBe('committed');
		}
	});

	it('maps nested procedure outcome metadata without flattening loss', () => {
		native.error = Object.assign(new Error('procedure committed'), {
			code: 'COMMIT_OUTCOME',
			remoteQueryError: {
				code: 'COMMIT_OUTCOME',
				queryId: 'unknown',
				outcomeKnown: true,
				committed: true,
				epochText: '18446744073709551615',
				retryable: false,
				serverState: 'committed'
			}
		});
		const remote = new RemoteDatabase('http://127.0.0.1:8453');
		expect(() => remote.dropProcedure('p')).toThrow(CommitOutcomeError);
		try {
			remote.dropProcedure('p');
		} catch (error) {
			const outcome = error as CommitOutcomeError;
			expect(outcome.committed).toBe(true);
			expect(outcome.lastCommitEpoch).toBe(18_446_744_073_709_551_615n);
			expect(outcome.retryable).toBe(false);
		}
	});

	it('keeps trigger outcome unknown typed and non-retryable', () => {
		native.error = Object.assign(new Error('commit status unknown'), {
			code: 'QUERY_OUTCOME_UNKNOWN',
			outcomeKnown: false,
			committed: null,
			retryable: false,
			status: 'outcome_unknown'
		});
		const remote = new RemoteDatabase('http://127.0.0.1:8453');
		try {
			remote.dropTrigger('t');
			expect.fail('trigger write should fail with unknown outcome');
		} catch (error) {
			expect(error).toBeInstanceOf(QueryOutcomeUnknownError);
			const outcome = error as QueryOutcomeUnknownError;
			expect(outcome.committed).toBeNull();
			expect(outcome.retryable).toBe(false);
			expect(outcome.serverState).toBe('outcome_unknown');
		}
	});

	it('fails closed when committed epoch fields conflict', () => {
		native.error = Object.assign(new Error('commit completed'), {
			code: 'COMMIT_OUTCOME',
			outcomeKnown: true,
			committed: true,
			epoch: 41n,
			epochText: '42',
			retryable: false
		});
		const remote = new RemoteDatabase('http://127.0.0.1:8453');
		expect(() => remote.commit('users')).toThrow(QueryOutcomeUnknownError);
	});

	it('fails closed when nested durable fields conflict', () => {
		native.error = Object.assign(new Error('commit completed'), {
			code: 'COMMIT_OUTCOME',
			queryId: 'unknown',
			outcomeKnown: true,
			committed: true,
			epoch: 42n,
			retryable: false,
			remoteQueryError: {
				code: 'COMMIT_OUTCOME',
				queryId: 'other',
				outcomeKnown: true,
				committed: false,
				epochText: '42',
				retryable: true
			}
		});
		const remote = new RemoteDatabase('http://127.0.0.1:8453');
		expect(() => remote.commit('users')).toThrow(QueryOutcomeUnknownError);
	});

	it('reports invalid successful write JSON as committed', () => {
		native.response = '{"epoch":42,"epoch":43}';
		const remote = new RemoteDatabase('http://127.0.0.1:8453');
		expect(() => remote.createProcedure({
			name: 'p',
			mode: 'read_write',
			body: { sql: 'SELECT 1' }
		})).toThrow(CommitOutcomeError);
	});

	it('requires explicit exact procedure commit metadata', () => {
		const remote = new RemoteDatabase('http://127.0.0.1:8453');
		for (const response of [
			{ status: 'ok', committed: false, epoch: null, epoch_text: null, result: null },
			{ status: 'ok', committed: true, epoch: 9, epoch_text: '9', result: {} }
		]) {
			native.response = JSON.stringify(response);
			expect(remote.callProcedure('p')).toEqual(response);
		}
		for (const response of [
			{ status: 'ok', epoch: null, epoch_text: null, result: null },
			{ status: 'ok', committed: false, epoch: 9, epoch_text: '9', result: null },
			{ status: 'ok', committed: true, epoch: 9, epoch_text: '09', result: null },
			{ status: 'ok', committed: true, epoch: null, epoch_text: null, result: null }
		]) {
			native.response = JSON.stringify(response);
			expect(() => remote.callProcedure('p')).toThrow(QueryOutcomeUnknownError);
		}
	});
});
