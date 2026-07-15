import { describe, expect, it, vi } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema } from './schema.js';
import { QueryCancelledError, QueryTimeoutError } from './errors.js';

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
			await expect(db.sql('SELECT 1', { signal: controller.signal })).rejects.toBeInstanceOf(QueryCancelledError);
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
				reject(new Error(`__QUERY_CANCELLED__:${queryId}:ClientRequest`));
				return true;
			});
			vi.spyOn(db.nativeDb, 'startSql').mockReturnValue({
				id: queryId,
				cancel,
				result: () => result
			});
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
				cancel: () => false,
				result: async () => {
					throw new Error(`__DEADLINE_EXCEEDED__:${queryId}:Some(1)`);
				}
			});
			await expect(db.sql('SELECT 1', { queryId, timeoutMs: 1 })).rejects.toBeInstanceOf(QueryTimeoutError);
			start.mockRestore();
			const rows = await db.sqlRows('SELECT 2 AS value');
			expect(rows[0].value).toBe(2n);
		} finally {
			db.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});
});
