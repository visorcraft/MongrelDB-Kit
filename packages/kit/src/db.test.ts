import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema } from './schema.js';

function makeTempDir(): string {
	return mkdtempSync(join(tmpdir(), 'kit-db-test-'));
}

describe('KitDatabase', () => {
	it('open creates temp directory, internal tables exist, app table list is empty', async () => {
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([]));
		try {
			expect(db.tableNames()).toEqual([]);
			await expect(db.allocateSequence('probe', 1)).resolves.toBe(1n);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('allocateSequence returns sequential values', async () => {
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([]));
		try {
			const a = await db.allocateSequence('foo', 1);
			const b = await db.allocateSequence('foo', 1);
			const c = await db.allocateSequence('foo', 5);
			const d = await db.allocateSequence('foo', 1);
			// 1-based (AUTO_INCREMENT): 1, then 2, then reserve 5 from 3, then 8.
			expect(a).toBe(1n);
			expect(b).toBe(2n);
			expect(c).toBe(3n);
			expect(d).toBe(8n);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('close does not throw', async () => {
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([]));
		expect(() => db.close()).not.toThrow();
		rmSync(dir, { recursive: true, force: true });
	});

	it('openSync creates temp directory, internal tables exist, app table list is empty', () => {
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([]));
		try {
			expect(db.tableNames()).toEqual([]);
			expect(db.allocateSequenceSync('probe', 1)).toBe(1n);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
