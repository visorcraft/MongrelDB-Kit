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
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
