import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema, table, int, text } from './schema.js';
import { eq } from './query.js';

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

	describe('encrypted databases', () => {
		const makeSchema = () => {
			const secrets = table('secrets', {
				columns: [int('id', { primaryKey: true }), text('value')],
				primaryKey: 'id'
			});
			return { schema: new Schema([secrets]), secrets };
		};

		it('createEncryptedSync round-trips rows and reopens with the same passphrase', () => {
			const dir = makeTempDir();
			const { schema, secrets } = makeSchema();
			const db = KitDatabase.createEncryptedSync(dir, schema, 'kit-test-passphrase');
			try {
				db.insertInto(secrets).values({ id: 1n, value: 'hello' }).executeSync();
				db.insertInto(secrets).values({ id: 2n, value: 'world' }).executeSync();
				expect(db.selectFrom(secrets).executeSync().map((r) => r.value)).toEqual([
					'hello',
					'world'
				]);
			} finally {
				db.close();
			}

			const reopened = KitDatabase.openEncryptedSync(dir, schema, 'kit-test-passphrase');
			try {
				const rows = reopened
					.selectFrom(secrets)
					.executeSync()
					.map((r) => ({ id: r.id, value: r.value }));
				expect(rows).toEqual([
					{ id: 1n, value: 'hello' },
					{ id: 2n, value: 'world' }
				]);
			} finally {
				reopened.close();
				rmSync(dir, { recursive: true, force: true });
			}
		});

		it('openEncryptedSync rejects the wrong passphrase', () => {
			const dir = makeTempDir();
			const { schema } = makeSchema();
			const db = KitDatabase.createEncryptedSync(dir, schema, 'correct-passphrase');
			db.close();
			try {
				expect(() =>
					KitDatabase.openEncryptedSync(dir, schema, 'wrong-passphrase')
				).toThrow();
			} finally {
				rmSync(dir, { recursive: true, force: true });
			}
		});

		it('openSync with encryption option creates an encrypted database and reopens it', () => {
			const dir = makeTempDir();
			const { schema, secrets } = makeSchema();
			const db = KitDatabase.openSync(dir, schema, {
				encryption: { passphrase: 'option-passphrase' }
			});
			try {
				db.insertInto(secrets).values({ id: 3n, value: 'option' }).executeSync();
			} finally {
				db.close();
			}

			const reopened = KitDatabase.openEncryptedSync(dir, schema, 'option-passphrase');
			try {
				const row = reopened
					.selectFrom(secrets)
					.where(eq(secrets.id, 3n))
					.executeSync()[0];
				expect(row?.value).toBe('option');
			} finally {
				reopened.close();
				rmSync(dir, { recursive: true, force: true });
			}
		});
	});
});
