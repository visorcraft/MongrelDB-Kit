import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, index } from './schema.js';
import { migrate, type Migration } from './migrate.js';
import { kitSchemaMigrations, kitSchemaCatalog, kitMigrationLocks } from './internalTables.js';
import { KitMigrationError } from './errors.js';
import './query.js';

function makeTempDir(): string {
	return mkdtempSync(join(tmpdir(), 'kit-migrate-'));
}

const users = table('users', {
	columns: [int('id', { primaryKey: true }), text('email', { nullable: false })],
	primaryKey: ['id'],
	indexes: [index(['email'], { name: 'idx_users_email' })]
});

async function withDb(fn: (db: KitDatabase) => Promise<void>): Promise<void> {
	const dir = makeTempDir();
	const db = await KitDatabase.open(dir, new Schema([]));
	try {
		await fn(db);
	} finally {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	}
}

describe('migrate', () => {
	it('creates internal tables and records an applied migration', async () => {
		await withDb(async (db) => {
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: async (ctx) => {
						await ctx.ensureTable(users);
					}
				}
			];

			await migrate(db, new Schema([users]), migrations);

			const records = await db.selectFrom(kitSchemaMigrations).execute();
			expect(records).toHaveLength(1);
			expect(records[0].version).toBe(1n);
			expect(records[0].name).toBe('init');
			expect(records[0].status).toBe('applied');

			const catalog = await db.selectFrom(kitSchemaCatalog).execute();
			expect(catalog).toHaveLength(1);
			expect(catalog[0].schema_version).toBe(1n);
		});
	});

	it('is idempotent when run again with the same migrations', async () => {
		await withDb(async (db) => {
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: async (ctx) => {
						await ctx.ensureTable(users);
					}
				}
			];

			await migrate(db, new Schema([users]), migrations);
			await migrate(db, new Schema([users]), migrations);

			const records = await db.selectFrom(kitSchemaMigrations).execute();
			expect(records).toHaveLength(1);
		});
	});

	it('applies pending migrations', async () => {
		await withDb(async (db) => {
			const first: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: async (ctx) => {
						await ctx.ensureTable(users);
					}
				}
			];
			await migrate(db, new Schema([users]), first);

			let secondRan = false;
			const second: Migration[] = [
				...first,
				{
					version: 2,
					name: 'add users seed',
					up: async (ctx) => {
						await ctx.kit.insertInto(users).values({ id: 1n, email: 'seed@example.com' }).execute();
						secondRan = true;
					}
				}
			];
			await migrate(db, new Schema([users]), second);

			const records = await db.selectFrom(kitSchemaMigrations).execute();
			expect(records).toHaveLength(2);
			expect(records[1].version).toBe(2n);
			expect(records[1].status).toBe('applied');
			expect(secondRan).toBe(true);
		});
	});

	it('prevents concurrent runs with a migration lock', async () => {
		await withDb(async (db) => {
			const future = new Date(Date.now() + 5 * 60 * 1000).toISOString();
			await db.insertInto(kitMigrationLocks).values({
				lock_name: 'default',
				holder: 'other',
				acquired_at: new Date().toISOString(),
				expires_at: future
			}).execute();

			await expect(
				migrate(db, new Schema([]), [{ version: 1, name: 'locked', up: () => undefined }])
			).rejects.toBeInstanceOf(KitMigrationError);
		});
	});

	it('records failed status and releases the lock when a migration fails', async () => {
		await withDb(async (db) => {
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'boom',
					up: async () => {
						throw new Error('intentional failure');
					}
				}
			];

			await expect(migrate(db, new Schema([]), migrations)).rejects.toBeInstanceOf(KitMigrationError);

			const records = await db.selectFrom(kitSchemaMigrations).execute();
			expect(records).toHaveLength(1);
			expect(records[0].status).toBe('failed');

			// Lock should be released so a subsequent run can acquire it.
			await expect(
				migrate(db, new Schema([]), [{ version: 2, name: 'ok', up: () => undefined }])
			).resolves.toBeUndefined();
		});
	});
});
