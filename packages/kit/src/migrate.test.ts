import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, index } from './schema.js';
import { migrate, type Migration } from './migrate.js';
import { kitSchemaMigrations, kitSchemaCatalog, kitMigrationLocks } from './internalTables.js';
import { KitMigrationError, KitSchemaDriftError } from './errors.js';
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

function withDbSync(fn: (db: KitDatabase) => void): void {
	const dir = makeTempDir();
	const db = KitDatabase.openSync(dir, new Schema([]));
	try {
		fn(db);
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

	it('rejects a renamed historical migration as schema drift', async () => {
		await withDb(async (db) => {
			const original: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: async (ctx) => {
						await ctx.ensureTable(users);
					}
				}
			];
			await migrate(db, new Schema([users]), original);

			// Rename the historical migration in the supplied list. The stored
			// checksum/name no longer matches.
			const renamed: Migration[] = [
				{ version: 1, name: 'edited', up: async () => undefined }
			];
			await expect(migrate(db, new Schema([users]), renamed)).rejects.toBeInstanceOf(
				KitSchemaDriftError
			);
		});
	});

	it('rejects a missing historical migration as schema drift', async () => {
		await withDb(async (db) => {
			const v1: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: async (ctx) => {
						await ctx.ensureTable(users);
					}
				}
			];
			await migrate(db, new Schema([users]), v1);

			// Supply a different migration list that omits version 1.
			const empty: Migration[] = [];
			await expect(migrate(db, new Schema([users]), empty)).rejects.toBeInstanceOf(
				KitSchemaDriftError
			);
		});
	});
});

describe('migrateSync', () => {
	it('creates internal tables and records an applied migration', () => {
		withDbSync((db) => {
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: (ctx) => {
						ctx.ensureTable(users);
					}
				}
			];

			db.migrateSync(new Schema([users]), migrations);

			const records = db.selectFrom(kitSchemaMigrations).executeSync();
			expect(records).toHaveLength(1);
			expect(records[0].version).toBe(1n);
			expect(records[0].name).toBe('init');
			expect(records[0].status).toBe('applied');

			const catalog = db.selectFrom(kitSchemaCatalog).executeSync();
			expect(catalog).toHaveLength(1);
			expect(catalog[0].schema_version).toBe(1n);
		});
	});

	it('is idempotent when run again with the same migrations', () => {
		withDbSync((db) => {
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: (ctx) => {
						ctx.ensureTable(users);
					}
				}
			];

			db.migrateSync(new Schema([users]), migrations);
			db.migrateSync(new Schema([users]), migrations);

			const records = db.selectFrom(kitSchemaMigrations).executeSync();
			expect(records).toHaveLength(1);
		});
	});

	it('applies pending migrations', () => {
		withDbSync((db) => {
			const first: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: (ctx) => {
						ctx.ensureTable(users);
					}
				}
			];
			db.migrateSync(new Schema([users]), first);

			let secondRan = false;
			const second: Migration[] = [
				...first,
				{
					version: 2,
					name: 'add users seed',
					up: (ctx) => {
						ctx.kit.insertInto(users).values({ id: 1n, email: 'seed@example.com' }).executeSync();
						secondRan = true;
					}
				}
			];
			db.migrateSync(new Schema([users]), second);

			const records = db.selectFrom(kitSchemaMigrations).executeSync();
			expect(records).toHaveLength(2);
			expect(records[1].version).toBe(2n);
			expect(records[1].status).toBe('applied');
			expect(secondRan).toBe(true);
		});
	});

	it('prevents concurrent runs with a migration lock', () => {
		withDbSync((db) => {
			const future = new Date(Date.now() + 5 * 60 * 1000).toISOString();
			db.insertInto(kitMigrationLocks)
				.values({
					lock_name: 'default',
					holder: 'other',
					acquired_at: new Date().toISOString(),
					expires_at: future
				})
				.executeSync();

			expect(() =>
				db.migrateSync(new Schema([]), [{ version: 1, name: 'locked', up: () => undefined }])
			).toThrow(KitMigrationError);
		});
	});

	it('records failed status and releases the lock when a migration fails', () => {
		withDbSync((db) => {
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'boom',
					up: () => {
						throw new Error('intentional failure');
					}
				}
			];

			expect(() => db.migrateSync(new Schema([]), migrations)).toThrow(KitMigrationError);

			const records = db.selectFrom(kitSchemaMigrations).executeSync();
			expect(records).toHaveLength(1);
			expect(records[0].status).toBe('failed');

			// Lock should be released so a subsequent run can acquire it.
			expect(() =>
				db.migrateSync(new Schema([]), [{ version: 2, name: 'ok', up: () => undefined }])
			).not.toThrow();
		});
	});

	it('rejects a renamed historical migration as schema drift', () => {
		withDbSync((db) => {
			const original: Migration[] = [
				{
					version: 1,
					name: 'init',
					up: (ctx) => {
						ctx.ensureTable(users);
					}
				}
			];
			db.migrateSync(new Schema([users]), original);

			const renamed: Migration[] = [
				{ version: 1, name: 'edited', up: () => undefined }
			];
			expect(() => db.migrateSync(new Schema([users]), renamed)).toThrow(KitSchemaDriftError);
		});
	});
});
