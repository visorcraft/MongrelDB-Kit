import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ColumnType, ConditionKind } from 'mongreldb/native.js';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, index, unique, foreignKey } from './schema.js';
import { sequenceDefault } from './defaults.js';
import {
	migrate,
	migrationChecksum,
	dropTable,
	addUnique,
	addForeignKey,
	type Migration
} from './migrate.js';
import type { TableSpec } from './types.js';
import {
	kitSchemaMigrations,
	kitSchemaCatalog,
	kitMigrationLocks,
	kitUniqueKeys,
	kitRowGuards
} from './internalTables.js';
import {
	KitMigrationError,
	KitSchemaDriftError,
	KitDuplicateError,
	KitForeignKeyError
} from './errors.js';
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

describe('migrationChecksum', () => {
	it('is content-aware and matches the Rust kit vectors', () => {
		// These exact hexes are also asserted by the Rust kit
		// (`crates/mongreldb-kit-core/src/migrations.rs`) for the same logical
		// migration, proving the canonical serialization is byte-identical.
		expect(
			migrationChecksum({
				version: 1,
				name: 'init',
				ops: [{ kind: 'createTable', name: 'users' }],
				up: () => undefined
			})
		).toBe('fe2f521793591207bd4d8645c2631e4b7ce43e30fe7ea5691a2846c74ea71cc3');
		expect(
			migrationChecksum({
				version: 2,
				name: 'add_email',
				ops: [
					{ kind: 'addColumn', table: 'users', column: 'email' },
					{ kind: 'addUnique', table: 'users', constraint: 'uq_email' }
				],
				up: () => undefined
			})
		).toBe('5b05a0c349b9c6091e7bd6329a64e2a0e1960a1867471896458de79ca996f2d3');
		expect(migrationChecksum({ version: 1, name: 'init', up: () => undefined })).toBe(
			'6408373a4372a2c49859db2a4548ea43308e5ba7dd3609998ca376606cf09757'
		);
	});

	it('changes when version, name, or any op changes', () => {
		const base = migrationChecksum({
			version: 1,
			name: 'init',
			ops: [{ kind: 'createTable', name: 'users' }],
			up: () => undefined
		});
		expect(base).not.toBe(
			migrationChecksum({
				version: 1,
				name: 'init',
				ops: [{ kind: 'createTable', name: 'accounts' }],
				up: () => undefined
			})
		);
		expect(base).not.toBe(
			migrationChecksum({
				version: 1,
				name: 'init',
				ops: [{ kind: 'dropTable', name: 'users' }],
				up: () => undefined
			})
		);
		expect(base).not.toBe(migrationChecksum({ version: 1, name: 'init', up: () => undefined }));
		expect(base).not.toBe(
			migrationChecksum({
				version: 2,
				name: 'init',
				ops: [{ kind: 'createTable', name: 'users' }],
				up: () => undefined
			})
		);
	});

	it('rejects a migration whose ops were edited after it was applied', async () => {
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([]));
		try {
			const original: Migration[] = [
				{
					version: 1,
					name: 'init',
					ops: [{ kind: 'createTable', name: 'users' }],
					up: async (ctx) => {
						await ctx.ensureTable(users);
					}
				}
			];
			await migrate(db, new Schema([users]), original);

			// Same version + name, but the declared ops changed: drift.
			const edited: Migration[] = [
				{
					version: 1,
					name: 'init',
					ops: [{ kind: 'createTable', name: 'accounts' }],
					up: async (ctx) => {
						await ctx.ensureTable(users);
					}
				}
			];
			await expect(migrate(db, new Schema([users]), edited)).rejects.toBeInstanceOf(
				KitSchemaDriftError
			);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe('migration ops', () => {
	async function withSchemaDb(
		tables: TableSpec[],
		fn: (db: KitDatabase) => Promise<void>
	): Promise<void> {
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema(tables));
		try {
			await fn(db);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	}

	function accountsTable() {
		return table('accounts', {
			columns: [int('id', { primaryKey: true }), text('email', { nullable: false })],
			primaryKey: ['id']
		});
	}

	function ownersTable() {
		return table('owners', {
			columns: [int('id', { primaryKey: true }), text('name', { nullable: false })],
			primaryKey: ['id']
		});
	}

	function petsTable() {
		return table('pets', {
			columns: [
				int('id', { primaryKey: true }),
				int('owner_id', { nullable: false }),
				text('name', { nullable: false })
			],
			primaryKey: ['id']
		});
	}

	// The all-text internal guard tables cannot be full-scanned via selectFrom;
	// query them through their bitmap index instead.
	function guardCount(
		db: KitDatabase,
		internalTable: TableSpec,
		columnName: string,
		value: string
	): number {
		const col = internalTable.columns.find((c) => c.name === columnName);
		if (!col) throw new Error(`column ${columnName} not found`);
		return db.nativeDb
			.table(internalTable.name)
			.query([{ kind: ConditionKind.BitmapEq, columnId: col.id, text: value }]).length;
	}

	it('dropTable removes the table and its guards', async () => {
		const accounts = accountsTable();
		await withSchemaDb([accounts], async (db) => {
			await db.insertInto(accounts).values({ id: 1n, email: 'a@example.com' }).execute();
			await addUnique(db, 'accounts', unique(['email'], { name: 'uq_accounts_email' }));

			expect(db.tableNames()).toContain('accounts');
			// One guard: the email unique guard. A single-column PK uses a native
			// existence check, not a guard row.
			expect(guardCount(db, kitUniqueKeys, 'owner_table', 'accounts')).toBe(1);

			await dropTable(db, 'accounts');

			expect(db.tableNames()).not.toContain('accounts');
			expect(guardCount(db, kitUniqueKeys, 'owner_table', 'accounts')).toBe(0);
		});
	});

	it('dropTable rejects an unknown table', async () => {
		await withSchemaDb([], async (db) => {
			await expect(dropTable(db, 'nope')).rejects.toBeInstanceOf(KitMigrationError);
		});
	});

	it('addUnique backfills guards and enforces the constraint afterward', async () => {
		const accounts = accountsTable();
		await withSchemaDb([accounts], async (db) => {
			await db.insertInto(accounts).values({ id: 1n, email: 'a@example.com' }).execute();
			await db.insertInto(accounts).values({ id: 2n, email: 'b@example.com' }).execute();

			await addUnique(db, 'accounts', unique(['email'], { name: 'uq_accounts_email' }));

			// Two email unique guards (single-column PKs use a native check, not a guard).
			expect(guardCount(db, kitUniqueKeys, 'owner_table', 'accounts')).toBe(2);

			// Constraint now enforced: a duplicate email is rejected.
			await expect(
				db.insertInto(accounts).values({ id: 3n, email: 'a@example.com' }).execute()
			).rejects.toBeInstanceOf(KitDuplicateError);

			// A fresh email is still accepted.
			await db.insertInto(accounts).values({ id: 4n, email: 'c@example.com' }).execute();
		});
	});

	it('addUnique rejects existing data that already violates uniqueness', async () => {
		const accounts = accountsTable();
		await withSchemaDb([accounts], async (db) => {
			await db.insertInto(accounts).values({ id: 1n, email: 'dup@example.com' }).execute();
			await db.insertInto(accounts).values({ id: 2n, email: 'dup@example.com' }).execute();

			await expect(
				addUnique(db, 'accounts', unique(['email'], { name: 'uq_accounts_email' }))
			).rejects.toBeInstanceOf(KitMigrationError);
		});
	});

	it('addForeignKey backfills parent row guards and enforces the FK afterward', async () => {
		const owners = ownersTable();
		const pets = petsTable();
		await withSchemaDb([owners, pets], async (db) => {
			await db.insertInto(owners).values({ id: 1n, name: 'Ada' }).execute();
			await db.insertInto(pets).values({ id: 1n, owner_id: 1n, name: 'Rex' }).execute();

			await addForeignKey(
				db,
				'pets',
				foreignKey(['owner_id'], { table: 'owners', columns: ['id'] }, { name: 'fk_pets_owner' })
			);

			expect(guardCount(db, kitRowGuards, 'table_name', 'owners')).toBe(1);

			// FK now enforced: a child pointing at a missing parent is rejected.
			await expect(
				db.insertInto(pets).values({ id: 2n, owner_id: 99n, name: 'Lost' }).execute()
			).rejects.toBeInstanceOf(KitForeignKeyError);
		});
	});

	it('addForeignKey rejects existing children that reference a missing parent', async () => {
		const owners = ownersTable();
		const pets = petsTable();
		await withSchemaDb([owners, pets], async (db) => {
			await db.insertInto(owners).values({ id: 1n, name: 'Ada' }).execute();
			// Insert an orphan child while no FK is enforced yet.
			await db.insertInto(pets).values({ id: 1n, owner_id: 42n, name: 'Orphan' }).execute();

			await expect(
				addForeignKey(
					db,
					'pets',
					foreignKey(['owner_id'], { table: 'owners', columns: ['id'] }, { name: 'fk_pets_owner' })
				)
			).rejects.toBeInstanceOf(KitForeignKeyError);
		});
	});

	it('addColumn forwards the autoIncrement flag for sequence-default columns', () => {
		const widgetsV1 = table('widgets', {
			columns: [text('name', { nullable: false })],
			primaryKey: []
		});
		const widgetsV2 = table('widgets', {
			columns: [
				int('id', { primaryKey: true, default: sequenceDefault('widgets_id_seq') }),
				text('name', { nullable: false })
			],
			primaryKey: ['id']
		});

		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([widgetsV2]));
		try {
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'create_widgets',
					up: (ctx) => {
						ctx.ensureTable(widgetsV1);
					}
				},
				{
					version: 2,
					name: 'add_id',
					up: (ctx) => {
						ctx.addColumn('widgets', widgetsV2.columns[0]);
					}
				}
			];

			db.migrateSync(new Schema([widgetsV2]), migrations);

			const row = db.insertInto(widgetsV2).values({ name: 'a' }).executeSync();
			expect(row.id).toBe(1n);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('seeds engine counters from a legacy __kit_sequences table', () => {
		const widgets = table('widgets', {
			columns: [
				int('id', { primaryKey: true, default: sequenceDefault('widgets_id_seq') }),
				text('name', { nullable: false })
			],
			primaryKey: ['id']
		});

		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([widgets]));
		try {
			// Manually create the pre-switch sequence table and bump the widgets
			// sequence past any existing rows.
			const native = db.nativeDb;
			native.createTable('__kit_sequences', {
				columns: [
					{
						id: 1,
						name: 'sequence_name',
						ty: ColumnType.Bytes,
						primaryKey: true,
						nullable: false
					},
					{
						id: 2,
						name: 'next_value',
						ty: ColumnType.Int64,
						primaryKey: false,
						nullable: false
					}
				],
				indexes: []
			});
			const seq = native.table('__kit_sequences');
			seq.put([
				{ columnId: 1, text: 'widgets_id_seq' },
				{ columnId: 2, int64: 100n }
			]);
			seq.commit();

			const migrations: Migration[] = [
				{
					version: 1,
					name: 'create_widgets',
					up: (ctx) => {
						ctx.ensureTable(widgets);
					}
				}
			];

			db.migrateSync(new Schema([widgets]), migrations);

			const row = db.insertInto(widgets).values({ name: 'a' }).executeSync();
			expect(row.id).toBeGreaterThanOrEqual(100n);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
