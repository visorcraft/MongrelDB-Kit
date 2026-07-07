import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ColumnType, ConditionKind } from '@visorcraft/mongreldb/native.js';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, real, json, timestamp, date, index, unique, foreignKey } from './schema.js';
import { sequenceDefault } from './defaults.js';
import {
	migrate,
	migrationContent,
	migrationChecksum,
	dropTable,
	addIndex,
	dropIndex,
	dropColumn,
	addUnique,
	addForeignKey,
	alterColumn,
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
import { trigger, newColumn, textValue } from './trigger.js';
import { virtualTable, createVirtualTableSql } from './external.js';
import { percentileCont, groupConcat, jsonExtract } from './sql.js';
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

	it('installs triggers through the migration context', () => {
		withDbSync((db) => {
			const audit = table('audit', {
				columns: [int('id', { primaryKey: true }), int('user_id'), text('note')],
				primaryKey: 'id'
			});
			const usersAi = trigger({
				name: 'users_ai',
				target: { kind: 'table', name: 'users' },
				timing: 'after',
				event: 'insert',
				program: {
					steps: [
						{
							kind: 'insert',
							table: 'audit',
							cells: [
								{ column_id: audit.id.id, value: newColumn(users.id.id) },
								{ column_id: audit.user_id.id, value: newColumn(users.id.id) },
								{ column_id: audit.note.id, value: textValue('created') }
							]
						}
					]
				}
			});
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'init with trigger',
					ops: [
						{ kind: 'createTable', name: 'users' },
						{ kind: 'createTable', name: 'audit' },
						{ kind: 'createTrigger', name: 'users_ai', trigger: usersAi }
					],
					up: (ctx) => {
						ctx.ensureTable(users);
						ctx.ensureTable(audit);
						ctx.createTrigger(usersAi);
					}
				}
			];

			db.migrateSync(new Schema([users, audit]), migrations);
			db.insertInto(users).values({ id: 11n, email: 'trigger@example.com' }).executeSync();

			expect(db.triggers().map((t) => t.name)).toEqual(['users_ai']);
			expect(db.selectFrom(audit).executeSync()).toEqual([
				{ id: 11n, user_id: 11n, note: 'created' }
			]);
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
		// An alter_column op (also shared with the Rust kit test).
		expect(
			migrationChecksum({
				version: 3,
				name: 'alter_payload_type',
				ops: [{ kind: 'alterColumn', table: 'weather_cache', column: 'payload_json' }],
				up: () => undefined
			})
		).toBe('eabab2122bc784d989e7b368e93f68d1ba1c08ec82ddd1aa132a94eaf6b5db66');
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

	it('covers trigger and virtual-table ops in canonical migration content', () => {
		const trig = trigger({
			name: 'users_ai',
			target: { kind: 'table', name: 'users' },
			timing: 'after',
			event: 'insert',
			program: {
				steps: [
					{
						kind: 'insert',
						table: 'audit',
						cells: [
							{ column_id: 1, value: newColumn(1) },
							{ column_id: 2, value: textValue('created') }
						]
					}
				]
			}
		});
		const content = migrationContent({
			version: 4,
			name: 'triggers_and_vtabs',
			ops: [
				{ kind: 'createTrigger', name: 'users_ai', trigger: trig },
				{ kind: 'createVirtualTable', table: virtualTable('docs', 'fts_docs') },
				{ kind: 'dropVirtualTable', name: 'old_docs' }
			],
			up: () => undefined
		});

		expect(content).toContain('"op":"create_trigger"');
		expect(content).toContain('"trigger":');
		expect(content).toContain('"op":"create_virtual_table"');
		expect(content).toContain('"module":"fts_docs"');
		expect(content).toContain('"op":"drop_virtual_table"');
		expect(migrationChecksum({ version: 4, name: 'triggers_and_vtabs', up: () => undefined })).not.toBe(
			migrationChecksum({
				version: 4,
				name: 'triggers_and_vtabs',
				ops: [{ kind: 'createTrigger', name: 'users_ai', trigger: trig }],
				up: () => undefined
			})
		);
	});

	it('generates Extended SQL and virtual-table SQL helpers', () => {
		expect(percentileCont(users.id, 0.5).sql).toBe('percentile_cont("id", 0.5)');
		expect(groupConcat(users.email, '|').sql).toBe(`group_concat("email", '|')`);
		expect(jsonExtract('{"a":1}', '$.a').sql).toBe(`json_extract('{"a":1}', '$.a')`);
		expect(createVirtualTableSql(virtualTable('docs', 'fts_docs', ["prefix=1"]))).toBe(
			'CREATE VIRTUAL TABLE "docs" USING "fts_docs"(prefix=1)'
		);
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

		it('addIndex rebuilds the table and preserves rows', async () => {
			const accounts = accountsTable();
			await withSchemaDb([accounts], async (db) => {
				await db.insertInto(accounts).values({ id: 1n, email: 'a@example.com' }).execute();
				await db.insertInto(accounts).values({ id: 2n, email: 'b@example.com' }).execute();

				await addIndex(db, 'accounts', index(['email'], { name: 'idx_accounts_email' }));

				expect(db.schema.table('accounts').indexes.map((idx) => idx.name)).toEqual([
					'idx_accounts_email'
				]);
				const rows = await db.selectFrom(accounts).execute();
				expect(rows.map((row) => row.email).sort()).toEqual(['a@example.com', 'b@example.com']);
			});
		});

		it('dropIndex rebuilds the table and preserves rows', async () => {
			const accounts = table('accounts', {
				columns: [int('id', { primaryKey: true }), text('email', { nullable: false })],
				primaryKey: ['id'],
				indexes: [index(['email'], { name: 'idx_accounts_email' })]
			});
			await withSchemaDb([accounts], async (db) => {
				await db.insertInto(accounts).values({ id: 1n, email: 'a@example.com' }).execute();

				await dropIndex(db, 'accounts', 'idx_accounts_email');

				expect(db.schema.table('accounts').indexes).toEqual([]);
				const rows = await db.selectFrom(accounts).execute();
				expect(rows).toEqual([{ id: 1n, email: 'a@example.com' }]);
			});
		});

		it('dropColumn rebuilds the table, preserves rows, and cleans unique guards', async () => {
			const accounts = accountsTable();
			const accountsWithoutEmail = table('accounts', {
				columns: [int('id', { primaryKey: true })],
				primaryKey: ['id']
			});
			await withSchemaDb([accounts], async (db) => {
				await db.insertInto(accounts).values({ id: 1n, email: 'a@example.com' }).execute();
				await addUnique(db, 'accounts', unique(['email'], { name: 'uq_accounts_email' }));
				expect(guardCount(db, kitUniqueKeys, 'owner_table', 'accounts')).toBe(1);

				await dropColumn(db, 'accounts', 'email');

				expect(db.nativeDb.tableColumns('accounts')).toEqual(['id']);
				expect(db.schema.table('accounts').columns.map((column) => column.name)).toEqual(['id']);
				expect(guardCount(db, kitUniqueKeys, 'owner_table', 'accounts')).toBe(0);
				const rows = await db.selectFrom(accountsWithoutEmail).execute();
				expect(rows).toEqual([{ id: 1n }]);
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

	it('creates timestamp and date columns as Bytes so inserts round-trip', () => {
		const events = table('events', {
			columns: [
				int('id', { primaryKey: true }),
				timestamp('created_at', { nullable: false }),
				date('event_date', { nullable: false })
			],
			primaryKey: ['id']
		});

		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([]));
		try {
			const migrations: Migration[] = [
				{
					version: 1,
					name: 'create_events',
					up: (ctx) => {
						ctx.ensureTable(events);
					}
				}
			];

			db.migrateSync(new Schema([events]), migrations);

			const createdAt = '2026-07-06T12:34:56.789Z';
			const eventDate = '2026-07-06';
			const row = db
				.insertInto(events)
				.values({ id: 1n, created_at: createdAt, event_date: eventDate })
				.executeSync();
			expect(row.created_at).toBe(createdAt);
			expect(row.event_date).toBe(eventDate);

			const selected = db.selectFrom(events).executeSync();
			expect(selected).toEqual([{ id: 1n, created_at: createdAt, event_date: eventDate }]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('alterColumn changes the application type and preserves existing rows', async () => {
		// Schema v1 declares payload as text; the table is created and a row is
		// inserted as a JSON string. Schema v2 redecls payload as json and a
		// migration alters the column in place.
		const cacheV1 = table('weather_cache', {
			columns: [
				int('id', { primaryKey: true }),
				text('payload', { nullable: false })
			],
			primaryKey: ['id']
		});
		const cacheV2 = table('weather_cache', {
			columns: [
				int('id', { primaryKey: true }),
				json('payload', { nullable: false })
			],
			primaryKey: ['id']
		});

		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([cacheV1]));
		try {
			const payload = JSON.stringify({ daily: { time: ['2026-01-01'] } });
			await db.insertInto(cacheV1).values({ id: 1n, payload }).execute();

			await alterColumn(db, 'weather_cache', 'payload', cacheV2.column('payload'));

			// text and json share ColumnType.Bytes, so the stored UTF-8 bytes are
			// untouched and the row reads back identically.
			const rows = await db.selectFrom(cacheV2).execute();
			expect(rows).toHaveLength(1);
			expect(rows[0].payload).toBe(payload);

			// The in-memory schema now reports the json application type.
			const col = db.schema.table('weather_cache').columns.find((c) => c.name === 'payload');
			expect(col?.applicationType).toBe('json');
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('alterColumn runs inside a migration and is recorded in the catalog', async () => {
		const cacheV1 = table('weather_cache', {
			columns: [
				int('id', { primaryKey: true }),
				text('payload', { nullable: false })
			],
			primaryKey: ['id']
		});
		const cacheV2 = table('weather_cache', {
			columns: [
				int('id', { primaryKey: true }),
				json('payload', { nullable: false })
			],
			primaryKey: ['id']
		});

		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([cacheV1]));
		try {
			await db.insertInto(cacheV1).values({ id: 1n, payload: '{"a":1}' }).execute();

			const migrations: Migration[] = [
				{
					version: 1,
					name: 'create_cache',
					ops: [{ kind: 'createTable', name: 'weather_cache' }],
					up: async (ctx) => {
						await ctx.ensureTable(cacheV1);
					}
				},
				{
					version: 2,
					name: 'payload_to_json',
					ops: [{ kind: 'alterColumn', table: 'weather_cache', column: 'payload' }],
					up: async (ctx) => {
						await ctx.alterColumn('weather_cache', 'payload', cacheV2.column('payload'));
					}
				}
			];

			// The schema passed to migrate() is the v2 shape (source of truth).
			await migrate(db, new Schema([cacheV2]), migrations);

			const records = await db.selectFrom(kitSchemaMigrations).execute();
			expect(records.map((r) => Number(r.version))).toEqual([1, 2]);

			const col = db.schema.table('weather_cache').columns.find((c) => c.name === 'payload');
			expect(col?.applicationType).toBe('json');

			const rows = await db.selectFrom(cacheV2).execute();
			expect(rows[0].payload).toBe('{"a":1}');
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('alterColumn renames a native column and preserves existing rows', async () => {
		const widgetsV1 = table('widgets', {
			columns: [int('id', { primaryKey: true }), text('label', { nullable: false })],
			primaryKey: ['id']
		});
		const widgetsV2 = table('widgets', {
			columns: [int('id', { primaryKey: true }), text('name', { nullable: false })],
			primaryKey: ['id']
		});
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([widgetsV1]));
		try {
			const labelId = widgetsV1.column('label').id;
			await db.insertInto(widgetsV1).values({ id: 1n, label: 'one' }).execute();

			await alterColumn(db, 'widgets', 'label', widgetsV2.column('name'));

			expect(db.nativeDb.tableColumns('widgets')).toEqual(['id', 'name']);
			expect(db.schema.table('widgets').column('name').id).toBe(labelId);
			const rows = await db.selectFrom(widgetsV2).execute();
			expect(rows).toEqual([{ id: 1n, name: 'one' }]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('alterColumn changes native storage type when no rows need conversion', async () => {
		const widgetsV1 = table('widgets', {
			columns: [int('id', { primaryKey: true }), int('qty', { nullable: false })],
			primaryKey: ['id']
		});
		const widgetsV2 = table('widgets', {
			columns: [int('id', { primaryKey: true }), real('qty', { nullable: false })],
			primaryKey: ['id']
		});
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([widgetsV1]));
		try {
			await alterColumn(db, 'widgets', 'qty', widgetsV2.column('qty'));
			expect(db.schema.table('widgets').column('qty').storageType).toBe('float64');

			await db.insertInto(widgetsV2).values({ id: 1n, qty: 1.5 }).execute();
			const rows = await db.selectFrom(widgetsV2).execute();
			expect(rows).toEqual([{ id: 1n, qty: 1.5 }]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('alterColumn rejects a storage-type change that would require re-encoding', async () => {
		const widgets = table('widgets', {
			columns: [int('id', { primaryKey: true }), int('qty', { nullable: false })],
			primaryKey: ['id']
		});
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([widgets]));
		try {
			await db.insertInto(widgets).values({ id: 1n, qty: 10n }).execute();
			// int64 -> float64 maps to a different engine ColumnType.
			const realQty = { ...widgets.column('qty'), storageType: 'float64' as const, applicationType: 'float64' as const };
			await expect(alterColumn(db, 'widgets', 'qty', realQty as any)).rejects.toBeInstanceOf(
				KitMigrationError
			);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('alterColumn drops NOT NULL natively', async () => {
		const widgetsV1 = table('widgets', {
			columns: [int('id', { primaryKey: true }), text('name', { nullable: false })],
			primaryKey: ['id']
		});
		const widgetsV2 = table('widgets', {
			columns: [int('id', { primaryKey: true }), text('name', { nullable: true })],
			primaryKey: ['id']
		});
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([widgetsV1]));
		try {
			await db.insertInto(widgetsV1).values({ id: 1n, name: 'one' }).execute();
			await alterColumn(db, 'widgets', 'name', widgetsV2.column('name'));
			expect(db.schema.table('widgets').column('name').nullable).toBe(true);

			await db.insertInto(widgetsV2).values({ id: 2n, name: null }).execute();
			const rows = await db.selectFrom(widgetsV2).execute();
			expect(rows).toEqual([{ id: 1n, name: 'one' }, { id: 2n, name: null }]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('alterColumn rejects SET NOT NULL when existing rows contain nulls', async () => {
		const widgetsV1 = table('widgets', {
			columns: [int('id', { primaryKey: true }), text('name', { nullable: true })],
			primaryKey: ['id']
		});
		const widgetsV2 = table('widgets', {
			columns: [int('id', { primaryKey: true }), text('name', { nullable: false })],
			primaryKey: ['id']
		});
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([widgetsV1]));
		try {
			await db.insertInto(widgetsV1).values({ id: 1n, name: null }).execute();
			await expect(alterColumn(db, 'widgets', 'name', widgetsV2.column('name'))).rejects.toBeInstanceOf(
				KitMigrationError
			);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('alterColumn rejects an unknown column', async () => {
		const widgets = table('widgets', {
			columns: [int('id', { primaryKey: true }), text('name', { nullable: false })],
			primaryKey: ['id']
		});
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([widgets]));
		try {
			await expect(
				alterColumn(db, 'widgets', 'ghost', text('ghost', { nullable: false }))
			).rejects.toBeInstanceOf(KitMigrationError);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
