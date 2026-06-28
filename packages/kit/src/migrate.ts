import { createHash, randomUUID } from 'node:crypto';
import type { Database as NativeDatabase, Cell, RowJs } from 'mongreldb/native.js';
import { ColumnType, IndexKindSpec, ConditionKind } from 'mongreldb/native.js';
import { tableFromIPC } from 'apache-arrow';
import { KitDatabase } from './db.js';
import type { Schema } from './schema.js';
import type { TableSpec, ColumnSpec, IndexSpec, UniqueSpec, ForeignKeySpec, ColumnStorageType } from './types.js';
import { toCells } from './constraints.js';
import { validateRow } from './validation.js';

import { KitMigrationError, KitSchemaDriftError } from './errors.js';
import { kitSchemaMigrations, kitSchemaCatalog, kitMigrationLocks } from './internalTables.js';


const I64_MIN = -9_223_372_036_854_775_808n;
const I64_MAX = 9_223_372_036_854_775_807n;
const LOCK_NAME = 'default';
const LOCK_HOLDER = 'kit';
const LOCK_TTL_MS = 5 * 60 * 1000;

export interface Migration {
	version: number;
	name: string;
	up: (ctx: MigrationContext) => Promise<void> | void;
}

export interface MigrationContext {
	kit: KitDatabase;
	db: NativeDatabase;
	ensureTable: (table: TableSpec) => Promise<void> | void;
	addColumn: (tableName: string, column: ColumnSpec) => Promise<void> | void;
	sql: (sql: string) => Promise<unknown[]> | unknown[];
}

type MongrelSchemaSpec = {
	columns: { id: number; name: string; ty: number; primaryKey: boolean; nullable: boolean }[];
	indexes: { name: string; columnId: number; kind: number }[];
};

function columnId(table: TableSpec, name: string): number {
	const col = table.columns.find((c) => c.name === name);
	if (!col) {
		throw new Error(`Column "${name}" not found in table "${table.name}"`);
	}
	return col.id;
}

function isoNow(): string {
	return new Date().toISOString();
}

function toMongrelColumnType(storageType: ColumnStorageType): number {
	switch (storageType) {
		case 'bool':
			return ColumnType.Bool;
		case 'int64':
		case 'timestamp':
			return ColumnType.Int64;
		case 'float64':
			return ColumnType.Float64;
		case 'date':
			return ColumnType.Date32;
		case 'text':
		case 'bytes':
		case 'json':
			return ColumnType.Bytes;
	}
}

function toMongrelSchema(table: TableSpec): MongrelSchemaSpec {
	const indexes = table.indexes.flatMap((idx) =>
		idx.columns.map((colName) => {
			const col = table.columns.find((c) => c.name === colName);
			if (!col) {
				throw new Error(`Index column "${colName}" not found in table "${table.name}"`);
			}
			return {
				name: `${idx.name}_${colName}`,
				columnId: col.id,
				kind: IndexKindSpec.Bitmap
			};
		})
	);

	const indexedColumns = new Set(table.indexes.flatMap((idx) => idx.columns));
	for (const pk of table.primaryKey) {
		if (indexedColumns.has(pk)) continue;
		const col = table.columns.find((c) => c.name === pk);
		if (!col) {
			throw new Error(`Primary key column "${pk}" not found in table "${table.name}"`);
		}
		indexes.push({
			name: `pk_${pk}`,
			columnId: col.id,
			kind: IndexKindSpec.Bitmap
		});
		indexedColumns.add(pk);
	}

	for (const fk of table.foreignKeys) {
		for (const colName of fk.columns) {
			if (indexedColumns.has(colName)) continue;
			const col = table.columns.find((c) => c.name === colName);
			if (!col) {
				throw new Error(`Foreign key column "${colName}" not found in table "${table.name}"`);
			}
			indexes.push({
				name: `fk_${fk.name}_${colName}`,
				columnId: col.id,
				kind: IndexKindSpec.Bitmap
			});
			indexedColumns.add(colName);
		}
	}

	return {
		columns: table.columns.map((col) => ({
			id: col.id,
			name: col.name,
			ty: toMongrelColumnType(col.storageType),
			primaryKey: col.primaryKey,
			nullable: col.nullable
		})),
		indexes
	};
}

function cellValue(cell: Cell | undefined): unknown {
	if (!cell) return null;
	if (cell.text !== undefined) return cell.text;
	if (cell.int64 !== undefined) return cell.int64;
	if (cell.boolean !== undefined) return cell.boolean;
	if (cell.float64 !== undefined) return cell.float64;
	if (cell.bytes !== undefined) return cell.bytes;
	return null;
}

function rowFromRowJs(table: TableSpec, rowJs: RowJs): Record<string, unknown> {
	const row: Record<string, unknown> = {};
	for (const col of table.columns) {
		const cell = rowJs.cells.find((c) => c.columnId === col.id);
		row[col.name] = cellValue(cell);
	}
	return row;
}

function fullScanCondition(table: TableSpec) {
	const intColumn = table.columns.find((c) => c.storageType === 'int64' || c.storageType === 'timestamp');
	if (intColumn) {
		return {
			kind: ConditionKind.RangeInt,
			columnId: intColumn.id,
			int64Lo: I64_MIN,
			int64Hi: I64_MAX
		};
	}

	const floatColumn = table.columns.find((c) => c.storageType === 'float64');
	if (floatColumn) {
		return {
			kind: ConditionKind.RangeF64,
			columnId: floatColumn.id,
			float64Lo: -Infinity,
			float64Hi: Infinity
		};
	}

	throw new KitMigrationError(
		`Full table scan on "${table.name}" requires an int64, timestamp, or float64 column`
	);
}

function fullScanRows(db: NativeDatabase, table: TableSpec): { rowId: bigint; row: Record<string, unknown> }[] {
	const condition = fullScanCondition(table);
	return db
		.table(table.name)
		.query([condition])
		.map((rowJs) => ({ rowId: rowJs.rowId, row: rowFromRowJs(table, rowJs) }));
}

function migrationChecksum(migration: Migration): string {
	return createHash('sha256').update(`${migration.version}:${migration.name}`).digest('hex');
}

function schemaCatalogJson(schema: Schema): string {
	return JSON.stringify(
		schema.tablesList().map((t) => ({
			tableId: t.tableId,
			name: t.name,
			columns: t.columns.map((c) => ({
				id: c.id,
				name: c.name,
				storageType: c.storageType,
				applicationType: c.applicationType,
				nullable: c.nullable,
				primaryKey: c.primaryKey,
				enumValues: c.enumValues,
				min: c.min,
				max: c.max,
				minLength: c.minLength,
				maxLength: c.maxLength,
				regex: c.regex?.source
			})),
			primaryKey: t.primaryKey,
			indexes: t.indexes,
			foreignKeys: t.foreignKeys,
			unique: t.unique,
			checks: t.checks.map((c) => ({ name: c.name }))
		}))
	);
}

async function runSql(db: NativeDatabase, sql: string): Promise<unknown[]> {
	const ipc = await db.sql(sql);
	const arrowTable = tableFromIPC(ipc);
	return [...arrowTable].map((row) => ({ ...row }));
}

async function acquireLock(kit: KitDatabase): Promise<void> {
	const db = kit.nativeDb;
	const locks = db.table('__kit_migration_locks');
	const now = new Date();
	const expiresAt = new Date(now.getTime() + LOCK_TTL_MS);

	const existing = locks.getByPkText(LOCK_NAME);
	if (existing) {
		const expiresAtCell = existing.cells.find((c) => c.columnId === columnId(kitMigrationLocks, 'expires_at'));
		const existingExpires = expiresAtCell?.text;
		if (existingExpires && new Date(existingExpires) > now) {
			throw new KitMigrationError('migration lock is already held');
		}
		locks.deleteByPkText(LOCK_NAME);
	}

	locks.put([
		{ columnId: columnId(kitMigrationLocks, 'lock_name'), text: LOCK_NAME },
		{ columnId: columnId(kitMigrationLocks, 'holder'), text: LOCK_HOLDER },
		{ columnId: columnId(kitMigrationLocks, 'acquired_at'), text: now.toISOString() },
		{ columnId: columnId(kitMigrationLocks, 'expires_at'), text: expiresAt.toISOString() }
	]);
	locks.commit();
}

async function releaseLock(kit: KitDatabase): Promise<void> {
	const locks = kit.nativeDb.table('__kit_migration_locks');
	locks.deleteByPkText(LOCK_NAME);
	locks.commit();
}

async function readAppliedMigrations(kit: KitDatabase): Promise<{ version: bigint; name: string; checksum: string; status: string }[]> {
	const rows = fullScanRows(kit.nativeDb, kitSchemaMigrations);
	return rows
		.map((m) => ({
			version: m.row.version as bigint,
			name: m.row.name as string,
			checksum: m.row.checksum as string,
			status: m.row.status as string
		}))
		.sort((a, b) => Number(a.version - b.version));
}

async function insertMigrationRecord(
	kit: KitDatabase,
	migration: Migration,
	status: 'in_progress' | 'applied' | 'failed'
): Promise<void> {
	const table = kit.nativeDb.table('__kit_schema_migrations');
	table.put([
		{ columnId: columnId(kitSchemaMigrations, 'version'), int64: BigInt(migration.version) },
		{ columnId: columnId(kitSchemaMigrations, 'name'), text: migration.name },
		{ columnId: columnId(kitSchemaMigrations, 'checksum'), text: migrationChecksum(migration) },
		{ columnId: columnId(kitSchemaMigrations, 'applied_at'), text: isoNow() },
		{ columnId: columnId(kitSchemaMigrations, 'kit_version'), text: kitVersion() },
		{ columnId: columnId(kitSchemaMigrations, 'status'), text: status }
	]);
	table.commit();
}

async function updateMigrationStatus(
	kit: KitDatabase,
	version: number,
	status: 'applied' | 'failed'
): Promise<void> {
	const table = kit.nativeDb.table('__kit_schema_migrations');
	const rowJs = table.getByPkInt64(BigInt(version));
	if (!rowJs) {
		throw new KitMigrationError(`migration record ${version} not found`);
	}
	const row = rowFromRowJs(kitSchemaMigrations, rowJs);
	const updated = { ...row, status };
	table.put(toCells(kitSchemaMigrations, updated));
	table.commit();
}

async function writeSchemaCatalog(kit: KitDatabase, schema: Schema): Promise<void> {
	const db = kit.nativeDb;
	const catalog = db.table('__kit_schema_catalog');
	const schemaJson = schemaCatalogJson(schema);
	const checksum = createHash('sha256').update(schemaJson).digest('hex');
	const now = isoNow();

	await db.transaction(async (txn) => {
		txn.put('__kit_schema_catalog', [
			{ columnId: columnId(kitSchemaCatalog, 'schema_version'), int64: 1n },
			{ columnId: columnId(kitSchemaCatalog, 'schema_json'), text: schemaJson },
			{ columnId: columnId(kitSchemaCatalog, 'checksum'), text: checksum },
			{ columnId: columnId(kitSchemaCatalog, 'written_at'), text: now }
		]);
	});
}

function kitVersion(): string {
	// Keep in sync with package.json. Avoiding a JSON import keeps the ESM bundle simple.
	return '0.1.0';
}

function makeContext(kit: KitDatabase): MigrationContext {
	return {
		kit,
		db: kit.nativeDb,
		ensureTable: (table: TableSpec) => createTable(kit, table),
		addColumn: (tableName: string, column: ColumnSpec) => addColumn(kit, tableName, column),
		sql: (sql: string) => runSql(kit.nativeDb, sql)
	};
}

function acquireLockSync(kit: KitDatabase): void {
	const db = kit.nativeDb;
	const locks = db.table('__kit_migration_locks');
	const now = new Date();
	const expiresAt = new Date(now.getTime() + LOCK_TTL_MS);

	const existing = locks.getByPkText(LOCK_NAME);
	if (existing) {
		const expiresAtCell = existing.cells.find(
			(c) => c.columnId === columnId(kitMigrationLocks, 'expires_at')
		);
		const existingExpires = expiresAtCell?.text;
		if (existingExpires && new Date(existingExpires) > now) {
			throw new KitMigrationError('migration lock is already held');
		}
		locks.deleteByPkText(LOCK_NAME);
	}

	locks.put([
		{ columnId: columnId(kitMigrationLocks, 'lock_name'), text: LOCK_NAME },
		{ columnId: columnId(kitMigrationLocks, 'holder'), text: LOCK_HOLDER },
		{ columnId: columnId(kitMigrationLocks, 'acquired_at'), text: now.toISOString() },
		{ columnId: columnId(kitMigrationLocks, 'expires_at'), text: expiresAt.toISOString() }
	]);
	locks.commit();
}

function releaseLockSync(kit: KitDatabase): void {
	const locks = kit.nativeDb.table('__kit_migration_locks');
	locks.deleteByPkText(LOCK_NAME);
	locks.commit();
}

function readAppliedMigrationsSync(kit: KitDatabase): { version: bigint; name: string; checksum: string; status: string }[] {
	const rows = fullScanRows(kit.nativeDb, kitSchemaMigrations);
	return rows
		.map((m) => ({
			version: m.row.version as bigint,
			name: m.row.name as string,
			checksum: m.row.checksum as string,
			status: m.row.status as string
		}))
		.sort((a, b) => Number(a.version - b.version));
}

function insertMigrationRecordSync(
	kit: KitDatabase,
	migration: Migration,
	status: 'in_progress' | 'applied' | 'failed'
): void {
	const table = kit.nativeDb.table('__kit_schema_migrations');
	table.put([
		{ columnId: columnId(kitSchemaMigrations, 'version'), int64: BigInt(migration.version) },
		{ columnId: columnId(kitSchemaMigrations, 'name'), text: migration.name },
		{ columnId: columnId(kitSchemaMigrations, 'checksum'), text: migrationChecksum(migration) },
		{ columnId: columnId(kitSchemaMigrations, 'applied_at'), text: isoNow() },
		{ columnId: columnId(kitSchemaMigrations, 'kit_version'), text: kitVersion() },
		{ columnId: columnId(kitSchemaMigrations, 'status'), text: status }
	]);
	table.commit();
}

function updateMigrationStatusSync(
	kit: KitDatabase,
	version: number,
	status: 'applied' | 'failed'
): void {
	const table = kit.nativeDb.table('__kit_schema_migrations');
	const rowJs = table.getByPkInt64(BigInt(version));
	if (!rowJs) {
		throw new KitMigrationError(`migration record ${version} not found`);
	}
	const row = rowFromRowJs(kitSchemaMigrations, rowJs);
	const updated = { ...row, status };
	table.put(toCells(kitSchemaMigrations, updated));
	table.commit();
}

function writeSchemaCatalogSync(kit: KitDatabase, schema: Schema): void {
	const db = kit.nativeDb;
	const schemaJson = schemaCatalogJson(schema);
	const checksum = createHash('sha256').update(schemaJson).digest('hex');
	const now = isoNow();

	const txn = db.begin();
	try {
		txn.put('__kit_schema_catalog', [
			{ columnId: columnId(kitSchemaCatalog, 'schema_version'), int64: 1n },
			{ columnId: columnId(kitSchemaCatalog, 'schema_json'), text: schemaJson },
			{ columnId: columnId(kitSchemaCatalog, 'checksum'), text: checksum },
			{ columnId: columnId(kitSchemaCatalog, 'written_at'), text: now }
		]);
		txn.commit();
	} catch (err) {
		txn.rollback();
		throw err;
	}
}

function makeContextSync(kit: KitDatabase): MigrationContext {
	return {
		kit,
		db: kit.nativeDb,
		ensureTable: (table: TableSpec) => createTableSync(kit, table),
		addColumn: (tableName: string, column: ColumnSpec) => addColumnSync(kit, tableName, column),
		sql: () => {
			throw new KitMigrationError('sql() is not available in synchronous migrations');
		}
	};
}

export function migrateSync(kit: KitDatabase, schema: Schema, migrations: Migration[]): void {
	acquireLockSync(kit);

	try {
		migrations = [...migrations].sort((a, b) => a.version - b.version);

		const applied = readAppliedMigrationsSync(kit);
		verifyMigrationChecksums(applied, migrations);

		const maxApplied = applied.reduce((max, m) => (m.version > max ? m.version : max), 0n);
		const pending = migrations.filter((m) => BigInt(m.version) > maxApplied);

		for (const migration of pending) {
			insertMigrationRecordSync(kit, migration, 'in_progress');

			const txn = kit.nativeDb.begin();
			try {
				const result = migration.up(makeContextSync(kit));
				if (result && typeof (result as Promise<void>).then === 'function') {
					throw new KitMigrationError('async migration up() cannot be used with migrateSync');
				}
				txn.commit();
				updateMigrationStatusSync(kit, migration.version, 'applied');
			} catch (cause) {
				txn.rollback();
				updateMigrationStatusSync(kit, migration.version, 'failed');
				const message = cause instanceof Error ? cause.message : String(cause);
				throw new KitMigrationError(`migration ${migration.version} failed: ${message}`);
			}
		}

		writeSchemaCatalogSync(kit, schema);
	} finally {
		releaseLockSync(kit);
	}
}

export async function migrate(kit: KitDatabase, schema: Schema, migrations: Migration[]): Promise<void> {
	await acquireLock(kit);

	try {
		migrations = [...migrations].sort((a, b) => a.version - b.version);

		const applied = await readAppliedMigrations(kit);
		verifyMigrationChecksums(applied, migrations);

		const maxApplied = applied.reduce((max, m) => (m.version > max ? m.version : max), 0n);
		const pending = migrations.filter((m) => BigInt(m.version) > maxApplied);

		for (const migration of pending) {
			await insertMigrationRecord(kit, migration, 'in_progress');

			try {
				await kit.nativeDb.transaction(async () => {
					await migration.up(makeContext(kit));
				});
				await updateMigrationStatus(kit, migration.version, 'applied');
			} catch (cause) {
				await updateMigrationStatus(kit, migration.version, 'failed');
				const message = cause instanceof Error ? cause.message : String(cause);
				throw new KitMigrationError(`migration ${migration.version} failed: ${message}`);
			}
		}

		await writeSchemaCatalog(kit, schema);
	} finally {
		await releaseLock(kit);
	}
}

/**
 * Reject edited or reordered historical migrations by comparing the stored
 * checksum/name of every applied migration record against the corresponding
 * migration in the supplied list. Drift here would silently change the meaning
 * of an already-applied migration and corrupt the schema catalog.
 */
function verifyMigrationChecksums(
	applied: { version: bigint; name: string; checksum: string; status: string }[],
	migrations: Migration[]
): void {
	const byVersion = new Map(migrations.map((m) => [BigInt(m.version), m]));
	for (const record of applied) {
		if (record.status === 'failed') {
			// Failed migrations are not part of the canonical history; the
			// caller is expected to repair them before re-running.
			continue;
		}
		const current = byVersion.get(record.version);
		if (!current) {
			throw new KitSchemaDriftError(
				`migration ${record.version} (${record.name}) is recorded as applied but is missing from the supplied migrations list`
			);
		}
		const expected = migrationChecksum(current);
		if (expected !== record.checksum || current.name !== record.name) {
			throw new KitSchemaDriftError(
				`migration ${record.version} (${record.name}) checksum mismatch: stored ${record.checksum}, expected ${expected}`
			);
		}
	}
}

function createTableSync(kit: KitDatabase, table: TableSpec): void {
	if (kit.nativeDb.tableNames().includes(table.name)) return;
	kit.nativeDb.createTable(table.name, toMongrelSchema(table));
}

export async function createTable(kit: KitDatabase, table: TableSpec): Promise<void> {
	createTableSync(kit, table);
}

export async function dropTable(_kit: KitDatabase, _tableName: string): Promise<void> {
	throw new KitMigrationError('dropTable is not implemented yet');
}

function computeDefaultValue(column: ColumnSpec, kit: KitDatabase): unknown {
	const source = column.default ?? (column.generated === 'uuid' ? { kind: 'uuid' } : column.generated === 'now' ? { kind: 'now' } : null);
	if (!source) return null;

	switch (source.kind) {
		case 'static':
			return source.value;
		case 'now':
			return isoNow();
		case 'uuid':
			return randomUUID();
		case 'sequence':
			// Synchronous allocation is not available from KitDatabase; sequence defaults
			// during migration backfills are not supported yet.
			throw new KitMigrationError(`sequence default for column "${column.name}" is not supported in migrations`);
		case 'custom':
			return source.fn();
		default:
			return null;
	}
}

function addColumnSync(kit: KitDatabase, tableName: string, column: ColumnSpec): void {
	if (!column.nullable && !column.default && !column.generated) {
		throw new KitMigrationError(
			`Column "${column.name}" on "${tableName}" must be nullable or have a default value`
		);
	}

	const db = kit.nativeDb;
	const table = kit.schema.table(tableName);
	if (table.columns.some((c) => c.name === column.name)) {
		throw new KitMigrationError(`Column "${column.name}" already exists on "${tableName}"`);
	}

	const updatedTable: TableSpec = {
		...table,
		columns: [...table.columns, column]
	};

	db.addColumn(tableName, {
		id: column.id,
		name: column.name,
		ty: toMongrelColumnType(column.storageType),
		primaryKey: column.primaryKey,
		nullable: column.nullable
	});

	if (!column.nullable) {
		const defaultValue = computeDefaultValue(column, kit);
		const rows = fullScanRows(db, table);
		for (const { rowId, row } of rows) {
			const backfilled = { ...row, [column.name]: defaultValue };
			validateRow(updatedTable, backfilled);
			db.table(tableName).put(toCells(updatedTable, backfilled));
		}
		db.table(tableName).commit();
	}
}

export async function addColumn(kit: KitDatabase, tableName: string, column: ColumnSpec): Promise<void> {
	addColumnSync(kit, tableName, column);
}

export async function addIndex(kit: KitDatabase, tableName: string, index: IndexSpec): Promise<void> {
	const table = kit.schema.table(tableName);
	if (table.indexes.some((idx) => idx.name === index.name)) {
		throw new KitMigrationError(`Index "${index.name}" already exists on "${tableName}"`);
	}

	const updatedTable: TableSpec = {
		...table,
		indexes: [...table.indexes, index]
	};

	const db = kit.nativeDb;
	const tempName = `__kit_tmp_addindex_${tableName}_${Date.now()}`;

	// Rebuild the table with the additional index. MongrelDB does not support
	// CREATE INDEX on an existing table, so we copy to a temp table and swap.
	db.createTable(tempName, toMongrelSchema(updatedTable));

	try {
		const rows = fullScanRows(db, table);
		for (const { row } of rows) {
			db.table(tempName).put(toCells(updatedTable, row));
		}
		db.table(tempName).commit();

		db.dropTable(tableName);
		db.createTable(tableName, toMongrelSchema(updatedTable));

		const tempRows = fullScanRows(db, { ...updatedTable, name: tempName });
		for (const { row } of tempRows) {
			db.table(tableName).put(toCells(updatedTable, row));
		}
		db.table(tableName).commit();
	} finally {
		if (db.tableNames().includes(tempName)) {
			db.dropTable(tempName);
		}
	}
}

export async function addUnique(_kit: KitDatabase, _tableName: string, _unique: UniqueSpec): Promise<void> {
	throw new KitMigrationError('addUnique is not implemented yet');
}

export async function addForeignKey(
	_kit: KitDatabase,
	_tableName: string,
	_fk: ForeignKeySpec
): Promise<void> {
	throw new KitMigrationError('addForeignKey is not implemented yet');
}
