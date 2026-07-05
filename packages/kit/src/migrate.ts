import { createHash, randomUUID } from 'node:crypto';
import type { Database as NativeDatabase, Cell, RowJs } from '@visorcraft/mongreldb/native.js';
import { ColumnType, IndexKindSpec, ConditionKind } from '@visorcraft/mongreldb/native.js';
import { tableFromIPC } from 'apache-arrow';
import { KitDatabase } from './db.js';
import { table, int, text, type Schema } from './schema.js';
import { rowFromRowJs } from './rows.js';
import type { TableSpec, ColumnSpec, IndexSpec, UniqueSpec, ForeignKeySpec, ColumnStorageType, PkValue } from './types.js';
import { toCells, pkValueFromRow, parentExists, type ConstraintKit } from './constraints.js';
import { validateRow } from './validation.js';
import { KIT_KEY_VERSION, encodedPk, encodeRowGuardKey, encodeUniqueKey } from './keys.js';

import { KitMigrationError, KitSchemaDriftError, KitForeignKeyError } from './errors.js';
import { procedureJson, type ProcedureSpec } from './procedure.js';
import { triggerJson, type TriggerSpec } from './trigger.js';
import {
	createViewSql,
	createVirtualTableSql,
	dropViewSql,
	dropVirtualTableSql,
	type ViewSpec,
	type VirtualTableSpec
} from './external.js';
import {
	kitSchemaMigrations,
	kitSchemaCatalog,
	kitMigrationLocks,
	kitUniqueKeys,
	kitRowGuards
} from './internalTables.js';

/**
 * Schema of the legacy Kit sequence table that existed before engine-native
 * auto-increment. When present, its rows are used as a one-time fallback to
 * seed per-table engine counters so upgraded databases do not hand out ids
 * below a sequence that was already advanced.
 */
const legacyKitSequences = table('__kit_sequences', {
	columns: [
		text('sequence_name', { primaryKey: true }),
		int('next_value', { nullable: false })
	],
	primaryKey: ['sequence_name']
});

function seedFromLegacyKitSequences(kit: KitDatabase): void {
	const db = kit.nativeDb;
	if (!db.tableNames().includes('__kit_sequences')) {
		return;
	}
	for (const { row } of fullScanRows(db, legacyKitSequences)) {
		const sequenceName = row.sequence_name as string | null;
		const nextValue = row.next_value as bigint | null;
		if (!sequenceName || nextValue === null || nextValue <= 1n) {
			continue;
		}
		const tableName = sequenceName.endsWith('_id_seq')
			? sequenceName.slice(0, -'_id_seq'.length)
			: sequenceName;
		if (!db.tableNames().includes(tableName)) {
			continue;
		}
		// Advance the engine counter until it is at least the legacy next value.
		// reserveAutoIncSync seeds from max(existing id) on its first call, so this
		// also covers the case where rows already exist with higher ids.
		while (true) {
			const reserved = kit.reserveAutoIncSync(tableName);
			if (reserved === null) break;
			if (reserved >= nextValue) break;
		}
	}
}

const I64_MIN = -9_223_372_036_854_775_808n;
const I64_MAX = 9_223_372_036_854_775_807n;
const LOCK_NAME = 'default';
const LOCK_HOLDER = 'kit';
const LOCK_TTL_MS = 5 * 60 * 1000;

/**
 * A declarative migration operation. Mirrors the Rust
 * `mongreldb_kit_core::migrations::MigrationOp` enum. These describe *what* a
 * migration changes; the imperative `up()` callback performs the change. When
 * present on a migration they are folded into its content-aware checksum so
 * editing the op list is detected as schema drift.
 */
export type MigrationOp =
	| { kind: 'createTable'; name: string }
	| { kind: 'dropTable'; name: string }
	| { kind: 'addColumn'; table: string; column: string }
	| { kind: 'dropColumn'; table: string; column: string }
	| { kind: 'alterColumn'; table: string; column: string }
	| { kind: 'addIndex'; table: string; index: string }
	| { kind: 'dropIndex'; table: string; index: string }
	| { kind: 'addUnique'; table: string; constraint: string }
	| { kind: 'dropUnique'; table: string; constraint: string }
	| { kind: 'addForeignKey'; table: string; constraint: string }
	| { kind: 'dropForeignKey'; table: string; constraint: string }
	| { kind: 'addCheck'; table: string; constraint: string }
	| { kind: 'dropCheck'; table: string; constraint: string }
	| { kind: 'createProcedure'; name: string; procedure: ProcedureSpec }
	| { kind: 'replaceProcedure'; name: string; procedure: ProcedureSpec }
	| { kind: 'dropProcedure'; name: string }
	| { kind: 'createTrigger'; name: string; trigger: TriggerSpec }
	| { kind: 'replaceTrigger'; name: string; trigger: TriggerSpec }
	| { kind: 'dropTrigger'; name: string }
	| { kind: 'createVirtualTable'; table: VirtualTableSpec }
	| { kind: 'dropVirtualTable'; name: string }
	| { kind: 'createView'; name: string; view: ViewSpec }
	| { kind: 'replaceView'; name: string; view: ViewSpec }
	| { kind: 'dropView'; name: string }
	| { kind: 'rawSql'; sql: string };

export interface Migration {
	version: number;
	name: string;
	/**
	 * Optional declarative description of the migration's operations. Including
	 * it makes the migration checksum content-aware so a later edit to the ops
	 * is rejected as drift. When omitted the checksum covers `version`/`name`
	 * with an empty op list.
	 */
	ops?: MigrationOp[];
	up: (ctx: MigrationContext) => Promise<void> | void;
}

export interface MigrationContext {
	kit: KitDatabase;
	db: NativeDatabase;
	ensureTable: (table: TableSpec) => Promise<void> | void;
	addColumn: (tableName: string, column: ColumnSpec) => Promise<void> | void;
	dropColumn: (tableName: string, columnName: string) => Promise<void> | void;
	alterColumn: (tableName: string, columnName: string, newColumn: ColumnSpec) => Promise<void> | void;
	addIndex: (tableName: string, index: IndexSpec) => Promise<void> | void;
	dropIndex: (tableName: string, indexName: string) => Promise<void> | void;
	createTrigger: (trigger: TriggerSpec) => Promise<void> | void;
	replaceTrigger: (trigger: TriggerSpec) => Promise<void> | void;
	dropTrigger: (name: string) => Promise<void> | void;
	createVirtualTable: (table: VirtualTableSpec) => Promise<void> | void;
	dropVirtualTable: (name: string) => Promise<void> | void;
	createView: (view: ViewSpec) => Promise<void> | void;
	replaceView: (view: ViewSpec) => Promise<void> | void;
	dropView: (name: string) => Promise<void> | void;
	sql: (sql: string) => Promise<unknown[]> | unknown[];
}

type MongrelSchemaSpec = {
	columns: MongrelColumnSpec[];
	indexes: { name: string; columnId: number; kind: number }[];
};

type MongrelColumnSpec = {
	id: number;
	name: string;
	ty: number;
	primaryKey: boolean;
	nullable: boolean;
	autoIncrement?: boolean;
	embeddingDim?: number;
};

type NativeAlterColumnDatabase = NativeDatabase & {
	alterColumn?: (table: string, columnName: string, column: MongrelColumnSpec) => bigint;
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
		case 'embedding':
			return ColumnType.Embedding;
		case 'sparse':
			return ColumnType.Bytes;
	}
}

function toMongrelColumnSpec(column: ColumnSpec): MongrelColumnSpec {
	return {
		id: column.id,
		name: column.name,
		ty: toMongrelColumnType(column.storageType),
		primaryKey: column.primaryKey,
		nullable: column.nullable,
		autoIncrement: column.default?.kind === 'sequence',
		embeddingDim: column.embeddingDim
	};
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
		columns: table.columns.map(toMongrelColumnSpec),
		indexes
	};
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

/**
 * Canonical, language-neutral serialization of a single migration op. The key
 * order is fixed and string values use standard JSON escaping (`JSON.stringify`),
 * so this is byte-for-byte identical to the Rust kit's `canonical_op`
 * (`crates/mongreldb-kit-core/src/migrations.rs`).
 */
function canonicalOp(op: MigrationOp): string {
	const s = (value: string): string => JSON.stringify(value);
	switch (op.kind) {
		case 'createTable':
			return `{"op":"create_table","name":${s(op.name)}}`;
		case 'dropTable':
			return `{"op":"drop_table","name":${s(op.name)}}`;
		case 'addColumn':
			return `{"op":"add_column","table":${s(op.table)},"column":${s(op.column)}}`;
		case 'dropColumn':
			return `{"op":"drop_column","table":${s(op.table)},"column":${s(op.column)}}`;
		case 'alterColumn':
			return `{"op":"alter_column","table":${s(op.table)},"column":${s(op.column)}}`;
		case 'addIndex':
			return `{"op":"add_index","table":${s(op.table)},"index":${s(op.index)}}`;
		case 'dropIndex':
			return `{"op":"drop_index","table":${s(op.table)},"index":${s(op.index)}}`;
		case 'addUnique':
			return `{"op":"add_unique","table":${s(op.table)},"constraint":${s(op.constraint)}}`;
		case 'dropUnique':
			return `{"op":"drop_unique","table":${s(op.table)},"constraint":${s(op.constraint)}}`;
		case 'addForeignKey':
			return `{"op":"add_foreign_key","table":${s(op.table)},"constraint":${s(op.constraint)}}`;
		case 'dropForeignKey':
			return `{"op":"drop_foreign_key","table":${s(op.table)},"constraint":${s(op.constraint)}}`;
		case 'addCheck':
			return `{"op":"add_check","table":${s(op.table)},"constraint":${s(op.constraint)}}`;
		case 'dropCheck':
			return `{"op":"drop_check","table":${s(op.table)},"constraint":${s(op.constraint)}}`;
		case 'createProcedure':
			return `{"op":"create_procedure","name":${s(op.name)},"procedure":${procedureJson(op.procedure)}}`;
		case 'replaceProcedure':
			return `{"op":"replace_procedure","name":${s(op.name)},"procedure":${procedureJson(op.procedure)}}`;
		case 'dropProcedure':
			return `{"op":"drop_procedure","name":${s(op.name)}}`;
		case 'createTrigger':
			return `{"op":"create_trigger","name":${s(op.name)},"trigger":${triggerJson(op.trigger)}}`;
		case 'replaceTrigger':
			return `{"op":"replace_trigger","name":${s(op.name)},"trigger":${triggerJson(op.trigger)}}`;
		case 'dropTrigger':
			return `{"op":"drop_trigger","name":${s(op.name)}}`;
		case 'createVirtualTable':
			return `{"op":"create_virtual_table","name":${s(op.table.name)},"module":${s(op.table.module)},"args":[${(op.table.args ?? []).map(s).join(',')}]}`;
		case 'dropVirtualTable':
			return `{"op":"drop_virtual_table","name":${s(op.name)}}`;
		case 'createView':
			return `{"op":"create_view","name":${s(op.name)},"sql":${s(op.view.sql)}}`;
		case 'replaceView':
			return `{"op":"replace_view","name":${s(op.name)},"sql":${s(op.view.sql)}}`;
		case 'dropView':
			return `{"op":"drop_view","name":${s(op.name)}}`;
		case 'rawSql':
			return `{"op":"raw_sql","sql":${s(op.sql)}}`;
		default: {
			const _exhaustive: never = op;
			throw new Error(`Unknown migration op: ${JSON.stringify(_exhaustive)}`);
		}
	}
}

/**
 * The canonical content string a migration's checksum is computed over. Shape:
 * `{"version":<n>,"name":<json>,"ops":[<op>,...]}` with no insignificant
 * whitespace, byte-identical to the Rust kit's `canonical_content`.
 */
export function migrationContent(migration: Migration): string {
	const opsJson = (migration.ops ?? []).map(canonicalOp).join(',');
	return `{"version":${migration.version},"name":${JSON.stringify(migration.name)},"ops":[${opsJson}]}`;
}

/**
 * Content-aware SHA-256 checksum for a migration. Covers the version, name, and
 * ordered op list via {@link migrationContent}; the same logical migration
 * produces the identical checksum in TypeScript, Rust, and Python.
 */
export function migrationChecksum(migration: Migration): string {
	return createHash('sha256').update(migrationContent(migration)).digest('hex');
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
	return '0.16.0';
}

function makeContext(kit: KitDatabase): MigrationContext {
	return {
		kit,
		db: kit.nativeDb,
		ensureTable: (table: TableSpec) => createTable(kit, table),
		addColumn: (tableName: string, column: ColumnSpec) => addColumn(kit, tableName, column),
		dropColumn: (tableName: string, columnName: string) => dropColumn(kit, tableName, columnName),
		alterColumn: (tableName: string, columnName: string, newColumn: ColumnSpec) =>
			alterColumn(kit, tableName, columnName, newColumn),
		addIndex: (tableName: string, index: IndexSpec) => addIndex(kit, tableName, index),
		dropIndex: (tableName: string, indexName: string) => dropIndex(kit, tableName, indexName),
		createTrigger: (trigger: TriggerSpec) => {
			kit.createTriggerSync(trigger);
		},
		replaceTrigger: (trigger: TriggerSpec) => {
			kit.createOrReplaceTriggerSync(trigger);
		},
		dropTrigger: (name: string) => {
			kit.dropTriggerSync(name);
		},
		createVirtualTable: (table: VirtualTableSpec) => createVirtualTable(kit, table),
		dropVirtualTable: (name: string) => dropVirtualTable(kit, name),
		createView: (v: ViewSpec) => createView(kit, v),
		replaceView: (v: ViewSpec) => replaceView(kit, v),
		dropView: (name: string) => dropView(kit, name),
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
		dropColumn: (tableName: string, columnName: string) => dropColumnSync(kit, tableName, columnName),
		alterColumn: (tableName: string, columnName: string, newColumn: ColumnSpec) =>
			alterColumnSync(kit, tableName, columnName, newColumn),
		addIndex: (tableName: string, index: IndexSpec) => addIndexSync(kit, tableName, index),
		dropIndex: (tableName: string, indexName: string) => dropIndexSync(kit, tableName, indexName),
		createTrigger: (trigger: TriggerSpec) => {
			kit.createTriggerSync(trigger);
		},
		replaceTrigger: (trigger: TriggerSpec) => {
			kit.createOrReplaceTriggerSync(trigger);
		},
		dropTrigger: (name: string) => {
			kit.dropTriggerSync(name);
		},
		createVirtualTable: (table: VirtualTableSpec) => {
			throw new KitMigrationError(
				`createVirtualTable(${table.name}) requires async migrations because it runs SQL`
			);
		},
		dropVirtualTable: (name: string) => {
			throw new KitMigrationError(
				`dropVirtualTable(${name}) requires async migrations because it runs SQL`
			);
		},
		createView: (v: ViewSpec) => {
			throw new KitMigrationError(
				`createView(${v.name}) requires async migrations because it runs SQL`
			);
		},
		replaceView: (v: ViewSpec) => {
			throw new KitMigrationError(
				`replaceView(${v.name}) requires async migrations because it runs SQL`
			);
		},
		dropView: (name: string) => {
			throw new KitMigrationError(
				`dropView(${name}) requires async migrations because it runs SQL`
			);
		},
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
		seedFromLegacyKitSequences(kit);
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
		seedFromLegacyKitSequences(kit);
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

export async function createVirtualTable(
	kit: KitDatabase,
	table: VirtualTableSpec
): Promise<void> {
	await runSql(kit.nativeDb, createVirtualTableSql(table));
}

export async function dropVirtualTable(kit: KitDatabase, name: string): Promise<void> {
	await runSql(kit.nativeDb, dropVirtualTableSql(name));
}

/** Create (or replace — the engine overwrites) a SQL view. Async-only: runs
 * `CREATE VIEW` through the kit's SQL surface. */
export async function createView(kit: KitDatabase, v: ViewSpec): Promise<void> {
	await runSql(kit.nativeDb, createViewSql(v));
}

/** Replace a SQL view (re-issues `CREATE VIEW`; the engine overwrites). */
export async function replaceView(kit: KitDatabase, v: ViewSpec): Promise<void> {
	await runSql(kit.nativeDb, createViewSql(v));
}

/** Drop a SQL view by name. Idempotent (`DROP VIEW IF EXISTS`). */
export async function dropView(kit: KitDatabase, name: string): Promise<void> {
	await runSql(kit.nativeDb, dropViewSql(name));
}

/**
 * Drop `tableName` and remove every unique-key and row guard it owned. The
 * application row data and its guards are gone after this; the schema catalog is
 * re-persisted by the migration runner once all ops complete.
 */
export async function dropTable(kit: KitDatabase, tableName: string): Promise<void> {
	const db = kit.nativeDb;
	if (!db.tableNames().includes(tableName)) {
		throw new KitMigrationError(`Table "${tableName}" does not exist`);
	}
	db.dropTable(tableName);
	cleanTableGuards(kit, tableName);
}

/** Delete unique-key and row guards owned by a dropped table. */
function cleanTableGuards(kit: KitDatabase, tableName: string): void {
	const db = kit.nativeDb;

	const ukHandle = db.table('__kit_unique_keys');
	const ukGuards = ukHandle.query([
		{
			kind: ConditionKind.BitmapEq,
			columnId: columnId(kitUniqueKeys, 'owner_table'),
			text: tableName
		}
	]);
	let ukMutated = false;
	for (const guard of ukGuards) {
		const key = guard.cells.find((c) => c.columnId === columnId(kitUniqueKeys, 'encoded_key'))?.text;
		if (key) {
			ukHandle.deleteByPkText(key);
			ukMutated = true;
		}
	}
	if (ukMutated) ukHandle.commit();

	const rgHandle = db.table('__kit_row_guards');
	const rgGuards = rgHandle.query([
		{
			kind: ConditionKind.BitmapEq,
			columnId: columnId(kitRowGuards, 'table_name'),
			text: tableName
		}
	]);
	let rgMutated = false;
	for (const guard of rgGuards) {
		const key = guard.cells.find(
			(c) => c.columnId === columnId(kitRowGuards, 'encoded_guard_key')
		)?.text;
		if (key) {
			rgHandle.deleteByPkText(key);
			rgMutated = true;
		}
	}
	if (rgMutated) rgHandle.commit();
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
			// Sequences are now engine-managed AUTO_INCREMENT, so a freshly
			// created table with a sequence column migrates fine (the engine
			// allocates on insert). What is NOT supported is back-filling an
			// AUTO_INCREMENT value onto existing rows during `addColumn` — each
			// row would need its own id and re-putting changes identities.
			throw new KitMigrationError(
				`AUTO_INCREMENT column "${column.name}" cannot be added with a NOT NULL backfill; add it nullable (or create the table with it)`
			);
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
	if (db.tableNames().includes(tableName)) {
		const dbColumns = db.tableColumns(tableName);
		if (dbColumns.includes(column.name)) return;
	}

	const updatedTable: TableSpec = {
		...table,
		columns: [...table.columns, column]
	};

	db.addColumn(tableName, toMongrelColumnSpec(column));

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

/**
 * Change the declared type or constraints of an existing column in place.
 *
 * Kit keeps application-only changes metadata-local when the old and new
 * storage types map to the same engine {@link ColumnType}. Native schema
 * changes (rename, storage type, nullability, primary-key/sequence flags) are
 * delegated to MongrelDB's native ALTER COLUMN validation.
 */
export async function alterColumn(
	kit: KitDatabase,
	tableName: string,
	columnName: string,
	newColumn: ColumnSpec
): Promise<void> {
	alterColumnSync(kit, tableName, columnName, newColumn);
}

function alterColumnSync(
	kit: KitDatabase,
	tableName: string,
	columnName: string,
	newColumn: ColumnSpec
): void {
	const table = kit.schema.table(tableName);
	const existingIndex = table.columns.findIndex((c) => c.name === columnName);
	if (existingIndex === -1) {
		throw new KitMigrationError(`Column "${columnName}" not found in table "${tableName}"`);
	}
	const existing = table.columns[existingIndex];
	const resolved: ColumnSpec = { ...newColumn, id: existing.id };

	if (
		resolved.name !== existing.name &&
		table.columns.some((c, idx) => idx !== existingIndex && c.name === resolved.name)
	) {
		throw new KitMigrationError(`Column "${resolved.name}" already exists in table "${tableName}"`);
	}

	const oldNativeTy = toMongrelColumnType(existing.storageType);
	const newNativeTy = toMongrelColumnType(resolved.storageType);
	const nativeChange =
		resolved.name !== existing.name ||
		newNativeTy !== oldNativeTy ||
		resolved.nullable !== existing.nullable ||
		resolved.primaryKey !== existing.primaryKey ||
		(resolved.default?.kind === 'sequence') !== (existing.default?.kind === 'sequence');

	if (nativeChange) {
		const db = kit.nativeDb as NativeAlterColumnDatabase;
		if (typeof db.alterColumn !== 'function') {
			throw new KitMigrationError(
				`alterColumn for "${tableName}"."${columnName}" requires a MongrelDB native addon with alterColumn support`
			);
		}
		try {
			db.alterColumn(tableName, columnName, toMongrelColumnSpec(resolved));
		} catch (cause) {
			const message = cause instanceof Error ? cause.message : String(cause);
			throw new KitMigrationError(`alterColumn failed for "${tableName}"."${columnName}": ${message}`);
		}
	}

	(table.columns as ColumnSpec[])[existingIndex] = resolved;
	if (resolved.name !== existing.name) {
		renameColumnReferences(table, existing.name, resolved.name);
		for (const candidate of kit.schema.tablesList()) {
			for (const fk of candidate.foreignKeys) {
				if (fk.referencesTable !== tableName) continue;
				fk.referencesColumns = fk.referencesColumns.map((name) =>
					name === existing.name ? resolved.name : name
				);
			}
		}
	}
}

function renameColumnReferences(table: TableSpec, oldName: string, newName: string): void {
	table.primaryKey = table.primaryKey.map((name) => (name === oldName ? newName : name));
	for (const idx of table.indexes) {
		idx.columns = idx.columns.map((name) => (name === oldName ? newName : name));
	}
	for (const constraint of table.unique) {
		constraint.columns = constraint.columns.map((name) => (name === oldName ? newName : name));
	}
	for (const fk of table.foreignKeys) {
		fk.columns = fk.columns.map((name) => (name === oldName ? newName : name));
	}
}

function rebuildTableSync(kit: KitDatabase, table: TableSpec, updatedTable: TableSpec): void {
	const db = kit.nativeDb;
	const tempName = `__kit_tmp_rebuild_${table.name}_${Date.now()}`;

	db.createTable(tempName, toMongrelSchema(updatedTable));
	try {
		const rows = fullScanRows(db, table);
		for (const { row } of rows) {
			db.table(tempName).put(toCells(updatedTable, row));
		}
		db.table(tempName).commit();

		db.dropTable(table.name);
		db.createTable(table.name, toMongrelSchema(updatedTable));

		const tempRows = fullScanRows(db, { ...updatedTable, name: tempName });
		for (const { row } of tempRows) {
			db.table(table.name).put(toCells(updatedTable, row));
		}
		db.table(table.name).commit();
	} finally {
		if (db.tableNames().includes(tempName)) {
			db.dropTable(tempName);
		}
	}
}

function dropUniqueGuards(kit: KitDatabase, tableName: string, constraintName: string): void {
	const handle = kit.nativeDb.table('__kit_unique_keys');
	const guards = handle.query([
		{
			kind: ConditionKind.BitmapEq,
			columnId: columnId(kitUniqueKeys, 'owner_table'),
			text: tableName
		}
	]);
	let mutated = false;
	for (const guard of guards) {
		const constraint = guard.cells.find(
			(c) => c.columnId === columnId(kitUniqueKeys, 'constraint_name')
		)?.text;
		if (constraint !== constraintName) continue;
		const key = guard.cells.find((c) => c.columnId === columnId(kitUniqueKeys, 'encoded_key'))?.text;
		if (!key) continue;
		handle.deleteByPkText(key);
		mutated = true;
	}
	if (mutated) handle.commit();
}

function addIndexSync(kit: KitDatabase, tableName: string, index: IndexSpec): void {
	const table = kit.schema.table(tableName);
	if (table.indexes.some((idx) => idx.name === index.name)) {
		throw new KitMigrationError(`Index "${index.name}" already exists on "${tableName}"`);
	}

	const updatedTable: TableSpec = {
		...table,
		indexes: [...table.indexes, index]
	};

	const addsUnique =
		index.unique &&
		!table.unique.some((constraint) => constraint.columns.join('\0') === index.columns.join('\0'));
	if (addsUnique) {
		addUniqueSync(kit, tableName, { name: index.name, columns: [...index.columns] });
	}
	try {
		rebuildTableSync(kit, table, updatedTable);
	} catch (cause) {
		if (addsUnique) {
			table.unique = table.unique.filter((constraint) => constraint.name !== index.name);
			dropUniqueGuards(kit, tableName, index.name);
		}
		throw cause;
	}
	table.indexes.push(index);
}

export async function addIndex(kit: KitDatabase, tableName: string, index: IndexSpec): Promise<void> {
	addIndexSync(kit, tableName, index);
}

function dropIndexSync(kit: KitDatabase, tableName: string, indexName: string): void {
	const table = kit.schema.table(tableName);
	const index = table.indexes.find((idx) => idx.name === indexName);
	if (!index) {
		throw new KitMigrationError(`Index "${indexName}" does not exist on "${tableName}"`);
	}

	const updatedTable: TableSpec = {
		...table,
		indexes: table.indexes.filter((idx) => idx.name !== indexName),
		unique: index.unique ? table.unique.filter((constraint) => constraint.name !== indexName) : table.unique
	};

	rebuildTableSync(kit, table, updatedTable);
	table.indexes = updatedTable.indexes;
	if (index.unique) {
		table.unique = updatedTable.unique;
		dropUniqueGuards(kit, tableName, indexName);
	}
}

export async function dropIndex(kit: KitDatabase, tableName: string, indexName: string): Promise<void> {
	dropIndexSync(kit, tableName, indexName);
}

function dropColumnSync(kit: KitDatabase, tableName: string, columnName: string): void {
	const table = kit.schema.table(tableName);
	if (!table.columns.some((column) => column.name === columnName)) {
		throw new KitMigrationError(`Column "${columnName}" does not exist on "${tableName}"`);
	}
	if (table.primaryKey.includes(columnName)) {
		throw new KitMigrationError(`Cannot drop primary-key column "${columnName}" from "${tableName}"`);
	}
	for (const candidate of kit.schema.tablesList()) {
		for (const fk of candidate.foreignKeys) {
			if (fk.referencesTable === tableName && fk.referencesColumns.includes(columnName)) {
				throw new KitMigrationError(
					`Cannot drop "${tableName}"."${columnName}" while foreign key "${fk.name}" references it`
				);
			}
		}
	}

	const removedUnique = table.unique
		.filter((constraint) => constraint.columns.includes(columnName))
		.map((constraint) => constraint.name);
	const updatedTable: TableSpec = {
		...table,
		columns: table.columns.filter((column) => column.name !== columnName),
		indexes: table.indexes.filter((idx) => !idx.columns.includes(columnName)),
		unique: table.unique.filter((constraint) => !constraint.columns.includes(columnName)),
		foreignKeys: table.foreignKeys.filter((fk) => !fk.columns.includes(columnName))
	};

	rebuildTableSync(kit, table, updatedTable);
	table.columns = updatedTable.columns;
	table.indexes = updatedTable.indexes;
	table.unique = updatedTable.unique;
	table.foreignKeys = updatedTable.foreignKeys;
	for (const constraint of removedUnique) {
		dropUniqueGuards(kit, tableName, constraint);
	}
}

export async function dropColumn(kit: KitDatabase, tableName: string, columnName: string): Promise<void> {
	dropColumnSync(kit, tableName, columnName);
}

/**
 * Add a unique constraint and backfill its `__kit_unique_keys` guards for every
 * existing row (PLAN "Migrations"). Rows whose unique columns are all non-null
 * reserve a guard; a guard key produced by two different rows means the existing
 * data already violates the constraint and the migration is rejected. The
 * constraint is added to the in-memory table so subsequent writes enforce it.
 */
function addUniqueSync(kit: KitDatabase, tableName: string, unique: UniqueSpec): void {
	const db = kit.nativeDb;
	const table = kit.schema.table(tableName);
	if (table.unique.some((u) => u.name === unique.name)) {
		throw new KitMigrationError(
			`Unique constraint "${unique.name}" already exists on "${tableName}"`
		);
	}
	for (const colName of unique.columns) {
		if (!table.columns.some((c) => c.name === colName)) {
			throw new KitMigrationError(`Unique column "${colName}" not found in table "${tableName}"`);
		}
	}

	const ukHandle = db.table('__kit_unique_keys');
	// Pre-existing guards for this table make the backfill idempotent.
	const existingKeys = new Set<string>();
	for (const guard of ukHandle.query([
		{ kind: ConditionKind.BitmapEq, columnId: columnId(kitUniqueKeys, 'owner_table'), text: tableName }
	])) {
		const key = guard.cells.find((c) => c.columnId === columnId(kitUniqueKeys, 'encoded_key'))?.text;
		if (key) existingKeys.add(key);
	}

	const seen = new Map<string, string>();
	const now = isoNow();
	let mutated = false;
	for (const { row } of fullScanRows(db, table)) {
		const values = unique.columns.map((colName) =>
			row[colName] === undefined ? null : row[colName]
		) as (string | bigint | null)[];
		if (values.some((value) => value === null)) continue; // nullable-unique: nulls never collide
		const encodedKey = encodeUniqueKey(KIT_KEY_VERSION, unique.name, values);
		const ownerPk = encodedPk(pkValueFromRow(table, row));
		const prior = seen.get(encodedKey);
		if (prior !== undefined && prior !== ownerPk) {
			throw new KitMigrationError(
				`cannot add unique constraint "${unique.name}" on "${tableName}": existing rows violate it`
			);
		}
		seen.set(encodedKey, ownerPk);
		if (existingKeys.has(encodedKey)) continue;
		ukHandle.put([
			{ columnId: columnId(kitUniqueKeys, 'encoded_key'), text: encodedKey },
			{ columnId: columnId(kitUniqueKeys, 'constraint_name'), text: unique.name },
			{ columnId: columnId(kitUniqueKeys, 'owner_table'), text: tableName },
			{ columnId: columnId(kitUniqueKeys, 'owner_pk'), text: ownerPk },
			{ columnId: columnId(kitUniqueKeys, 'created_at'), text: now }
		]);
		mutated = true;
	}
	if (mutated) ukHandle.commit();

	table.unique.push(unique);
}

export async function addUnique(
	kit: KitDatabase,
	tableName: string,
	unique: UniqueSpec
): Promise<void> {
	addUniqueSync(kit, tableName, unique);
}

/** Build the parent primary-key value referenced by a foreign key. */
function buildFkParentPk(
	parent: TableSpec,
	fkValues: unknown[]
): PkValue {
	if (parent.primaryKey.length === 1) {
		const value = fkValues[0];
		if (typeof value !== 'string' && typeof value !== 'bigint') {
			throw new KitMigrationError('Foreign key value must be string or bigint');
		}
		return value;
	}
	return fkValues.map((value) => {
		if (value === null || value === undefined) return null;
		if (typeof value !== 'string' && typeof value !== 'bigint') {
			throw new KitMigrationError('Foreign key value must be string, bigint, or null');
		}
		return value;
	});
}

/**
 * Add a foreign key and backfill parent `__kit_row_guards` (PLAN "Migrations").
 * Each existing child row with a non-null FK must reference an existing parent;
 * a missing parent rejects the migration. The referenced parent's row guard is
 * touched so a later concurrent parent delete conflicts. The FK is added to the
 * in-memory table so subsequent writes enforce it.
 */
export async function addForeignKey(
	kit: KitDatabase,
	tableName: string,
	fk: ForeignKeySpec
): Promise<void> {
	const db = kit.nativeDb;
	const table = kit.schema.table(tableName);
	const parent = kit.schema.table(fk.referencesTable);
	if (table.foreignKeys.some((f) => f.name === fk.name)) {
		throw new KitMigrationError(`Foreign key "${fk.name}" already exists on "${tableName}"`);
	}
	const ckit: ConstraintKit = { db, schema: kit.schema };
	const rgHandle = db.table('__kit_row_guards');
	const touched = new Set<string>();
	let mutated = false;

	for (const { row } of fullScanRows(db, table)) {
		const fkValues = fk.columns.map((colName) => row[colName]);
		if (fkValues.some((value) => value === null || value === undefined)) continue;
		const parentPk = buildFkParentPk(parent, fkValues);
		if (!parentExists(ckit, parent.name, parentPk)) {
			throw new KitForeignKeyError(tableName, fk.name);
		}
		const guardKey = encodeRowGuardKey(parent.name, parentPk);
		if (touched.has(guardKey)) continue;
		touched.add(guardKey);
		const existing = rgHandle.getByPkText(guardKey);
		const version = existing
			? (existing.cells.find((c) => c.columnId === columnId(kitRowGuards, 'version'))?.int64 ??
					0n) + 1n
			: 1n;
		rgHandle.put([
			{ columnId: columnId(kitRowGuards, 'encoded_guard_key'), text: guardKey },
			{ columnId: columnId(kitRowGuards, 'table_name'), text: parent.name },
			{ columnId: columnId(kitRowGuards, 'primary_key'), text: encodedPk(parentPk) },
			{ columnId: columnId(kitRowGuards, 'version'), int64: version },
			{ columnId: columnId(kitRowGuards, 'updated_at'), text: isoNow() }
		]);
		mutated = true;
	}
	if (mutated) rgHandle.commit();

	table.foreignKeys.push(fk);
}
