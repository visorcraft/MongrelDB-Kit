import { ConditionKind } from '@visorcraft/mongreldb/native.js';
import type { Database as NativeDatabase, Transaction, Cell, RowJs } from '@visorcraft/mongreldb/native.js';
import type { Schema } from './schema.js';
import type { TableSpec, ColumnSpec, ForeignKeySpec, PkValue } from './types.js';
import { validateRow } from './validation.js';
import { kitUniqueKeys, kitRowGuards } from './internalTables.js';
import {
	KitDuplicateError,
	KitForeignKeyError,
	KitNotFoundError,
	KitRestrictError,
	KitError
} from './errors.js';
import { KIT_KEY_VERSION, encodedPk, encodeRowGuardKey, encodeUniqueKey } from './keys.js';
import { cellValue, rowFromRowJs } from './rows.js';

export interface ConstraintKit {
	db: MongrelDatabase;
	schema: Schema;
}

type MongrelDatabase = NativeDatabase & {
	transaction(
		fn: (txn: Transaction) => void | Promise<void>,
		opts?: { maxRetries?: number; baseDelayMs?: number }
	): Promise<bigint>;
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

/** True when some table in the schema has a foreign key referencing `tableName`. */
export function isReferencedTable(schema: Schema, tableName: string): boolean {
	return schema
		.tablesList()
		.some((t) => t.foreignKeys.some((fk) => fk.referencesTable === tableName));
}

export function toCells(table: TableSpec, row: Record<string, unknown>): Cell[] {
	return table.columns.map((col) => {
		const value = row[col.name];
		if (value === null || value === undefined) {
			// A cell with only `columnId` (no typed value) stores SQL NULL, so a
			// nullable column reads back as null rather than a zero/empty sentinel.
			return { columnId: col.id };
		}
		switch (col.storageType) {
			case 'bool':
				return { columnId: col.id, boolean: value as boolean };
			case 'int64':
				return { columnId: col.id, int64: value as bigint };
			case 'float64':
				return { columnId: col.id, float64: value as number };
			case 'text':
			case 'timestamp':
			case 'date':
			case 'json':
				return { columnId: col.id, text: value as string };
			case 'bytes':
				return { columnId: col.id, bytes: Buffer.from(value as Uint8Array) };
			case 'embedding':
				return { columnId: col.id, embedding: (value as number[]).map(Number) };
			case 'sparse': {
				const terms = value as [number, number][];
				return {
					columnId: col.id,
					sparseTokens: terms.map((p) => p[0]),
					sparseWeights: terms.map((p) => p[1])
				};
			}
			default: {
				const _exhaustive: string = col.storageType;
				throw new Error(`Unsupported storage type for cell conversion: ${_exhaustive}`);
			}
		}
	});
}

export function findByPk(db: NativeDatabase, table: TableSpec, pkValue: PkValue): RowJs | null {
	if (table.primaryKey.length === 1) {
		const scalar = Array.isArray(pkValue) ? pkValue[0] : pkValue;
		if (scalar === null) {
			throw new Error(`Primary key value cannot be null in table "${table.name}"`);
		}
		const pkCol = table.columns.find((c) => c.name === table.primaryKey[0]);
		if (!pkCol) {
			throw new Error(`Primary key column not found in table "${table.name}"`);
		}
		const results = db.table(table.name).query([equalityCondition(pkCol, scalar)]);
		return results[0] ?? null;
	}

	if (!Array.isArray(pkValue) || pkValue.length !== table.primaryKey.length) {
		throw new Error(
			`Primary key value for "${table.name}" must be an array with ${table.primaryKey.length} components`
		);
	}
	const conditions = table.primaryKey.map((name, i) => {
		const col = table.columns.find((c) => c.name === name);
		if (!col) {
			throw new Error(`Primary key column "${name}" not found in table "${table.name}"`);
		}
		return equalityCondition(col, pkValue[i]);
	});
	const results = db.table(table.name).query(conditions);
	return results[0] ?? null;
}

export function pkValueFromRow(table: TableSpec, row: Record<string, unknown>): PkValue {
	if (table.primaryKey.length === 1) {
		const value = row[table.primaryKey[0]];
		if (typeof value !== 'string' && typeof value !== 'bigint') {
			throw new Error(`Primary key value must be string or bigint`);
		}
		return value;
	}

	return table.primaryKey.map((name) => {
		const value = row[name];
		if (value === null || value === undefined) {
			return null;
		}
		if (typeof value !== 'string' && typeof value !== 'bigint') {
			throw new Error(`Primary key component "${name}" must be string, bigint, or null`);
		}
		return value;
	});
}

export function pkValuesEqual(a: PkValue, b: PkValue): boolean {
	const left = Array.isArray(a) ? a : [a];
	const right = Array.isArray(b) ? b : [b];
	if (left.length !== right.length) return false;
	for (let i = 0; i < left.length; i++) {
		if (left[i] !== right[i]) return false;
	}
	return true;
}

function buildParentPk(
	parentTable: TableSpec,
	referencesColumns: string[],
	fkValues: unknown[]
): PkValue {
	if (referencesColumns.length !== parentTable.primaryKey.length) {
		throw new Error(
			`Foreign key references ${referencesColumns.length} columns but parent primary key has ${parentTable.primaryKey.length} columns`
		);
	}
	if (referencesColumns.length === 1) {
		const value = fkValues[0];
		if (typeof value !== 'string' && typeof value !== 'bigint') {
			throw new Error(`Foreign key value must be string or bigint`);
		}
		return value;
	}
	return fkValues.map((value) => {
		if (value === null || value === undefined) {
			return null;
		}
		if (typeof value !== 'string' && typeof value !== 'bigint') {
			throw new Error(`Foreign key value must be string, bigint, or null`);
		}
		return value;
	});
}

function equalityCondition(col: ColumnSpec, value: unknown) {
	if (col.storageType === 'int64') {
		return {
			kind: ConditionKind.RangeInt,
			columnId: col.id,
			int64Lo: value as bigint,
			int64Hi: value as bigint
		};
	}
	return {
		kind: ConditionKind.BitmapEq,
		columnId: col.id,
		text: String(value)
	};
}

function queryChildren(
	db: NativeDatabase,
	childTable: TableSpec,
	fk: ForeignKeySpec,
	parentRow: Record<string, unknown>
): RowJs[] {
	const conditions = fk.columns.map((colName, i) => {
		const col = childTable.columns.find((c) => c.name === colName);
		if (!col) {
			throw new Error(`FK column "${colName}" not found in table "${childTable.name}"`);
		}
		const refCol = fk.referencesColumns[i];
		return equalityCondition(col, parentRow[refCol]);
	});
	return db.table(childTable.name).query(conditions);
}

export function stageUniqueGuards(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	row: Record<string, unknown>,
	pkValue: PkValue
): void {
	const ownerPk = encodedPk(pkValue);
	const now = isoNow();

	for (const uq of table.unique) {
		const values = uq.columns.map((colName) =>
			row[colName] === undefined ? null : row[colName]
		) as (string | bigint | null)[];
		if (values.some((value) => value === null || value === undefined)) {
			continue;
		}
		const encodedKey = encodeUniqueKey(KIT_KEY_VERSION, uq.name, values);
		const existing = findByPk(kit.db, kitUniqueKeys, encodedKey);
		if (existing) {
			const ownerTableCell = existing.cells.find(
				(c) => c.columnId === columnId(kitUniqueKeys, 'owner_table')
			);
			const ownerPkCell = existing.cells.find(
				(c) => c.columnId === columnId(kitUniqueKeys, 'owner_pk')
			);
			const existingTable = String(cellValue(ownerTableCell) ?? '');
			const existingPk = String(cellValue(ownerPkCell) ?? '');
			if (existingTable !== table.name || existingPk !== ownerPk) {
				throw new KitDuplicateError(table.name, uq.name);
			}
			continue;
		}
		txn.put('__kit_unique_keys', [
			{ columnId: columnId(kitUniqueKeys, 'encoded_key'), text: encodedKey },
			{ columnId: columnId(kitUniqueKeys, 'constraint_name'), text: uq.name },
			{ columnId: columnId(kitUniqueKeys, 'owner_table'), text: table.name },
			{ columnId: columnId(kitUniqueKeys, 'owner_pk'), text: ownerPk },
			{ columnId: columnId(kitUniqueKeys, 'created_at'), text: now }
		]);
	}
}

function pkGuardConstraintName(table: TableSpec): string {
	return `__pk_${table.name}`;
}

export function stagePkGuard(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	pkValue: PkValue,
	pkExplicit: boolean,
	pkSeen?: Set<string>
): void {
	// An auto-assigned (sequence) primary key is guaranteed unique, so it needs
	// no duplicate check — this keeps inserts (and bulk loads) cheap.
	if (!pkExplicit) return;

	// A single-column explicit PK is checked for duplicates. A batch passes a
	// pre-loaded set of existing + already-staged PKs so the check stays O(1) per
	// row (one scan up front) instead of a per-row lookup; a single insert checks
	// the table directly.
	if (table.primaryKey.length === 1) {
		if (pkSeen) {
			const k = encodedPk(pkValue);
			if (pkSeen.has(k)) {
				throw new KitDuplicateError(table.name, pkGuardConstraintName(table));
			}
			pkSeen.add(k);
		} else if (findByPk(kit.db, table, pkValue)) {
			throw new KitDuplicateError(table.name, pkGuardConstraintName(table));
		}
		return;
	}

	// A composite explicit PK has no single native key to check, so it uses a
	// guard row (conflict-safe) like the unique-constraint machinery.
	const pkValues = pkValue as (string | bigint | null)[];
	if (pkValues.some((value) => value === null || value === undefined)) {
		throw new Error(`Primary key components cannot be null in table "${table.name}"`);
	}
	const constraintName = pkGuardConstraintName(table);
	const encodedKey = encodeUniqueKey(KIT_KEY_VERSION, constraintName, pkValues);
	const existing = findByPk(kit.db, kitUniqueKeys, encodedKey);
	if (existing) {
		throw new KitDuplicateError(table.name, constraintName);
	}
	txn.put('__kit_unique_keys', [
		{ columnId: columnId(kitUniqueKeys, 'encoded_key'), text: encodedKey },
		{ columnId: columnId(kitUniqueKeys, 'constraint_name'), text: constraintName },
		{ columnId: columnId(kitUniqueKeys, 'owner_table'), text: table.name },
		{ columnId: columnId(kitUniqueKeys, 'owner_pk'), text: encodedPk(pkValue) },
		{ columnId: columnId(kitUniqueKeys, 'created_at'), text: isoNow() }
	]);
}

export function deletePkGuard(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	pkValue: PkValue
): void {
	// Single-column PKs use a native existence check (no guard row to delete).
	if (table.primaryKey.length === 1) return;
	const pkValues = pkValue as (string | bigint | null)[];
	if (pkValues.some((value) => value === null || value === undefined)) return;
	const constraintName = pkGuardConstraintName(table);
	const encodedKey = encodeUniqueKey(KIT_KEY_VERSION, constraintName, pkValues);
	const existing = findByPk(kit.db, kitUniqueKeys, encodedKey);
	if (existing) {
		txn.delete('__kit_unique_keys', existing.rowId);
	}
}

export function deleteUniqueGuards(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	pkValue: PkValue,
	onlyConstraints?: string[]
): void {
	// No unique constraints means no guard rows to clean up — skip the per-row
	// query on __kit_unique_keys entirely (a hot cost in bulk deletes).
	if (table.unique.length === 0) return;
	const ownerPk = encodedPk(pkValue);
	const ownerTableCol = columnId(kitUniqueKeys, 'owner_table');
	const allowed = onlyConstraints
		? new Set(onlyConstraints)
		: new Set(table.unique.map((uq) => uq.name));
	const existing = kit.db.table('__kit_unique_keys').query([
		{ kind: ConditionKind.BitmapEq, columnId: ownerTableCol, text: table.name }
	]);
	for (const guard of existing) {
		const constraintCell = guard.cells.find(
			(c) => c.columnId === columnId(kitUniqueKeys, 'constraint_name')
		);
		if (!allowed.has(String(cellValue(constraintCell) ?? ''))) {
			continue;
		}
		const ownerPkCell = guard.cells.find(
			(c) => c.columnId === columnId(kitUniqueKeys, 'owner_pk')
		);
		if (cellValue(ownerPkCell) === ownerPk) {
			txn.delete('__kit_unique_keys', guard.rowId);
		}
	}
}

export function touchRowGuard(
	kit: ConstraintKit,
	txn: Transaction,
	tableName: string,
	pkValue: PkValue
): void {
	const encodedKey = encodeRowGuardKey(tableName, pkValue);
	const existing = findByPk(kit.db, kitRowGuards, encodedKey);
	const version = existing
		? (existing.cells.find((c) => c.columnId === columnId(kitRowGuards, 'version'))?.int64 ??
				0n) + 1n
		: 1n;
	const now = isoNow();
	txn.put('__kit_row_guards', [
		{ columnId: columnId(kitRowGuards, 'encoded_guard_key'), text: encodedKey },
		{ columnId: columnId(kitRowGuards, 'table_name'), text: tableName },
		{ columnId: columnId(kitRowGuards, 'primary_key'), text: encodedPk(pkValue) },
		{ columnId: columnId(kitRowGuards, 'version'), int64: version },
		{ columnId: columnId(kitRowGuards, 'updated_at'), text: now }
	]);
}

export function deleteRowGuard(
	kit: ConstraintKit,
	txn: Transaction,
	tableName: string,
	pkValue: PkValue
): void {
	const encodedKey = encodeRowGuardKey(tableName, pkValue);
	const existing = findByPk(kit.db, kitRowGuards, encodedKey);
	if (existing) {
		txn.delete('__kit_row_guards', existing.rowId);
	}
}

export function parentExists(kit: ConstraintKit, tableName: string, pkValue: PkValue): boolean {
	const table = kit.schema.table(tableName);
	return findByPk(kit.db, table, pkValue) !== null;
}

export function enforceForeignKeys(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	row: Record<string, unknown>
): void {
	for (const fk of table.foreignKeys) {
		const values = fk.columns.map((colName) => row[colName]);
		if (values.some((v) => v === null || v === undefined)) {
			continue;
		}
		const parentTable = kit.schema.table(fk.referencesTable);
		const parentPk = buildParentPk(parentTable, fk.referencesColumns, values);
		if (!parentExists(kit, parentTable.name, parentPk)) {
			throw new KitForeignKeyError(table.name, fk.name);
		}
		touchRowGuard(kit, txn, parentTable.name, parentPk);
	}
}

export function planDelete(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	pkValue: PkValue,
	known?: { row: Record<string, unknown>; rowId: bigint }
): void {
	planDeleteRecursive(kit, txn, table, pkValue, new Set(), new Set(), known);
}

function planDeleteRecursive(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	pkValue: PkValue,
	currentPath: Set<string>,
	deleted: Set<string>,
	known?: { row: Record<string, unknown>; rowId: bigint }
): void {
	const visitKey = `${table.name}:${encodedPk(pkValue)}`;
	if (deleted.has(visitKey)) return;
	if (currentPath.has(visitKey)) {
		throw new KitError(`Circular delete detected involving ${table.name}`);
	}
	currentPath.add(visitKey);

	// The caller (e.g. a bulk delete) often already has the row from its scan;
	// reuse it instead of re-reading per row, which would be O(n^2) on a delete.
	let row: Record<string, unknown>;
	let rowId: bigint;
	if (known) {
		row = known.row;
		rowId = known.rowId;
	} else {
		const rowJs = findByPk(kit.db, table, pkValue);
		if (!rowJs) {
			throw new KitNotFoundError(table.name, pkValue);
		}
		row = rowFromRowJs(table, rowJs);
		rowId = rowJs.rowId;
	}

	for (const childTable of kit.schema.tablesList()) {
		for (const fk of childTable.foreignKeys) {
			if (fk.referencesTable !== table.name) {
				continue;
			}
			const children = queryChildren(kit.db, childTable, fk, row);
			if (children.length === 0) {
				continue;
			}
			if (fk.onDelete === 'restrict') {
				throw new KitRestrictError(childTable.name, fk.name);
			}
			if (fk.onDelete === 'set null') {
				for (const childJs of children) {
					const childRow = rowFromRowJs(childTable, childJs);
					const childPk = pkValueFromRow(childTable, childRow);
					const patched = { ...childRow };
					for (const colName of fk.columns) {
						patched[colName] = null;
					}
					validateRow(childTable, patched);
					deleteUniqueGuards(kit, txn, childTable, childPk);
					txn.delete(childTable.name, childJs.rowId);
					txn.put(childTable.name, toCells(childTable, patched));
					stageUniqueGuards(kit, txn, childTable, patched, childPk);
				}
			} else if (fk.onDelete === 'cascade') {
				for (const childJs of children) {
					const childRow = rowFromRowJs(childTable, childJs);
					const childPk = pkValueFromRow(childTable, childRow);
					planDeleteRecursive(kit, txn, childTable, childPk, currentPath, deleted);
				}
			}
		}
	}

	txn.delete(table.name, rowId);
	deleteUniqueGuards(kit, txn, table, pkValue);
	deletePkGuard(kit, txn, table, pkValue);
	deleteRowGuard(kit, txn, table.name, pkValue);
	deleted.add(visitKey);
	currentPath.delete(visitKey);
}

/** Delete every Kit guard row owned by `tableName` (used by `truncateTable`). */
export function deleteGuardsForTable(
	kit: ConstraintKit,
	txn: Transaction,
	tableName: string
): void {
	const ownerTableCol = columnId(kitUniqueKeys, 'owner_table');
	const uniqueKeys = kit.db.table('__kit_unique_keys').query([
		{ kind: ConditionKind.BitmapEq, columnId: ownerTableCol, text: tableName }
	]);
	for (const row of uniqueKeys) {
		txn.delete('__kit_unique_keys', row.rowId);
	}

	const tableNameCol = columnId(kitRowGuards, 'table_name');
	const rowGuards = kit.db.table('__kit_row_guards').query([
		{ kind: ConditionKind.BitmapEq, columnId: tableNameCol, text: tableName }
	]);
	for (const row of rowGuards) {
		txn.delete('__kit_row_guards', row.rowId);
	}
}
