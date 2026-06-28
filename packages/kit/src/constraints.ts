import { ConditionKind } from 'mongreldb/native.js';
import type { Database as NativeDatabase, Transaction, Cell, RowJs } from 'mongreldb/native.js';
import type { Schema } from './schema.js';
import type { TableSpec, ColumnSpec, ForeignKeySpec } from './types.js';
import { validateRow } from './validation.js';
import { kitUniqueKeys, kitRowGuards } from './internalTables.js';
import {
	KitDuplicateError,
	KitForeignKeyError,
	KitNotFoundError,
	KitRestrictError,
	KitError
} from './errors.js';
import { KIT_KEY_VERSION, encodePkValue, encodeRowGuardKey, encodeUniqueKey } from './keys.js';

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

function cellValue(cell: Cell | undefined): unknown {
	if (!cell) return null;
	if (cell.text !== undefined) return cell.text;
	if (cell.int64 !== undefined) return cell.int64;
	if (cell.boolean !== undefined) return cell.boolean;
	if (cell.float64 !== undefined) return cell.float64;
	if (cell.bytes !== undefined) return cell.bytes;
	return null;
}

export function toCells(table: TableSpec, row: Record<string, unknown>): Cell[] {
	return table.columns.map((col) => {
		const value = row[col.name];
		if (value === null || value === undefined) {
			return { columnId: col.id };
		}
		switch (col.storageType) {
			case 'bool':
				return { columnId: col.id, boolean: value as boolean };
			case 'int64':
			case 'timestamp':
				return { columnId: col.id, int64: value as bigint };
			case 'float64':
				return { columnId: col.id, float64: value as number };
			case 'text':
			case 'date':
			case 'json':
				return { columnId: col.id, text: value as string };
			case 'bytes':
				return { columnId: col.id, bytes: Buffer.from(value as Uint8Array) };
			default: {
				const _exhaustive: never = col.storageType;
				throw new Error(`Unsupported storage type for cell conversion: ${_exhaustive}`);
			}
		}
	});
}

function rowFromRowJs(table: TableSpec, rowJs: RowJs): Record<string, unknown> {
	const row: Record<string, unknown> = {};
	for (const col of table.columns) {
		const cell = rowJs.cells.find((c) => c.columnId === col.id);
		row[col.name] = cellValue(cell);
	}
	return row;
}

function findByPk(db: NativeDatabase, table: TableSpec, pkValue: string | bigint): RowJs | null {
	if (table.primaryKey.length !== 1) {
		throw new Error(`Composite primary keys are not supported yet`);
	}
	const pkCol = table.columns.find((c) => c.name === table.primaryKey[0]);
	if (!pkCol) {
		throw new Error(`Primary key column not found in table "${table.name}"`);
	}
	const results = db.table(table.name).query([equalityCondition(pkCol, pkValue)]);
	return results[0] ?? null;
}

function pkValueFromRow(table: TableSpec, row: Record<string, unknown>): string | bigint {
	if (table.primaryKey.length !== 1) {
		throw new Error(`Composite primary keys are not supported yet`);
	}
	const value = row[table.primaryKey[0]];
	if (typeof value !== 'string' && typeof value !== 'bigint') {
		throw new Error(`Primary key value must be string or bigint`);
	}
	return value;
}

function buildParentPk(
	parentTable: TableSpec,
	referencesColumns: string[],
	fkValues: unknown[]
): string | bigint {
	if (referencesColumns.length !== 1) {
		throw new Error(`Composite foreign keys are not supported yet`);
	}
	const value = fkValues[0];
	if (typeof value !== 'string' && typeof value !== 'bigint') {
		throw new Error(`Foreign key value must be string or bigint`);
	}
	return value;
}

function equalityCondition(col: ColumnSpec, value: unknown) {
	if (col.storageType === 'int64' || col.storageType === 'timestamp') {
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
	pkValue: string | bigint
): void {
	if (table.primaryKey.length !== 1) {
		throw new Error(`Composite primary keys are not supported yet`);
	}
	const ownerPk = encodePkValue(pkValue);
	const now = isoNow();

	for (const uq of table.unique) {
		const values = uq.columns.map((colName) =>
			row[colName] === undefined ? null : row[colName]
		) as (string | bigint | null)[];
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

export function deleteUniqueGuards(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	pkValue: string | bigint
): void {
	const ownerPk = encodePkValue(pkValue);
	const ownerTableCol = columnId(kitUniqueKeys, 'owner_table');
	const existing = kit.db.table('__kit_unique_keys').query([
		{ kind: ConditionKind.BitmapEq, columnId: ownerTableCol, text: table.name }
	]);
	for (const guard of existing) {
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
	pkValue: string | bigint
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
		{ columnId: columnId(kitRowGuards, 'primary_key'), text: encodePkValue(pkValue) },
		{ columnId: columnId(kitRowGuards, 'version'), int64: version },
		{ columnId: columnId(kitRowGuards, 'updated_at'), text: now }
	]);
}

export function deleteRowGuard(
	kit: ConstraintKit,
	txn: Transaction,
	tableName: string,
	pkValue: string | bigint
): void {
	const encodedKey = encodeRowGuardKey(tableName, pkValue);
	const existing = findByPk(kit.db, kitRowGuards, encodedKey);
	if (existing) {
		txn.delete('__kit_row_guards', existing.rowId);
	}
}

export function parentExists(
	kit: ConstraintKit,
	tableName: string,
	pkValue: string | bigint
): boolean {
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
	pkValue: string | bigint
): void {
	planDeleteRecursive(kit, txn, table, pkValue, new Set(), new Set());
}

function planDeleteRecursive(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	pkValue: string | bigint,
	currentPath: Set<string>,
	deleted: Set<string>
): void {
	const visitKey = `${table.name}:${String(pkValue)}`;
	if (deleted.has(visitKey)) return;
	if (currentPath.has(visitKey)) {
		throw new KitError(`Circular delete detected involving ${table.name}`);
	}
	currentPath.add(visitKey);

	const pkCol = table.columns.find((c) => c.name === table.primaryKey[0]);
	if (!pkCol) {
		throw new Error(`Primary key column not found in table "${table.name}"`);
	}
	const rowJs = kit.db.table(table.name).query([equalityCondition(pkCol, pkValue)])[0] ?? null;
	if (!rowJs) {
		throw new KitNotFoundError(table.name, pkValue);
	}
	const row = rowFromRowJs(table, rowJs);

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

	txn.delete(table.name, rowJs.rowId);
	deleteUniqueGuards(kit, txn, table, pkValue);
	deleteRowGuard(kit, txn, table.name, pkValue);
	deleted.add(visitKey);
	currentPath.delete(visitKey);
}
