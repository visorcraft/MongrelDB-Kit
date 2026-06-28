import { randomUUID } from 'node:crypto';
import { ConditionKind } from 'mongreldb/native.js';
import type { Database as NativeDatabase, Cell, RowJs } from 'mongreldb/native.js';
import { KitDatabase } from './db.js';
import type { TableSpec, ColumnSpec } from './types.js';
import type { Row, Insert, Update } from './types.js';
import type { DefaultContext } from './defaults.js';
import { applyDefaults } from './defaults.js';
import { validateRow } from './validation.js';
import {
	toCells,
	stageUniqueGuards,
	deleteUniqueGuards,
	enforceForeignKeys,
	planDelete,
	type ConstraintKit
} from './constraints.js';
import { KitError } from './errors.js';

declare module './db.js' {
	interface KitDatabase {
		selectFrom<T extends TableSpec>(table: T): SelectBuilder<T>;
		insertInto<T extends TableSpec>(table: T): InsertBuilder<T>;
		updateTable<T extends TableSpec>(table: T): UpdateBuilder<T>;
		deleteFrom<T extends TableSpec>(table: T): DeleteBuilder<T>;
	}
}

type ApplicationTypeMap = {
	bool: boolean;
	int64: bigint;
	float64: number;
	timestamp: unknown;
	date: unknown;
	text: string;
	bytes: unknown;
	json: unknown;
};

export type ColumnValue<T extends ColumnSpec> = T['applicationType'] extends keyof ApplicationTypeMap
	? ApplicationTypeMap[T['applicationType']]
	: never;

export type Predicate =
	| { kind: 'and'; predicates: Predicate[] }
	| { kind: 'or'; predicates: Predicate[] }
	| { kind: 'eq'; column: ColumnSpec; value: unknown }
	| { kind: 'ne'; column: ColumnSpec; value: unknown }
	| { kind: 'gt'; column: ColumnSpec; value: unknown }
	| { kind: 'gte'; column: ColumnSpec; value: unknown }
	| { kind: 'lt'; column: ColumnSpec; value: unknown }
	| { kind: 'lte'; column: ColumnSpec; value: unknown }
	| { kind: 'null'; column: ColumnSpec; not: boolean }
	| { kind: 'in'; column: ColumnSpec; values: unknown[] };

export type OrderBy = { column: ColumnSpec; direction: 'asc' | 'desc' };

type MatchedRow = { rowId: bigint; row: Record<string, unknown> };

const I64_MIN = -9_223_372_036_854_775_808n;
const I64_MAX = 9_223_372_036_854_775_807n;

export function eq<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'eq', column, value };
}

export function ne<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'ne', column, value };
}

export function gt<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'gt', column, value };
}

export function gte<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'gte', column, value };
}

export function lt<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'lt', column, value };
}

export function lte<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'lte', column, value };
}

export function isNull(column: ColumnSpec): Predicate {
	return { kind: 'null', column, not: false };
}

export function isNotNull(column: ColumnSpec): Predicate {
	return { kind: 'null', column, not: true };
}

export function inList<T extends ColumnSpec>(column: T, values: ColumnValue<T>[]): Predicate {
	return { kind: 'in', column, values };
}

export function and(...predicates: Predicate[]): Predicate {
	return { kind: 'and', predicates };
}

export function or(...predicates: Predicate[]): Predicate {
	return { kind: 'or', predicates };
}

export function asc(column: ColumnSpec): OrderBy {
	return { column, direction: 'asc' };
}

export function desc(column: ColumnSpec): OrderBy {
	return { column, direction: 'desc' };
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

function isIndexed(table: TableSpec, columnName: string): boolean {
	if (table.primaryKey.includes(columnName)) return true;
	if (table.indexes.some((idx) => idx.columns.includes(columnName))) return true;
	if (table.foreignKeys.some((fk) => fk.columns.includes(columnName))) return true;
	return false;
}

function makeEqCondition(table: TableSpec, column: ColumnSpec, value: unknown) {
	if (value === null || value === undefined) return null;

	if (column.storageType === 'int64' || column.storageType === 'timestamp') {
		return {
			kind: ConditionKind.RangeInt,
			columnId: column.id,
			int64Lo: value as bigint,
			int64Hi: value as bigint
		};
	}

	if (isIndexed(table, column.name)) {
		return {
			kind: ConditionKind.BitmapEq,
			columnId: column.id,
			text: String(value)
		};
	}

	return null;
}

function makeRangeCondition(
	table: TableSpec,
	column: ColumnSpec,
	op: 'gt' | 'gte' | 'lt' | 'lte',
	value: unknown
) {
	if (column.storageType === 'int64' || column.storageType === 'timestamp') {
		const v = value as bigint;
		let lo = I64_MIN;
		let hi = I64_MAX;
		switch (op) {
			case 'gt':
			case 'gte':
				lo = v;
				break;
			case 'lt':
			case 'lte':
				hi = v;
				break;
		}
		return {
			kind: ConditionKind.RangeInt,
			columnId: column.id,
			int64Lo: lo,
			int64Hi: hi
		};
	}

	if (column.storageType === 'float64') {
		const v = value as number;
		let lo = -Infinity;
		let hi = Infinity;
		switch (op) {
			case 'gt':
			case 'gte':
				lo = v;
				break;
			case 'lt':
			case 'lte':
				hi = v;
				break;
		}
		return {
			kind: ConditionKind.RangeF64,
			columnId: column.id,
			float64Lo: lo,
			float64Hi: hi
		};
	}

	return null;
}

function makeRangeFilter(
	op: 'gt' | 'gte' | 'lt' | 'lte',
	column: ColumnSpec,
	value: unknown
): (row: Record<string, unknown>) => boolean {
	return (row) => {
		const actual = row[column.name];
		if (actual === null || actual === undefined) return false;
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const a = actual as any;
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const v = value as any;
		switch (op) {
			case 'gt':
				return a > v;
			case 'gte':
				return a >= v;
			case 'lt':
				return a < v;
			case 'lte':
				return a <= v;
		}
	};
}

function fullScanCondition(table: TableSpec) {
	const intColumn = table.columns.find(
		(c) => c.storageType === 'int64' || c.storageType === 'timestamp'
	);
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

	throw new KitError(
		`Full table scan on "${table.name}" requires an int64, timestamp, or float64 column`
	);
}

function fullScanRows(db: NativeDatabase, table: TableSpec): MatchedRow[] {
	const condition = fullScanCondition(table);
	return db
		.table(table.name)
		.query([condition])
		.map((rowJs) => ({ rowId: rowJs.rowId, row: rowFromRowJs(table, rowJs) }));
}

function evaluateLeafPredicate(
	db: NativeDatabase,
	table: TableSpec,
	predicate: Predicate
): MatchedRow[] {
	switch (predicate.kind) {
		case 'eq': {
			const condition = makeEqCondition(table, predicate.column, predicate.value);
			const filter = (row: Record<string, unknown>) => row[predicate.column.name] === predicate.value;
			if (condition) {
				return db
					.table(table.name)
					.query([condition])
					.map((rowJs) => ({ rowId: rowJs.rowId, row: rowFromRowJs(table, rowJs) }))
					.filter((m) => filter(m.row));
			}
			return fullScanRows(db, table).filter((m) => filter(m.row));
		}
		case 'ne': {
			const filter = (row: Record<string, unknown>) => row[predicate.column.name] !== predicate.value;
			return fullScanRows(db, table).filter((m) => filter(m.row));
		}
		case 'gt':
		case 'gte':
		case 'lt':
		case 'lte': {
			const condition = makeRangeCondition(table, predicate.column, predicate.kind, predicate.value);
			const filter = makeRangeFilter(predicate.kind, predicate.column, predicate.value);
			const rows = condition
				? db
						.table(table.name)
						.query([condition])
						.map((rowJs) => ({ rowId: rowJs.rowId, row: rowFromRowJs(table, rowJs) }))
				: fullScanRows(db, table);
			return rows.filter((m) => filter(m.row));
		}
		case 'null': {
			const filter = predicate.not
				? (row: Record<string, unknown>) => row[predicate.column.name] != null
				: (row: Record<string, unknown>) => row[predicate.column.name] == null;
			return fullScanRows(db, table).filter((m) => filter(m.row));
		}
		case 'in': {
			if (predicate.values.length === 0) return [];
			const pushable = predicate.values.every(
				(v) => makeEqCondition(table, predicate.column, v) !== null
			);
			if (pushable) {
				const seen = new Set<bigint>();
				const matched: MatchedRow[] = [];
				for (const value of predicate.values) {
					const condition = makeEqCondition(table, predicate.column, value)!;
					for (const rowJs of db.table(table.name).query([condition])) {
						if (!seen.has(rowJs.rowId)) {
							seen.add(rowJs.rowId);
							matched.push({ rowId: rowJs.rowId, row: rowFromRowJs(table, rowJs) });
						}
					}
				}
				return matched;
			}
			return fullScanRows(db, table).filter((m) =>
				predicate.values.some((v) => m.row[predicate.column.name] === v)
			);
		}
		default:
			throw new KitError('Unexpected predicate kind');
	}
}

function unionRows(a: MatchedRow[], b: MatchedRow[]): MatchedRow[] {
	const map = new Map<bigint, MatchedRow>();
	for (const row of a) map.set(row.rowId, row);
	for (const row of b) {
		if (!map.has(row.rowId)) map.set(row.rowId, row);
	}
	return Array.from(map.values());
}

function intersectRows(a: MatchedRow[], b: MatchedRow[]): MatchedRow[] {
	const map = new Map<bigint, MatchedRow>();
	for (const row of a) map.set(row.rowId, row);
	const result: MatchedRow[] = [];
	for (const row of b) {
		if (map.has(row.rowId)) result.push(row);
	}
	return result;
}

function evaluatePredicate(
	db: NativeDatabase,
	table: TableSpec,
	predicate: Predicate
): MatchedRow[] {
	if (predicate.kind === 'and') {
		if (predicate.predicates.length === 0) return fullScanRows(db, table);
		let result = evaluatePredicate(db, table, predicate.predicates[0]!);
		for (let i = 1; i < predicate.predicates.length; i++) {
			result = intersectRows(result, evaluatePredicate(db, table, predicate.predicates[i]!));
			if (result.length === 0) break;
		}
		return result;
	}

	if (predicate.kind === 'or') {
		let result: MatchedRow[] = [];
		for (const sub of predicate.predicates) {
			result = unionRows(result, evaluatePredicate(db, table, sub));
		}
		return result;
	}

	return evaluateLeafPredicate(db, table, predicate);
}

function compareValues(a: unknown, b: unknown, direction: 'asc' | 'desc'): number {
	if (a === null || a === undefined) return direction === 'asc' ? 1 : -1;
	if (b === null || b === undefined) return direction === 'asc' ? -1 : 1;

	let cmp = 0;
	if (typeof a === 'bigint' && typeof b === 'bigint') {
		cmp = a < b ? -1 : a > b ? 1 : 0;
	} else if (typeof a === 'number' && typeof b === 'number') {
		cmp = a - b;
	} else if (typeof a === 'string' && typeof b === 'string') {
		cmp = a < b ? -1 : a > b ? 1 : 0;
	} else if (typeof a === 'boolean' && typeof b === 'boolean') {
		cmp = a === b ? 0 : a ? 1 : -1;
	} else {
		cmp = String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
	}

	return direction === 'asc' ? cmp : -cmp;
}

function applyOrderBy(rows: MatchedRow[], orders: OrderBy[]): MatchedRow[] {
	if (orders.length === 0) return rows;
	return [...rows].sort((a, b) => {
		for (const order of orders) {
			const cmp = compareValues(a.row[order.column.name], b.row[order.column.name], order.direction);
			if (cmp !== 0) return cmp;
		}
		return 0;
	});
}

function applyLimitOffset(rows: MatchedRow[], limit?: number, offset?: number): MatchedRow[] {
	let result = rows;
	if (offset !== undefined && offset > 0) {
		result = result.slice(offset);
	}
	if (limit !== undefined) {
		result = result.slice(0, limit);
	}
	return result;
}

function makeConstraintKit(kit: KitDatabase): ConstraintKit {
	return { db: kit.nativeDb, schema: kit.schema };
}

function makeDefaultContext(kit: KitDatabase): DefaultContext {
	return {
		now: new Date().toISOString(),
		uuid: () => randomUUID(),
		allocateSequence: () => {
			throw new KitError('Sequence defaults must be allocated before applyDefaults');
		}
	};
}

async function prepareInsertRow(
	kit: KitDatabase,
	table: TableSpec,
	row: Record<string, unknown>
): Promise<Record<string, unknown>> {
	const withSequence: Record<string, unknown> = { ...row };
	for (const col of table.columns) {
		if (
			(withSequence[col.name] === undefined || withSequence[col.name] === null) &&
			col.default?.kind === 'sequence'
		) {
			withSequence[col.name] = await kit.allocateSequence(col.default.name, 1);
		}
	}
	return applyDefaults(table, withSequence, makeDefaultContext(kit));
}

function pkValueFromRow(table: TableSpec, row: Record<string, unknown>): string | bigint {
	if (table.primaryKey.length !== 1) {
		throw new KitError(`Composite primary keys are not supported yet`);
	}
	const value = row[table.primaryKey[0]!];
	if (typeof value !== 'string' && typeof value !== 'bigint') {
		throw new KitError(`Primary key value must be string or bigint`);
	}
	return value;
}

function applyUpdateDefaults(
	table: TableSpec,
	merged: Record<string, unknown>,
	patch: Record<string, unknown>,
	ctx: DefaultContext
): void {
	for (const col of table.columns) {
		if (patch[col.name] !== undefined) continue;
		if (col.generated === 'now' || col.default?.kind === 'now') {
			merged[col.name] = ctx.now;
		}
	}
}

function hasForeignKeyChange(table: TableSpec, patch: Record<string, unknown>): boolean {
	return table.foreignKeys.some((fk) => fk.columns.some((colName) => patch[colName] !== undefined));
}

export class SelectBuilder<T extends TableSpec, TResult = Row<T>[]> {
	private _where?: Predicate;
	private _orderBy: OrderBy[] = [];
	private _limit?: number;
	private _offset?: number;
	private _columns?: ColumnSpec[];
	private _count = false;

	constructor(
		private readonly kit: KitDatabase,
		private readonly table: T
	) {}

	where(predicate: Predicate): SelectBuilder<T, TResult> {
		this._where = predicate;
		return this;
	}

	orderBy(...orders: OrderBy[]): SelectBuilder<T, TResult> {
		this._orderBy.push(...orders);
		return this;
	}

	limit(n: number): SelectBuilder<T, TResult> {
		this._limit = n;
		return this;
	}

	offset(n: number): SelectBuilder<T, TResult> {
		this._offset = n;
		return this;
	}

	select<C extends ColumnSpec>(columns: C[]): SelectBuilder<T, Pick<Row<T>, C['name']>> {
		const next = new SelectBuilder<T, Pick<Row<T>, C['name']>>(this.kit, this.table);
		next._where = this._where;
		next._orderBy = this._orderBy;
		next._limit = this._limit;
		next._offset = this._offset;
		next._columns = columns;
		return next;
	}

	selectCount(): SelectBuilder<T, bigint> {
		const next = new SelectBuilder<T, bigint>(this.kit, this.table);
		next._where = this._where;
		next._count = true;
		return next;
	}

	async execute(): Promise<TResult> {
		const db = this.kit.nativeDb;

		if (this._count) {
			if (!this._where) {
				return db.table(this.table.name).count() as TResult;
			}
			const matched = evaluatePredicate(db, this.table, this._where);
			return BigInt(matched.length) as TResult;
		}

		const matched = this._where
			? evaluatePredicate(db, this.table, this._where)
			: fullScanRows(db, this.table);

		let rows = applyOrderBy(matched, this._orderBy);
		rows = applyLimitOffset(rows, this._limit, this._offset);

		if (this._columns) {
			return rows.map((m) => {
				const projected: Record<string, unknown> = {};
				for (const col of this._columns!) {
					projected[col.name] = m.row[col.name];
				}
				return projected;
			}) as TResult;
		}

		return rows.map((m) => m.row) as TResult;
	}
}

export class InsertBuilder<T extends TableSpec> {
	private _row?: Insert<T>;

	constructor(
		private readonly kit: KitDatabase,
		private readonly table: T
	) {}

	values(row: Insert<T>): this {
		this._row = row;
		return this;
	}

	async execute(): Promise<Row<T>> {
		if (this._row === undefined) {
			throw new KitError('values() must be called before execute()');
		}
		const row = this._row as Record<string, unknown>;
		const defaulted = await prepareInsertRow(this.kit, this.table, row);
		validateRow(this.table, defaulted);
		const pkValue = pkValueFromRow(this.table, defaulted);
		const kit = makeConstraintKit(this.kit);

		await this.kit.nativeDb.transaction(async (txn) => {
			enforceForeignKeys(kit, txn, this.table, defaulted);
			stageUniqueGuards(kit, txn, this.table, defaulted, pkValue);
			txn.put(this.table.name, toCells(this.table, defaulted));
		});

		return defaulted as Row<T>;
	}
}

export class UpdateBuilder<T extends TableSpec> {
	private _patch?: Update<T>;
	private _where?: Predicate;

	constructor(
		private readonly kit: KitDatabase,
		private readonly table: T
	) {}

	set(patch: Update<T>): this {
		this._patch = patch;
		return this;
	}

	where(predicate: Predicate): this {
		this._where = predicate;
		return this;
	}

	async execute(): Promise<Row<T>[]> {
		if (this._patch === undefined) {
			throw new KitError('set() must be called before execute()');
		}
		const db = this.kit.nativeDb;
		const matches = this._where
			? evaluatePredicate(db, this.table, this._where)
			: fullScanRows(db, this.table);
		const patch = this._patch as Record<string, unknown>;
		const ctx = makeDefaultContext(this.kit);
		const kit = makeConstraintKit(this.kit);
		const updated: Record<string, unknown>[] = [];

		await this.kit.nativeDb.transaction(async (txn) => {
			for (const matched of matches) {
				const merged = { ...matched.row, ...patch };
				applyUpdateDefaults(this.table, merged, patch, ctx);
				validateRow(this.table, merged);
				const pkValue = pkValueFromRow(this.table, merged);
				deleteUniqueGuards(kit, txn, this.table, pkValue);
				if (hasForeignKeyChange(this.table, patch)) {
					enforceForeignKeys(kit, txn, this.table, merged);
				}
				txn.put(this.table.name, toCells(this.table, merged));
				updated.push(merged);
			}
		});

		return updated as Row<T>[];
	}
}

export class DeleteBuilder<T extends TableSpec> {
	private _where?: Predicate;

	constructor(
		private readonly kit: KitDatabase,
		private readonly table: T
	) {}

	where(predicate: Predicate): this {
		this._where = predicate;
		return this;
	}

	async execute(): Promise<bigint> {
		const db = this.kit.nativeDb;
		const matches = this._where
			? evaluatePredicate(db, this.table, this._where)
			: fullScanRows(db, this.table);
		const kit = makeConstraintKit(this.kit);
		let deleted = 0;

		await this.kit.nativeDb.transaction(async (txn) => {
			for (const matched of matches) {
				const pkValue = pkValueFromRow(this.table, matched.row);
				planDelete(kit, txn, this.table, pkValue);
				deleted++;
			}
		});

		return BigInt(deleted);
	}
}

(KitDatabase.prototype as any).selectFrom = function <T extends TableSpec>(
	this: KitDatabase,
	table: T
): SelectBuilder<T> {
	return new SelectBuilder(this, table);
};

(KitDatabase.prototype as any).insertInto = function <T extends TableSpec>(
	this: KitDatabase,
	table: T
): InsertBuilder<T> {
	return new InsertBuilder(this, table);
};

(KitDatabase.prototype as any).updateTable = function <T extends TableSpec>(
	this: KitDatabase,
	table: T
): UpdateBuilder<T> {
	return new UpdateBuilder(this, table);
};

(KitDatabase.prototype as any).deleteFrom = function <T extends TableSpec>(
	this: KitDatabase,
	table: T
): DeleteBuilder<T> {
	return new DeleteBuilder(this, table);
};
