import { randomUUID } from 'node:crypto';
import { ConditionKind } from 'mongreldb/native.js';
import type { Database as NativeDatabase, Cell, RowJs, Transaction } from 'mongreldb/native.js';
import { KitDatabase } from './db.js';
import type { TableSpec, ColumnSpec, PkValue } from './types.js';
import type { Row, Insert, Update } from './types.js';
import type { DefaultContext } from './defaults.js';
import { applyDefaults } from './defaults.js';
import { validateRow } from './validation.js';
import {
	toCells,
	stageUniqueGuards,
	stagePkGuard,
	deleteUniqueGuards,
	deletePkGuard,
	enforceForeignKeys,
	planDelete,
	pkValueFromRow,
	pkValuesEqual,
	type ConstraintKit
} from './constraints.js';
import { KitError, isRetryableConflict } from './errors.js';
import { encodedPk } from './keys.js';

declare module './db.js' {
	interface KitDatabase {
		selectFrom<T extends TableSpec>(table: T): SelectBuilder<T>;
		insertInto<T extends TableSpec>(table: T): InsertBuilder<T>;
		updateTable<T extends TableSpec>(table: T): UpdateBuilder<T>;
		deleteFrom<T extends TableSpec>(table: T): DeleteBuilder<T>;
		/**
		 * Materialize `builder` as a named CTE and return a scope whose
		 * `selectFrom(name)` reads those rows in memory. Chain `.with(...)` for
		 * additional CTEs.
		 */
		with(
			name: string,
			builder: { _materialize(): { rows: Record<string, unknown>[]; columns: ColumnSpec[] } }
		): CteScope;
	}
}

type ApplicationTypeMap = {
	bool: boolean;
	int64: bigint;
	float64: number;
	timestamp: string;
	date: string;
	text: string;
	bytes: unknown;
	json: unknown;
};

export type ColumnValue<T extends ColumnSpec> = T['applicationType'] extends keyof ApplicationTypeMap
	? ApplicationTypeMap[T['applicationType']]
	: never;

/**
 * Minimal contract a {@link SelectBuilder} satisfies so it can supply the value
 * set / existence test for `inSubquery` / `exists` / `notExists`. Decoupled from
 * the builder's generic parameters so `Predicate` need not depend on them.
 */
export interface Subquery {
	/** Values of the subquery's single selected column (for `IN (...)`). */
	scalarValuesSync(): unknown[];
	/** True when the subquery matches at least one row (for `EXISTS`). */
	hasRowsSync(): boolean;
}

export type Predicate =
	| { kind: 'and'; predicates: Predicate[] }
	| { kind: 'or'; predicates: Predicate[] }
	| { kind: 'not'; predicate: Predicate }
	| { kind: 'eq'; column: ColumnSpec; value: unknown }
	| { kind: 'ne'; column: ColumnSpec; value: unknown }
	| { kind: 'gt'; column: ColumnSpec; value: unknown }
	| { kind: 'gte'; column: ColumnSpec; value: unknown }
	| { kind: 'lt'; column: ColumnSpec; value: unknown }
	| { kind: 'lte'; column: ColumnSpec; value: unknown }
	| { kind: 'null'; column: ColumnSpec; not: boolean }
	| { kind: 'in'; column: ColumnSpec; values: unknown[] }
	| { kind: 'notIn'; column: ColumnSpec; values: unknown[] }
	| { kind: 'like'; column: ColumnSpec; pattern: string }
	| { kind: 'contains'; column: ColumnSpec; substr: string }
	| { kind: 'inSub'; column: ColumnSpec; subquery: Subquery }
	| { kind: 'exists'; subquery: Subquery; negate: boolean };

export type OrderBy = { column: ColumnSpec; direction: 'asc' | 'desc' };

type MatchedRow = { rowId: bigint; row: Record<string, unknown> };

/** Scalar aggregate kinds computed over a value set (count is handled apart). */
type ScalarAggKind = 'sum' | 'min' | 'max' | 'avg';

/** Result row of a join: keyed by table name, each side a row or null. */
export type JoinRow = Record<string, Record<string, unknown> | null>;

/** Join condition / post-join filter evaluated in JS over a {@link JoinRow}. */
export type JoinPredicate = (row: JoinRow) => boolean;

/** A single column aggregate used inside `GroupBuilder.aggregate`. */
export type AggregateSpec = { fn: 'count' | ScalarAggKind; column?: ColumnSpec };

/** One result row from a grouped query: group columns plus aggregate aliases. */
export type GroupRow = Record<string, unknown>;

const I64_MIN = -9_223_372_036_854_775_808n;
const I64_MAX = 9_223_372_036_854_775_807n;

/**
 * Validate that `column` is a real column spec. Guards against the footgun
 * where a column whose name shadows a table property (e.g. `name`) is accessed
 * as `table.name` and yields the table name string instead of the column.
 */
function asColumn(column: ColumnSpec): ColumnSpec {
	if (
		!column ||
		typeof column !== 'object' ||
		typeof (column as ColumnSpec).storageType !== 'string' ||
		typeof (column as ColumnSpec).id !== 'number'
	) {
		const got = typeof column === 'string' ? `the string "${column}"` : String(column);
		throw new KitError(
			`Expected a column, received ${got}. If your column name shadows a table ` +
				`property (e.g. "name"), access it with table.column('<name>').`
		);
	}
	return column;
}

export function eq<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'eq', column: asColumn(column), value };
}

export function ne<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'ne', column: asColumn(column), value };
}

export function gt<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'gt', column: asColumn(column), value };
}

export function gte<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'gte', column: asColumn(column), value };
}

export function lt<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'lt', column: asColumn(column), value };
}

export function lte<T extends ColumnSpec>(column: T, value: ColumnValue<T>): Predicate {
	return { kind: 'lte', column: asColumn(column), value };
}

export function isNull(column: ColumnSpec): Predicate {
	return { kind: 'null', column: asColumn(column), not: false };
}

export function isNotNull(column: ColumnSpec): Predicate {
	return { kind: 'null', column: asColumn(column), not: true };
}

export function inList<T extends ColumnSpec>(column: T, values: ColumnValue<T>[]): Predicate {
	return { kind: 'in', column: asColumn(column), values };
}

export function and(...predicates: Predicate[]): Predicate {
	return { kind: 'and', predicates };
}

export function or(...predicates: Predicate[]): Predicate {
	return { kind: 'or', predicates };
}

export function asc(column: ColumnSpec): OrderBy {
	return { column: asColumn(column), direction: 'asc' };
}

export function desc(column: ColumnSpec): OrderBy {
	return { column: asColumn(column), direction: 'desc' };
}

/** Negates a predicate (logical NOT). */
export function not(predicate: Predicate): Predicate {
	return { kind: 'not', predicate };
}

/** `column NOT IN (values)`. */
export function notInList<T extends ColumnSpec>(column: T, values: ColumnValue<T>[]): Predicate {
	return { kind: 'notIn', column: asColumn(column), values };
}

/**
 * SQL `LIKE` against a text column. `%` matches any run of characters and `_`
 * matches a single character; all other characters are literal. Case-sensitive.
 */
export function like<T extends ColumnSpec>(column: T, pattern: string): Predicate {
	return { kind: 'like', column: asColumn(column), pattern };
}

/** Case-sensitive substring match: `column LIKE '%substr%'` with no wildcards. */
export function contains<T extends ColumnSpec>(column: T, substr: string): Predicate {
	return { kind: 'contains', column: asColumn(column), substr };
}

/** `column IN (subquery)`. The subquery must select exactly one column. */
export function inSubquery<T extends ColumnSpec>(column: T, subquery: Subquery): Predicate {
	return { kind: 'inSub', column, subquery };
}

/**
 * `EXISTS (subquery)`. The subquery is uncorrelated: it is evaluated once and
 * gates the whole outer scan.
 * // ponytail: no correlated-subquery support; correlation would require
 * // re-binding the outer row into the subquery per candidate.
 */
export function exists(subquery: Subquery): Predicate {
	return { kind: 'exists', subquery, negate: false };
}

/** `NOT EXISTS (subquery)`. Uncorrelated, like {@link exists}. */
export function notExists(subquery: Subquery): Predicate {
	return { kind: 'exists', subquery, negate: true };
}

/** Aggregate descriptor: `COUNT(*)` for a group. */
export function count(): AggregateSpec {
	return { fn: 'count' };
}

/** Aggregate descriptor: `SUM(column)` for a group. */
export function sum(column: ColumnSpec): AggregateSpec {
	return { fn: 'sum', column };
}

/** Aggregate descriptor: `MIN(column)` for a group. */
export function min(column: ColumnSpec): AggregateSpec {
	return { fn: 'min', column };
}

/** Aggregate descriptor: `MAX(column)` for a group. */
export function max(column: ColumnSpec): AggregateSpec {
	return { fn: 'max', column };
}

/** Aggregate descriptor: `AVG(column)` for a group (always a float). */
export function avg(column: ColumnSpec): AggregateSpec {
	return { fn: 'avg', column };
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

	if (column.storageType === 'int64') {
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
	if (column.storageType === 'int64') {
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
	const intColumn = table.columns.find((c) => c.storageType === 'int64');
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

	const pkColumn = table.columns.find((c) => table.primaryKey.includes(c.name));
	if (pkColumn) {
		return {
			kind: ConditionKind.Pk,
			columnId: pkColumn.id
		};
	}

	throw new KitError(`Full table scan on "${table.name}" requires an int64, float64, or primary key`);
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

	if (predicate.kind === 'not') {
		const inner = evaluatePredicate(db, table, predicate.predicate);
		const removed = new Set(inner.map((m) => m.rowId));
		return fullScanRows(db, table).filter((m) => !removed.has(m.rowId));
	}

	if (predicate.kind === 'exists') {
		// Uncorrelated EXISTS: evaluate the subquery once; it gates the scan.
		return predicate.subquery.hasRowsSync() !== predicate.negate ? fullScanRows(db, table) : [];
	}

	if (predicate.kind === 'inSub') {
		const values = predicate.subquery.scalarValuesSync();
		return evaluateLeafPredicate(db, table, { kind: 'in', column: predicate.column, values });
	}

	if (predicate.kind === 'like' || predicate.kind === 'contains' || predicate.kind === 'notIn') {
		// ponytail: no native FM-index / set pushdown for these; the only index
		// kind the kit creates is Bitmap, so we full-scan and match in JS.
		return fullScanRows(db, table).filter((m) => matchRowPredicate(m.row, predicate));
	}

	return evaluateLeafPredicate(db, table, predicate);
}

/**
 * Pure in-memory predicate evaluation against a single plain row. Used for
 * CTE-materialized sources (no native table to push down into) and for the
 * leaf kinds with no native condition. Mirrors {@link evaluatePredicate}.
 */
function matchRowPredicate(row: Record<string, unknown>, predicate: Predicate): boolean {
	switch (predicate.kind) {
		case 'and':
			return predicate.predicates.every((p) => matchRowPredicate(row, p));
		case 'or':
			return predicate.predicates.some((p) => matchRowPredicate(row, p));
		case 'not':
			return !matchRowPredicate(row, predicate.predicate);
		case 'eq':
			return row[predicate.column.name] === predicate.value;
		case 'ne':
			return row[predicate.column.name] !== predicate.value;
		case 'gt':
		case 'gte':
		case 'lt':
		case 'lte':
			return makeRangeFilter(predicate.kind, predicate.column, predicate.value)(row);
		case 'null':
			return predicate.not
				? row[predicate.column.name] != null
				: row[predicate.column.name] == null;
		case 'in':
			return predicate.values.some((v) => row[predicate.column.name] === v);
		case 'notIn':
			return !predicate.values.some((v) => row[predicate.column.name] === v);
		case 'like': {
			const value = row[predicate.column.name];
			return value != null && likeToRegex(predicate.pattern).test(String(value));
		}
		case 'contains': {
			const value = row[predicate.column.name];
			return value != null && String(value).includes(predicate.substr);
		}
		case 'inSub':
			return predicate.subquery
				.scalarValuesSync()
				.some((v) => row[predicate.column.name] === v);
		case 'exists':
			return predicate.subquery.hasRowsSync() !== predicate.negate;
		default:
			throw new KitError('Unexpected predicate kind');
	}
}

/** Compiles a SQL `LIKE` pattern into an anchored, case-sensitive RegExp. */
function likeToRegex(pattern: string): RegExp {
	let out = '^';
	for (const ch of pattern) {
		if (ch === '%') out += '.*';
		else if (ch === '_') out += '.';
		else out += ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
	}
	out += '$';
	return new RegExp(out, 's');
}

/**
 * Resolve the rows for a builder honoring its `where`, choosing the native
 * pushdown path or — when the builder is backed by a CTE-materialized source —
 * a pure in-memory filter.
 */
function resolveRows(
	db: NativeDatabase,
	table: TableSpec,
	where: Predicate | undefined,
	source: MatchedRow[] | undefined
): MatchedRow[] {
	if (source) {
		return where ? source.filter((m) => matchRowPredicate(m.row, where)) : source;
	}
	return where ? evaluatePredicate(db, table, where) : fullScanRows(db, table);
}

/** Stable string key for a value, distinguishing types (for distinct/group). */
function valueKey(value: unknown): string {
	if (value === null || value === undefined) return '\u0000';
	const t = typeof value;
	if (t === 'bigint') return 'b' + (value as bigint).toString();
	if (t === 'number') return 'n' + String(value);
	if (t === 'boolean') return value ? 'T' : 'F';
	if (t === 'string') return 's' + (value as string);
	return 'j' + JSON.stringify(value);
}

function compositeKey(values: unknown[]): string {
	return values.map(valueKey).join('\u0001');
}

/** Computes a scalar aggregate over matched rows, honoring NULL skipping. */
function computeAggregate(
	kind: ScalarAggKind,
	column: ColumnSpec,
	rows: MatchedRow[]
): bigint | number | string | null {
	const isInt = column.storageType === 'int64';
	const values = rows
		.map((m) => m.row[column.name])
		.filter((v) => v !== null && v !== undefined);
	switch (kind) {
		case 'sum': {
			if (isInt) {
				let s = 0n;
				for (const v of values) s += v as bigint;
				return s;
			}
			let s = 0;
			for (const v of values) s += Number(v);
			return s;
		}
		case 'avg': {
			if (values.length === 0) return null;
			let s = 0;
			for (const v of values) s += Number(v);
			return s / values.length;
		}
		case 'min': {
			if (values.length === 0) return null;
			let best = values[0];
			for (const v of values) if (compareValues(v, best, 'asc') < 0) best = v;
			return best as bigint | number | string;
		}
		case 'max': {
			if (values.length === 0) return null;
			let best = values[0];
			for (const v of values) if (compareValues(v, best, 'asc') > 0) best = v;
			return best as bigint | number | string;
		}
	}
}

/** Synthetic table spec backing an in-memory CTE source. */
function syntheticTable(name: string, columns: ColumnSpec[]): TableSpec {
	return {
		tableId: 0,
		name,
		columns,
		primaryKey: [],
		indexes: [],
		foreignKeys: [],
		unique: [],
		checks: [],
		column(columnName: string): ColumnSpec {
			const col = columns.find((c) => c.name === columnName);
			if (!col) throw new KitError(`Column "${columnName}" not found in CTE "${name}"`);
			return col;
		}
	};
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
	return applyArrayLimitOffset(rows, limit, offset);
}

function applyArrayLimitOffset<T>(rows: T[], limit?: number, offset?: number): T[] {
	let result = rows;
	if (offset !== undefined && offset > 0) {
		result = result.slice(offset);
	}
	if (limit !== undefined) {
		result = result.slice(0, limit);
	}
	return result;
}

/** Deduplicate output rows by the listed column names (keeps first seen). */
function dedupeRows(
	rows: Record<string, unknown>[],
	columnNames: string[]
): Record<string, unknown>[] {
	const seen = new Set<string>();
	const out: Record<string, unknown>[] = [];
	for (const row of rows) {
		const key = compositeKey(columnNames.map((n) => row[n]));
		if (!seen.has(key)) {
			seen.add(key);
			out.push(row);
		}
	}
	return out;
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

function prepareInsertRowSync(
	kit: KitDatabase,
	table: TableSpec,
	row: Record<string, unknown>
): Record<string, unknown> {
	const withSequence: Record<string, unknown> = { ...row };
	for (const col of table.columns) {
		if (
			(withSequence[col.name] === undefined || withSequence[col.name] === null) &&
			col.default?.kind === 'sequence'
		) {
			withSequence[col.name] = kit.allocateSequenceSync(col.default.name, 1);
		}
	}
	const withDefaults = applyDefaults(table, withSequence, makeDefaultContext(kit));
	// Normalize any column still unset to explicit null, so the stored row and
	// the returned row agree (an unset nullable column reads back as null).
	for (const col of table.columns) {
		if (withDefaults[col.name] === undefined) withDefaults[col.name] = null;
	}
	return withDefaults;
}

function runSyncTxn(
	kit: KitDatabase,
	fn: (txn: Transaction) => void,
	opts: { maxRetries?: number; baseDelayMs?: number } = {}
): void {
	const maxRetries = opts.maxRetries ?? 5;
	const baseDelayMs = opts.baseDelayMs ?? 1;

	let lastErr: unknown;
	for (let attempt = 0; attempt <= maxRetries; attempt++) {
		const txn = kit.begin();
		try {
			fn(txn);
			txn.commit();
			return;
		} catch (err) {
			try {
				txn.rollback();
			} catch {
				// ignore rollback errors
			}
			lastErr = err;
			// Kit-level conflicts (e.g. unique-guard or row-guard write-write
			// races) and native MongrelDB conflicts are both retryable. The
			// native addon prefixes commit-time conflict messages with
			// `__CONFLICT__:`; the kit throws KitConflictError.
			if (attempt < maxRetries && isRetryableConflict(err)) {
				// Synchronous bounded backoff. Keep the delay small so single-
				// threaded tests stay fast; the loop bound guarantees forward
				// progress.
				const delay = baseDelayMs * (attempt + 1);
				if (delay > 0) {
					const start = Date.now();
					while (Date.now() - start < delay) {
						// busy wait
					}
				}
				continue;
			}
			throw err;
		}
	}
	throw lastErr;
}

function applyUpdateDefaults(
	table: TableSpec,
	merged: Record<string, unknown>,
	patch: Record<string, unknown>,
	ctx: DefaultContext
): void {
	for (const col of table.columns) {
		if (patch[col.name] !== undefined) continue;
		// Only `generated: 'now'` columns are write-managed timestamps that refresh
		// on every update (e.g. updatedAt). A plain `default: nowDefault()` is an
		// insert-time value (e.g. createdAt) and must NOT change on update.
		if (col.generated === 'now') {
			merged[col.name] = ctx.now;
		}
	}
}

function hasForeignKeyChange(table: TableSpec, patch: Record<string, unknown>): boolean {
	return table.foreignKeys.some((fk) => fk.columns.some((colName) => patch[colName] !== undefined));
}

/** SUM over an int64 column yields bigint; over a float64 column yields number. */
type SumResult<C extends ColumnSpec> = C['applicationType'] extends 'int64' ? bigint : number;

export class SelectBuilder<T extends TableSpec, TResult = Row<T>[]> implements Subquery {
	private _where?: Predicate;
	private _orderBy: OrderBy[] = [];
	private _limit?: number;
	private _offset?: number;
	private _columns?: ColumnSpec[];
	private _count = false;
	private _distinct = false;
	private _aggregate?: { kind: ScalarAggKind; column: ColumnSpec };
	/** Internal: in-memory rows backing a CTE source instead of a native table. */
	_source?: MatchedRow[];

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

	/** Remove duplicate result rows (over the selected columns). */
	distinct(): SelectBuilder<T, TResult> {
		this._distinct = true;
		return this;
	}

	select<C extends ColumnSpec>(columns: C[]): SelectBuilder<T, Pick<Row<T>, C['name']>> {
		const next = new SelectBuilder<T, Pick<Row<T>, C['name']>>(this.kit, this.table);
		next._where = this._where;
		next._orderBy = this._orderBy;
		next._limit = this._limit;
		next._offset = this._offset;
		next._columns = columns;
		next._distinct = this._distinct;
		next._source = this._source;
		return next;
	}

	private cloneScalar<R>(): SelectBuilder<T, R> {
		const next = new SelectBuilder<T, R>(this.kit, this.table);
		next._where = this._where;
		next._source = this._source;
		return next;
	}

	selectCount(): SelectBuilder<T, bigint> {
		const next = this.cloneScalar<bigint>();
		next._count = true;
		return next;
	}

	selectSum<C extends ColumnSpec>(column: C): SelectBuilder<T, SumResult<C>> {
		const next = this.cloneScalar<SumResult<C>>();
		next._aggregate = { kind: 'sum', column };
		return next;
	}

	selectAvg(column: ColumnSpec): SelectBuilder<T, number | null> {
		const next = this.cloneScalar<number | null>();
		next._aggregate = { kind: 'avg', column };
		return next;
	}

	selectMin<C extends ColumnSpec>(column: C): SelectBuilder<T, ColumnValue<C> | null> {
		const next = this.cloneScalar<ColumnValue<C> | null>();
		next._aggregate = { kind: 'min', column };
		return next;
	}

	selectMax<C extends ColumnSpec>(column: C): SelectBuilder<T, ColumnValue<C> | null> {
		const next = this.cloneScalar<ColumnValue<C> | null>();
		next._aggregate = { kind: 'max', column };
		return next;
	}

	/** Start an INNER JOIN. The `on` predicate runs in JS over the joined row. */
	innerJoin(table: TableSpec, on: JoinPredicate): JoinBuilder {
		return this.startJoin().innerJoin(table, on);
	}

	/** Start a LEFT JOIN; unmatched right side is null in the result row. */
	leftJoin(table: TableSpec, on: JoinPredicate): JoinBuilder {
		return this.startJoin().leftJoin(table, on);
	}

	/** Start a CROSS JOIN (cartesian product; no predicate). */
	crossJoin(table: TableSpec): JoinBuilder {
		return this.startJoin().crossJoin(table);
	}

	private startJoin(): JoinBuilder {
		return new JoinBuilder(this.kit, this.table, this._where, this._source);
	}

	/** Group matched rows by the given columns and compute aggregates per group. */
	groupBy(...columns: ColumnSpec[]): GroupBuilder<T> {
		return new GroupBuilder<T>(this.kit, this.table, columns, this._where, this._source);
	}

	private resolveMatched(): MatchedRow[] {
		return resolveRows(this.kit.nativeDb, this.table, this._where, this._source);
	}

	/** Bind an in-memory source (used by CTE materialization). Internal. */
	_bindSource(rows: MatchedRow[]): this {
		this._source = rows;
		return this;
	}

	/** Run the query and capture its rows + output columns for CTE materialization. */
	_materialize(): { rows: Record<string, unknown>[]; columns: ColumnSpec[] } {
		const result = this.executeSync();
		if (!Array.isArray(result)) {
			throw new KitError('Only a row-returning select can back a CTE');
		}
		const columns = this._columns ?? [...this.table.columns];
		return { rows: result as Record<string, unknown>[], columns };
	}

	scalarValuesSync(): unknown[] {
		if (this._count || this._aggregate) {
			throw new KitError('A subquery used in IN/EXISTS must select rows, not an aggregate');
		}
		let colName: string | undefined;
		if (this._columns) {
			if (this._columns.length !== 1) {
				throw new KitError('An IN subquery must select exactly one column');
			}
			colName = this._columns[0].name;
		} else if (this.table.primaryKey.length === 1) {
			colName = this.table.primaryKey[0];
		} else {
			colName = this.table.columns[0]?.name;
		}
		if (!colName) return [];
		return this.resolveMatched().map((m) => m.row[colName!]);
	}

	hasRowsSync(): boolean {
		return this.resolveMatched().length > 0;
	}

	executeSync(): TResult {
		const db = this.kit.nativeDb;

		if (this._aggregate) {
			return computeAggregate(
				this._aggregate.kind,
				this._aggregate.column,
				this.resolveMatched()
			) as TResult;
		}

		if (this._count) {
			if (!this._where && !this._source) {
				return db.table(this.table.name).count() as TResult;
			}
			return BigInt(this.resolveMatched().length) as TResult;
		}

		const matched = this.resolveMatched();
		let rows = applyOrderBy(matched, this._orderBy);
		if (!this._distinct) {
			rows = applyLimitOffset(rows, this._limit, this._offset);
		}

		const project = (m: MatchedRow): Record<string, unknown> => {
			if (!this._columns) return m.row;
			const projected: Record<string, unknown> = {};
			for (const col of this._columns) projected[col.name] = m.row[col.name];
			return projected;
		};

		let out = rows.map(project);

		if (this._distinct) {
			const columnNames = this._columns
				? this._columns.map((c) => c.name)
				: this.table.columns.map((c) => c.name);
			out = dedupeRows(out, columnNames);
			out = applyArrayLimitOffset(out, this._limit, this._offset);
		}

		return out as TResult;
	}

	async execute(): Promise<TResult> {
		return this.executeSync();
	}
}

/**
 * Nested-loop join executed entirely in JS. The result is a {@link JoinRow}
 * keyed by table name — e.g. `{ users: { ... }, orders: { ... } }`. For a LEFT
 * JOIN with no match, the joined side is `null`.
 */
export class JoinBuilder {
	private readonly clauses: { table: TableSpec; kind: 'inner' | 'left' | 'cross'; on?: JoinPredicate }[] =
		[];
	private _where?: JoinPredicate;
	private _limit?: number;
	private _offset?: number;

	constructor(
		private readonly kit: KitDatabase,
		private readonly baseTable: TableSpec,
		private readonly baseWhere?: Predicate,
		private readonly baseSource?: MatchedRow[]
	) {}

	innerJoin(table: TableSpec, on: JoinPredicate): this {
		this.clauses.push({ table, kind: 'inner', on });
		return this;
	}

	leftJoin(table: TableSpec, on: JoinPredicate): this {
		this.clauses.push({ table, kind: 'left', on });
		return this;
	}

	crossJoin(table: TableSpec): this {
		this.clauses.push({ table, kind: 'cross' });
		return this;
	}

	/** Post-join filter over the assembled {@link JoinRow}. */
	where(predicate: JoinPredicate): this {
		this._where = predicate;
		return this;
	}

	limit(n: number): this {
		this._limit = n;
		return this;
	}

	offset(n: number): this {
		this._offset = n;
		return this;
	}

	executeSync(): JoinRow[] {
		const db = this.kit.nativeDb;
		const baseRows = resolveRows(db, this.baseTable, this.baseWhere, this.baseSource);
		let combos: JoinRow[] = baseRows.map((m) => ({ [this.baseTable.name]: m.row }));

		for (const clause of this.clauses) {
			// ponytail: nested-loop join with no predicate pushdown — the joined
			// table is fully scanned in JS once and re-evaluated per combination.
			const joinRows = fullScanRows(db, clause.table).map((m) => m.row);
			const next: JoinRow[] = [];
			for (const combo of combos) {
				if (clause.kind === 'cross') {
					for (const jr of joinRows) next.push({ ...combo, [clause.table.name]: jr });
					continue;
				}
				let matched = false;
				for (const jr of joinRows) {
					const candidate: JoinRow = { ...combo, [clause.table.name]: jr };
					if (clause.on!(candidate)) {
						next.push(candidate);
						matched = true;
					}
				}
				if (clause.kind === 'left' && !matched) {
					next.push({ ...combo, [clause.table.name]: null });
				}
			}
			combos = next;
		}

		if (this._where) combos = combos.filter(this._where);
		return applyArrayLimitOffset(combos, this._limit, this._offset);
	}

	async execute(): Promise<JoinRow[]> {
		return this.executeSync();
	}
}

/**
 * Grouped aggregation executed in JS. Each result row carries the group-by
 * column values plus one entry per named aggregate.
 */
export class GroupBuilder<T extends TableSpec> {
	private _aggregates: Record<string, AggregateSpec> = {};
	private _having?: (row: GroupRow) => boolean;

	constructor(
		private readonly kit: KitDatabase,
		private readonly table: T,
		private readonly groupColumns: ColumnSpec[],
		private readonly _where?: Predicate,
		private readonly _source?: MatchedRow[]
	) {}

	/** Declare the named aggregates to compute per group. */
	aggregate(spec: Record<string, AggregateSpec>): this {
		this._aggregates = spec;
		return this;
	}

	/** Filter groups after aggregation (HAVING), over the assembled group row. */
	having(predicate: (row: GroupRow) => boolean): this {
		this._having = predicate;
		return this;
	}

	executeSync(): GroupRow[] {
		const rows = resolveRows(this.kit.nativeDb, this.table, this._where, this._source);
		const groups = new Map<string, { values: unknown[]; rows: MatchedRow[] }>();
		for (const m of rows) {
			const values = this.groupColumns.map((c) => m.row[c.name]);
			const key = compositeKey(values);
			let g = groups.get(key);
			if (!g) {
				g = { values, rows: [] };
				groups.set(key, g);
			}
			g.rows.push(m);
		}

		const out: GroupRow[] = [];
		for (const g of groups.values()) {
			const result: GroupRow = {};
			this.groupColumns.forEach((c, i) => {
				result[c.name] = g.values[i];
			});
			for (const [alias, spec] of Object.entries(this._aggregates)) {
				result[alias] =
					spec.fn === 'count'
						? BigInt(g.rows.length)
						: computeAggregate(spec.fn, spec.column!, g.rows);
			}
			if (!this._having || this._having(result)) out.push(result);
		}
		return out;
	}

	async execute(): Promise<GroupRow[]> {
		return this.executeSync();
	}
}

/**
 * A scope of materialized common table expressions (CTEs). Each `with` runs its
 * builder eagerly and stores the result rows in memory so a later `selectFrom`
 * can read them as if they were a table.
 * // ponytail: full in-memory materialization — CTEs are not lazy/recursive.
 */
export class CteScope {
	private readonly ctes = new Map<string, { rows: MatchedRow[]; table: TableSpec }>();

	constructor(private readonly kit: KitDatabase) {}

	with(
		name: string,
		builder: { _materialize(): { rows: Record<string, unknown>[]; columns: ColumnSpec[] } }
	): CteScope {
		const { rows, columns } = builder._materialize();
		const table = syntheticTable(name, columns);
		const matched: MatchedRow[] = rows.map((row, i) => ({ rowId: BigInt(i), row }));
		this.ctes.set(name, { rows: matched, table });
		return this;
	}

	selectFrom(name: string): SelectBuilder<TableSpec, Record<string, unknown>[]> {
		const cte = this.ctes.get(name);
		if (!cte) {
			throw new KitError(`CTE "${name}" is not defined in this scope`);
		}
		return new SelectBuilder<TableSpec, Record<string, unknown>[]>(
			this.kit,
			cte.table
		)._bindSource(cte.rows);
	}
}

/** Compute the row passed through defaults + the PK-explicit flag for an insert. */
function prepareInsertSync<T extends TableSpec>(
	kit: KitDatabase,
	table: T,
	row: Record<string, unknown>
): { defaulted: Record<string, unknown>; pkValue: PkValue; pkExplicit: boolean } {
	const pkExplicit = table.primaryKey.every(
		(name) => row[name] !== undefined && row[name] !== null
	);
	const defaulted = prepareInsertRowSync(kit, table, row);
	validateRow(table, defaulted);
	const pkValue = pkValueFromRow(table, defaulted);
	return { defaulted, pkValue, pkExplicit };
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

	/**
	 * Insert many rows in a single transaction. Each row still passes through
	 * defaults, validation, and constraint checks, but the whole batch commits
	 * once — far faster than a row-at-a-time loop for bulk loads.
	 */
	valuesMany(rows: Insert<T>[]): InsertManyBuilder<T> {
		return new InsertManyBuilder<T>(this.kit, this.table, rows);
	}

	executeSync(): Row<T> {
		if (this._row === undefined) {
			throw new KitError('values() must be called before execute()');
		}
		const { defaulted, pkValue, pkExplicit } = prepareInsertSync(
			this.kit,
			this.table,
			this._row as Record<string, unknown>
		);
		const kit = makeConstraintKit(this.kit);

		runSyncTxn(this.kit, (txn) => {
			enforceForeignKeys(kit, txn, this.table, defaulted);
			stageUniqueGuards(kit, txn, this.table, defaulted, pkValue);
			stagePkGuard(kit, txn, this.table, pkValue, pkExplicit);
			txn.put(this.table.name, toCells(this.table, defaulted));
		});

		return defaulted as Row<T>;
	}

	async execute(): Promise<Row<T>> {
		return this.executeSync();
	}
}

export class InsertManyBuilder<T extends TableSpec> {
	constructor(
		private readonly kit: KitDatabase,
		private readonly table: T,
		private readonly rows: Insert<T>[]
	) {}

	executeSync(): Row<T>[] {
		const kit = makeConstraintKit(this.kit);
		const results: Record<string, unknown>[] = [];

		runSyncTxn(this.kit, (txn) => {
			results.length = 0;
			// For a single-column PK, load the existing PKs once so the per-row
			// duplicate check is O(1) instead of a per-row table scan.
			const pkSeen =
				this.table.primaryKey.length === 1
					? new Set(
							fullScanRows(this.kit.nativeDb, this.table).map((m) =>
								encodedPk(pkValueFromRow(this.table, m.row))
							)
						)
					: undefined;
			for (const input of this.rows) {
				const { defaulted, pkValue, pkExplicit } = prepareInsertSync(
					this.kit,
					this.table,
					input as Record<string, unknown>
				);
				enforceForeignKeys(kit, txn, this.table, defaulted);
				stageUniqueGuards(kit, txn, this.table, defaulted, pkValue);
				stagePkGuard(kit, txn, this.table, pkValue, pkExplicit, pkSeen);
				txn.put(this.table.name, toCells(this.table, defaulted));
				results.push(defaulted);
			}
		});

		return results as Row<T>[];
	}

	async execute(): Promise<Row<T>[]> {
		return this.executeSync();
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

	executeSync(): Row<T>[] {
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

		runSyncTxn(this.kit, (txn) => {
			for (const matched of matches) {
				const merged = { ...matched.row, ...patch };
				applyUpdateDefaults(this.table, merged, patch, ctx);
				validateRow(this.table, merged);
				const oldPkValue = pkValueFromRow(this.table, matched.row);
				const newPkValue = pkValueFromRow(this.table, merged);
				const pkChanged = !pkValuesEqual(oldPkValue, newPkValue);
				deleteUniqueGuards(kit, txn, this.table, oldPkValue);
				if (pkChanged) {
					deletePkGuard(kit, txn, this.table, oldPkValue);
				}
				if (hasForeignKeyChange(this.table, patch)) {
					enforceForeignKeys(kit, txn, this.table, merged);
				}
				txn.delete(this.table.name, matched.rowId);
				txn.put(this.table.name, toCells(this.table, merged));
				stageUniqueGuards(kit, txn, this.table, merged, newPkValue);
				if (pkChanged) {
					stagePkGuard(kit, txn, this.table, newPkValue, true);
				}
				updated.push(merged);
			}
		});

		return updated as Row<T>[];
	}

	async execute(): Promise<Row<T>[]> {
		return this.executeSync();
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

	executeSync(): bigint {
		const db = this.kit.nativeDb;
		const matches = this._where
			? evaluatePredicate(db, this.table, this._where)
			: fullScanRows(db, this.table);
		const kit = makeConstraintKit(this.kit);
		let deleted = 0;

		runSyncTxn(this.kit, (txn) => {
			deleted = 0;
			for (const matched of matches) {
				const pkValue = pkValueFromRow(this.table, matched.row);
				// Reuse the row already fetched by the scan to avoid an O(n^2) re-read.
				planDelete(kit, txn, this.table, pkValue, {
					row: matched.row,
					rowId: matched.rowId
				});
				deleted++;
			}
		});

		return BigInt(deleted);
	}

	async execute(): Promise<bigint> {
		return this.executeSync();
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(KitDatabase.prototype as any).with = function (
	this: KitDatabase,
	name: string,
	builder: { _materialize(): { rows: Record<string, unknown>[]; columns: ColumnSpec[] } }
): CteScope {
	return new CteScope(this).with(name, builder);
};
