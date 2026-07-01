import { randomUUID } from 'node:crypto';
import { tableFromIPC, type Table as ArrowTable } from 'apache-arrow';
import { ConditionKind } from 'mongreldb/native.js';
import type { Database as NativeDatabase, ConditionSpec, Transaction } from 'mongreldb/native.js';
import { KitDatabase, runSyncTxn } from './db.js';
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
	findByPk,
	isReferencedTable,
	type ConstraintKit
} from './constraints.js';
import { KitError } from './errors.js';
import { encodedPk } from './keys.js';
import { packRows, packRowIds } from './packing.js';
import type { Schema } from './schema.js';
import { rowFromRowJs } from './rows.js';

declare module './db.js' {
	interface KitDatabase {
		selectFrom<T extends TableSpec>(table: T): SelectBuilder<T>;
		insertInto<T extends TableSpec>(table: T): InsertBuilder<T>;
		updateTable<T extends TableSpec>(table: T): UpdateBuilder<T>;
		deleteFrom<T extends TableSpec>(table: T): DeleteBuilder<T>;
		truncateTable(tableName: string): void;
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
	: unknown;

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

/** The column-equality a {@link joinEq} predicate carries so the join builder
 * can probe the right table by index instead of full-scanning it. */
export interface JoinEqKey {
	leftTable: string;
	leftColumn: ColumnSpec;
	rightTable: string;
	rightColumn: ColumnSpec;
}

function joinValuesEqual(a: unknown, b: unknown): boolean {
	if (a === null || a === undefined || b === null || b === undefined) return a === b;
	if (typeof a === 'bigint' || typeof b === 'bigint') return BigInt(a as never) === BigInt(b as never);
	return a === b;
}

/**
 * A declarative join predicate equating `leftTable.leftColumn` with
 * `rightTable.rightColumn`. Behaves like the closure form, but the builder can
 * introspect the equality to fetch the right table by an index probe over the
 * distinct left keys instead of a full scan. Prefer this over a hand-written
 * closure for FK joins.
 */
export function joinEq(
	leftTable: TableSpec,
	leftColumn: ColumnSpec,
	rightTable: TableSpec,
	rightColumn: ColumnSpec
): JoinPredicate {
	const pred: JoinPredicate & { __eqKey?: JoinEqKey } = (row) =>
		joinValuesEqual(
			(row[leftTable.name] as Record<string, unknown> | null)?.[leftColumn.name],
			(row[rightTable.name] as Record<string, unknown> | null)?.[rightColumn.name]
		);
	pred.__eqKey = {
		leftTable: leftTable.name,
		leftColumn,
		rightTable: rightTable.name,
		rightColumn
	};
	return pred;
}

/** A single column aggregate used inside `GroupBuilder.aggregate`. `distinct`
 * de-duplicates the column's values (e.g. `COUNT(DISTINCT col)`); it requires a
 * `column` and is a no-op for `min`/`max`. */
export type AggregateSpec = { fn: 'count' | ScalarAggKind; column?: ColumnSpec; distinct?: boolean };

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

/** Aggregate descriptor: `COUNT(column)` — non-null values in a group. */
export function countColumn(column: ColumnSpec): AggregateSpec {
	return { fn: 'count', column };
}

/** Aggregate descriptor: `COUNT(DISTINCT column)` — unique non-null values. */
export function countDistinct(column: ColumnSpec): AggregateSpec {
	return { fn: 'count', column, distinct: true };
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

function isIndexed(table: TableSpec, columnName: string): boolean {
	if (table.primaryKey.includes(columnName)) return true;
	if (table.indexes.some((idx) => idx.columns.includes(columnName))) return true;
	if (table.foreignKeys.some((fk) => fk.columns.includes(columnName))) return true;
	return false;
}

type PredicatePlan = {
	conditions: ConditionSpec[];
	residual?: Predicate;
	alwaysFalse?: boolean;
};

function isBitmapTextColumn(column: ColumnSpec): boolean {
	return (
		column.storageType === 'text' ||
		column.storageType === 'timestamp' ||
		column.storageType === 'date' ||
		column.storageType === 'json'
	);
}

function makeEqCondition(table: TableSpec, column: ColumnSpec, value: unknown): ConditionSpec | null {
	if (value === null || value === undefined) return null;

	if (column.storageType === 'int64') {
		if (typeof value !== 'bigint') return null;
		return {
			kind: ConditionKind.RangeInt,
			columnId: column.id,
			int64Lo: value,
			int64Hi: value
		};
	}

	if (column.storageType === 'float64') {
		if (typeof value !== 'number' || Number.isNaN(value)) return null;
		return {
			kind: ConditionKind.RangeF64,
			columnId: column.id,
			float64Lo: value,
			float64Hi: value
		};
	}

	if (isIndexed(table, column.name) && isBitmapTextColumn(column)) {
		return {
			kind: ConditionKind.BitmapEq,
			columnId: column.id,
			text: String(value)
		};
	}

	return null;
}

function makeInCondition(
	table: TableSpec,
	column: ColumnSpec,
	values: unknown[]
): ConditionSpec | null {
	if (!isIndexed(table, column.name) || !isBitmapTextColumn(column)) return null;
	if (values.length === 0 || values.some((v) => v === null || v === undefined)) return null;
	return {
		kind: ConditionKind.BitmapIn,
		columnId: column.id,
		values: [...new Set(values.map((v) => String(v)))]
	};
}

/** Build an `FmContains` condition when `column` has an FM index; else `null`
 * (the caller falls back to an in-memory substring check). The engine returns a
 * superset, so the caller keeps the predicate as a residual. */
function makeContainsCondition(
	table: TableSpec,
	column: ColumnSpec,
	substr: string
): ConditionSpec | null {
	const hasFm = table.indexes.some(
		(idx) => idx.kind === 'fm' && idx.columns.includes(column.name)
	);
	if (!hasFm || column.storageType !== 'text') return null;
	return { kind: ConditionKind.FmContains, columnId: column.id, text: substr };
}

/** Build an `FmContainsAll` condition of a LIKE pattern's literal runs (a
 * superset the caller re-checks). Requires an FM index; escaped patterns and
 * pure-wildcard patterns fall back to an in-memory match. */
function makeLikeCondition(
	table: TableSpec,
	column: ColumnSpec,
	pattern: string
): ConditionSpec | null {
	const hasFm = table.indexes.some(
		(idx) => idx.kind === 'fm' && idx.columns.includes(column.name)
	);
	if (!hasFm || column.storageType !== 'text' || pattern.includes('\\')) return null;
	const segments = pattern.split(/[%_]/).filter((s) => s.length > 0);
	if (segments.length === 0) return null;
	return { kind: ConditionKind.FmContainsAll, columnId: column.id, values: segments };
}

type RangeConditionPlan =
	| { condition: ConditionSpec; residual?: Predicate }
	| { alwaysFalse: true };

function makeRangeCondition(
	column: ColumnSpec,
	op: 'gt' | 'gte' | 'lt' | 'lte',
	value: unknown
): RangeConditionPlan | null {
	if (column.storageType === 'int64') {
		if (typeof value !== 'bigint') return null;
		const v = value;
		let lo = I64_MIN;
		let hi = I64_MAX;
		switch (op) {
			case 'gt': {
				if (v < I64_MIN) break;
				if (v >= I64_MAX) return { alwaysFalse: true };
				lo = v + 1n;
				break;
			}
			case 'gte':
				if (v <= I64_MIN) break;
				if (v > I64_MAX) return { alwaysFalse: true };
				lo = v;
				break;
			case 'lt': {
				if (v <= I64_MIN) return { alwaysFalse: true };
				if (v > I64_MAX) break;
				hi = v - 1n;
				break;
			}
			case 'lte':
				if (v < I64_MIN) return { alwaysFalse: true };
				if (v >= I64_MAX) break;
				hi = v;
				break;
		}
		return {
			condition: {
				kind: ConditionKind.RangeInt,
				columnId: column.id,
				int64Lo: lo,
				int64Hi: hi
			}
		};
	}

	if (column.storageType === 'float64') {
		if (typeof value !== 'number' || Number.isNaN(value)) return { alwaysFalse: true };
		const v = value;
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
			condition: {
				kind: ConditionKind.RangeF64,
				columnId: column.id,
				float64Lo: lo,
				float64Hi: hi
			},
			residual: op === 'gt' || op === 'lt' ? { kind: op, column, value } : undefined
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

function fullScanRows(db: NativeDatabase, table: TableSpec): MatchedRow[] {
	return queryNativeRows(db, table, []);
}

function queryNativeRows(
	db: NativeDatabase,
	table: TableSpec,
	conditions: ConditionSpec[]
): MatchedRow[] {
	return db
		.table(table.name)
		.query(conditions)
		.map((rowJs) => ({ rowId: rowJs.rowId, row: rowFromRowJs(table, rowJs) }));
}

/**
 * Fetch the right-side rows for a join clause. For an FK-equality clause built
 * with {@link joinEq} whose right column is probe-able, fetch only the rows
 * matching the distinct left keys — one indexed query per key, unioned — instead
 * of scanning the whole table. Falls back to a full scan otherwise. The clause
 * predicate is still re-checked per combined row, so the result is identical.
 */
function joinRightRows(
	db: NativeDatabase,
	clause: { table: TableSpec; kind: 'inner' | 'left' | 'cross'; on?: JoinPredicate },
	leftRows: JoinRow[]
): Record<string, unknown>[] {
	const key =
		clause.kind !== 'cross'
			? (clause.on as (JoinPredicate & { __eqKey?: JoinEqKey }) | undefined)?.__eqKey
			: undefined;
	if (key && key.rightTable === clause.table.name) {
		const seen = new Set<string>();
		const values: unknown[] = [];
		for (const combo of leftRows) {
			const v = (combo[key.leftTable] as Record<string, unknown> | null)?.[key.leftColumn.name];
			if (v !== null && v !== undefined) {
				const k = String(v);
				if (!seen.has(k)) {
					seen.add(k);
					values.push(v);
				}
			}
		}
		const rows: Record<string, unknown>[] = [];
		const rowSeen = new Set<bigint>();
		let probed = true;
		for (const v of values) {
			const cond = makeEqCondition(clause.table, key.rightColumn, v);
			if (!cond) {
				probed = false;
				break;
			}
			for (const m of queryNativeRows(db, clause.table, [cond])) {
				if (!rowSeen.has(m.rowId)) {
					rowSeen.add(m.rowId);
					rows.push(m.row);
				}
			}
		}
		if (probed) return rows;
	}
	return fullScanRows(db, clause.table).map((m) => m.row);
}

function andResidual(predicates: Predicate[]): Predicate | undefined {
	const residuals = predicates.filter((p): p is Predicate => p !== undefined);
	if (residuals.length === 0) return undefined;
	if (residuals.length === 1) return residuals[0];
	return { kind: 'and', predicates: residuals };
}

function residualPlan(predicate: Predicate): PredicatePlan {
	return { conditions: [], residual: predicate };
}

const CONDITION_LABELS: Record<number, string> = {
	[ConditionKind.Pk]: 'Pk',
	[ConditionKind.PkInt64]: 'PkInt64',
	[ConditionKind.BitmapEq]: 'BitmapEq',
	[ConditionKind.BitmapIn]: 'BitmapIn',
	[ConditionKind.RangeInt]: 'RangeInt',
	[ConditionKind.RangeF64]: 'RangeF64',
	[ConditionKind.FmContains]: 'FmContains',
	[ConditionKind.FmContainsAll]: 'FmContainsAll',
	[ConditionKind.IsNull]: 'IsNull',
	[ConditionKind.IsNotNull]: 'IsNotNull',
	[ConditionKind.Ann]: 'Ann',
	[ConditionKind.SparseMatch]: 'SparseMatch'
};

/** How a `where` predicate would execute: which native conditions push down. */
export type ExplainPlan = {
	indexAccelerated: boolean;
	exact: boolean;
	pushedConditions: string[];
};

function compilePredicate(table: TableSpec, predicate: Predicate): PredicatePlan {
	switch (predicate.kind) {
		case 'eq': {
			const condition = makeEqCondition(table, predicate.column, predicate.value);
			return condition ? { conditions: [condition] } : residualPlan(predicate);
		}
		case 'gt':
		case 'gte':
		case 'lt':
		case 'lte': {
			const condition = makeRangeCondition(predicate.column, predicate.kind, predicate.value);
			if (!condition) return residualPlan(predicate);
			if ('alwaysFalse' in condition) return { conditions: [], alwaysFalse: true };
			return {
				conditions: [condition.condition],
				residual: condition.residual
			};
		}
		case 'in': {
			if (predicate.values.length === 0) return { conditions: [], alwaysFalse: true };
			if (predicate.values.length === 1) {
				return compilePredicate(table, {
					kind: 'eq',
					column: predicate.column,
					value: predicate.values[0]
				});
			}
			const condition = makeInCondition(table, predicate.column, predicate.values);
			return condition ? { conditions: [condition] } : residualPlan(predicate);
		}
		case 'and': {
			const conditions: ConditionSpec[] = [];
			const residuals: Predicate[] = [];
			for (const child of predicate.predicates) {
				const plan = compilePredicate(table, child);
				if (plan.alwaysFalse) return { conditions: [], alwaysFalse: true };
				conditions.push(...plan.conditions);
				if (plan.residual) residuals.push(plan.residual);
			}
			return { conditions, residual: andResidual(residuals) };
		}
		case 'inSub': {
			return compilePredicate(table, {
				kind: 'in',
				column: predicate.column,
				values: predicate.subquery.scalarValuesSync()
			});
		}
		case 'exists':
			return predicate.subquery.hasRowsSync() !== predicate.negate
				? { conditions: [] }
				: { conditions: [], alwaysFalse: true };
		case 'or': {
			const inPlan = compileOrAsIn(table, predicate.predicates);
			return inPlan ?? residualPlan(predicate);
		}
		case 'contains': {
			// Push FmContains on an FM-indexed column; keep the substring check as
			// a residual since the engine returns a superset.
			const condition = makeContainsCondition(table, predicate.column, predicate.substr);
			return condition ? { conditions: [condition], residual: predicate } : residualPlan(predicate);
		}
		case 'null': {
			// Push the engine's page-stat-aware IsNull / IsNotNull; keep the null
			// check as a residual (the engine returns a superset).
			const cond: ConditionSpec = {
				kind: predicate.not ? ConditionKind.IsNotNull : ConditionKind.IsNull,
				columnId: predicate.column.id
			};
			return { conditions: [cond], residual: predicate };
		}
		case 'like': {
			// Push FmContainsAll of the pattern's literal runs on an FM column;
			// keep the LIKE match as a residual (the engine returns a superset).
			const condition = makeLikeCondition(table, predicate.column, predicate.pattern);
			return condition ? { conditions: [condition], residual: predicate } : residualPlan(predicate);
		}
		default:
			return residualPlan(predicate);
	}
}

function compileOrAsIn(table: TableSpec, predicates: Predicate[]): PredicatePlan | null {
	let column: ColumnSpec | undefined;
	const values: unknown[] = [];
	for (const predicate of predicates) {
		if (predicate.kind === 'eq') {
			if (!column) column = predicate.column;
			if (column.id !== predicate.column.id) return null;
			values.push(predicate.value);
			continue;
		}
		if (predicate.kind === 'in') {
			if (!column) column = predicate.column;
			if (column.id !== predicate.column.id) return null;
			values.push(...predicate.values);
			continue;
		}
		return null;
	}
	if (!column) return { conditions: [], alwaysFalse: true };
	if (values.length === 0) return { conditions: [], alwaysFalse: true };
	const condition = makeInCondition(table, column, values);
	return condition ? { conditions: [condition] } : null;
}

function evaluatePredicate(db: NativeDatabase, table: TableSpec, predicate: Predicate): MatchedRow[] {
	const plan = compilePredicate(table, predicate);
	if (plan.alwaysFalse) return [];
	const rows =
		plan.conditions.length > 0 ? queryNativeRows(db, table, plan.conditions) : fullScanRows(db, table);
	return plan.residual ? rows.filter((m) => matchRowPredicate(m.row, plan.residual!)) : rows;
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

/** `COUNT(*)` (no column), `COUNT(col)` (non-null), or `COUNT(DISTINCT col)`
 * (unique non-null) over a group. */
function computeCount(spec: AggregateSpec, rows: MatchedRow[]): bigint {
	if (!spec.column) return BigInt(rows.length); // COUNT(*)
	const name = spec.column.name;
	const nonNull = rows.map((m) => m.row[name]).filter((v) => v !== null && v !== undefined);
	if (spec.distinct) {
		const seen = new Set<string>();
		for (const v of nonNull) seen.add(valueKey(v));
		return BigInt(seen.size);
	}
	return BigInt(nonNull.length);
}

/** Computes a scalar aggregate over matched rows, honoring NULL skipping.
 * `distinct` de-duplicates the value set first (a no-op for MIN/MAX). */
function computeAggregate(
	kind: ScalarAggKind,
	column: ColumnSpec,
	rows: MatchedRow[],
	distinct = false
): bigint | number | string | null {
	const isInt = column.storageType === 'int64';
	let values = rows
		.map((m) => m.row[column.name])
		.filter((v) => v !== null && v !== undefined);
	if (distinct) {
		const seen = new Set<string>();
		values = values.filter((v) => {
			const k = valueKey(v);
			if (seen.has(k)) return false;
			seen.add(k);
			return true;
		});
	}
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

function makeDefaultContext(): DefaultContext {
	return {
		now: new Date().toISOString(),
		uuid: () => randomUUID()
	};
}

/** Project `row` to only the requested columns, or return it unchanged. */
function projectRow(
	row: Record<string, unknown>,
	columns?: ColumnSpec[]
): Record<string, unknown> {
	if (!columns) return row;
	const projected: Record<string, unknown> = {};
	for (const col of columns) projected[col.name] = row[col.name];
	return projected;
}

function prepareInsertRowSync(
	kit: KitDatabase,
	table: TableSpec,
	row: Record<string, unknown>
): Record<string, unknown> {
	const withSequence: Record<string, unknown> = { ...row };
	// Engine-native AUTO_INCREMENT: reserve the id up front so the cross-table
	// transaction can stage the row with an explicit value (the engine counter
	// is in-memory and becomes durable when the row commits — no __kit_sequences
	// hot row, no extra commit). At most one sequence column per table.
	for (const col of table.columns) {
		if (
			(withSequence[col.name] === undefined || withSequence[col.name] === null) &&
			col.default?.kind === 'sequence'
		) {
			const reserved = kit.reserveAutoIncSync(table.name);
			if (reserved !== null) {
				withSequence[col.name] = reserved;
			}
		}
	}
	const withDefaults = applyDefaults(table, withSequence, makeDefaultContext());
	// Normalize any column still unset to explicit null, so the stored row and
	// the returned row agree (an unset nullable column reads back as null).
	for (const col of table.columns) {
		if (withDefaults[col.name] === undefined) withDefaults[col.name] = null;
	}
	return withDefaults;
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

/**
 * Apply `patch` to `existingRow` inside `txn`, updating guards and foreign-key
 * touches as needed. Returns the merged row.
 */
function uniqueConstraintChanged(
	uq: { name: string; columns: string[] },
	existingRow: Record<string, unknown>,
	merged: Record<string, unknown>
): boolean {
	return uq.columns.some((colName) => existingRow[colName] !== merged[colName]);
}

function applyUpdateInTxn(
	kit: ConstraintKit,
	txn: Transaction,
	table: TableSpec,
	existingRow: Record<string, unknown>,
	existingRowId: bigint,
	patch: Record<string, unknown>
): Record<string, unknown> {
	const merged = { ...existingRow, ...patch };
	applyUpdateDefaults(table, merged, patch, makeDefaultContext());
	validateRow(table, merged);
	const oldPkValue = pkValueFromRow(table, existingRow);
	const newPkValue = pkValueFromRow(table, merged);
	const pkChanged = !pkValuesEqual(oldPkValue, newPkValue);

	// Only delete guards for unique constraints whose values actually changed.
	// Deleting unchanged guards and then re-staging them can race with the same
	// transaction's visibility and silently drop the guard.
	const changedConstraints = table.unique
		.filter((uq) => uniqueConstraintChanged(uq, existingRow, merged))
		.map((uq) => uq.name);
	if (changedConstraints.length > 0) {
		deleteUniqueGuards(kit, txn, table, oldPkValue, changedConstraints);
	}
	if (pkChanged) {
		deletePkGuard(kit, txn, table, oldPkValue);
	}
	if (hasForeignKeyChange(table, patch)) {
		enforceForeignKeys(kit, txn, table, merged);
	}
	txn.delete(table.name, existingRowId);
	txn.put(table.name, toCells(table, merged));
	stageUniqueGuards(kit, txn, table, merged, newPkValue);
	if (pkChanged) {
		stagePkGuard(kit, txn, table, newPkValue, true);
	}

	return merged;
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
	private _ann?: { column: ColumnSpec; vector: number[]; k: number };
	private _sparse?: { column: ColumnSpec; query: [number, number][]; k: number };
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

	/**
	 * Approximate nearest-neighbour search: return the `k` rows whose `column`
	 * (an `embedding`) is closest to `vector`, resolved by the column's ANN
	 * index. Terminal — call `executeSync()`/`execute()` next.
	 */
	annSearch(column: ColumnSpec, vector: number[], k: number): SelectBuilder<T, Row<T>[]> {
		const next = this as unknown as SelectBuilder<T, Row<T>[]>;
		next._ann = { column, vector, k };
		return next;
	}

	/**
	 * Learned-sparse (SPLADE) retrieval: return the `k` rows whose `column` (a
	 * sparse token vector) best matches the weighted `query` `[token, weight]`
	 * pairs. Terminal — call `executeSync()`/`execute()` next.
	 */
	sparseMatch(
		column: ColumnSpec,
		query: [number, number][],
		k: number
	): SelectBuilder<T, Row<T>[]> {
		const next = this as unknown as SelectBuilder<T, Row<T>[]>;
		next._sparse = { column, query, k };
		return next;
	}

	executeSync(): TResult {
		const db = this.kit.nativeDb;

		if (this._ann) {
			const cond: ConditionSpec = {
				kind: ConditionKind.Ann,
				columnId: this._ann.column.id,
				embedding: this._ann.vector,
				k: this._ann.k
			};
			return queryNativeRows(db, this.table, [cond]).map((m) => m.row) as TResult;
		}

		if (this._sparse) {
			const cond: ConditionSpec = {
				kind: ConditionKind.SparseMatch,
				columnId: this._sparse.column.id,
				sparseTokens: this._sparse.query.map((p) => p[0]),
				sparseWeights: this._sparse.query.map((p) => p[1]),
				k: this._sparse.k
			};
			return queryNativeRows(db, this.table, [cond]).map((m) => m.row) as TResult;
		}

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
			if (this._where && !this._source) {
				const plan = compilePredicate(this.table, this._where);
				if (plan.alwaysFalse) return 0n as TResult;
				if (!plan.residual) {
					if (plan.conditions.length === 0) {
						return db.table(this.table.name).count() as TResult;
					}
					return db.table(this.table.name).countWhere(plan.conditions) as TResult;
				}
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

	/**
	 * Execute against the native engine and return the matching rows as an Arrow
	 * (columnar) table — zero-copy from the engine. TypeScript-only: the
	 * Rust/Python kit returns row maps.
	 *
	 * The native Arrow path is index-driven and needs at least one pushed-down
	 * condition, so a `where`/`annSearch`/`sparseMatch` clause is required. It
	 * applies only the pushed-down predicate (exact for `=`/range/`in`, a
	 * superset for `contains`/`like`) and returns every column — `orderBy`,
	 * `limit`, `offset`, and column projection are NOT applied. Use
	 * {@link executeSync} for full query semantics.
	 */
	executeArrow(): ArrowTable {
		if (this._source) {
			throw new KitError('executeArrow is not supported for joined/CTE sources');
		}
		const db = this.kit.nativeDb;
		let conditions: ConditionSpec[];
		if (this._ann) {
			conditions = [
				{
					kind: ConditionKind.Ann,
					columnId: this._ann.column.id,
					embedding: this._ann.vector,
					k: this._ann.k
				}
			];
		} else if (this._sparse) {
			conditions = [
				{
					kind: ConditionKind.SparseMatch,
					columnId: this._sparse.column.id,
					sparseTokens: this._sparse.query.map((p) => p[0]),
					sparseWeights: this._sparse.query.map((p) => p[1]),
					k: this._sparse.k
				}
			];
		} else if (this._where) {
			const plan = compilePredicate(this.table, this._where);
			if (plan.alwaysFalse || plan.conditions.length === 0) {
				throw new KitError(
					'executeArrow requires a pushed-down condition; this predicate has none — use executeSync'
				);
			}
			conditions = plan.conditions;
		} else {
			throw new KitError('executeArrow requires a where/annSearch/sparseMatch clause');
		}
		return tableFromIPC(db.table(this.table.name).queryArrow(conditions));
	}

	/**
	 * Describe how this query's `where`/`annSearch`/`sparseMatch` clause would
	 * push down to native index conditions — a diagnostic that plans but does
	 * not run the query. `exact` is true when the whole predicate translated (no
	 * JS residual re-filtering).
	 */
	explain(): ExplainPlan {
		if (this._source) {
			return { indexAccelerated: false, exact: false, pushedConditions: [] };
		}
		if (this._ann) {
			return { indexAccelerated: true, exact: false, pushedConditions: ['Ann'] };
		}
		if (this._sparse) {
			return { indexAccelerated: true, exact: false, pushedConditions: ['SparseMatch'] };
		}
		if (!this._where) {
			return { indexAccelerated: false, exact: true, pushedConditions: [] };
		}
		const plan = compilePredicate(this.table, this._where);
		if (plan.alwaysFalse) {
			return { indexAccelerated: false, exact: true, pushedConditions: [] };
		}
		return {
			indexAccelerated: plan.conditions.length > 0,
			exact: !plan.residual,
			pushedConditions: plan.conditions.map((c) => CONDITION_LABELS[c.kind] ?? String(c.kind))
		};
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
			// FK-equality clauses (joinEq) probe the right table by index over the
			// distinct left keys; other clauses full-scan. Either way the predicate
			// below re-checks each combination, so results are identical.
			const joinRows = joinRightRows(db, clause, combos);
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
						? computeCount(spec, g.rows)
						: computeAggregate(spec.fn, spec.column!, g.rows, spec.distinct);
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

export class InsertBuilder<T extends TableSpec, TResult = Row<T>> {
	private _row?: Insert<T>;
	private _returning?: ColumnSpec[];
	private _onConflict?:
		| { kind: 'do_nothing' }
		| { kind: 'do_update'; patch: Record<string, unknown> };

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

	returning<C extends ColumnSpec[]>(...columns: [...C]): InsertBuilder<T, Pick<Row<T>, C[number]['name']>> {
		const next = new InsertBuilder<T, Pick<Row<T>, C[number]['name']>>(this.kit, this.table);
		next._row = this._row;
		next._returning = columns;
		next._onConflict = this._onConflict;
		return next;
	}

	// `onConflict*` mutate this builder because the result type does not
	// change; `returning` clones because it changes the generic result type.
	onConflictDoNothing(): InsertBuilder<T, TResult> {
		this._onConflict = { kind: 'do_nothing' };
		return this;
	}

	onConflictDoUpdate(patch: Partial<Row<T>>): InsertBuilder<T, TResult> {
		this._onConflict = { kind: 'do_update', patch: patch as Record<string, unknown> };
		return this;
	}

	executeSync(): TResult {
		if (this._row === undefined) {
			throw new KitError('values() must be called before execute()');
		}
		const { defaulted, pkValue, pkExplicit } = prepareInsertSync(
			this.kit,
			this.table,
			this._row as Record<string, unknown>
		);

		if (this._onConflict) {
			const existingRowJs = findByPk(this.kit.nativeDb, this.table, pkValue);
			if (existingRowJs) {
				const existingRow = rowFromRowJs(this.table, existingRowJs);
				if (this._onConflict.kind === 'do_nothing') {
					return projectRow(existingRow, this._returning) as TResult;
				}
				const patch = this._onConflict.patch;
				const kit = makeConstraintKit(this.kit);
				let merged: Record<string, unknown>;
				runSyncTxn(this.kit, (txn) => {
					merged = applyUpdateInTxn(kit, txn, this.table, existingRow, existingRowJs.rowId, patch);
				});

				return projectRow(merged!, this._returning) as TResult;
			}
		}

		const kit = makeConstraintKit(this.kit);
		runSyncTxn(this.kit, (txn) => {
			enforceForeignKeys(kit, txn, this.table, defaulted);
			stageUniqueGuards(kit, txn, this.table, defaulted, pkValue);
			stagePkGuard(kit, txn, this.table, pkValue, pkExplicit);
			txn.put(this.table.name, toCells(this.table, defaulted));
		});

		return projectRow(defaulted, this._returning) as TResult;
	}

	async execute(): Promise<TResult> {
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
				results.push(defaulted);
			}
			// Stage all main-table rows in one packed crossing instead of a
			// per-row NAPI `put`. Guard rows above are already staged into the
			// same transaction; everything commits atomically.
			if (results.length > 0) {
				txn.putPacked(this.table.name, packRows(this.table, results));
			}
		});

		return results as Row<T>[];
	}

	async execute(): Promise<Row<T>[]> {
		return this.executeSync();
	}
}

export class UpdateBuilder<T extends TableSpec, TResult = Row<T>[]> {
	private _patch?: Update<T>;
	private _where?: Predicate;
	private _returning?: ColumnSpec[];

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

	returning<C extends ColumnSpec[]>(...columns: [...C]): UpdateBuilder<T, Pick<Row<T>, C[number]['name']>[]> {
		const next = new UpdateBuilder<T, Pick<Row<T>, C[number]['name']>[]>(this.kit, this.table);
		next._patch = this._patch;
		next._where = this._where;
		next._returning = columns;
		return next;
	}

	executeSync(): TResult {
		if (this._patch === undefined) {
			throw new KitError('set() must be called before execute()');
		}
		const db = this.kit.nativeDb;
		const matches = this._where
			? evaluatePredicate(db, this.table, this._where)
			: fullScanRows(db, this.table);
		const patch = this._patch as Record<string, unknown>;
		const kit = makeConstraintKit(this.kit);
		const updated: Record<string, unknown>[] = [];

		runSyncTxn(this.kit, (txn) => {
			for (const matched of matches) {
				const merged = applyUpdateInTxn(kit, txn, this.table, matched.row, matched.rowId, patch);
				updated.push(merged);
			}
		});

		return updated.map((row) => projectRow(row, this._returning)) as TResult;
	}

	async execute(): Promise<TResult> {
		return this.executeSync();
	}
}

export class DeleteBuilder<T extends TableSpec, TResult = bigint> {
	private _where?: Predicate;
	private _returning?: ColumnSpec[];

	constructor(
		private readonly kit: KitDatabase,
		private readonly table: T
	) {}

	where(predicate: Predicate): this {
		this._where = predicate;
		return this;
	}

	returning<C extends ColumnSpec[]>(...columns: [...C]): DeleteBuilder<T, Pick<Row<T>, C[number]['name']>[]> {
		const next = new DeleteBuilder<T, Pick<Row<T>, C[number]['name']>[]>(this.kit, this.table);
		next._where = this._where;
		next._returning = columns;
		return next;
	}

	executeSync(): TResult {
		const db = this.kit.nativeDb;
		const matches = this._where
			? evaluatePredicate(db, this.table, this._where)
			: fullScanRows(db, this.table);
		const kit = makeConstraintKit(this.kit);

		if (this._returning) {
			const projected = matches.map((m) => projectRow(m.row, this._returning));

			if (
				this.table.primaryKey.length === 1 &&
				this.table.unique.length === 0 &&
				!isReferencedTable(this.kit.schema, this.table.name)
			) {
				if (matches.length > 0) {
					runSyncTxn(this.kit, (txn) => {
						txn.deletePacked(this.table.name, packRowIds(matches.map((m) => m.rowId)));
					});
				}
				return projected as TResult;
			}

			runSyncTxn(this.kit, (txn) => {
				for (const matched of matches) {
					const pkValue = pkValueFromRow(this.table, matched.row);
					// Reuse the row already fetched by the scan to avoid an O(n^2) re-read.
					planDelete(kit, txn, this.table, pkValue, {
						row: matched.row,
						rowId: matched.rowId
					});
				}
			});
			return projected as TResult;
		}

		// Fast path: a single-column-PK table with no unique constraints and no
		// incoming foreign keys has no guard rows and no cascade work, so a delete
		// reduces to dropping the matched row ids. Batch them in one packed
		// crossing instead of per-row planDelete (which would also run a
		// __kit_unique_keys / __kit_row_guards query per row).
		if (
			this.table.primaryKey.length === 1 &&
			this.table.unique.length === 0 &&
			!isReferencedTable(this.kit.schema, this.table.name)
		) {
			if (matches.length === 0) return 0n as TResult;
			runSyncTxn(this.kit, (txn) => {
				txn.deletePacked(this.table.name, packRowIds(matches.map((m) => m.rowId)));
			});
			return BigInt(matches.length) as TResult;
		}

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

		return BigInt(deleted) as TResult;
	}

	async execute(): Promise<TResult> {
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
