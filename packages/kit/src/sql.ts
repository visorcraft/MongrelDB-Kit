import type { ColumnSpec } from './types.js';
import { quoteIdent } from './external.js';

export type SqlArg = SqlExpr | ColumnSpec | string | number | bigint | boolean | null;

export interface SqlExpr {
	sql: string;
}

export function rawSql(sql: string): SqlExpr {
	return { sql };
}

export function sqlColumn(column: ColumnSpec): SqlExpr {
	return rawSql(quoteIdent(column.name));
}

export function sqlLiteral(value: SqlArg): SqlExpr {
	if (isSqlExpr(value)) return value;
	if (isColumn(value)) return sqlColumn(value);
	if (value === null) return rawSql('NULL');
	if (typeof value === 'bigint') return rawSql(value.toString());
	if (typeof value === 'number') return rawSql(Number.isFinite(value) ? String(value) : 'NULL');
	if (typeof value === 'boolean') return rawSql(value ? 'TRUE' : 'FALSE');
	return rawSql(`'${String(value).replaceAll("'", "''")}'`);
}

export function callSql(name: string, ...args: SqlArg[]): SqlExpr {
	return rawSql(`${name}(${args.map((arg) => sqlLiteral(arg).sql).join(', ')})`);
}

export function percentile(column: ColumnSpec, p: number): SqlExpr {
	return callSql('percentile', column, p);
}

export function percentileCont(column: ColumnSpec, p: number): SqlExpr {
	return callSql('percentile_cont', column, p);
}

export function percentileDisc(column: ColumnSpec, p: number): SqlExpr {
	return callSql('percentile_disc', column, p);
}

export function groupConcat(column: ColumnSpec, separator = ','): SqlExpr {
	return callSql('group_concat', column, separator);
}

export function stringAgg(column: ColumnSpec, separator: string): SqlExpr {
	return callSql('string_agg', column, separator);
}

export function jsonExtract(value: SqlArg, path: string): SqlExpr {
	return callSql('json_extract', value, path);
}

export function jsonValid(value: SqlArg): SqlExpr {
	return callSql('json_valid', value);
}

export function dateTime(value: SqlArg = 'now', ...modifiers: string[]): SqlExpr {
	return callSql('datetime', value, ...modifiers);
}

export function dateOnly(value: SqlArg = 'now', ...modifiers: string[]): SqlExpr {
	return callSql('date', value, ...modifiers);
}

export function unixEpoch(value: SqlArg = 'now', ...modifiers: string[]): SqlExpr {
	return callSql('unixepoch', value, ...modifiers);
}

export function mathFn(name: string, ...args: SqlArg[]): SqlExpr {
	return callSql(name, ...args);
}

function isSqlExpr(value: SqlArg): value is SqlExpr {
	return typeof value === 'object' && value !== null && 'sql' in value;
}

function isColumn(value: SqlArg): value is ColumnSpec {
	return typeof value === 'object' && value !== null && 'id' in value && 'storageType' in value;
}
