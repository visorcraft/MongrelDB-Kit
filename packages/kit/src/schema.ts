import type {
	ColumnSpec,
	ColumnStorageType,
	ColumnApplicationType,
	TableSpec,
	IndexSpec,
	ForeignKeySpec,
	UniqueSpec,
	CheckSpec
} from './types.js';
import type { DefaultValue } from './defaults.js';

export type ColumnOptions = {
	nullable?: boolean;
	primaryKey?: boolean;
	default?: DefaultValue;
	generated?: 'uuid' | 'now';
	enumValues?: string[];
	check?: (value: unknown) => boolean | string;
	min?: number;
	max?: number;
	minLength?: number;
	maxLength?: number;
	regex?: RegExp;
};

type OptsNull<TOpts extends ColumnOptions> = TOpts extends { nullable: true } ? true : false;
type OptsDefault<TOpts extends ColumnOptions> = TOpts extends { default: infer D }
	? Exclude<D, undefined>
	: null;
type OptsGenerated<TOpts extends ColumnOptions> = TOpts extends { generated: infer G }
	? Exclude<G, undefined>
	: null;

export function column<
	const TName extends string,
	TApp extends ColumnStorageType,
	const TOpts extends ColumnOptions = {}
>(
	name: TName,
	storageType: TApp,
	opts?: TOpts
): ColumnSpec<TName, TApp, OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return {
		id: 0,
		name,
		storageType,
		applicationType: storageType,
		nullable: (opts?.nullable ?? false) as OptsNull<TOpts>,
		primaryKey: opts?.primaryKey ?? false,
		default: opts?.default as OptsDefault<TOpts>,
		generated: opts?.generated as OptsGenerated<TOpts>,
		enumValues: opts?.enumValues,
		check: opts?.check,
		min: opts?.min,
		max: opts?.max,
		minLength: opts?.minLength,
		maxLength: opts?.maxLength,
		regex: opts?.regex
	};
}

export function int<const TName extends string, const TOpts extends ColumnOptions = {}>(
	name: TName,
	opts?: TOpts
): ColumnSpec<TName, 'int64', OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return column(name, 'int64', opts);
}

export function text<const TName extends string, const TOpts extends ColumnOptions = {}>(
	name: TName,
	opts?: TOpts
): ColumnSpec<TName, 'text', OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return column(name, 'text', opts);
}

export function real<const TName extends string, const TOpts extends ColumnOptions = {}>(
	name: TName,
	opts?: TOpts
): ColumnSpec<TName, 'float64', OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return column(name, 'float64', opts);
}

export function bool<const TName extends string, const TOpts extends ColumnOptions = {}>(
	name: TName,
	opts?: TOpts
): ColumnSpec<TName, 'bool', OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return column(name, 'bool', opts);
}

export function json<const TName extends string, const TOpts extends ColumnOptions = {}>(
	name: TName,
	opts?: TOpts
): ColumnSpec<TName, 'json', OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return column(name, 'json', opts);
}

export function timestamp<const TName extends string, const TOpts extends ColumnOptions = {}>(
	name: TName,
	opts?: TOpts
): ColumnSpec<TName, 'timestamp', OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return column(name, 'timestamp', opts);
}

export function date<const TName extends string, const TOpts extends ColumnOptions = {}>(
	name: TName,
	opts?: TOpts
): ColumnSpec<TName, 'date', OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return column(name, 'date', opts);
}

export function blob<const TName extends string, const TOpts extends ColumnOptions = {}>(
	name: TName,
	opts?: TOpts
): ColumnSpec<TName, 'bytes', OptsNull<TOpts>, OptsDefault<TOpts>, OptsGenerated<TOpts>> {
	return column(name, 'bytes', opts);
}

export interface IndexOptions {
	name?: string;
	unique?: boolean;
}

export interface UniqueOptions {
	name?: string;
}

export interface ForeignKeyOptions {
	name?: string;
	onDelete?: ForeignKeySpec['onDelete'];
}

export interface ForeignKeyReference {
	table: string;
	columns: string[];
}

export function index(columns: string[], opts: IndexOptions = {}): IndexSpec {
	return {
		name: opts.name ?? `idx_${columns.join('_')}`,
		columns,
		unique: opts.unique ?? false
	};
}

export function unique(columns: string[], opts: UniqueOptions = {}): UniqueSpec {
	return {
		name: opts.name ?? `uq_${columns.join('_')}`,
		columns
	};
}

export function foreignKey(
	columns: string[],
	references: ForeignKeyReference,
	opts: ForeignKeyOptions = {}
): ForeignKeySpec {
	return {
		name: opts.name ?? `fk_${columns.join('_')}_${references.table}`,
		columns,
		referencesTable: references.table,
		referencesColumns: references.columns,
		onDelete: opts.onDelete ?? 'restrict'
	};
}

export function check(name: string, expr: CheckSpec['expr']): CheckSpec {
	return { name, expr };
}

export interface TableOptions<TColumns extends readonly ColumnSpec[]> {
	id?: number;
	columns: TColumns;
	primaryKey: string | string[];
	indexes?: IndexSpec[];
	foreignKeys?: ForeignKeySpec[];
	unique?: UniqueSpec[];
	checks?: CheckSpec[];
}

type ColumnMap<TColumns extends readonly ColumnSpec[]> = {
	[K in TColumns[number] as K['name'] extends keyof TableSpec<TColumns>
		? never
		: K['name']]: K;
};

let nextTableId = 1;

export function table<const TColumns extends readonly ColumnSpec[]>(
	name: string,
	options: TableOptions<TColumns>
): TableSpec<TColumns> & ColumnMap<TColumns> {
	const columns = options.columns;
	for (let i = 0; i < columns.length; i++) {
		columns[i].id = columns[i].id || i + 1;
	}

	const primaryKey = Array.isArray(options.primaryKey)
		? options.primaryKey
		: [options.primaryKey];
	const indexes = options.indexes ?? [];
	const foreignKeys = options.foreignKeys ?? [];
	const unique = options.unique ?? [];
	const checks = options.checks ?? [];

	const columnNames = new Set<string>();
	const columnIds = new Set<number>();
	for (const col of columns) {
		if (columnNames.has(col.name)) {
			throw new Error(`Duplicate column name "${col.name}" in table "${name}"`);
		}
		columnNames.add(col.name);
		if (columnIds.has(col.id)) {
			throw new Error(`Duplicate column id ${col.id} in table "${name}"`);
		}
		columnIds.add(col.id);
	}

	for (const pk of primaryKey) {
		if (!columnNames.has(pk)) {
			throw new Error(`Primary key column "${pk}" not found in table "${name}"`);
		}
	}

	for (const idx of indexes) {
		for (const c of idx.columns) {
			if (!columnNames.has(c)) {
				throw new Error(`Index column "${c}" not found in table "${name}"`);
			}
		}
	}

	for (const u of unique) {
		for (const c of u.columns) {
			if (!columnNames.has(c)) {
				throw new Error(`Unique column "${c}" not found in table "${name}"`);
			}
		}
	}

	for (const fk of foreignKeys) {
		for (const c of fk.columns) {
			if (!columnNames.has(c)) {
				throw new Error(`Foreign key column "${c}" not found in table "${name}"`);
			}
		}
	}

	const spec: Record<string, unknown> = {
		tableId: options.id ?? nextTableId++,
		name,
		columns,
		primaryKey,
		indexes,
		foreignKeys,
		unique,
		checks
	};
	for (const col of columns) {
		if (!(col.name in spec)) {
			spec[col.name] = col;
		}
	}
	return spec as TableSpec<TColumns> & ColumnMap<TColumns>;
}

export class Schema {
	private readonly byName = new Map<string, TableSpec>();
	private readonly byId = new Map<number, TableSpec>();

	constructor(tables: TableSpec[]) {
		for (const t of tables) {
			if (this.byName.has(t.name)) {
				throw new Error(`Duplicate table name "${t.name}"`);
			}
			if (this.byId.has(t.tableId)) {
				throw new Error(`Duplicate table id ${t.tableId}`);
			}
			this.byName.set(t.name, t);
			this.byId.set(t.tableId, t);
		}
	}

	tablesList(): TableSpec[] {
		return Array.from(this.byName.values());
	}

	table(name: string): TableSpec {
		const t = this.byName.get(name);
		if (!t) {
			throw new Error(`Table "${name}" not found in schema`);
		}
		return t;
	}

	hasTable(name: string): boolean {
		return this.byName.has(name);
	}
}
