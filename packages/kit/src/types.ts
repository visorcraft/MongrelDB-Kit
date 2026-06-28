export type ColumnStorageType =
	| 'bool'
	| 'int64'
	| 'float64'
	| 'timestamp'
	| 'date'
	| 'text'
	| 'bytes'
	| 'json';

export type ColumnApplicationType = ColumnStorageType;

export type DefaultValue =
	| { kind: 'literal'; value: unknown }
	| { kind: 'raw'; expr: string }
	| { kind: 'now' }
	| { kind: 'uuid' };

export interface ColumnSpec<
	TName extends string = string,
	TApp extends ColumnApplicationType = ColumnApplicationType,
	TNull extends boolean = boolean,
	TDefault extends DefaultValue | null = DefaultValue | null,
	TGenerated extends 'uuid' | 'now' | null = 'uuid' | 'now' | null
> {
	id: number;
	name: TName;
	storageType: ColumnStorageType;
	applicationType: TApp;
	nullable: TNull;
	primaryKey: boolean;
	default: TDefault;
	generated: TGenerated;
	enumValues?: string[];
	check?: (value: unknown) => boolean | string;
	min?: number;
	max?: number;
	minLength?: number;
	maxLength?: number;
	regex?: RegExp;
}

export interface IndexSpec {
	name: string;
	columns: string[];
	unique: boolean;
}

export interface ForeignKeySpec {
	name: string;
	columns: string[];
	referencesTable: string;
	referencesColumns: string[];
	onDelete: 'cascade' | 'set null' | 'restrict';
}

export interface UniqueSpec {
	name: string;
	columns: string[];
}

export interface CheckSpec {
	name: string;
	expr: (row: Record<string, unknown>) => boolean | string;
}

export interface TableSpec<TColumns extends readonly ColumnSpec[] = readonly ColumnSpec[]> {
	id: number;
	name: string;
	columns: TColumns;
	primaryKey: string[];
	indexes: IndexSpec[];
	foreignKeys: ForeignKeySpec[];
	unique: UniqueSpec[];
	checks: CheckSpec[];
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

type ApplicationType<T extends ColumnApplicationType> = ApplicationTypeMap[T];

export type Row<T extends TableSpec> = {
	[K in T['columns'][number] as K['name']]: K['nullable'] extends true
		? ApplicationType<K['applicationType']> | null
		: ApplicationType<K['applicationType']>;
};

type ColumnToDefaultName<C> = C extends {
	name: infer N;
	nullable: infer Null;
	default: infer D;
	generated: infer G;
}
	? N extends string
		? Null extends true
			? never
			: D extends DefaultValue
				? N
				: G extends 'uuid' | 'now'
					? N
					: never
		: never
	: never;

type ColumnsWithDefault<T extends TableSpec> = ColumnToDefaultName<T['columns'][number]>;

export type Insert<T extends TableSpec> = Omit<Row<T>, ColumnsWithDefault<T>>;
export type Update<T extends TableSpec> = Partial<Row<T>>;
