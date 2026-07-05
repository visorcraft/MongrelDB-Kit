import type { DefaultValue } from './defaults.js';

export type ColumnStorageType =
	| 'bool'
	| 'int64'
	| 'float64'
	| 'timestamp'
	| 'date'
	| 'date64'
	| 'time64'
	| 'interval'
	| 'text'
	| 'bytes'
	| 'json'
	| 'embedding'
	| 'sparse'
	| 'decimal128';

export type PkValue = string | bigint | (string | bigint | null)[];

export type ColumnApplicationType = ColumnStorageType;

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
	/** Vector dimension for an `embedding` column (required for ANN). */
	embeddingDim?: number;
	/** Encrypt this column at rest (requires an encrypted database). */
	encrypted?: boolean;
	/** Encrypt but keep queryable via deterministic tokens. */
	encryptedIndexable?: boolean;
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
	/** Index kind; defaults to `bitmap`. `fm` enables FM substring search so
	 * `contains(col, needle)` pushes down to the engine instead of scanning.
	 * `learned_range` builds a PGM zonemap that accelerates range predicates
	 * (`gt`/`gte`/`lt`/`lte`) on numeric/timestamp columns. */
	kind?: 'bitmap' | 'fm' | 'ann' | 'sparse' | 'minhash' | 'learned_range';
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
	tableId: number;
	name: string;
	columns: TColumns;
	primaryKey: string[];
	indexes: IndexSpec[];
	foreignKeys: ForeignKeySpec[];
	unique: UniqueSpec[];
	checks: CheckSpec[];
	/**
	 * Look up a column spec by name. Use this for columns whose name shadows a
	 * table property (e.g. a column literally named `name`), which are not
	 * reachable as a direct `table.<column>` accessor.
	 */
	column(name: string): ColumnSpec;
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
	embedding: number[];
	sparse: [number, number][];
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

type NullableColumnName<C> = C extends { name: infer N; nullable: infer Null }
	? N extends string
		? Null extends true
			? N
			: never
		: never
	: never;

type NullableColumns<T extends TableSpec> = NullableColumnName<T['columns'][number]>;

// Columns that may be omitted on insert: nullable ones (default to NULL) and
// ones with a default/generated value (the kit supplies them when omitted). A
// defaulted column — e.g. an AUTO_INCREMENT/sequence primary key — may still be
// supplied explicitly, matching SQL semantics, so it is optional rather than
// omitted.
type OptionalInsertColumns<T extends TableSpec> = Extract<
	NullableColumns<T> | ColumnsWithDefault<T>,
	keyof Row<T>
>;

// Insert input: non-nullable columns without a default are required; everything
// else (nullable or defaulted) is optional.
export type Insert<T extends TableSpec> = Omit<
	Row<T>,
	ColumnsWithDefault<T> | NullableColumns<T>
> &
	Partial<Pick<Row<T>, OptionalInsertColumns<T>>>;
export type Update<T extends TableSpec> = Partial<Row<T>>;
