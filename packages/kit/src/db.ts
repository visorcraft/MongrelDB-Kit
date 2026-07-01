import { createHash } from 'node:crypto';
import mongreldb from 'mongreldb';
import type {
	Database as NativeDatabase,
	Transaction,
	ColumnType as NativeColumnType,
	IndexKindSpec as NativeIndexKindSpec,
	ConditionKind as NativeConditionKind
} from 'mongreldb/native.js';
import { Schema } from './schema.js';
import { rowsToTsv, tsvToRows } from './tsv.js';
import { rowFromRowJs } from './rows.js';
import {
	internalTables,
	kitSchemaCatalog
} from './internalTables.js';
import type { TableSpec, ColumnStorageType, CheckSpec } from './types.js';
import { migrateSync as runMigrateSync, type Migration } from './migrate.js';
import { isReferencedTable, deleteGuardsForTable, type ConstraintKit } from './constraints.js';
import { KitError, isRetryableConflict } from './errors.js';

/** Members of a set-valued cell: a JSON array, or a JSON string of one. */
function parseStringSet(value: unknown): Set<string> {
	let arr: unknown = value;
	if (typeof value === 'string') {
		try {
			arr = JSON.parse(value);
		} catch {
			arr = null;
		}
	}
	const out = new Set<string>();
	if (Array.isArray(arr)) {
		for (const v of arr) {
			if (typeof v === 'string') out.add(v);
			else if (typeof v === 'number' || typeof v === 'boolean') out.add(String(v));
		}
	}
	return out;
}

/** A reservoir-sampled approximate aggregate with a confidence interval. */
export interface ApproxAggregate {
	point: number;
	ci_low: number;
	ci_high: number;
	n_population: number;
	n_sample_live: number;
	n_passing: number;
}

type MongrelColumnSpec = {
	id: number;
	name: string;
	ty: number;
	primaryKey: boolean;
	nullable: boolean;
	autoIncrement?: boolean;
	embeddingDim?: number;
	encrypted?: boolean;
	encryptedIndexable?: boolean;
};

type MongrelDatabase = NativeDatabase & {
	transaction(
		fn: (txn: Transaction) => void | Promise<void>,
		opts?: { maxRetries?: number; baseDelayMs?: number }
	): Promise<bigint>;
	alterColumn(table: string, columnName: string, column: MongrelColumnSpec): bigint;
};

type MongrelModule = {
	Database: {
		open(path: string): MongrelDatabase;
		withPath(path: string): MongrelDatabase;
		createEncrypted(path: string, passphrase: string): MongrelDatabase;
		openEncrypted(path: string, passphrase: string): MongrelDatabase;
	};
	ColumnType: typeof NativeColumnType;
	IndexKindSpec: typeof NativeIndexKindSpec;
	ConditionKind: typeof NativeConditionKind;
};

const addon = mongreldb as unknown as MongrelModule;

type MongrelIndexSpec = {
	name: string;
	columnId: number;
	kind: number;
};

type MongrelSchemaSpec = {
	columns: MongrelColumnSpec[];
	indexes: MongrelIndexSpec[];
};

function toMongrelColumnType(storageType: ColumnStorageType): number {
	switch (storageType) {
		case 'bool':
			return addon.ColumnType.Bool;
		case 'int64':
			return addon.ColumnType.Int64;
		case 'float64':
			return addon.ColumnType.Float64;
		case 'text':
		case 'timestamp':
		case 'date':
		case 'bytes':
		case 'json':
			return addon.ColumnType.Bytes;
		case 'embedding':
			return addon.ColumnType.Embedding;
		case 'sparse':
			// A sparse column is stored as bincoded bytes; the sparse index reads
			// the tokens from those bytes.
			return addon.ColumnType.Bytes;
	}
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
				kind:
					idx.kind === 'fm'
						? addon.IndexKindSpec.FmIndex
						: idx.kind === 'ann'
							? addon.IndexKindSpec.Ann
							: idx.kind === 'sparse'
								? addon.IndexKindSpec.Sparse
								: idx.kind === 'minhash'
									? addon.IndexKindSpec.MinHash
									: addon.IndexKindSpec.Bitmap
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
			kind: addon.IndexKindSpec.Bitmap
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
				kind: addon.IndexKindSpec.Bitmap
			});
			indexedColumns.add(colName);
		}
	}

	return {
		columns: table.columns.map((col) => ({
			id: col.id,
			name: col.name,
			ty: toMongrelColumnType(col.storageType),
			primaryKey: col.primaryKey,
			nullable: col.nullable,
			// A Kit sequence-default column maps to the engine's native
			// AUTO_INCREMENT allocator (a per-table WAL-durable counter) instead
			// of the legacy __kit_sequences hot row.
			autoIncrement: col.default?.kind === 'sequence',
			embeddingDim: col.embeddingDim,
			encrypted: col.encrypted,
			encryptedIndexable: col.encryptedIndexable
		})),
		indexes
	};
}

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

/**
 * Run `fn` inside a fresh transaction, retrying bounded-exponentially on
 * retryable conflicts. Exported so query builders and `KitDatabase` methods
 * can share one implementation.
 */
export function runSyncTxn(
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
			if (attempt < maxRetries && isRetryableConflict(err)) {
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

export class KitDatabase {
	private constructor(
		private readonly db: MongrelDatabase,
		public readonly schema: Schema
	) {}

	static async open(path: string, schema: Schema): Promise<KitDatabase> {
		let db: MongrelDatabase;
		try {
			db = addon.Database.open(path);
		} catch {
			db = addon.Database.withPath(path);
		}

		const kitDb = new KitDatabase(db, schema);
		kitDb.ensureInternalTables();
		for (const table of schema.tablesList()) {
			kitDb.ensureAppTable(table);
		}
		kitDb.writeSchemaCatalog();
		return kitDb;
	}

	static openSync(
		path: string,
		schema: Schema,
		options?: { encryption?: { passphrase: string } }
	): KitDatabase {
		if (options?.encryption?.passphrase) {
			try {
				return KitDatabase.openEncryptedSync(path, schema, options.encryption.passphrase);
			} catch {
				return KitDatabase.createEncryptedSync(path, schema, options.encryption.passphrase);
			}
		}

		let db: MongrelDatabase;
		try {
			db = addon.Database.open(path);
		} catch {
			db = addon.Database.withPath(path);
		}

		return KitDatabase.initialize(db, schema);
	}

	static createEncryptedSync(path: string, schema: Schema, passphrase: string): KitDatabase {
		if (!passphrase) {
			throw new Error('createEncryptedSync requires a non-empty passphrase');
		}
		const db = addon.Database.createEncrypted(path, passphrase);
		return KitDatabase.initialize(db, schema);
	}

	static openEncryptedSync(path: string, schema: Schema, passphrase: string): KitDatabase {
		if (!passphrase) {
			throw new Error('openEncryptedSync requires a non-empty passphrase');
		}
		const db = addon.Database.openEncrypted(path, passphrase);
		return KitDatabase.initialize(db, schema);
	}

	private static initialize(db: MongrelDatabase, schema: Schema): KitDatabase {
		const kitDb = new KitDatabase(db, schema);
		kitDb.ensureInternalTables();
		for (const table of schema.tablesList()) {
			kitDb.ensureAppTable(table);
		}
		kitDb.writeSchemaCatalog();
		return kitDb;
	}

	private ensureInternalTables(): void {
		for (const table of internalTables) {
			if (!this.db.tableNames().includes(table.name)) {
				this.db.createTable(table.name, toMongrelSchema(table));
			}
		}
	}

	private ensureAppTable(table: TableSpec): void {
		if (this.db.tableNames().includes(table.name)) return;
		this.db.createTable(table.name, toMongrelSchema(table));
	}

	private writeSchemaCatalog(): void {
		const catalog = this.db.table('__kit_schema_catalog');
		const schemaJson = JSON.stringify(
			this.schema.tablesList().map((t) => ({
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
				checks: t.checks.map((c: CheckSpec) => ({ name: c.name }))
			}))
		);
		const checksum = createHash('sha256').update(schemaJson).digest('hex');
		catalog.put([
			{ columnId: columnId(kitSchemaCatalog, 'schema_version'), int64: 1n },
			{ columnId: columnId(kitSchemaCatalog, 'schema_json'), text: schemaJson },
			{ columnId: columnId(kitSchemaCatalog, 'checksum'), text: checksum },
			{ columnId: columnId(kitSchemaCatalog, 'written_at'), text: isoNow() }
		]);
		catalog.commit();
	}

	migrateSync(schema: Schema, migrations: Migration[]): void {
		runMigrateSync(this, schema, migrations);
	}

	close(): void {
		this.db.close();
	}

	get nativeDb(): MongrelDatabase {
		return this.db;
	}

	tableNames(): string[] {
		return this.db.tableNames().filter((name) => !name.startsWith('__kit_'));
	}

	/** Verify run footer checksums; returns integrity issues grouped by table. */
	check(): unknown {
		return JSON.parse(this.db.check());
	}

	/** Drop corrupt runs; returns the doctor report. */
	doctor(): unknown {
		return JSON.parse(this.db.doctor());
	}

	/**
	 * Flush every table's in-memory writes to durable sorted runs. Besides
	 * durability, this empties the memtable, which enables the engine's
	 * incremental-aggregate fast path (see `incrementalAggregate`).
	 */
	flush(): void {
		for (const name of this.db.tableNames()) {
			this.db.table(name).flush();
		}
	}

	/** The current visible commit epoch (monotonically increasing version). */
	snapshotEpoch(): bigint {
		return this.db.snapshotEpoch();
	}

	/** Export every visible row of `table` as a TSV document. */
	exportTsv(table: string): string {
		const spec = this.schema.table(table);
		const rows = this.selectFrom(spec).executeSync() as Record<string, unknown>[];
		return rowsToTsv(spec, rows);
	}

	/** Read every row of `table` visible at commit `epoch` (MVCC time-travel). */
	rowsAtEpoch(table: string, epoch: bigint): Record<string, unknown>[] {
		const spec = this.schema.table(table);
		return this.db
			.table(table)
			.rowsAtEpoch(epoch)
			.map((rj) => rowFromRowJs(spec, rj) as Record<string, unknown>);
	}

	/**
	 * Reservoir-sampled approximate aggregate (`count`/`sum`/`avg`) with a
	 * `z`-score confidence interval (default ~95%). Returns
	 * `{ point, ci_low, ci_high, n_population, n_sample_live, n_passing }`, or
	 * `null` when the reservoir is empty. `column` is required for `sum`/`avg`.
	 */
	approxAggregate(
		table: string,
		agg: 'count' | 'sum' | 'avg',
		column?: string,
		z = 1.96
	): ApproxAggregate | null {
		const spec = this.schema.table(table);
		let columnId: number | undefined;
		if (column !== undefined) {
			const col = spec.columns.find((c) => c.name === column);
			if (!col) throw new KitError(`unknown column '${column}'`);
			columnId = col.id;
		} else if (agg !== 'count') {
			throw new KitError(`approx ${agg} requires a column`);
		}
		const raw = this.db.table(table).approxAggregate(agg, columnId, z);
		return raw === null ? null : (JSON.parse(raw) as ApproxAggregate);
	}

	/**
	 * Stream `table` in batches of at most `batchSize` rows, invoking `cb` once
	 * per batch. Implemented with `limit`/`offset` pagination, so memory stays
	 * bounded to one batch. (Not snapshot-consistent across batches, and O(n·
	 * pages) — the Rust/Python kit's `scan_batched` uses the engine's native
	 * single-pass cursor.)
	 */
	scanBatched(
		table: string,
		batchSize: number,
		cb: (rows: Record<string, unknown>[]) => void
	): void {
		const spec = this.schema.table(table);
		const size = Math.max(1, Math.floor(batchSize));
		let offset = 0;
		for (;;) {
			const rows = this.selectFrom(spec)
				.limit(size)
				.offset(offset)
				.executeSync() as Record<string, unknown>[];
			if (rows.length === 0) break;
			cb(rows);
			if (rows.length < size) break;
			offset += rows.length;
		}
	}

	/**
	 * Rank rows of `table` by Jaccard set-similarity between `query` and the
	 * string set stored (as a JSON array) in `column`, returning the top `k`
	 * with similarity `> 0`, highest first. The set-similarity / dedup-join
	 * primitive the `MinHash` index kind is meant to serve; an exact linear scan
	 * (a sub-linear MinHash/LSH index remains engine future-work).
	 */
	setSimilarity(
		table: string,
		column: string,
		query: string[],
		k: number
	): { row: Record<string, unknown>; similarity: number }[] {
		const spec = this.schema.table(table);
		const col = spec.columns.find((c) => c.name === column);
		if (!col) {
			throw new KitError(`unknown column '${column}' on table '${table}'`);
		}
		const q = new Set(query);

		// Fast path: a MinHash index generates sub-linear candidates via LSH,
		// which are then re-verified with exact Jaccard below.
		const hasMinhash = (spec.indexes ?? []).some(
			(idx) => idx.kind === 'minhash' && idx.columns.includes(column)
		);
		let rows: Record<string, unknown>[];
		if (hasMinhash) {
			const candidateBudget = Math.max(k * 8, k + 64);
			const cond = {
				kind: addon.ConditionKind.MinHashSimilar,
				columnId: col.id,
				values: query,
				k: candidateBudget
			};
			rows = this.db
				.table(table)
				.query([cond])
				.map((rj) => rowFromRowJs(spec, rj) as Record<string, unknown>);
		} else {
			rows = this.selectFrom(spec).executeSync() as Record<string, unknown>[];
		}

		const scored: { row: Record<string, unknown>; similarity: number }[] = [];
		for (const row of rows) {
			const set = parseStringSet(row[column]);
			let inter = 0;
			for (const x of set) if (q.has(x)) inter++;
			const union = set.size + q.size - inter;
			const sim = union === 0 ? 0 : inter / union;
			if (sim > 0) scored.push({ row, similarity: sim });
		}
		scored.sort((a, b) => b.similarity - a.similarity);
		return scored.slice(0, Math.max(0, k));
	}

	/** Import a TSV document into `table`; returns the number of rows inserted. */
	importTsv(table: string, text: string): number {
		const spec = this.schema.table(table);
		const rows = tsvToRows(spec, text);
		if (rows.length === 0) return 0;
		this.insertInto(spec)
			.valuesMany(rows as never)
			.executeSync();
		return rows.length;
	}

	/**
	 * Rename a live table from `oldName` to `newName`. The source must exist and
	 * be live; the target must not collide with an existing table (the engine
	 * enforces both). The rename is durable: it is logged to the WAL and applied
	 * again on reopen. The `table_id`, schema, and on-disk layout are unchanged,
	 * so outstanding handles and indexes remain valid.
	 *
	 * Internal `__kit_`-prefixed names are off-limits in both directions: an app
	 * table cannot be renamed to a `__kit_` name (it would vanish from
	 * {@link KitDatabase.tableNames}, which filters that prefix) and an internal
	 * table cannot be renamed away from its expected name (the Kit looks internal
	 * tables up by name). This keeps the internal-table namespace invariant
	 * intact without the engine needing to know about Kit conventions.
	 */
	renameTable(oldName: string, newName: string): void {
		if (oldName.startsWith('__kit_') || newName.startsWith('__kit_')) {
			throw new Error(
				"renameTable: names beginning with '__kit_' are reserved for internal tables"
			);
		}
		this.db.renameTable(oldName, newName);
	}

	begin(): Transaction {
		return this.db.begin();
	}

	/**
	 * Reserve (without inserting) the next engine-native AUTO_INCREMENT value for
	 * `tableName`, advancing the engine's per-table counter. Returns `null` when
	 * the table has no auto-increment column.
	 *
	 * This is the replacement for the legacy `allocateSequenceSync` hot-row
	 * scheme: it is a pure in-memory counter bump (no `__kit_sequences` row, no
	 * extra commit) that becomes durable when a row carrying the reserved id
	 * commits. An aborted reservation simply leaves a gap, which the never-reuse
	 * rule permits. Used by `prepareInsertRowSync` so a transaction can stage the
	 * row with an explicit id and still return it from `executeSync()`.
	 */
	reserveAutoIncSync(tableName: string): bigint | null {
		const reserved = this.db.table(tableName).reserveAutoInc();
		return reserved ?? null;
	}

	/**
	 * Remove every row from `tableName` and clear the Kit guard rows owned by
	 * that table. Throws when another table has a foreign key referencing it.
	 */
	truncateTable(tableName: string): void {
		this.schema.table(tableName);
		const references = this.schema
			.tablesList()
			.flatMap((t) =>
				t.foreignKeys
					.filter((fk) => fk.referencesTable === tableName)
					.map((fk) => `${t.name}.${fk.name}`)
			);
		if (references.length > 0) {
			throw new KitError(
				`table ${tableName} is referenced by foreign key(s): ${references.join(', ')}`,
				'RESTRICT'
			);
		}
		const kit: ConstraintKit = { db: this.nativeDb, schema: this.schema };
		runSyncTxn(this, (txn) => {
			txn.truncate(tableName);
			deleteGuardsForTable(kit, txn, tableName);
		});
	}
}
