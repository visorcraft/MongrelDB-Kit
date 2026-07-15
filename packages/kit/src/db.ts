import { createHash, randomBytes } from 'node:crypto';
import mongreldb from '@visorcraft/mongreldb';
import { tableFromIPC, type Table as ArrowTable } from 'apache-arrow';
import type {
	Database as NativeDatabase,
	Transaction,
	ColumnType as NativeColumnType,
	IndexKindSpec as NativeIndexKindSpec,
	ConditionKind as NativeConditionKind,
	ConditionSpec,
	Cell,
	PutResult,
	RowJs,
	TypedColumn,
	CacheStatsJs,
	TriggerConfigJs
} from '@visorcraft/mongreldb/native.js';
import { IndexBuildPolicyJs, WriteBuffer } from '@visorcraft/mongreldb/native.js';
import { Schema } from './schema.js';
import { rowsToTsv, tsvToRows } from './tsv.js';
import { rowFromRowJs } from './rows.js';
import {
	internalTables,
	kitSchemaCatalog
} from './internalTables.js';
import type { TableSpec, ColumnStorageType, CheckSpec } from './types.js';
import { procedureJson, type ProcedureCallOptions, type ProcedureCallResult, type ProcedureSpec } from './procedure.js';
import { triggerJson, type TriggerSpec } from './trigger.js';
import {
	createViewSql,
	createVirtualTableSql,
	dropViewSql,
	dropVirtualTableSql,
	type ViewSpec,
	type VirtualTableSpec
} from './external.js';
import { migrateSync as runMigrateSync, type Migration } from './migrate.js';
import { isReferencedTable, deleteGuardsForTable, toCells, type ConstraintKit } from './constraints.js';
import { KitError, QueryCancelledError, isRetryableConflict, mapSqlError } from './errors.js';

export interface SqlOptions {
	timeoutMs?: number;
	signal?: AbortSignal;
	queryId?: string;
}

export interface SqlQuery<T> {
	readonly id: string;
	readonly result: Promise<T>;
	cancel(): Promise<void> | void;
}

type NativeSqlOptions = {
	queryId?: string;
	timeoutMs?: number;
};

type NativeSqlQuery = {
	readonly id: string;
	cancel(): boolean;
	result(): Promise<Buffer>;
};

function sqlQueryId(): string {
	return randomBytes(16).toString('hex');
}

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
	defaultValue?: Cell;
	defaultExpr?: string;
	enumVariants?: string[];
	encrypted?: boolean;
	encryptedIndexable?: boolean;
};

type MongrelDatabase = NativeDatabase & {
	transaction(
		fn: (txn: Transaction) => void | Promise<void>,
		opts?: { maxRetries?: number; baseDelayMs?: number }
	): Promise<bigint>;
	alterColumn(table: string, columnName: string, column: MongrelColumnSpec): bigint;
	tableColumnSpecs(table: string): MongrelColumnSpec[];
	createProcedure(spec: { json: string }): bigint;
	createOrReplaceProcedure(spec: { json: string }): bigint;
	dropProcedure(name: string): void;
	procedures(): { json: string }[];
	procedure(name: string): { json: string } | null;
	callProcedure(name: string, opts?: { argsJson?: string; idempotencyKey?: string }): { epoch?: bigint; resultJson: string };
	createTrigger(spec: { json: string }): bigint;
	createOrReplaceTrigger(spec: { json: string }): bigint;
	dropTrigger(name: string): void;
	triggers(): { json: string }[];
	trigger(name: string): { json: string } | null;
	sql(sql: string): Promise<Buffer>;
	sqlWithOptions(sql: string, options?: NativeSqlOptions): Promise<Buffer>;
	startSql(sql: string, options?: NativeSqlOptions): NativeSqlQuery;
	cancelSql(queryId: string): boolean;
	// User/role/credentials (NAPI addon methods)
	createUser(username: string, password: string): void;
	dropUser(username: string): void;
	alterUserPassword(username: string, newPassword: string): void;
	verifyUser(username: string, password: string): boolean;
	setUserAdmin(username: string, isAdmin: boolean): void;
	users(): string[];
	createRole(name: string): void;
	dropRole(name: string): void;
	roles(): string[];
	grantRole(username: string, roleName: string): void;
	revokeRole(username: string, roleName: string): void;
	grantPermission(roleName: string, permission: string): void;
	revokePermission(roleName: string, permission: string): void;
	// Credential enforcement (NAPI addon methods)
	enableAuth(adminUsername: string, adminPassword: string): void;
	disableAuth(): void;
	requireAuthEnabled(): boolean;
	refreshPrincipal(): void;
};

type MongrelModule = {
	Database: {
		open(path: string): MongrelDatabase;
		withPath(path: string): MongrelDatabase;
		createEncrypted(path: string, passphrase: string): MongrelDatabase;
		openEncrypted(path: string, passphrase: string): MongrelDatabase;
		openWithCredentials(path: string, username: string, password: string): MongrelDatabase;
		createWithCredentials(path: string, adminUsername: string, adminPassword: string): MongrelDatabase;
		openEncryptedWithCredentials(path: string, passphrase: string, username: string, password: string): MongrelDatabase;
		createEncryptedWithCredentials(path: string, passphrase: string, adminUsername: string, adminPassword: string): MongrelDatabase;
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
		case 'timestamp':
		case 'date':
		case 'text':
		case 'bytes':
		case 'json':
			return addon.ColumnType.Bytes;
		case 'date64':
			return addon.ColumnType.Date64;
		case 'time64':
			return addon.ColumnType.Time64;
		case 'interval':
			return addon.ColumnType.Interval;
		case 'decimal128':
			return addon.ColumnType.Decimal128;
		case 'uuid':
			return addon.ColumnType.Uuid;
		case 'json_native':
			return addon.ColumnType.Json;
		case 'array':
			return addon.ColumnType.Array;
		case 'embedding':
			return addon.ColumnType.Embedding;
		case 'sparse':
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
									: idx.kind === 'learned_range'
										? addon.IndexKindSpec.LearnedRange
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
			ty: col.enumValues ? addon.ColumnType.Enum : toMongrelColumnType(col.storageType),
			primaryKey: col.primaryKey,
			nullable: col.nullable,
			// A Kit sequence-default column maps to the engine's native
			// AUTO_INCREMENT allocator (a per-table WAL-durable counter).
			autoIncrement: col.default?.kind === 'sequence',
			embeddingDim: col.embeddingDim,
			defaultValue:
				col.default?.kind === 'static'
					? toCells(table, { [col.name]: col.default.value }).find(
							(cell) => cell.columnId === col.id
						)
					: undefined,
			defaultExpr:
				col.default?.kind === 'now' || col.default?.kind === 'uuid'
					? col.default.kind
					: (col.generated ?? undefined),
			enumVariants: col.enumValues,
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

function alignTableColumnIds(table: TableSpec, physicalColumns: MongrelColumnSpec[]): void {
	const physicalByName = new Map(physicalColumns.map((col) => [col.name, col.id]));
	const used = new Set<number>();

	for (const col of table.columns) {
		const physicalId = physicalByName.get(col.name);
		if (physicalId === undefined) continue;
		if (used.has(physicalId)) {
			throw new Error(`Duplicate physical column id ${physicalId} in table "${table.name}"`);
		}
		col.id = physicalId;
		used.add(physicalId);
	}

	let nextId = 1;
	for (const col of table.columns) {
		if (physicalByName.has(col.name)) continue;
		if (col.idExplicit) {
			if (used.has(col.id)) {
				throw new Error(
					`Column "${col.name}" in table "${table.name}" uses existing physical id ${col.id}`
				);
			}
			used.add(col.id);
			continue;
		}
		while (used.has(nextId)) nextId++;
		col.id = nextId++;
		used.add(col.id);
	}
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

/**
 * Async twin of {@link runSyncTxn}: run `fn` inside a fresh transaction,
 * committing via the native `Transaction.commitAsync()` (off the Node event
 * loop) and retrying bounded-exponentially on retryable conflicts. `fn` may
 * itself be async; the staged writes are committed atomically after it
 * resolves. Exported so query builders and `KitDatabase` methods can share one
 * implementation.
 */
export async function runTxn(
	kit: KitDatabase,
	fn: (txn: Transaction) => Promise<void> | void,
	opts: { maxRetries?: number; baseDelayMs?: number } = {}
): Promise<void> {
	const maxRetries = opts.maxRetries ?? 5;
	const baseDelayMs = opts.baseDelayMs ?? 1;

	let lastErr: unknown;
	for (let attempt = 0; attempt <= maxRetries; attempt++) {
		const txn = kit.begin();
		try {
			await fn(txn);
			await txn.commitAsync();
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
					await new Promise((resolve) => setTimeout(resolve, delay));
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
		} catch (e) {
			// Propagate AuthRequired — do NOT fall back to create, which would
			// silently bypass credential enforcement on an existing database.
			const errMsg = e instanceof Error ? e.message : String(e);
			if (errMsg.includes('AuthRequired') || errMsg.includes('authentication required')) {
				throw e;
			}
			db = addon.Database.withPath(path);
		}

		const kitDb = new KitDatabase(db, schema);
		kitDb.ensureInternalTables();
		kitDb.alignExistingTableColumnIds(internalTables);
		kitDb.alignExistingTableColumnIds(schema.tablesList());
		for (const table of schema.tablesList()) {
			kitDb.ensureAppTable(table);
		}
		kitDb.writeSchemaCatalog();
		return kitDb;
	}

	static openSync(
		path: string,
		schema: Schema,
		options?: {
			encryption?: { passphrase: string };
			credentials?: { username: string; password: string };
		}
	): KitDatabase {
		if (options?.encryption?.passphrase && options?.credentials) {
			const { passphrase } = options.encryption;
			const { username, password } = options.credentials;
			const db = addon.Database.openEncryptedWithCredentials(path, passphrase, username, password);
			return KitDatabase.initialize(db, schema);
		}
		if (options?.encryption?.passphrase) {
			try {
				return KitDatabase.openEncryptedSync(path, schema, options.encryption.passphrase);
			} catch {
				return KitDatabase.createEncryptedSync(path, schema, options.encryption.passphrase);
			}
		}
		if (options?.credentials) {
			const { username, password } = options.credentials;
			const db = addon.Database.openWithCredentials(path, username, password);
			return KitDatabase.initialize(db, schema);
		}

		let db: MongrelDatabase;
		try {
			db = addon.Database.open(path);
		} catch (e) {
			// Propagate AuthRequired — do NOT fall back to create, which would
			// silently bypass credential enforcement on an existing database.
			const errMsg = e instanceof Error ? e.message : String(e);
			if (errMsg.includes('AuthRequired') || errMsg.includes('authentication required')) {
				throw e;
			}
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

	/** Create a fresh database with `require_auth = true`, a single admin user,
	 * and the given schema. The returned handle is already authenticated as
	 * the admin. */
	static createWithCredentialsSync(
		path: string,
		schema: Schema,
		adminUsername: string,
		adminPassword: string
	): KitDatabase {
		const db = addon.Database.createWithCredentials(path, adminUsername, adminPassword);
		return KitDatabase.initialize(db, schema);
	}

	/** Create a fresh encrypted database with `require_auth = true` and a single
	 * admin user. Composes encryption-at-rest with credential enforcement. */
	static createEncryptedWithCredentialsSync(
		path: string,
		schema: Schema,
		passphrase: string,
		adminUsername: string,
		adminPassword: string
	): KitDatabase {
		const db = addon.Database.createEncryptedWithCredentials(
			path,
			passphrase,
			adminUsername,
			adminPassword
		);
		return KitDatabase.initialize(db, schema);
	}

	private static initialize(db: MongrelDatabase, schema: Schema): KitDatabase {
		const kitDb = new KitDatabase(db, schema);
		kitDb.ensureInternalTables();
		kitDb.alignExistingTableColumnIds(internalTables);
		kitDb.alignExistingTableColumnIds(schema.tablesList());
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

	private alignExistingTableColumnIds(tables: TableSpec[]): void {
		const names = new Set(this.db.tableNames());
		for (const table of tables) {
			if (!names.has(table.name)) continue;
			alignTableColumnIds(table, this.db.tableColumnSpecs(table.name));
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

	createProcedureSync(spec: ProcedureSpec): bigint {
		return this.db.createProcedure({ json: procedureJson(spec) });
	}

	createOrReplaceProcedureSync(spec: ProcedureSpec): bigint {
		return this.db.createOrReplaceProcedure({ json: procedureJson(spec) });
	}

	dropProcedureSync(name: string): void {
		this.db.dropProcedure(name);
	}

	procedures(): ProcedureSpec[] {
		return this.db.procedures().map((p) => JSON.parse(p.json) as ProcedureSpec);
	}

	procedure(name: string): ProcedureSpec | null {
		const proc = this.db.procedure(name);
		return proc ? (JSON.parse(proc.json) as ProcedureSpec) : null;
	}

	callProcedureSync(name: string, opts: ProcedureCallOptions = {}): ProcedureCallResult {
		const result = this.db.callProcedure(name, {
			argsJson: JSON.stringify(opts.args ?? {}),
			idempotencyKey: opts.idempotencyKey
		});
		return {
			epoch: result.epoch,
			result: JSON.parse(result.resultJson)
		};
	}

	createTriggerSync(spec: TriggerSpec): bigint {
		return this.db.createTrigger({ json: triggerJson(spec) });
	}

	createOrReplaceTriggerSync(spec: TriggerSpec): bigint {
		return this.db.createOrReplaceTrigger({ json: triggerJson(spec) });
	}

	dropTriggerSync(name: string): void {
		this.db.dropTrigger(name);
	}

	triggers(): TriggerSpec[] {
		return this.db.triggers().map((trigger) => JSON.parse(trigger.json) as TriggerSpec);
	}

	trigger(name: string): TriggerSpec | null {
		const trigger = this.db.trigger(name);
		return trigger ? (JSON.parse(trigger.json) as TriggerSpec) : null;
	}

	startSql(sql: string, options: SqlOptions = {}): SqlQuery<ArrowTable> {
		const queryId = options.queryId ?? sqlQueryId();
		if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0)) {
			throw new RangeError('timeoutMs must be a positive safe integer');
		}
		if (options.signal?.aborted) {
			return {
				id: queryId,
				result: Promise.reject(new QueryCancelledError(queryId)),
				cancel() {}
			};
		}

		const native = this.db.startSql(sql, {
			queryId,
			timeoutMs: options.timeoutMs
		});
		const cancel = () => {
			native.cancel();
		};
		const onAbort = () => cancel();
		options.signal?.addEventListener('abort', onAbort, { once: true });
		const result = native
			.result()
			.then((bytes) => tableFromIPC(bytes))
			.catch((error: unknown) => {
				throw mapSqlError(error, queryId);
			})
			.finally(() => options.signal?.removeEventListener('abort', onAbort));
		return { id: native.id, result, cancel };
	}

	async sql(sql: string, options?: SqlOptions): Promise<ArrowTable> {
		return this.startSql(sql, options).result;
	}

	async sqlRows(sql: string, options?: SqlOptions): Promise<Record<string, unknown>[]> {
		return [...(await this.sql(sql, options))].map((row) => ({ ...row }));
	}

	async createVirtualTable(spec: VirtualTableSpec): Promise<ArrowTable> {
		return this.sql(createVirtualTableSql(spec));
	}

	async dropVirtualTable(name: string): Promise<ArrowTable> {
		return this.sql(dropVirtualTableSql(name));
	}

	/** Create a SQL view (`CREATE VIEW <name> AS <select>`). The engine
	 * overwrites any existing view with the same name, so this also serves as
	 * replace. The view lives in the kit's long-lived SQL session — see
	 * [SQL views](./migrations.md#sql-views). */
	async createView(spec: ViewSpec): Promise<ArrowTable> {
		return this.sql(createViewSql(spec));
	}

	/** Drop a SQL view by name (idempotent — `DROP VIEW IF EXISTS`). */
	async dropView(name: string): Promise<ArrowTable> {
		return this.sql(dropViewSql(name));
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

	/** Compact every table: merge sorted runs into one clean run each so
	 * query latency stays flat. Returns `{compacted, skipped}`. */
	compactAll(): { compacted: number; skipped: number } {
		return this.db.compactAll();
	}

	/** Compact a single table by name. Returns `true` if compacted, `false`
	 * if skipped (fewer than two runs). */
	compactTable(table: string): boolean {
		return this.db.compactTable(table);
	}

	/** Rebuild statistics/metadata for every table's indexes (the engine's
	 * `ANALYZE` equivalent). Routes through the SQL surface for parity with the
	 * engine's own definition. Safe to run at any time; useful after bulk loads
	 * so the query planner and learned indexes have fresh data. */
	async analyze(): Promise<void> {
		await this.sql('ANALYZE');
	}

	/** Reclaim space across all tables (the engine's `VACUUM` equivalent:
	 * compact every sorted run, then gc). Routes through the SQL surface for
	 * parity with the engine's own definition. Safe to run at any time. */
	async vacuum(): Promise<void> {
		await this.sql('VACUUM');
	}

	/** The current visible commit epoch (monotonically increasing version). */
	snapshotEpoch(): bigint {
		return this.db.snapshotEpoch();
	}

	/**
	 * Set how many committed epochs of history to retain for MVCC time-travel
	 * reads ({@link rowsAtEpoch}). The engine default keeps only the latest
	 * epoch, so raise this *before* writing data you want to read back at a
	 * past snapshot. The setting persists across close/reopen and cannot
	 * restore history that was already pruned.
	 */
	setHistoryRetentionEpochs(epochs: number): void {
		this.db.setHistoryRetentionEpochs(epochs);
	}

	/** The configured history-retention depth — how many committed epochs are kept for time-travel reads. */
	historyRetentionEpochs(): bigint {
		return this.db.historyRetentionEpochs();
	}

	/** The oldest epoch still retained for time-travel reads ({@link rowsAtEpoch}). */
	earliestRetainedEpoch(): bigint {
		return this.db.earliestRetainedEpoch();
	}

	/**
	 * Async twin of {@link flush}. Yields a microtask between table flushes; the
	 * underlying engine `flush` is synchronous (the addon ships no
	 * `flushAsync` on `Database`), so each call still blocks — but the await
	 * points let other pending microtasks run.
	 */
	async flushAsync(): Promise<void> {
		for (const name of this.db.tableNames()) {
			await this.db.table(name).flushAsync();
		}
	}

	/**
	 * Async twin of {@link compactAll}. **Caveat:** the addon has no native
	 * `compactAllAsync`, so this wraps the sync call in a `Promise` — it
	 * matches the async signature but the underlying compaction still blocks
	 * the event loop. Use it for signature parity in async maintenance loops.
	 */
	async compactAllAsync(): Promise<{ compacted: number; skipped: number }> {
		return Promise.resolve(this.db.compactAll());
	}

	/**
	 * Async twin of {@link compactTable}. Same caveat as
	 * {@link compactAllAsync}: no native async variant; the call blocks.
	 */
	async compactTableAsync(table: string): Promise<boolean> {
		return Promise.resolve(this.db.compactTable(table));
	}

	/** Async twin of {@link snapshotEpoch}. Resolves immediately. */
	async snapshotEpochAsync(): Promise<bigint> {
		return Promise.resolve(this.db.snapshotEpoch());
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
	 * Async twin of {@link approxAggregate}. **Caveat:** the addon has no native
	 * `approxAggregateAsync`, so this wraps the sync call in a `Promise` — the
	 * underlying reservoir read still blocks the event loop.
	 */
	async approxAggregateAsync(
		table: string,
		agg: 'count' | 'sum' | 'avg',
		column?: string,
		z = 1.96
	): Promise<ApproxAggregate | null> {
		return Promise.resolve(this.approxAggregate(table, agg, column, z));
	}

	/** Stream `table` in batches of at most `batchSize` rows. */
	scanBatched(
		table: string,
		batchSize: number,
		cb: (rows: Record<string, unknown>[]) => void
	): void {
		const spec = this.schema.table(table);
		const size = Math.min(10_000, Math.max(1, Math.floor(batchSize)));
		const nativeTable = this.db.table(table) as unknown as {
			queryPage(conditions: ConditionSpec[], limit: number, offset: number): RowJs[];
		};
		let offset = 0;
		for (;;) {
			const rows = nativeTable
				.queryPage([], size, offset)
				.map((row) => rowFromRowJs(spec, row));
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

	/**
	 * Async twin of {@link setSimilarity}. The MinHash candidate fetch runs via
	 * the native `queryAsync` (genuinely off the event loop via
	 * `spawn_blocking`); the exact Jaccard re-scoring runs in JS. Falls back to
	 * a sync `selectFrom().executeSync()` scan when there's no MinHash index.
	 */
	async setSimilarityAsync(
		table: string,
		column: string,
		query: string[],
		k: number
	): Promise<{ row: Record<string, unknown>; similarity: number }[]> {
		const spec = this.schema.table(table);
		const col = spec.columns.find((c) => c.name === column);
		if (!col) {
			throw new KitError(`unknown column '${column}' on table '${table}'`);
		}
		const q = new Set(query);
		const hasMinhash = (spec.indexes ?? []).some(
			(idx) => idx.kind === 'minhash' && idx.columns.includes(column)
		);
		let rows: Record<string, unknown>[];
		if (hasMinhash) {
			const candidateBudget = Math.max(k * 8, k + 64);
			const cond: ConditionSpec = {
				kind: addon.ConditionKind.MinHashSimilar,
				columnId: col.id,
				values: query,
				k: candidateBudget
			};
			const rj = await this.db.table(table).queryAsync([cond]);
			rows = rj.map((r) => rowFromRowJs(spec, r) as Record<string, unknown>);
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

	// ── native async TableHandle wrappers ───────────────────────────────────
	//
	// These wrap the addon's `spawn_blocking` async variants so hot read/write
	// paths don't block the Node event loop. The sync counterparts are reached
	// via `kit.nativeDb.table(name)`; these are the typed Kit-level async
	// surface. All bypass Kit-level constraint enforcement (defaults, unique
	// guards, FK checks) — use the transactional `insert*`/`update*`/`delete*`
	// builders for constrained writes, and these for raw, high-throughput I/O.

	/** Async put of one row (`cells` = `{ columnId, int64|float64|text|... }[]`)
	 * via the native `putAsync`. Returns the row id and any auto-increment
	 * value. Bypasses Kit constraints. */
	putAsync(table: string, cells: RowJs['cells']): Promise<PutResult> {
		return this.db.table(table).putAsync(cells);
	}

	/** Async point-read by row id via the native `getAsync`. */
	getAsync(table: string, rowId: bigint): Promise<RowJs | null> {
		return this.db.table(table).getAsync(rowId);
	}

	/** Async condition query via the native `queryAsync` — returns raw
	 * `RowJs[]` (row id + typed cells). Build conditions with the `ConditionKind`
	 * constants; the kit's `Predicate`/`compilePredicate` produces these too. */
	queryAsync(table: string, conditions: ConditionSpec[]): Promise<RowJs[]> {
		return this.db.table(table).queryAsync(conditions);
	}

	/** Async count of all rows in `table` via the native `countAsync`. */
	countAsync(table: string): Promise<bigint> {
		return this.db.table(table).countAsync();
	}

	/** Async count of rows matching `conditions` via the native
	 * `countWhereAsync`. */
	countWhereAsync(table: string, conditions: ConditionSpec[]): Promise<bigint> {
		return this.db.table(table).countWhereAsync(conditions);
	}

	/** Async condition query returning Arrow IPC bytes (zero-copy columnar) via
	 * the native `queryArrowAsync`. Decode with `apache-arrow`'s
	 * `tableFromIPC`. */
	queryArrowAsync(table: string, conditions: ConditionSpec[]): Promise<Buffer> {
		return this.db.table(table).queryArrowAsync(conditions);
	}

	/**
	 * Fastest ingest path: bulk-load typed columns (`Int64`/`Float64`/`Bool` only)
	 * in one shot, bypassing the per-cell `Value` enum. **Commits internally**
	 * and returns the commit epoch — not transactional, cannot be staged into a
	 * `Transaction`. `Bytes`/`Embedding`/text columns are not supported here
	 * (use {@link InsertManyBuilder} / `putBatch` for those). Bypasses Kit
	 * constraints (defaults, unique guards, FK checks).
	 *
	 * Each `TypedColumn` carries a contiguous little-endian `data` buffer
	 * (`Int64` = N×8 bytes, `Float64` = N×8 bytes, `Bool` = N bytes) plus an
	 * optional `validity` bitmap (1 byte per row, `1`=non-null). All columns
	 * must have the same row count.
	 */
	bulkLoadTyped(table: string, columns: TypedColumn[]): bigint {
		return this.db.table(table).bulkLoadTyped(columns);
	}

	// ── storage tuning & introspection (Tier 3) ─────────────────────────────

	/** Set the per-table spill threshold (bytes). */
	setSpillThreshold(bytes: bigint | number): void {
		this.db.setSpillThreshold(Number(bytes));
	}

	/** Enable or disable recursive trigger execution (database-wide). */
	setRecursiveTriggers(enabled: boolean): void {
		this.db.setRecursiveTriggers(enabled);
	}

	/** Read the current trigger execution policy. */
	triggerConfig(): TriggerConfigJs {
		return this.db.triggerConfig();
	}

	/** Set the trigger execution policy. `max_depth` must be > 0. */
	setTriggerConfig(config: TriggerConfigJs): void {
		this.db.setTriggerConfig(config);
	}

	/** Set a table's compaction zstd level (-1 = default, 0 = none, 1..22). */
	setTableCompactionZstdLevel(table: string, level: number): void {
		this.db.setTableCompactionZstdLevel(table, level);
	}

	/** Set a table's result-cache max bytes. */
	setTableResultCacheMaxBytes(table: string, maxBytes: bigint | number): void {
		this.db.setTableResultCacheMaxBytes(table, Number(maxBytes));
	}

	/** Set a table's mutable-run spill threshold (bytes). */
	setTableMutableRunSpillBytes(table: string, bytes: bigint | number): void {
		this.db.setTableMutableRunSpillBytes(table, Number(bytes));
	}

	/** Set a table's WAL sync byte threshold (bytes between group-syncs). */
	setTableSyncByteThreshold(table: string, threshold: bigint | number): void {
		this.db.setTableSyncByteThreshold(table, Number(threshold));
	}

	/** Set a table's index build policy (`Deferred` for fast ingest, `Eager`
	 * for fast first query). */
	setTableIndexBuildPolicy(table: string, policy: IndexBuildPolicyJs): void {
		this.db.setTableIndexBuildPolicy(table, policy);
	}

	/** Page-cache statistics for a table. */
	tablePageCacheStats(table: string): CacheStatsJs {
		return this.db.tablePageCacheStats(table);
	}

	/** Number of sorted runs a table currently has (compaction target: 1). */
	tableRunCount(table: string): number {
		return this.db.tableRunCount(table);
	}

	/** Memtable length (uncommitted staged rows) for a table. */
	tableMemtableLen(table: string): number {
		return this.db.tableMemtableLen(table);
	}

	/** Mutable-run length for a table. */
	tableMutableRunLen(table: string): number {
		return this.db.tableMutableRunLen(table);
	}

	/** Page-cache entry count for a table. */
	tablePageCacheLen(table: string): number {
		return this.db.tablePageCacheLen(table);
	}

	/** Decoded-page-cache entry count for a table. */
	tableDecodedCacheLen(table: string): number {
		return this.db.tableDecodedCacheLen(table);
	}

	/**
	 * Create a {@link WriteBuffer} over `table` — opt-in write micro-batching
	 * where writes are **not durable until `flush()`** (the opposite contract of
	 * `put()`). `threshold` rows trigger an auto-flush (default 1000). Useful
	 * for high-throughput ingest where per-row commit latency isn't acceptable.
	 * Bypasses Kit constraints (defaults, unique guards, FK checks).
	 */
	writeBuffer(table: string, threshold?: number): WriteBuffer {
		return new WriteBuffer(this.db.table(table), threshold);
	}

	// ── user/role/credentials management ─────────────────────────────────────

	/** Create a catalog user with an Argon2id-hashed password. */
	createUser(username: string, password: string): void {
		this.db.createUser(username, password);
	}

	/** Drop a user by username. */
	dropUser(username: string): void {
		this.db.dropUser(username);
	}

	/** Change a user's password. */
	alterUserPassword(username: string, newPassword: string): void {
		this.db.alterUserPassword(username, newPassword);
	}

	/** Verify credentials. Returns `true` on success. */
	verifyUser(username: string, password: string): boolean {
		return this.db.verifyUser(username, password);
	}

	/** Grant or revoke admin privileges on a user. */
	setUserAdmin(username: string, isAdmin: boolean): void {
		this.db.setUserAdmin(username, isAdmin);
	}

	/** List all usernames. */
	users(): string[] {
		return this.db.users();
	}

	/** Create a role. */
	createRole(name: string): void {
		this.db.createRole(name);
	}

	/** Drop a role. */
	dropRole(name: string): void {
		this.db.dropRole(name);
	}

	/** List all role names. */
	roles(): string[] {
		return this.db.roles();
	}

	/** Grant a role to a user. */
	grantRole(username: string, roleName: string): void {
		this.db.grantRole(username, roleName);
	}

	/** Revoke a role from a user. */
	revokeRole(username: string, roleName: string): void {
		this.db.revokeRole(username, roleName);
	}

	/** Grant a permission to a role. Permission format: `"all"`, `"ddl"`,
	 * `"admin"`, or `"select:table"`, `"insert:table"`, `"update:table"`,
	 * `"delete:table"`. */
	grantPermission(roleName: string, permission: string): void {
		this.db.grantPermission(roleName, permission);
	}

	/** Revoke a permission from a role. */
	revokePermission(roleName: string, permission: string): void {
		this.db.revokePermission(roleName, permission);
	}

	// ── credential enforcement ─────────────────────────────────────────────

	/** Convert a credentialless database to a credentialed one in place.
	 * Creates the first admin user, sets `require_auth = true`, and caches the
	 * admin principal on this handle. */
	enableAuth(adminUsername: string, adminPassword: string): void {
		this.db.enableAuth(adminUsername, adminPassword);
	}

	/** Disable `require_auth`, reverting to credentialless mode (recovery).
	 * Users and roles are preserved but no longer enforced. */
	disableAuth(): void {
		this.db.disableAuth();
	}

	/** Returns `true` if this database has `require_auth = true`. */
	requireAuthEnabled(): boolean {
		return this.db.requireAuthEnabled();
	}

	/** Re-resolve the cached principal from the on-disk catalog, picking up
	 * role/permission changes made by other handles. */
	refreshPrincipal(): void {
		this.db.refreshPrincipal();
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
	 * Pure in-memory counter bump (no extra commit) that becomes durable when a
	 * row carrying the reserved id commits. An aborted reservation simply leaves
	 * a gap, which the never-reuse rule permits. Used by `prepareInsertRowSync`
	 * so a transaction can stage the row with an explicit id and still return it
	 * from `executeSync()`.
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
