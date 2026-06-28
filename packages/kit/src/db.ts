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
import {
	internalTables,
	kitSchemaCatalog,
	kitSequences
} from './internalTables.js';
import type { TableSpec, ColumnStorageType, CheckSpec } from './types.js';
import { migrateSync as runMigrateSync, type Migration } from './migrate.js';

type MongrelDatabase = NativeDatabase & {
	transaction(
		fn: (txn: Transaction) => void | Promise<void>,
		opts?: { maxRetries?: number; baseDelayMs?: number }
	): Promise<bigint>;
};

type MongrelModule = {
	Database: {
		open(path: string): MongrelDatabase;
		withPath(path: string): MongrelDatabase;
	};
	ColumnType: typeof NativeColumnType;
	IndexKindSpec: typeof NativeIndexKindSpec;
	ConditionKind: typeof NativeConditionKind;
};

const addon = mongreldb as unknown as MongrelModule;

type MongrelColumnSpec = {
	id: number;
	name: string;
	ty: number;
	primaryKey: boolean;
	nullable: boolean;
};

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
				kind: addon.IndexKindSpec.Bitmap
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
			nullable: col.nullable
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

	static openSync(path: string, schema: Schema): KitDatabase {
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

	begin(): Transaction {
		return this.db.begin();
	}

	allocateSequenceSync(name: string, count = 1): bigint {
		const txn = this.db.begin();
		const seqTable = txn.table('__kit_sequences');
		const handle = this.db.table('__kit_sequences');
		const matches = handle.query([
			{
				kind: addon.ConditionKind.BitmapEq,
				columnId: columnId(kitSequences, 'sequence_name'),
				text: name
			}
		]);
		const existing = matches[0] ?? null;
		const now = isoNow();
		let start = 0n;
		if (!existing) {
			start = 0n;
			seqTable.put([
				{ columnId: columnId(kitSequences, 'sequence_name'), text: name },
				{ columnId: columnId(kitSequences, 'next_value'), int64: BigInt(count) },
				{ columnId: columnId(kitSequences, 'updated_at'), text: now }
			]);
		} else {
			const nextCell = existing.cells.find(
				(c) => c.columnId === columnId(kitSequences, 'next_value')
			);
			const current = nextCell?.int64 ?? 0n;
			start = current;
			seqTable.put([
				{ columnId: columnId(kitSequences, 'sequence_name'), text: name },
				{ columnId: columnId(kitSequences, 'next_value'), int64: current + BigInt(count) },
				{ columnId: columnId(kitSequences, 'updated_at'), text: now }
			]);
		}
		txn.commit();
		return start;
	}

	async allocateSequence(name: string, count = 1): Promise<bigint> {
		return this.allocateSequenceSync(name, count);
	}
}
