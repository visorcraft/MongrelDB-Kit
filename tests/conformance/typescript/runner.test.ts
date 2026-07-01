import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
	KitDatabase,
	Schema,
	column,
	table,
	index,
	unique,
	foreignKey,
	staticDefault,
	nowDefault,
	uuidDefault,
	sequenceDefault,
	customDefault,
	eq,
	ne,
	gt,
	gte,
	lt,
	lte,
	isNull,
	isNotNull,
	and,
	or,
	inSubquery,
	exists,
	notExists,
	joinEq,
	contains,
	like,
	asc,
	desc,
	KitError,
	KitMigrationError,
	migrate,
	addUnique,
	encodedPk,
	encodeUniqueKey,
	encodeRowGuardKey
} from '../../../packages/kit/src/index.js';
import type {
	TableSpec,
	ColumnSpec,
	DefaultValue,
	Migration,
	JoinPredicate
} from '../../../packages/kit/src/index.js';

const FIXTURES_DIR = new URL('../fixtures', import.meta.url).pathname;

function loadJson(name: string): any {
	return JSON.parse(fs.readFileSync(path.join(FIXTURES_DIR, name), 'utf-8'));
}

function defaultFromJson(d: any): DefaultValue {
	if (d === 'now') return nowDefault();
	if (d === 'uuid') return uuidDefault();
	if (typeof d === 'object' && d.static !== undefined) return staticDefault(d.static);
	if (typeof d === 'object' && d.sequence !== undefined) return sequenceDefault(d.sequence);
	if (typeof d === 'object' && d.custom_name !== undefined)
		return customDefault(() => {
			throw new Error('custom default not supported');
		});
	throw new Error(`unknown default: ${JSON.stringify(d)}`);
}

function columnFromJson(col: any): ColumnSpec {
	const opts: any = {
		nullable: col.nullable,
		primaryKey: col.primary_key
	};
	if (col.default !== undefined) opts.default = defaultFromJson(col.default);
	if (col.enum_values !== undefined) opts.enumValues = col.enum_values;
	if (col.min !== undefined) opts.min = col.min;
	if (col.max !== undefined) opts.max = col.max;
	if (col.min_length !== undefined) opts.minLength = col.min_length;
	if (col.max_length !== undefined) opts.maxLength = col.max_length;
	if (col.regex !== undefined) opts.regex = new RegExp(col.regex);
	if (col.encrypted !== undefined) opts.encrypted = col.encrypted;
	if (col.encrypted_indexable !== undefined) opts.encryptedIndexable = col.encrypted_indexable;
	const c = column(col.name, col.storage_type, opts) as ColumnSpec;
	c.id = col.id;
	if (col.embedding_dim !== undefined) c.embeddingDim = col.embedding_dim;
	return c;
}

function schemaFromFixture(raw: any): Schema {
	const tables = raw.tables.map((t: any) => {
		const cols = t.columns.map(columnFromJson);
		const idxs = (t.indexes ?? []).map((i: any) =>
			index(i.columns, {
				name: i.name,
				unique: i.unique,
				fm: i.kind === 'fm',
				ann: i.kind === 'ann',
				sparse: i.kind === 'sparse'
			})
		);
		const uqs = (t.unique_constraints ?? []).map((u: any) => unique(u.columns, { name: u.name }));
		const fks = (t.foreign_keys ?? []).map((fk: any) => {
			const onDelete = fk.on_delete === 'set_null' ? 'set null' : fk.on_delete;
			return foreignKey(fk.columns, { table: fk.references_table, columns: fk.references_columns }, { name: fk.name, onDelete });
		});
		return table(t.name, {
			id: t.id,
			columns: cols,
			primaryKey: t.primary_key,
			indexes: idxs,
			foreignKeys: fks,
			unique: uqs,
			checks: []
		});
	});
	return new Schema(tables);
}

function migrationFromFixture(raw: any, schema: Schema): Migration {
	return {
		version: raw.version,
		name: raw.name,
		up(ctx) {
			for (const op of raw.ops) {
				if (op.create_table) {
					ctx.ensureTable(schema.table(op.create_table.name));
				} else if (op.add_column) {
					throw new KitMigrationError('add_column migration op not supported in conformance runner');
				}
			}
		}
	};
}

function isIntColumn(table: TableSpec, name: string): boolean {
	return table.columns.some((c) => c.name === name && c.storageType === 'int64');
}

function normalizeRowForTs(table: TableSpec, row: Record<string, any>): Record<string, any> {
	const out: Record<string, any> = {};
	for (const [key, value] of Object.entries(row)) {
		if (value === null || value === undefined) {
			out[key] = null;
		} else if (isIntColumn(table, key)) {
			out[key] = BigInt(value);
		} else {
			out[key] = value;
		}
	}
	return out;
}

function defaultForStorageType(storageType: string): unknown {
	switch (storageType) {
		case 'int64':
			return 0n;
		case 'float64':
			return 0;
		case 'bool':
			return false;
		case 'text':
		case 'json':
		case 'timestamp':
		case 'date':
		case 'bytes':
			return '';
		default:
			return null;
	}
}

function valuesEqual(a: unknown, b: unknown): boolean {
	if (a === null || a === undefined || b === null || b === undefined) return a === b;
	if (typeof a === 'bigint' || typeof b === 'bigint') return BigInt(a as any) === BigInt(b as any);
	return a === b;
}

function normalizeValueForCompare(value: unknown): unknown {
	if (typeof value === 'bigint') return Number(value);
	if (Array.isArray(value)) return value.map(normalizeValueForCompare);
	if (value && typeof value === 'object') {
		const out: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(value)) out[k] = normalizeValueForCompare(v);
		return out;
	}
	return value;
}

function normalizeRowForCompare(table: TableSpec, row: Record<string, unknown>): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(row)) {
		const col = table.columns.find((c) => c.name === k);
		if (col && col.nullable && valuesEqual(v, defaultForStorageType(col.storageType))) {
			out[k] = null;
		} else {
			out[k] = normalizeValueForCompare(v);
		}
	}
	return out;
}

function buildPredicate(table: TableSpec, filter: Record<string, any>): any {
	const parts: any[] = [];
	for (const [key, val] of Object.entries(filter)) {
		const col = table.columns.find((c) => c.name === key);
		if (!col) throw new Error(`column ${key} not found`);
		if (val === null) {
			if (col.nullable) {
				const defaultValue = defaultForStorageType(col.storageType);
				parts.push(or(isNull(col), eq(col, defaultValue as any)));
			} else {
				parts.push(isNull(col));
			}
		} else if (typeof val === 'object' && !Array.isArray(val)) {
			const [[op, operand]] = Object.entries(val);
			const v = isIntColumn(table, key) ? BigInt(operand as number) : operand;
			switch (op) {
				case 'eq':
					parts.push(eq(col, v));
					break;
				case 'ne':
					parts.push(ne(col, v));
					break;
				case 'gt':
					parts.push(gt(col, v));
					break;
				case 'gte':
					parts.push(gte(col, v));
					break;
				case 'lt':
					parts.push(lt(col, v));
					break;
				case 'lte':
					parts.push(lte(col, v));
					break;
				case 'is_null':
					parts.push(isNull(col));
					break;
				case 'is_not_null':
					parts.push(isNotNull(col));
					break;
				case 'like':
					parts.push(like(col, operand as string));
					break;
				default:
					throw new Error(`unknown operator ${op}`);
			}
		} else {
			const v = isIntColumn(table, key) ? BigInt(val as number) : val;
			parts.push(eq(col, v));
		}
	}
	return parts.length === 1 ? parts[0] : and(...parts);
}

function buildOrder(table: TableSpec, order: string): any[] {
	return order.split(',').map((part) => {
		part = part.trim();
		const descFlag = part.startsWith('-');
		const name = descFlag ? part.slice(1) : part.startsWith('+') ? part.slice(1) : part;
		const col = table.columns.find((c) => c.name === name);
		if (!col) throw new Error(`order column ${name} not found`);
		return descFlag ? desc(col) : asc(col);
	});
}

function errorCode(err: unknown): string {
	if (err instanceof KitError) {
		return err.code;
	}
	return 'UNKNOWN';
}

function assertOutcome<T>(scenarioName: string, fn: () => T, expected: any, normalize: (v: T) => any): void {
	try {
		const actual = normalize(fn());
		if (expected.error) {
			throw new Error(`scenario ${scenarioName} expected error ${expected.error} but succeeded with ${JSON.stringify(actual)}`);
		}
		expect(actual).toEqual(expected.row ?? expected.rows ?? expected.count ?? expected);
	} catch (err) {
		if (!expected.error) throw err;
		expect(errorCode(err)).toBe(expected.error);
	}
}

function returningColumns(table: TableSpec, names: string[]): ColumnSpec[] {
	return names.map((name) => {
		const col = table.columns.find((c) => c.name === name);
		if (!col) throw new Error(`returning column ${name} not found in ${table.name}`);
		return col;
	});
}

function safeStringify(value: unknown): string {
	return JSON.stringify(value, (_key, val) =>
		typeof val === 'bigint' ? String(val) : val
	);
}

function assertReturningColumnOrder(
	scenarioName: string,
	actual: Record<string, unknown> | Record<string, unknown>[],
	returning: string[]
): void {
	if (Array.isArray(actual)) {
		for (let i = 0; i < actual.length; i++) {
			expect(Object.keys(actual[i]), `${scenarioName} row ${i} returning column order`).toEqual(
				returning
			);
		}
	} else {
		expect(Object.keys(actual), `${scenarioName} returning column order`).toEqual(returning);
	}
}

function runPhase1Step(step: any, expected: any, kit: KitDatabase, schema: Schema): void {
	const tableSpec = schema.table(step.table);
	const returningNames = step.returning ?? [];
	const returning = returningColumns(tableSpec, returningNames);
	try {
		let actual: any;
		if (step.op === 'insert_returning') {
			actual = kit
				.insertInto(tableSpec)
				.values(normalizeRowForTs(tableSpec, step.row))
				.returning(...returning)
				.executeSync();
		} else if (step.op === 'upsert') {
			let builder = kit
				.insertInto(tableSpec)
				.values(normalizeRowForTs(tableSpec, step.row));
			if (step.on_conflict === 'do_nothing') {
				builder = builder.onConflictDoNothing();
			} else if (step.on_conflict && step.on_conflict.do_update) {
				builder = builder.onConflictDoUpdate(
					normalizeRowForTs(tableSpec, step.on_conflict.do_update)
				);
			} else {
				throw new Error(`unsupported on_conflict for ${step.name}: ${JSON.stringify(step.on_conflict)}`);
			}
			actual = builder.returning(...returning).executeSync();
		} else if (step.op === 'update_where') {
			actual = kit
				.updateTable(tableSpec)
				.set(normalizeRowForTs(tableSpec, step.patch))
				.where(buildPredicate(tableSpec, step.filter))
				.returning(...returning)
				.executeSync();
		} else if (step.op === 'delete_where') {
			actual = kit
				.deleteFrom(tableSpec)
				.where(buildPredicate(tableSpec, step.filter))
				.returning(...returning)
				.executeSync();
		} else if (step.op === 'truncate') {
			kit.truncateTable(step.table);
			actual = {};
		} else {
			throw new Error(`unknown op ${step.op}`);
		}

		if (expected.error) {
			throw new Error(
				`scenario ${step.name} expected error ${expected.error} but succeeded with ${safeStringify(actual)}`
			);
		}
		if ('row' in expected) {
			const normalized = normalizeRowForCompare(tableSpec, actual as Record<string, unknown>);
			assertReturningColumnOrder(step.name, normalized, returningNames);
			expect(normalized, `${step.name} row mismatch`).toEqual(expected.row);
		} else if ('rows' in expected) {
			const normalized = (actual as Record<string, unknown>[]).map((row) =>
				normalizeRowForCompare(tableSpec, row)
			);
			assertReturningColumnOrder(step.name, normalized, returningNames);
			expect(normalized, `${step.name} rows mismatch`).toEqual(expected.rows);
		} else {
			expect(actual, `${step.name} empty result mismatch`).toEqual({});
		}
	} catch (err) {
		if (!expected.error) throw err;
		expect(errorCode(err), `${step.name} error code mismatch`).toBe(expected.error);
	}
}

function runPhase1StateChecks(checks: any[], kit: KitDatabase, schema: Schema): void {
	for (const check of checks) {
		const tableSpec = schema.table(check.table);
		let builder = kit.selectFrom(tableSpec);
		if (check.filter) {
			builder = builder.where(buildPredicate(tableSpec, check.filter));
		}
		const order = check.order ?? '+id';
		builder = builder.orderBy(...buildOrder(tableSpec, order));
		const rows = (builder.executeSync() as Record<string, unknown>[]).map((row) =>
			normalizeRowForCompare(tableSpec, row)
		);
		expect(rows, `state check ${check.table}`).toEqual(check.rows);
	}
}

async function runScenario(scenario: any, expected: any, kit: KitDatabase, schema: Schema): Promise<void> {
	const tableSpec = schema.table(scenario.table);
	if (scenario.row !== undefined) {
		const row = normalizeRowForTs(tableSpec, scenario.row);
		assertOutcome(
			scenario.name,
			() => kit.insertInto(tableSpec).values(row).executeSync(),
			expected,
			(r) => normalizeRowForCompare(tableSpec, r as Record<string, unknown>)
		);
	} else if (scenario.patch !== undefined) {
		const pkCol = tableSpec.columns.find((c) => c.name === tableSpec.primaryKey[0])!;
		const pkValue = isIntColumn(tableSpec, pkCol.name) ? BigInt(scenario.pk) : scenario.pk;
		const patch = normalizeRowForTs(tableSpec, scenario.patch);
		assertOutcome(
			scenario.name,
			() => kit.updateTable(tableSpec).set(patch).where(eq(pkCol, pkValue)).executeSync()[0],
			expected,
			(r) => normalizeRowForCompare(tableSpec, r as Record<string, unknown>)
		);
	} else if (scenario.pk !== undefined && scenario.patch === undefined) {
		const pkCol = tableSpec.columns.find((c) => c.name === tableSpec.primaryKey[0])!;
		const pkValue = isIntColumn(tableSpec, pkCol.name) ? BigInt(scenario.pk) : scenario.pk;
		try {
			kit.deleteFrom(tableSpec).where(eq(pkCol, pkValue)).executeSync();
			if (expected.error) {
				throw new Error(`scenario ${scenario.name} expected error ${expected.error} but delete succeeded`);
			}
			for (const tableName of ['users', 'posts', 'comments']) {
				const t = schema.table(tableName);
				const rows = kit
					.selectFrom(t)
					.orderBy(...buildOrder(t, '+id'))
					.executeSync()
					.map((row) => normalizeRowForCompare(t, row as Record<string, unknown>));
				expect(rows).toEqual(expected[tableName]);
			}
		} catch (err) {
			if (!expected.error) throw err;
			expect(errorCode(err)).toBe(expected.error);
		}
	} else if (scenario.table && (scenario.filter !== undefined || scenario.order !== undefined || scenario.count || scenario.select)) {
		let result: any;
		if (scenario.count) {
			let builder = kit.selectFrom(tableSpec);
			if (scenario.filter) builder = builder.where(buildPredicate(tableSpec, scenario.filter));
			result = { count: Number(builder.selectCount().executeSync()) };
		} else if (scenario.select) {
			let builder = kit.selectFrom(tableSpec);
			if (scenario.filter) builder = builder.where(buildPredicate(tableSpec, scenario.filter));
			if (scenario.order) builder = builder.orderBy(...buildOrder(tableSpec, scenario.order));
			if (scenario.limit !== undefined) builder = builder.limit(scenario.limit);
			if (scenario.offset !== undefined) builder = builder.offset(scenario.offset);
			const cols = scenario.select.map((name: string) => tableSpec.columns.find((c) => c.name === name)!);
			result = { rows: builder.select(cols).executeSync().map((row) => normalizeRowForCompare(tableSpec, row as Record<string, unknown>)) };
		} else {
			let builder = kit.selectFrom(tableSpec);
			if (scenario.filter) builder = builder.where(buildPredicate(tableSpec, scenario.filter));
			if (scenario.order) builder = builder.orderBy(...buildOrder(tableSpec, scenario.order));
			if (scenario.limit !== undefined) builder = builder.limit(scenario.limit);
			if (scenario.offset !== undefined) builder = builder.offset(scenario.offset);
			result = { rows: builder.executeSync().map((row: any) => normalizeRowForCompare(tableSpec, row as Record<string, unknown>)) };
		}
		expect(result).toEqual(expected);
	}
}

function keyComponent(c: any): string | bigint | null {
	if (c.int !== undefined) return BigInt(c.int);
	if (c.text !== undefined) return c.text as string;
	if (c.null !== undefined) return null;
	throw new Error(`invalid key component: ${JSON.stringify(c)}`);
}

/** Aggregate results carry bigints for int64 count/sum/min/max; the shared
 * expected JSON uses plain numbers, so normalize for comparison. */
function normalizeAggRow(row: Record<string, unknown>): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(row)) out[k] = typeof v === 'bigint' ? Number(v) : v;
	return out;
}

/** Sort aggregate result rows by a `+col`/`-col` order key for a deterministic
 * comparison independent of group iteration order. */
function sortAggRows(rows: Record<string, unknown>[], order: string): void {
	const desc = order.startsWith('-');
	const col = order.replace(/^[+-]/, '');
	rows.sort((a, b) => {
		const av = a[col] as any;
		const bv = b[col] as any;
		const cmp = av === bv ? 0 : av < bv ? -1 : 1;
		return desc ? -cmp : cmp;
	});
}

/** Resolve a qualified `table.column` reference inside a join result row; the
 * unmatched (`null`) side of a LEFT join resolves to `null`. */
function joinValueAt(row: Record<string, any>, qualified: string): unknown {
	const [table, col] = qualified.split('.');
	const source = row[table];
	return source == null ? null : source[col];
}

/** Turn the fixture's declarative `{ eq: [{column}, {column}] }` predicate into
 * a `joinEq` predicate so the builder takes its indexed FK-probe fast path. */
function makeJoinOn(schema: Schema, rightTable: TableSpec, on: any): JoinPredicate {
	const parse = (qualified: string): [TableSpec, ColumnSpec] => {
		const [t, col] = qualified.split('.');
		const tspec = schema.table(t);
		return [tspec, tspec.columns.find((c) => c.name === col)!];
	};
	const [t1, c1] = parse(on.eq[0].column);
	const [t2, c2] = parse(on.eq[1].column);
	// The right side is the table being joined; the other side is the left.
	return t1.name === rightTable.name ? joinEq(t2, c2, t1, c1) : joinEq(t1, c1, t2, c2);
}

/** Sort join result rows by qualified `table.column` keys for a deterministic
 * comparison; `null` (unmatched LEFT) sorts first. */
function sortJoinRows(rows: Record<string, any>[], order: string[]): void {
	rows.sort((a, b) => {
		for (const key of order) {
			const av = joinValueAt(a, key);
			const bv = joinValueAt(b, key);
			if (av === bv) continue;
			if (av === null || av === undefined) return -1;
			if (bv === null || bv === undefined) return 1;
			return av < bv ? -1 : 1;
		}
		return 0;
	});
}

/** Build a subquery SelectBuilder from a `{ table, filter?, columns? }` spec. */
function subqueryBuilder(kit: any, schema: Schema, spec: any): any {
	const tspec = schema.table(spec.table);
	let builder: any = kit.selectFrom(tspec);
	if (spec.filter) builder = builder.where(buildPredicate(tspec, spec.filter));
	if (spec.columns) {
		builder = builder.select(
			spec.columns.map((n: string) => tspec.columns.find((c) => c.name === n)!)
		);
	}
	return builder;
}

/** Translate the single-key friendly subquery filter into a TS predicate. */
function buildSubqueryPredicate(kit: any, schema: Schema, outer: TableSpec, filter: any): any {
	const [key, val] = Object.entries(filter)[0] as [string, any];
	if (key === 'exists') return exists(subqueryBuilder(kit, schema, val));
	if (key === 'not_exists') return notExists(subqueryBuilder(kit, schema, val));
	const [op, spec] = Object.entries(val)[0] as [string, any];
	if (op === 'in_subquery') {
		const col = outer.columns.find((c) => c.name === key)!;
		return inSubquery(col, subqueryBuilder(kit, schema, spec));
	}
	throw new Error(`unsupported subquery filter: ${key}.${op}`);
}

describe('mongreldb-kit conformance', () => {
	it('encodes keys byte-identically to the shared vectors', () => {
		const fixture = loadJson('keys.json');
		for (const c of fixture.cases) {
			const comps = c.components.map(keyComponent);
			const pkValue = comps.length === 1 ? comps[0] : comps;
			let actual: string;
			switch (c.kind) {
				case 'pk':
					actual = encodedPk(pkValue);
					break;
				case 'unique':
					actual = encodeUniqueKey(c.version, c.constraint, comps);
					break;
				case 'row_guard':
					actual = encodeRowGuardKey(c.table, pkValue);
					break;
				default:
					throw new Error(`unknown key kind: ${c.kind}`);
			}
			expect(actual, c.name).toBe(c.expected);
		}
	});

	it('rejects a unique-constraint backfill that existing rows violate', async () => {
		const fail = loadJson('migration_failure.json');
		const createSchema = schemaFromFixture(fail.create_schema);
		const accounts = createSchema.table(fail.table);
		const createMigration: Migration = {
			version: fail.create_migration.version,
			name: fail.create_migration.name,
			up: (ctx) => ctx.ensureTable(accounts)
		};
		const failingMigration: Migration = {
			version: fail.failing_migration.version,
			name: fail.failing_migration.name,
			up: async (ctx) => {
				await addUnique(ctx.kit, fail.table, unique(fail.unique.columns, { name: fail.unique.name }));
			}
		};
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-migfail-'));
		const kit = KitDatabase.openSync(tmpDir, createSchema);
		try {
			await migrate(kit, createSchema, [createMigration]);
			for (const s of fail.seed) {
				kit.insertInto(accounts).values(normalizeRowForTs(accounts, s.row)).executeSync();
			}
			await expect(migrate(kit, createSchema, [createMigration, failingMigration])).rejects.toBeInstanceOf(
				KitMigrationError
			);
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs shared fixtures against the TypeScript kit', async () => {
		const schemaRaw = loadJson('schema.json');
		const migrationsRaw = loadJson('migrations.json');
		const inserts = loadJson('inserts.json');
		const updates = loadJson('updates.json');
		const deletes = loadJson('deletes.json');
		const queries = loadJson('queries.json');
		const expected = {
			inserts: loadJson('expected/inserts.json'),
			updates: loadJson('expected/updates.json'),
			deletes: loadJson('expected/deletes.json'),
			queries: loadJson('expected/queries.json')
		};

		const schema = schemaFromFixture(schemaRaw);
		const migrations = migrationsRaw.map((m: any) => migrationFromFixture(m, schema));
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-conformance-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			kit.migrateSync(schema, migrations);

			for (const scenario of inserts) {
				await runScenario(scenario, expected.inserts[scenario.name], kit, schema);
			}
			for (const scenario of updates) {
				await runScenario(scenario, expected.updates[scenario.name], kit, schema);
			}
			for (const scenario of deletes) {
				await runScenario(scenario, expected.deletes[scenario.name], kit, schema);
			}
			for (const scenario of queries) {
				await runScenario(scenario, expected.queries[scenario.name], kit, schema);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs aggregate scenarios against the TypeScript kit', () => {
		const raw = loadJson('aggregates.json');
		const expected = loadJson('expected/aggregates.json');
		const schema = schemaFromFixture(raw.schema);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-agg-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			const seedTable = schema.table(raw.schema.tables[0].name);
			for (const row of raw.rows) {
				kit.insertInto(seedTable).values(normalizeRowForTs(seedTable, row)).executeSync();
			}
			for (const scenario of raw.scenarios) {
				const tspec = schema.table(scenario.table);
				const groupCols = (scenario.group_by ?? []).map((n: string) =>
					tspec.columns.find((c) => c.name === n)!
				);
				const specs: Record<string, any> = {};
				for (const a of scenario.aggregates) {
					const column = a.column ? tspec.columns.find((c) => c.name === a.column) : undefined;
					specs[a.alias] = { fn: a.func, column, distinct: !!a.distinct };
				}
				const rows = kit
					.selectFrom(tspec)
					.groupBy(...groupCols)
					.aggregate(specs)
					.executeSync()
					.map(normalizeAggRow);
				if (scenario.order) sortAggRows(rows, scenario.order);
				expect({ rows }, `aggregate ${scenario.name}`).toEqual(expected[scenario.name]);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs join scenarios against the TypeScript kit', () => {
		const raw = loadJson('joins.json');
		const expected = loadJson('expected/joins.json');
		const schema = schemaFromFixture(raw.schema);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-join-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			for (const [tableName, rows] of Object.entries(raw.seed)) {
				const tspec = schema.table(tableName);
				for (const row of rows as any[]) {
					kit.insertInto(tspec).values(normalizeRowForTs(tspec, row)).executeSync();
				}
			}
			for (const scenario of raw.scenarios) {
				const query = scenario.query;
				// The TS builder keys join rows by table name (no aliases) and takes a
				// JS predicate, so translate the declarative fixture into builder calls.
				let builder: any = kit.selectFrom(schema.table(query.table));
				for (const clause of query.joins) {
					const joined = schema.table(clause.table);
					if (clause.kind === 'cross') {
						builder = builder.crossJoin(joined);
					} else if (clause.kind === 'left') {
						builder = builder.leftJoin(joined, makeJoinOn(schema, joined, clause.on));
					} else {
						builder = builder.innerJoin(joined, makeJoinOn(schema, joined, clause.on));
					}
				}
				const order: string[] = scenario.order ?? [];
				const actual = (builder.executeSync() as any[]).map(normalizeValueForCompare);
				sortJoinRows(actual, order);
				const exp = (expected[scenario.name].rows as any[]).map(normalizeValueForCompare);
				sortJoinRows(exp, order);
				expect(actual, `join ${scenario.name}`).toEqual(exp);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs CTE scenarios against the TypeScript kit', () => {
		const raw = loadJson('ctes.json');
		const expected = loadJson('expected/ctes.json');
		const schema = schemaFromFixture(raw.schema);
		const baseTable = schema.table(raw.schema.tables[0].name);
		const baseNames = new Set<string>(raw.schema.tables.map((t: any) => t.name));
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-cte-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			for (const row of raw.rows) {
				kit.insertInto(baseTable).values(normalizeRowForTs(baseTable, row)).executeSync();
			}
			for (const scenario of raw.scenarios) {
				// Materialize each CTE in order; a later CTE reads an earlier one via
				// the scope. Predicates reuse the base table's specs (CTE sources carry
				// the same columns and evaluate by name).
				let scope: any;
				for (const cte of scenario.ctes) {
					const src = baseNames.has(cte.table)
						? kit.selectFrom(schema.table(cte.table))
						: scope.selectFrom(cte.table);
					const builder = cte.filter ? src.where(buildPredicate(baseTable, cte.filter)) : src;
					scope = scope ? scope.with(cte.name, builder) : kit.with(cte.name, builder);
				}
				const rows = (scope.selectFrom(scenario.body).executeSync() as any[]).map(
					normalizeValueForCompare
				);
				const exp = (expected[scenario.name].rows as any[]).map(normalizeValueForCompare);
				if (scenario.order) {
					sortAggRows(rows, scenario.order);
					sortAggRows(exp, scenario.order);
				}
				expect(rows, `cte ${scenario.name}`).toEqual(exp);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs FM contains scenarios against the TypeScript kit', () => {
		const raw = loadJson('contains.json');
		const expected = loadJson('expected/contains.json');
		const schema = schemaFromFixture(raw.schema);
		const baseTable = schema.table(raw.schema.tables[0].name);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-fm-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			for (const row of raw.rows) {
				kit.insertInto(baseTable).values(normalizeRowForTs(baseTable, row)).executeSync();
			}
			for (const scenario of raw.scenarios) {
				const tspec = schema.table(scenario.table);
				const col = tspec.columns.find((c) => c.name === scenario.column)!;
				const rows = (
					kit.selectFrom(tspec).where(contains(col, scenario.needle)).executeSync() as any[]
				).map((r) => normalizeRowForCompare(tspec, r));
				const exp = (expected[scenario.name].rows as any[]).map((r) =>
					normalizeRowForCompare(tspec, r)
				);
				if (scenario.order) {
					sortAggRows(rows, scenario.order);
					sortAggRows(exp, scenario.order);
				}
				expect(rows, `contains ${scenario.name}`).toEqual(exp);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs ANN scenarios against the TypeScript kit', () => {
		const raw = loadJson('ann.json');
		const schema = schemaFromFixture(raw.schema);
		const baseTable = schema.table(raw.schema.tables[0].name);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-ann-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			for (const row of raw.rows) {
				kit.insertInto(baseTable).values(normalizeRowForTs(baseTable, row)).executeSync();
			}
			for (const scenario of raw.scenarios) {
				const tspec = schema.table(scenario.table);
				const col = tspec.columns.find((c) => c.name === scenario.column)!;
				const rows = kit
					.selectFrom(tspec)
					.annSearch(col, scenario.query, scenario.k)
					.executeSync() as any[];
				const ids = rows.map((r) => Number(r.id)).sort((a, b) => a - b);
				const want = [...(scenario.expect_ids as number[])].sort((a, b) => a - b);
				expect(ids, `ann ${scenario.name}`).toEqual(want);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs LIKE scenarios against the TypeScript kit', () => {
		const raw = loadJson('like.json');
		const expected = loadJson('expected/like.json');
		const schema = schemaFromFixture(raw.schema);
		const baseTable = schema.table(raw.schema.tables[0].name);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-like-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			for (const row of raw.rows) {
				kit.insertInto(baseTable).values(normalizeRowForTs(baseTable, row)).executeSync();
			}
			for (const scenario of raw.scenarios) {
				const tspec = schema.table(scenario.table);
				const rows = (
					kit.selectFrom(tspec).where(buildPredicate(tspec, scenario.filter)).executeSync() as any[]
				).map((r) => normalizeRowForCompare(tspec, r));
				const exp = (expected[scenario.name].rows as any[]).map((r) =>
					normalizeRowForCompare(tspec, r)
				);
				if (scenario.order) {
					sortAggRows(rows, scenario.order);
					sortAggRows(exp, scenario.order);
				}
				expect(rows, `like ${scenario.name}`).toEqual(exp);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs sparse scenarios against the TypeScript kit', () => {
		const raw = loadJson('sparse.json');
		const schema = schemaFromFixture(raw.schema);
		const baseTable = schema.table(raw.schema.tables[0].name);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-sparse-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			for (const row of raw.rows) {
				kit.insertInto(baseTable).values(normalizeRowForTs(baseTable, row)).executeSync();
			}
			for (const scenario of raw.scenarios) {
				const tspec = schema.table(scenario.table);
				const col = tspec.columns.find((c) => c.name === scenario.column)!;
				const rows = kit
					.selectFrom(tspec)
					.sparseMatch(col, scenario.query, scenario.k)
					.executeSync() as any[];
				const ids = rows.map((r) => Number(r.id)).sort((a, b) => a - b);
				const want = [...(scenario.expect_ids as number[])].sort((a, b) => a - b);
				expect(ids, `sparse ${scenario.name}`).toEqual(want);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs encrypted-column scenarios against the TypeScript kit', () => {
		const raw = loadJson('encrypted.json');
		const expected = loadJson('expected/encrypted.json');
		const schema = schemaFromFixture(raw.schema);
		const baseTable = schema.table(raw.schema.tables[0].name);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-enc-'));
		const kit = KitDatabase.openSync(tmpDir, schema, {
			encryption: { passphrase: raw.passphrase }
		});
		try {
			for (const row of raw.rows) {
				kit.insertInto(baseTable).values(normalizeRowForTs(baseTable, row)).executeSync();
			}
			for (const scenario of raw.scenarios) {
				const tspec = schema.table(scenario.table);
				const rows = (
					kit.selectFrom(tspec).where(buildPredicate(tspec, scenario.filter)).executeSync() as any[]
				).map((r) => normalizeRowForCompare(tspec, r));
				const exp = (expected[scenario.name].rows as any[]).map((r) =>
					normalizeRowForCompare(tspec, r)
				);
				if (scenario.order) {
					sortAggRows(rows, scenario.order);
					sortAggRows(exp, scenario.order);
				}
				expect(rows, `encrypted ${scenario.name}`).toEqual(exp);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs null-filter scenarios against the TypeScript kit', () => {
		const raw = loadJson('null_filter.json');
		const expected = loadJson('expected/null_filter.json');
		const schema = schemaFromFixture(raw.schema);
		const baseTable = schema.table(raw.schema.tables[0].name);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-null-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			for (const row of raw.rows) {
				kit.insertInto(baseTable).values(normalizeRowForTs(baseTable, row)).executeSync();
			}
			for (const scenario of raw.scenarios) {
				const tspec = schema.table(scenario.table);
				const rows = (
					kit.selectFrom(tspec).where(buildPredicate(tspec, scenario.filter)).executeSync() as any[]
				).map((r) => normalizeRowForCompare(tspec, r));
				const exp = (expected[scenario.name].rows as any[]).map((r) =>
					normalizeRowForCompare(tspec, r)
				);
				if (scenario.order) {
					sortAggRows(rows, scenario.order);
					sortAggRows(exp, scenario.order);
				}
				expect(rows, `null ${scenario.name}`).toEqual(exp);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs subquery scenarios against the TypeScript kit', () => {
		const raw = loadJson('subqueries.json');
		const expected = loadJson('expected/subqueries.json');
		const schema = schemaFromFixture(raw.schema);
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-subq-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			for (const [tableName, rows] of Object.entries(raw.seed)) {
				const tspec = schema.table(tableName);
				for (const row of rows as any[]) {
					kit.insertInto(tspec).values(normalizeRowForTs(tspec, row)).executeSync();
				}
			}
			for (const scenario of raw.scenarios) {
				const outer = schema.table(scenario.table);
				const predicate = buildSubqueryPredicate(kit, schema, outer, scenario.filter);
				const actual = (kit.selectFrom(outer).where(predicate).executeSync() as any[]).map((r) =>
					normalizeRowForCompare(outer, r)
				);
				const exp = (expected[scenario.name].rows as any[]).map((r) =>
					normalizeRowForCompare(outer, r)
				);
				if (scenario.order) {
					sortAggRows(actual, scenario.order);
					sortAggRows(exp, scenario.order);
				}
				expect(actual, `subquery ${scenario.name}`).toEqual(exp);
			}
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it('runs Phase 1 DML shared fixture', async () => {
		const schemaRaw = loadJson('schema.json');
		const migrationsRaw = loadJson('migrations.json');
		const fixture = loadJson('phase1_dml.json');

		const schema = schemaFromFixture(schemaRaw);
		const migrations = migrationsRaw.map((m: any) => migrationFromFixture(m, schema));
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mongreldb-kit-phase1-'));
		const kit = KitDatabase.openSync(tmpDir, schema);
		try {
			kit.migrateSync(schema, migrations);

			for (const step of fixture.steps) {
				runPhase1Step(step, step.expected, kit, schema);
			}
			runPhase1StateChecks(fixture.state_checks, kit, schema);
		} finally {
			kit.close();
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});
});
