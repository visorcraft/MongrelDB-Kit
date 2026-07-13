import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, bool, real, json, index } from './schema.js';
import { eq, gt, gte } from './query.js';
import { procedure } from './procedure.js';
import { newColumn, textValue, trigger } from './trigger.js';
import { nowDefault, staticDefault } from './defaults.js';

function makeTempDir(): string {
	return mkdtempSync(join(tmpdir(), 'kit-db-test-'));
}

describe('KitDatabase', () => {
	it('passes enum and defaults into the native engine schema', () => {
		const users = table('users', {
			columns: [
				int('id', { primaryKey: true }),
				text('role', { enumValues: ['user', 'admin'] }),
				text('label', { default: staticDefault('new') }),
				text('created_at', { default: nowDefault() })
			],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([users]));
		try {
			const specs = db.nativeDb.tableColumnSpecs('users');
			expect(specs.find((column) => column.name === 'role')?.enumVariants).toEqual([
				'user',
				'admin'
			]);
			expect(specs.find((column) => column.name === 'created_at')?.defaultExpr).toBe('now');
			const nativeUsers = db.nativeDb.table('users');
			nativeUsers.put([
				{ columnId: 1, int64: 1n },
				{ columnId: 2, text: 'user' }
			]);
			nativeUsers.commit();
			const [row] = db.selectFrom(users).executeSync();
			expect(row).toMatchObject({ id: 1n, role: 'user', label: 'new' });
			expect(row.created_at).toEqual(expect.any(String));
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('static-default matrix is reflected in native column specs and applied on insert', () => {
		const t = table('defaults', {
			columns: [
				int('id', { primaryKey: true }),
				text('s', { default: staticDefault('draft') }),
				int('n', { default: staticDefault(7) }),
				bool('b', { default: staticDefault(true) }),
				text('nil', { nullable: true, default: staticDefault(null) }),
				text('literal_now', { default: staticDefault('now') }),
				text('dynamic_now', { default: nowDefault() })
			],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([t]));
		try {
			const specs = db.nativeDb.tableColumnSpecs('defaults');
			const get = (name: string) => specs.find((c) => c.name === name)!;

			expect(get('s').defaultValue?.text).toBe('draft');
			expect(get('n').defaultValue?.int64).toBe(7n);
			expect(get('b').defaultValue?.boolean).toBe(true);

			const nilSpec = get('nil');
			expect(nilSpec.defaultValue).toBeDefined();
			expect(Object.keys(nilSpec.defaultValue!).filter((k) => k !== 'columnId')).toHaveLength(0);

			expect(get('literal_now').defaultValue?.text).toBe('now');
			expect(get('literal_now').defaultExpr).toBeUndefined();
			expect(get('dynamic_now').defaultExpr).toBe('now');
			expect(get('dynamic_now').defaultValue).toBeUndefined();

			const row = db.insertInto(t).values({ id: 1n }).executeSync();
			expect(row.s).toBe('draft');
			expect(row.n).toBe(7n);
			expect(row.b).toBe(true);
			expect(row.nil).toBeNull();
			expect(row.literal_now).toBe('now');
			expect(row.dynamic_now).toEqual(expect.any(String));
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('open creates temp directory, internal tables exist, app table list is empty', async () => {
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([]));
		try {
			expect(db.tableNames()).toEqual([]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('close does not throw', async () => {
		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([]));
		expect(() => db.close()).not.toThrow();
		rmSync(dir, { recursive: true, force: true });
	});

	it('openSync creates temp directory, internal tables exist, app table list is empty', () => {
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([]));
		try {
			expect(db.tableNames()).toEqual([]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('reconciles existing table column ids by name on open', () => {
		const v1 = table('widgets', {
			columns: [int('id', { primaryKey: true }), text('name'), int('count')],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		let db = KitDatabase.openSync(dir, new Schema([v1]));
		try {
			db.insertInto(v1).values({ id: 1n, name: 'adapter', count: 3n }).executeSync();
		} finally {
			db.close();
		}

		const reordered = table('widgets', {
			columns: [int('id', { primaryKey: true }), int('count'), text('name')],
			primaryKey: 'id'
		});
		db = KitDatabase.openSync(dir, new Schema([reordered]));
		try {
			expect(reordered.columns.map((c) => [c.name, c.id])).toEqual([
				['id', 1],
				['count', 3],
				['name', 2]
			]);
			expect(db.selectFrom(reordered).executeSync()).toEqual([
				{ id: 1n, count: 3n, name: 'adapter' }
			]);
		} finally {
			db.close();
		}

		const withInsertedCodeColumn = table('widgets', {
			columns: [
				int('id', { primaryKey: true }),
				text('new_field', { nullable: true }),
				text('name'),
				int('count')
			],
			primaryKey: 'id'
		});
		db = KitDatabase.openSync(dir, new Schema([withInsertedCodeColumn]));
		try {
			expect(withInsertedCodeColumn.columns.map((c) => [c.name, c.id])).toEqual([
				['id', 1],
				['new_field', 4],
				['name', 2],
				['count', 3]
			]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('exportTsv/importTsv round-trips rows across a fresh database', () => {
		const t = table('t', {
			columns: [
				int('id', { primaryKey: true }),
				text('name'),
				real('score', { nullable: true }),
				json('tags', { nullable: true })
			],
			primaryKey: 'id'
		});
		const schema = new Schema([t]);

		const tagsJson = JSON.stringify(['x', 'y']);
		const srcDir = makeTempDir();
		const src = KitDatabase.openSync(srcDir, schema);
		src.insertInto(t).values({ id: 1n, name: 'a\tb\nc', score: 1.5, tags: tagsJson }).executeSync();
		src.insertInto(t).values({ id: 2n, name: 'plain', score: null, tags: null }).executeSync();

		const tsv = src.exportTsv('t');
		// Header + 2 data rows; tab/newline inside the string stay escaped.
		expect(tsv.trimEnd().split('\n')).toHaveLength(3);
		expect(tsv).toContain('a\\tb\\nc');

		const dstDir = makeTempDir();
		const dst = KitDatabase.openSync(dstDir, schema);
		try {
			expect(dst.importTsv('t', tsv)).toBe(2);
			const rows = dst.selectFrom(t).executeSync();
			expect(rows).toEqual([
				{ id: 1n, name: 'a\tb\nc', score: 1.5, tags: tagsJson },
				{ id: 2n, name: 'plain', score: null, tags: null }
			]);
		} finally {
			src.close();
			dst.close();
			rmSync(srcDir, { recursive: true, force: true });
			rmSync(dstDir, { recursive: true, force: true });
		}
	});

	it('rowsAtEpoch reads a table as of a past commit epoch', () => {
		const t = table('t', {
			columns: [int('id', { primaryKey: true }), text('name')],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([t]));
		try {
			db.setHistoryRetentionEpochs(100);
			expect(db.historyRetentionEpochs()).toBe(100n);
			db.insertInto(t).values({ id: 1n, name: 'orig' }).executeSync();
			const e1 = db.snapshotEpoch();
			db.updateTable(t).set({ name: 'updated' }).where(eq(t.id, 1n)).executeSync();

			const past = db.rowsAtEpoch('t', e1);
			expect(past).toEqual([{ id: 1n, name: 'orig' }]);
			const now = db.rowsAtEpoch('t', db.snapshotEpoch());
			expect(now).toEqual([{ id: 1n, name: 'updated' }]);
			expect(db.earliestRetainedEpoch()).toBeLessThanOrEqual(e1);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('approxAggregate estimates a count with a confidence interval', () => {
		const t = table('t', {
			columns: [int('id', { primaryKey: true }), real('val')],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([t]));
		try {
			for (let i = 1n; i <= 300n; i++) {
				db.insertInto(t).values({ id: i, val: Number(i) }).executeSync();
			}
			const res = db.approxAggregate('t', 'count');
			expect(res).not.toBeNull();
			expect(res!.n_population).toBe(300);
			expect(Math.abs(res!.point - 300)).toBeLessThan(1e-6);
			expect(res!.ci_low).toBeLessThanOrEqual(res!.point);
			// sum/avg without a column throws.
			expect(() => db.approxAggregate('t', 'avg')).toThrow();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('scanBatched streams every row in bounded batches', () => {
		const t = table('t', {
			columns: [int('id', { primaryKey: true })],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([t]));
		try {
			for (let i = 1n; i <= 250n; i++) db.insertInto(t).values({ id: i }).executeSync();
			const ids: number[] = [];
			let maxBatch = 0;
			db.scanBatched('t', 100, (rows) => {
				maxBatch = Math.max(maxBatch, rows.length);
				for (const r of rows) ids.push(Number(r.id));
			});
			expect(ids.length).toBe(250);
			expect(maxBatch).toBeLessThanOrEqual(100);
			expect([...ids].sort((a, b) => a - b)).toEqual(
				Array.from({ length: 250 }, (_, i) => i + 1)
			);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	}, 30_000);

	it('reads and batches rows beyond the native 10000-row page', () => {
		const t = table('t', {
			columns: [int('id', { primaryKey: true })],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([t]));
		try {
			db.insertInto(t)
				.valuesMany(Array.from({ length: 10_050 }, (_, i) => ({ id: BigInt(i + 1) })))
				.executeSync();
			expect(db.selectFrom(t).limit(25).offset(10_000).executeSync()).toHaveLength(25);
			expect(
				db.selectFrom(t).where(gte(t.id, 1n)).limit(25).offset(10_000).executeSync()
			).toHaveLength(25);
			let count = 0;
			db.scanBatched('t', 4_000, (rows) => {
				count += rows.length;
			});
			expect(count).toBe(10_050);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	}, 30_000);

	it('setSimilarity ranks rows by Jaccard set-similarity', () => {
		const t = table('t', {
			columns: [int('id', { primaryKey: true }), json('tags')],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([t]));
		try {
			db.insertInto(t).values({ id: 1n, tags: JSON.stringify(['a', 'b', 'c']) }).executeSync();
			db.insertInto(t).values({ id: 2n, tags: JSON.stringify(['a', 'b', 'x', 'y']) }).executeSync();
			db.insertInto(t).values({ id: 3n, tags: JSON.stringify(['z']) }).executeSync();

			const hits = db.setSimilarity('t', 'tags', ['a', 'b', 'c'], 10);
			expect(hits.map((h) => Number(h.row.id))).toEqual([1, 2]); // row 3 excluded
			expect(hits[0].similarity).toBeCloseTo(1.0, 9);
			expect(hits[1].similarity).toBeCloseTo(0.4, 9); // |{a,b}| / |{a,b,c,x,y}|

			expect(db.setSimilarity('t', 'tags', ['a', 'b', 'c'], 1)).toHaveLength(1);
			expect(() => db.setSimilarity('t', 'missing', ['a'], 1)).toThrow();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('stored procedure installs and calls through TypeScript Kit', () => {
		const users = table('users', {
			columns: [int('id', { primaryKey: true }), text('status')],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([users]));
		try {
			db.insertInto(users).values({ id: 1n, status: 'active' }).executeSync();
			db.createProcedureSync(
				procedure({
					name: 'read_users',
					mode: 'read_only',
					body: {
						steps: [
							{
								kind: 'native_query',
								id: 'read',
								table: 'users',
								conditions: [],
								projection: [1, 2],
								limit: 10
							}
						],
						return_value: { kind: 'step_rows', value: 'read' }
					}
				})
			);

			const result = db.callProcedureSync('read_users').result as any;
			expect(result.kind).toBe('rows');
			expect(result.value).toHaveLength(1);
			expect(result.value[0].columns['1'].Int64).toBe(1);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('trigger installs through TypeScript Kit and fires on Kit writes', () => {
		const users = table('users', {
			columns: [int('id', { primaryKey: true }), text('name')],
			primaryKey: 'id'
		});
		const audit = table('audit', {
			columns: [int('id', { primaryKey: true }), int('user_id'), text('note')],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([users, audit]));
		try {
			db.createTriggerSync(
				trigger({
					name: 'users_ai',
					target: { kind: 'table', name: 'users' },
					timing: 'after',
					event: 'insert',
					program: {
						steps: [
							{
								kind: 'insert',
								table: 'audit',
								cells: [
									{ column_id: audit.id.id, value: newColumn(users.id.id) },
									{ column_id: audit.user_id.id, value: newColumn(users.id.id) },
									{ column_id: audit.note.id, value: textValue('created') }
								]
							}
						]
					}
				})
			);

			db.insertInto(users).values({ id: 7n, name: 'ada' }).executeSync();
			expect(db.triggers().map((t) => t.name)).toEqual(['users_ai']);
			expect(db.trigger('users_ai')?.event).toBe('insert');
			expect(db.selectFrom(audit).executeSync()).toEqual([
				{ id: 7n, user_id: 7n, note: 'created' }
			]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('incrementalAggregate returns exact count/sum/avg, with an optional filter', () => {
		const t = table('t', {
			columns: [int('id', { primaryKey: true }), real('amount')],
			primaryKey: 'id'
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([t]));
		try {
			for (let i = 1n; i <= 50n; i++) {
				db.insertInto(t).values({ id: i, amount: Number(i) }).executeSync();
			}
			db.flush();
			expect(db.incrementalAggregate('t', 'count').value).toBe(50);
			expect(db.incrementalAggregate('t', 'sum', 'amount').value).toBe(1275); // 1+..+50
			expect(db.incrementalAggregate('t', 'avg', 'amount').value).toBe(25.5);
			// Exact filter: amount >= 46 -> {46..50} (gte on a float has no residual).
			expect(db.incrementalAggregate('t', 'count', undefined, gte(t.amount, 46)).value).toBe(5);
			// An inexact filter (float `>` needs a residual) is rejected.
			expect(() => db.incrementalAggregate('t', 'count', undefined, gt(t.amount, 45))).toThrow();
			// sum without a column throws.
			expect(() => db.incrementalAggregate('t', 'sum')).toThrow();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('setSimilarity uses a MinHash index and re-verifies exactly', () => {
		const t = table('t', {
			columns: [int('id', { primaryKey: true }), json('tags')],
			primaryKey: 'id',
			indexes: [index(['tags'], { minhash: true })]
		});
		const dir = makeTempDir();
		const db = KitDatabase.openSync(dir, new Schema([t]));
		try {
			const identical = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
			const near = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'x'];
			const disjoint = ['p', 'q', 'r', 's', 't', 'u', 'v', 'w'];
			db.insertInto(t).values({ id: 1n, tags: JSON.stringify(identical) }).executeSync();
			db.insertInto(t).values({ id: 2n, tags: JSON.stringify(near) }).executeSync();
			db.insertInto(t).values({ id: 3n, tags: JSON.stringify(disjoint) }).executeSync();

			const hits = db.setSimilarity('t', 'tags', identical, 10);
			const ids = hits.map((h) => Number(h.row.id));
			expect(ids).toContain(1); // identical recalled
			expect(ids).toContain(2); // high-Jaccard recalled
			expect(ids).not.toContain(3); // disjoint excluded
			expect(hits[0].row.id).toBe(1n);
			expect(hits[0].similarity).toBeCloseTo(1.0, 9);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	describe('encrypted databases', () => {
		const makeSchema = () => {
			const secrets = table('secrets', {
				columns: [int('id', { primaryKey: true }), text('value')],
				primaryKey: 'id'
			});
			return { schema: new Schema([secrets]), secrets };
		};

		it('createEncryptedSync round-trips rows and reopens with the same passphrase', () => {
			const dir = makeTempDir();
			const { schema, secrets } = makeSchema();
			const db = KitDatabase.createEncryptedSync(dir, schema, 'kit-test-passphrase');
			try {
				db.insertInto(secrets).values({ id: 1n, value: 'hello' }).executeSync();
				db.insertInto(secrets).values({ id: 2n, value: 'world' }).executeSync();
				expect(db.selectFrom(secrets).executeSync().map((r) => r.value)).toEqual([
					'hello',
					'world'
				]);
			} finally {
				db.close();
			}

			const reopened = KitDatabase.openEncryptedSync(dir, schema, 'kit-test-passphrase');
			try {
				const rows = reopened
					.selectFrom(secrets)
					.executeSync()
					.map((r) => ({ id: r.id, value: r.value }));
				expect(rows).toEqual([
					{ id: 1n, value: 'hello' },
					{ id: 2n, value: 'world' }
				]);
			} finally {
				reopened.close();
				rmSync(dir, { recursive: true, force: true });
			}
		});

		it('openEncryptedSync rejects the wrong passphrase', () => {
			const dir = makeTempDir();
			const { schema } = makeSchema();
			const db = KitDatabase.createEncryptedSync(dir, schema, 'correct-passphrase');
			db.close();
			try {
				expect(() =>
					KitDatabase.openEncryptedSync(dir, schema, 'wrong-passphrase')
				).toThrow();
			} finally {
				rmSync(dir, { recursive: true, force: true });
			}
		});

		it('openSync with encryption option creates an encrypted database and reopens it', () => {
			const dir = makeTempDir();
			const { schema, secrets } = makeSchema();
			const db = KitDatabase.openSync(dir, schema, {
				encryption: { passphrase: 'option-passphrase' }
			});
			try {
				db.insertInto(secrets).values({ id: 3n, value: 'option' }).executeSync();
			} finally {
				db.close();
			}

			const reopened = KitDatabase.openEncryptedSync(dir, schema, 'option-passphrase');
			try {
				const row = reopened
					.selectFrom(secrets)
					.where(eq(secrets.id, 3n))
					.executeSync()[0];
				expect(row?.value).toBe('option');
			} finally {
				reopened.close();
				rmSync(dir, { recursive: true, force: true });
			}
		});
	});
});
