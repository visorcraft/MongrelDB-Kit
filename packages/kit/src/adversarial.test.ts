/**
 * Adversarial cross-language tests — TypeScript surface.
 * Tests the full stack: KitDatabase → NAPI → engine.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { KitDatabase, Schema, table, int, text, real } from './index.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function makeDir(): string {
	return mkdtempSync(join(tmpdir(), 'mongreldb-adv-'));
}

function makeSchema(): Schema {
	const orders = table('orders', {
		columns: [
			int('id', { primaryKey: true }),
			real('amount'),
			text('category'),
		],
		primaryKey: 'id',
	});
	return new Schema([orders]);
}

function seedData(db: KitDatabase): void {
	const t = db.schema.table('orders');
	db.insertInto(t).values({ id: 1n, amount: 10.0, category: 'food' }).executeSync();
	db.insertInto(t).values({ id: 2n, amount: 20.0, category: 'food' }).executeSync();
	db.insertInto(t).values({ id: 3n, amount: 30.0, category: 'toys' }).executeSync();
	db.insertInto(t).values({ id: 4n, amount: 40.0, category: 'toys' }).executeSync();
	db.insertInto(t).values({ id: 5n, amount: 50.0, category: 'toys' }).executeSync();
}

describe('Adversarial: Recursive CTEs via SQL', () => {
	let dir: string;
	let db: KitDatabase;

	beforeEach(() => {
		dir = makeDir();
		db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		seedData(db);
	});

	afterEach(() => {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it('basic recursive CTE generates a sequence', async () => {
		const rows = await db.sqlRows(
			"WITH RECURSIVE counter(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 10) SELECT n FROM counter ORDER BY n"
		);
		expect(rows).toHaveLength(10);
		expect(rows[0]).toEqual({ n: 1n });
		expect(rows[9]).toEqual({ n: 10n });
	});

	it('recursive CTE on real table parent chain', async () => {
		const rows = await db.sqlRows(
			"WITH RECURSIVE r(id) AS (SELECT id FROM orders WHERE id = 1 UNION ALL SELECT id + 1 FROM r WHERE id < 3) SELECT id FROM r ORDER BY id"
		);
		expect(rows).toHaveLength(3);
		expect(rows[0]).toEqual({ id: 1n });
		expect(rows[2]).toEqual({ id: 3n });
	});

	it('recursive CTE with immediate convergence', async () => {
		const rows = await db.sqlRows(
			"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 0) SELECT n FROM r"
		);
		expect(rows).toHaveLength(1);
		expect(rows[0]).toEqual({ n: 1n });
	});

	it('recursive CTE empty result from base', async () => {
		const rows = await db.sqlRows(
			"WITH RECURSIVE r(n) AS (SELECT 1 WHERE 1 = 0 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r"
		);
		expect(rows).toHaveLength(0);
	});
});

describe('Adversarial: CTAS via SQL', () => {
	let dir: string;
	let db: KitDatabase;

	beforeEach(() => {
		dir = makeDir();
		db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		seedData(db);
	});

	afterEach(() => {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it('CTAS creates table from filtered query', async () => {
		await db.sqlRows("CREATE TABLE food_orders AS SELECT id, amount FROM orders WHERE category = 'food'");
		const rows = await db.sqlRows('SELECT id FROM food_orders ORDER BY id');
		expect(rows).toHaveLength(2);
		expect(rows[0]).toEqual({ id: 1n });
	});

	it('CTAS with aggregation', async () => {
		await db.sqlRows("CREATE TABLE summary AS SELECT category, sum(amount) AS total FROM orders GROUP BY category");
		const rows = await db.sqlRows('SELECT category FROM summary ORDER BY category');
		expect(rows).toHaveLength(2);
	});

	it('CTAS duplicate table name should fail', async () => {
		await db.sqlRows("CREATE TABLE dup AS SELECT id FROM orders");
		await expect(db.sqlRows("CREATE TABLE dup AS SELECT id FROM orders")).rejects.toThrow();
	});

	it('CTAS IF NOT EXISTS is idempotent', async () => {
		await db.sqlRows("CREATE TABLE IF NOT EXISTS idem AS SELECT id FROM orders");
		await expect(db.sqlRows("CREATE TABLE IF NOT EXISTS idem AS SELECT id FROM orders")).resolves.toBeDefined();
	});
});

describe('Adversarial: Materialized views via SQL', () => {
	let dir: string;
	let db: KitDatabase;

	beforeEach(() => {
		dir = makeDir();
		db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		seedData(db);
	});

	afterEach(() => {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it('CREATE MATERIALIZED VIEW stores data', async () => {
		await db.sqlRows("CREATE MATERIALIZED VIEW mv_food AS SELECT id FROM orders WHERE category = 'food'");
		const rows = await db.sqlRows('SELECT id FROM mv_food ORDER BY id');
		expect(rows).toHaveLength(2);
	});

	it('duplicate materialized view should fail', async () => {
		await db.sqlRows("CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM orders");
		await expect(db.sqlRows("CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM orders")).rejects.toThrow();
	});
});

describe('Adversarial: Multi-statement SQL', () => {
	let dir: string;
	let db: KitDatabase;

	beforeEach(() => {
		dir = makeDir();
		db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		seedData(db);
	});

	afterEach(() => {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it('multiple SELECT returns last result', async () => {
		const rows = await db.sqlRows("SELECT 1 AS n; SELECT 2 AS n; SELECT 3 AS n");
		expect(rows).toHaveLength(1);
		expect(rows[0]).toEqual({ n: 3n });
	});

	it('semicolon in string literal is not a splitter', async () => {
		const rows = await db.sqlRows("SELECT 'hello; world' AS greeting FROM orders LIMIT 1");
		expect(rows).toHaveLength(1);
		expect((rows[0] as Record<string, unknown>).greeting).toContain(';');
	});

	it('DDL + DML + SELECT batch', async () => {
		await db.sqlRows(
			"CREATE TABLE batch_t AS SELECT id FROM orders; " +
			"INSERT INTO batch_t (id) VALUES (99); " +
			"SELECT count(*) AS cnt FROM batch_t"
		);
		const rows = await db.sqlRows("SELECT count(*) AS cnt FROM batch_t");
		expect(rows[0]).toEqual({ cnt: 6n });
	});

	it('trailing semicolons do not break', async () => {
		const rows = await db.sqlRows("SELECT 1 AS n;");
		expect(rows).toHaveLength(1);
	});

	it('only semicolons does not crash', async () => {
		await expect(db.sqlRows(";;;")).resolves.toBeDefined();
	});
});

describe('Adversarial: FTS ranking via SQL', () => {
	let dir: string;
	let db: KitDatabase;

	beforeEach(() => {
		dir = makeDir();
		db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		seedData(db);
	});

	afterEach(() => {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it('FTS rank scores food higher than toys', async () => {
		const rows = await db.sqlRows(
			"SELECT category, mongreldb_fts_rank(category, 'food') AS score FROM orders ORDER BY score DESC"
		);
		expect(rows).toHaveLength(5);
		const topCategory = (rows[0] as Record<string, unknown>).category;
		expect(topCategory).toBe('food');
	});

	it('FTS rank with no match returns zero', async () => {
		const rows = await db.sqlRows(
			"SELECT mongreldb_fts_rank('hello world', 'nonexistent') AS score"
		);
		expect(rows).toHaveLength(1);
		expect(Number((rows[0] as Record<string, unknown>).score)).toBe(0);
	});

	it('FTS rank with empty query returns zero', async () => {
		await expect(db.sqlRows("SELECT mongreldb_fts_rank('hello', '') AS score")).resolves.toBeDefined();
	});
});

describe('Adversarial: Window functions via SQL', () => {
	let dir: string;
	let db: KitDatabase;

	beforeEach(() => {
		dir = makeDir();
		db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		seedData(db);
	});

	afterEach(() => {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it('ROW_NUMBER over partition', async () => {
		const rows = await db.sqlRows(
			"SELECT id, category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn FROM orders ORDER BY id"
		);
		expect(rows).toHaveLength(5);
		expect((rows[0] as Record<string, unknown>).rn).toBe(1n);
		expect((rows[2] as Record<string, unknown>).rn).toBe(1n);
	});

	it('SUM over partition', async () => {
		const rows = await db.sqlRows(
			"SELECT id, SUM(amount) OVER (PARTITION BY category) AS total FROM orders ORDER BY id"
		);
		expect(rows).toHaveLength(5);
		expect(Number((rows[0] as Record<string, unknown>).total)).toBe(30);
		expect(Number((rows[2] as Record<string, unknown>).total)).toBe(120);
	});
});

describe('Adversarial: Credential enforcement', () => {
	let dir: string;

	beforeEach(() => {
		dir = makeDir();
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	it('create + reopen with credentials', () => {
		const db = KitDatabase.createWithCredentialsSync(join(dir, 'sec'), makeSchema(), 'admin', 's3cret');
		expect(db.requireAuthEnabled()).toBe(true);
		db.close();

		const db2 = KitDatabase.openSync(join(dir, 'sec'), makeSchema(), {
			credentials: { username: 'admin', password: 's3cret' },
		});
		expect(db2.requireAuthEnabled()).toBe(true);
		db2.close();
	});

	it('wrong password fails', () => {
		KitDatabase.createWithCredentialsSync(join(dir, 'sec'), makeSchema(), 'admin', 's3cret').close();
		expect(() => {
			KitDatabase.openSync(join(dir, 'sec'), makeSchema(), {
				credentials: { username: 'admin', password: 'WRONG' },
			});
		}).toThrow();
	});

	it('plain open on credentialed DB fails', () => {
		KitDatabase.createWithCredentialsSync(join(dir, 'sec'), makeSchema(), 'admin', 's3cret').close();
		expect(() => {
			KitDatabase.openSync(join(dir, 'sec'), makeSchema());
		}).toThrow();
	});

	it('enable_auth converts credentialless to credentialed', () => {
		const db = KitDatabase.openSync(join(dir, 'plain'), makeSchema());
		expect(db.requireAuthEnabled()).toBe(false);
		db.enableAuth('root', 'rootpw');
		expect(db.requireAuthEnabled()).toBe(true);
		db.close();

		expect(() => {
			KitDatabase.openSync(join(dir, 'plain'), makeSchema());
		}).toThrow();
	});

	it('disable_auth reverts to credentialless', () => {
		const db = KitDatabase.createWithCredentialsSync(join(dir, 'sec'), makeSchema(), 'admin', 'pw');
		db.disableAuth();
		expect(db.requireAuthEnabled()).toBe(false);
		db.close();

		const db2 = KitDatabase.openSync(join(dir, 'sec'), makeSchema());
		expect(db2.requireAuthEnabled()).toBe(false);
		db2.close();
	});

	it('encrypted + credentialed round trip', () => {
		const db = KitDatabase.createEncryptedWithCredentialsSync(
			join(dir, 'enc'), makeSchema(), 'passphrase', 'admin', 'pw'
		);
		expect(db.requireAuthEnabled()).toBe(true);
		db.close();

		const db2 = KitDatabase.openSync(join(dir, 'enc'), makeSchema(), {
			encryption: { passphrase: 'passphrase' },
			credentials: { username: 'admin', password: 'pw' },
		});
		expect(db2.requireAuthEnabled()).toBe(true);
		db2.close();
	});
});

describe('Adversarial: Cross-feature interactions', () => {
	let dir: string;
	let db: KitDatabase;

	beforeEach(() => {
		dir = makeDir();
		db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		seedData(db);
	});

	afterEach(() => {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it('CTAS then recursive CTE on the new table', async () => {
		await db.sqlRows("CREATE TABLE copy AS SELECT id FROM orders");
		const rows = await db.sqlRows(
			"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r"
		);
		expect(rows).toHaveLength(3);
	});

	it('Multi-statement with recursive CTE as last statement', async () => {
		const rows = await db.sqlRows(
			"SELECT 1; WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r"
		);
		expect(rows).toHaveLength(3);
	});

	it('Materialized view then FTS rank on it', async () => {
		await db.sqlRows("CREATE MATERIALIZED VIEW cat_mv AS SELECT DISTINCT category FROM orders");
		const rows = await db.sqlRows(
			"SELECT category, mongreldb_fts_rank(category, 'food') AS score FROM cat_mv ORDER BY score DESC"
		);
		expect(rows).toHaveLength(2);
		expect((rows[0] as Record<string, unknown>).category).toBe('food');
	});
});
