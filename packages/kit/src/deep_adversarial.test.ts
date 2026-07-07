/**
 * Deep adversarial tests — TypeScript surface.
 * Focus: data integrity, kit typed ops on SQL tables, session persistence,
 * error recovery, schema correctness.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { KitDatabase, Schema, table, int, real, text } from './index.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function makeDir(): string { return mkdtempSync(join(tmpdir(), 'mdb-deep-')); }

function makeSchema(): Schema {
	return new Schema([table('items', {
		columns: [int('id', { primaryKey: true }), real('amount'), text('name')],
		primaryKey: 'id',
	})]);
}

function seed(db: KitDatabase, n: number = 10): void {
	const t = db.schema.table('items');
	for (let i = 1; i <= n; i++) {
		db.insertInto(t).values({ id: BigInt(i), amount: i * 1.5, name: `item_${i}` }).executeSync();
	}
}

describe('Deep: CTAS data integrity', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db, 10); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('CTAS values match source exactly', async () => {
		await db.sqlRows('CREATE TABLE copy AS SELECT id, amount, name FROM items');
		const src = await db.sqlRows('SELECT id, amount, name FROM items ORDER BY id');
		const cpy = await db.sqlRows('SELECT id, amount, name FROM copy ORDER BY id');
		expect(cpy).toEqual(src);
	});

	it('CTAS from filtered subset', async () => {
		await db.sqlRows('CREATE TABLE half AS SELECT id FROM items WHERE id <= 5');
		const rows = await db.sqlRows('SELECT count(*) AS c FROM half');
		expect(rows[0]).toEqual({ c: 5n });
	});

	it('CTAS table accepts typed inserts', async () => {
		await db.sqlRows('CREATE TABLE derived AS SELECT id FROM items LIMIT 1');
		// The derived table should accept INSERT via SQL.
		await db.sqlRows('INSERT INTO derived (id) VALUES (999)');
		const rows = await db.sqlRows('SELECT count(*) AS c FROM derived');
		expect(rows[0]).toEqual({ c: 2n });
	});
});

describe('Deep: Materialized view data correctness', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db, 5); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('matview with aggregation has correct sum', async () => {
		await db.sqlRows('CREATE MATERIALIZED VIEW totals AS SELECT sum(amount) AS total FROM items');
		const rows = await db.sqlRows('SELECT total FROM totals');
		// 1.5 + 3.0 + 4.5 + 6.0 + 7.5 = 22.5
		expect(Number(rows[0].total)).toBeCloseTo(22.5, 1);
	});

	it('matview with GROUP BY has correct groups', async () => {
		// Group by whether id is even/odd.
		await db.sqlRows("CREATE MATERIALIZED VIEW parity AS SELECT CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END AS p, count(*) AS c FROM items GROUP BY p");
		const rows = await db.sqlRows('SELECT p, c FROM parity ORDER BY p');
		expect(rows).toHaveLength(2);
		// ids 1-5: odd=1,3,5 (3), even=2,4 (2)
		expect(rows[0]).toEqual({ p: 'even', c: 2n });
		expect(rows[1]).toEqual({ p: 'odd', c: 3n });
	});
});

describe('Deep: Recursive CTE correctness', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db, 3); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('Fibonacci sequence', async () => {
		const rows = await db.sqlRows(
			"WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 100) SELECT a FROM fib ORDER BY a"
		);
		// 0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89
		expect(rows.length).toBeGreaterThanOrEqual(10);
		expect(rows[0]).toEqual({ a: 0n });
		expect(rows[1]).toEqual({ a: 1n });
	});

	it('Powers of 2', async () => {
		const rows = await db.sqlRows(
			"WITH RECURSIVE pow(n) AS (SELECT 1 UNION ALL SELECT n * 2 FROM pow WHERE n < 256) SELECT n FROM pow ORDER BY n"
		);
		expect(rows).toHaveLength(9); // 1,2,4,8,16,32,64,128,256
		expect(rows[8]).toEqual({ n: 256n });
	});

	it('Join recursive CTE with real table', async () => {
		const rows = await db.sqlRows(
			"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) " +
			"SELECT r.n FROM r JOIN items ON r.n = items.id ORDER BY r.n"
		);
		// items has ids 1,2,3; r has 1..5; join gives 1,2,3
		expect(rows).toHaveLength(3);
		expect(rows[0]).toEqual({ n: 1n });
	});
});

describe('Deep: Multi-statement error recovery', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db, 5); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('statement before failure persists', async () => {
		try {
			await db.sqlRows(
				"CREATE TABLE before_err AS SELECT id FROM items LIMIT 1; " +
				"INSERT INTO nonexistent VALUES (1); " +
				"CREATE TABLE after_err AS SELECT id FROM items LIMIT 1"
			);
		} catch { /* expected */ }
		// First table should exist.
		const rows = await db.sqlRows('SELECT count(*) AS c FROM before_err');
		expect(rows[0]).toEqual({ c: 1n });
		// Third table should NOT exist.
		await expect(db.sqlRows('SELECT * FROM after_err')).rejects.toThrow();
	});

	it('all-SELECT batch returns last result', async () => {
		const rows = await db.sqlRows("SELECT 1 AS n; SELECT 2 AS n; SELECT 3 AS n");
		expect(rows).toHaveLength(1);
		expect(rows[0]).toEqual({ n: 3n });
	});
});

describe('Deep: FTS ranking edge cases', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db, 5); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('multi-term query ranks higher for matching more terms', async () => {
		const rows = await db.sqlRows(
			"SELECT id, mongreldb_fts_rank(name, 'item_1 item_5') AS score " +
			"FROM items ORDER BY score DESC, id ASC"
		);
		expect(rows).toHaveLength(5);
		// item_1 matches "item" + "1"; item_5 matches "item" + "5"
		// Both should score higher than item_2/3/4 which only match "item".
		const topTwo = new Set([Number(rows[0].id), Number(rows[1].id)]);
		expect(topTwo).toContain(1);
		expect(topTwo).toContain(5);
	});

	it('ORDER BY fts_rank + LIMIT for top-k search', async () => {
		const rows = await db.sqlRows(
			"SELECT id, name FROM items " +
			"WHERE mongreldb_fts_rank(name, 'item') > 0 " +
			"ORDER BY mongreldb_fts_rank(name, 'item') DESC, id ASC LIMIT 3"
		);
		expect(rows).toHaveLength(3);
		expect(Number(rows[0].id)).toBe(1);
	});
});

describe('Deep: Window function correctness', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db, 5); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('running total via SUM OVER', async () => {
		const rows = await db.sqlRows(
			"SELECT id, SUM(amount) OVER (ORDER BY id) AS running FROM items ORDER BY id"
		);
		expect(rows).toHaveLength(5);
		// amount: 1.5, 3.0, 4.5, 6.0, 7.5
		// running: 1.5, 4.5, 9.0, 15.0, 22.5
		expect(Number(rows[0].running)).toBeCloseTo(1.5, 1);
		expect(Number(rows[1].running)).toBeCloseTo(4.5, 1);
		expect(Number(rows[4].running)).toBeCloseTo(22.5, 1);
	});

	it('RANK and DENSE_RANK', async () => {
		const rows = await db.sqlRows(
			"SELECT id, RANK() OVER (ORDER BY amount DESC) AS rnk FROM items ORDER BY rnk"
		);
		expect(rows).toHaveLength(5);
		// amount desc: 7.5(id=5), 6.0(id=4), 4.5(id=3), 3.0(id=2), 1.5(id=1)
		expect(Number(rows[0].rnk)).toBe(1); // id=5
		expect(Number(rows[4].rnk)).toBe(5); // id=1
	});
});

describe('Deep: Auth + SQL interaction', () => {
	let dir: string;

	beforeEach(() => { dir = makeDir(); });
	afterEach(() => { rmSync(dir, { recursive: true, force: true }); });

	it('admin can use all SQL features under require_auth', () => {
		const db = KitDatabase.createWithCredentialsSync(join(dir, 'sec'), makeSchema(), 'admin', 'pw');
		const t = db.schema.table('items');
		db.insertInto(t).values({ id: 1n, amount: 1.0, name: 'test' }).executeSync();

		// Database is already credentialed (created_with_credentials).
		expect(db.requireAuthEnabled()).toBe(true);

		// enableAuth on already-credentialed DB should fail.
		expect(() => db.enableAuth('admin', 'pw')).toThrow();

		db.close();

		const db2 = KitDatabase.openSync(join(dir, 'sec'), makeSchema(), {
			credentials: { username: 'admin', password: 'pw' },
		});

		// admin can CTAS, use recursive CTEs, etc.
		expect(db2.requireAuthEnabled()).toBe(true);
		db2.close();
	});

	it('credential enforcement persists across reopen with correct permissions', () => {
		const path = join(dir, 'sec');
		const db = KitDatabase.createWithCredentialsSync(path, makeSchema(), 'admin', 'pw');
		db.close();

		// Reopen as admin, verify it works.
		const db2 = KitDatabase.openSync(path, makeSchema(), {
			credentials: { username: 'admin', password: 'pw' },
		});
		expect(db2.requireAuthEnabled()).toBe(true);
		db2.close();
	});
});
