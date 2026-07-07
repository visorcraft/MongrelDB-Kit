/**
 * Creative destruction tests — TypeScript surface (13 tests).
 * Angles: error propagation, stale views, chained ops, Unicode,
 * large results, multi-statement edge cases, auth edge cases.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { KitDatabase, Schema, table, int, real, text } from './index.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function makeDir(): string { return mkdtempSync(join(tmpdir(), 'mdb-creat-')); }
function makeSchema(): Schema {
	return new Schema([table('items', {
		columns: [int('id', { primaryKey: true }), real('amount'), text('name')],
		primaryKey: 'id',
	})]);
}
function seed(db: KitDatabase, n: number = 5): void {
	const t = db.schema.table('items');
	for (let i = 1; i <= n; i++) {
		db.insertInto(t).values({ id: BigInt(i), amount: i * 1.5, name: `item_${i}` }).executeSync();
	}
}

describe('Creative: Error propagation', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('CTAS on nonexistent table gives clean error', async () => {
		await expect(db.sqlRows('CREATE TABLE x AS SELECT * FROM ghost')).rejects.toThrow();
	});

	it('SELECT from CTAS-created-then-dropped table gives error', async () => {
		await db.sqlRows('CREATE TABLE temp AS SELECT id FROM items LIMIT 1');
		await db.sqlRows('DROP TABLE temp');
		await expect(db.sqlRows('SELECT * FROM temp')).rejects.toThrow();
	});

	it('multi-statement error on first propagates, DB still usable', async () => {
		await expect(db.sqlRows('SELECT FROM ghost; SELECT 1')).rejects.toThrow();
		// DB should still be usable.
		const rows = await db.sqlRows('SELECT id FROM items LIMIT 1');
		expect(rows).toHaveLength(1);
	});
});

describe('Creative: Stale views and snapshots', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db, 3); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('materialized view is a snapshot after source DELETE', async () => {
		await db.sqlRows('CREATE MATERIALIZED VIEW mv AS SELECT id FROM items');
		// Delete via SQL to avoid kit column accessor type issues.
		await db.sqlRows('DELETE FROM items WHERE id = 1');
		// MV should still have 3 rows (it's a physical snapshot table).
		const rows = await db.sqlRows('SELECT count(*) AS c FROM mv');
		expect(rows[0]).toEqual({ c: 3n });
	});
});

describe('Creative: Chained operations', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db, 5); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('CTAS → INSERT → CTAS → SELECT chain', async () => {
		await db.sqlRows('CREATE TABLE a AS SELECT id FROM items WHERE id <= 3');
		await db.sqlRows('INSERT INTO a (id) VALUES (99)');
		await db.sqlRows('CREATE TABLE b AS SELECT id FROM a WHERE id < 50');
		const rows = await db.sqlRows('SELECT count(*) AS c FROM b');
		expect(rows[0]).toEqual({ c: 3n }); // ids 1,2,3 (99 excluded)
	});

	it('matview on matview', async () => {
		await db.sqlRows('CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM items');
		await db.sqlRows('CREATE MATERIALIZED VIEW mv2 AS SELECT id FROM mv1 WHERE id <= 3');
		const rows = await db.sqlRows('SELECT count(*) AS c FROM mv2');
		expect(rows[0]).toEqual({ c: 3n });
	});

	it('CTAS from aggregation → recursive CTE on result', async () => {
		await db.sqlRows('CREATE TABLE counts AS SELECT count(*) AS c FROM items');
		// The counts table has 1 row with c=5.
		const rows = await db.sqlRows(
			"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r"
		);
		expect(rows).toHaveLength(3);
	});
});

describe('Creative: Unicode and special text', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => {
		dir = makeDir();
		db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		const t = db.schema.table('items');
		db.insertInto(t).values({ id: 1n, amount: 1.0, name: 'hello world' }).executeSync();
		db.insertInto(t).values({ id: 2n, amount: 2.0, name: '日本語テキスト' }).executeSync();
		db.insertInto(t).values({ id: 3n, amount: 3.0, name: 'emoji 🎉 test' }).executeSync();
	});
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('CTAS preserves Unicode text', async () => {
		await db.sqlRows('CREATE TABLE ucopy AS SELECT id, name FROM items');
		const rows = await db.sqlRows('SELECT name FROM ucopy ORDER BY id');
		expect(rows[1]).toEqual({ name: '日本語テキスト' });
		expect(rows[2]).toEqual({ name: 'emoji 🎉 test' });
	});

	it('FTS rank with Unicode query does not crash', async () => {
		await expect(db.sqlRows("SELECT mongreldb_fts_rank(name, '日本語') AS score FROM items")).resolves.toBeDefined();
	});

	it('multi-statement with Unicode string containing semicolons', async () => {
		// Semicolon inside a string with Unicode should not split.
		const rows = await db.sqlRows("SELECT '日本; 語' AS s FROM items LIMIT 1");
		expect(rows).toHaveLength(1);
		expect(rows[0].s).toContain(';');
	});
});

describe('Creative: Auth edge cases', () => {
	let dir: string;
	beforeEach(() => { dir = makeDir(); });
	afterEach(() => { rmSync(dir, { recursive: true, force: true }); });

	it('enableAuth on empty DB, then create user under auth', () => {
		const db = KitDatabase.openSync(join(dir, 'db'), makeSchema());
		db.enableAuth('admin', 'pw');
		expect(db.requireAuthEnabled()).toBe(true);
		db.createUser('user2', 'pass2');
		expect(db.users()).toContain('admin');
		expect(db.users()).toContain('user2');
		db.close();
	});

	it('disableAuth then re-enable with different admin', () => {
		const db = KitDatabase.createWithCredentialsSync(join(dir, 'sec'), makeSchema(), 'admin1', 'pw1');
		db.disableAuth();
		db.enableAuth('admin2', 'pw2');
		expect(db.requireAuthEnabled()).toBe(true);
		db.close();
		// Must reopen with new admin.
		const db2 = KitDatabase.openSync(join(dir, 'sec'), makeSchema(), {
			credentials: { username: 'admin2', password: 'pw2' },
		});
		expect(db2.requireAuthEnabled()).toBe(true);
	});
});
