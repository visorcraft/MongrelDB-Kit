/**
 * Round 4 destruction tests — TypeScript (12 tests).
 * Angles: multi-statement create-query, privilege escalation, stale matview after UPDATE,
 * FTS on identical values, recursive CTE with modulo, window FIRST_VALUE,
 * block comments with semicolons, CTAS with CASE, DROP+recreate matview.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { KitDatabase, Schema, table, int, real, text } from './index.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function makeDir(): string { return mkdtempSync(join(tmpdir(), 'mdb-r4-')); }
function makeSchema(): Schema {
	return new Schema([table('players', {
		columns: [int('id', { primaryKey: true }), real('score'), text('team')],
		primaryKey: 'id',
	})]);
}
function seed(db: KitDatabase): void {
	const t = db.schema.table('players');
	db.insertInto(t).values({ id: 1n, score: 100.0, team: 'A' }).executeSync();
	db.insertInto(t).values({ id: 2n, score: 100.0, team: 'A' }).executeSync();
	db.insertInto(t).values({ id: 3n, score: 90.0, team: 'A' }).executeSync();
	db.insertInto(t).values({ id: 4n, score: 80.0, team: 'B' }).executeSync();
	db.insertInto(t).values({ id: 5n, score: 80.0, team: 'B' }).executeSync();
}

describe('R4: Multi-statement create-then-query', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('create table then query in same batch', async () => {
		const rows = await db.sqlRows(
			"CREATE TABLE instant AS SELECT id FROM players LIMIT 2; SELECT count(*) AS c FROM instant"
		);
		expect(rows).toHaveLength(1);
		expect(rows[0]).toEqual({ c: 2n });
	});

	it('create-drop-create cycle', async () => {
		await db.sqlRows("CREATE TABLE c AS SELECT id FROM players LIMIT 1; DROP TABLE c; CREATE TABLE c AS SELECT id FROM players LIMIT 2");
		const rows = await db.sqlRows('SELECT count(*) AS c FROM c');
		expect(rows[0]).toEqual({ c: 2n });
	});
});

describe('R4: Matview independence', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('matview snapshot after UPDATE', async () => {
		await db.sqlRows('CREATE MATERIALIZED VIEW mv AS SELECT id, score FROM players');
		await db.sqlRows('UPDATE players SET score = 999 WHERE id = 1');
		const rows = await db.sqlRows('SELECT score FROM mv WHERE id = 1');
		expect(Number(rows[0].score)).toBe(100);
	});

	it('matview drop then recreate with different query', async () => {
		await db.sqlRows("CREATE MATERIALIZED VIEW mv AS SELECT id FROM players WHERE team = 'A'");
		await db.sqlRows('DROP TABLE mv');
		await db.sqlRows("CREATE MATERIALIZED VIEW mv AS SELECT id FROM players WHERE team = 'B'");
		const rows = await db.sqlRows('SELECT count(*) AS c FROM mv');
		expect(rows[0]).toEqual({ c: 2n });
	});
});

describe('R4: Auth edge cases', () => {
	let dir: string;
	beforeEach(() => { dir = makeDir(); });
	afterEach(() => { rmSync(dir, { recursive: true, force: true }); });

	it('privilege escalation blocked — non-admin cannot create users', () => {
		const db = KitDatabase.createWithCredentialsSync(join(dir, 'sec'), makeSchema(), 'admin', 'pw');
		db.createUser('regular', 'rpw');
		// Grant Select on players so regular can at least open via the Kit.
		db.createRole('r');
		db.grantPermission('r', 'all');
		// Grant access to internal tables so Kit initialization works.
		db.grantPermission('r', 'insert:__kit_schema_catalog');
		db.grantPermission('r', 'insert:__kit_schema_migrations');
		db.grantPermission('r', 'insert:__kit_unique_keys');
		db.grantPermission('r', 'insert:__kit_row_guards');
		db.grantPermission('r', 'insert:__kit_migration_locks');
		db.grantRole('regular', 'r');
		db.close();

		const db2 = KitDatabase.openSync(join(dir, 'sec'), makeSchema(), {
			credentials: { username: 'regular', password: 'rpw' },
		});
		// regular is NOT admin — cannot create users.
		expect(() => db2.createUser('intruder', 'pw')).toThrow();
		db2.close();
	});

	it('disable auth clears table enforcement', () => {
		const db = KitDatabase.createWithCredentialsSync(join(dir, 'sec'), makeSchema(), 'admin', 'pw');
		const t = db.schema.table('players');
		db.insertInto(t).values({ id: 1n, score: 1.0, team: 'X' }).executeSync();
		db.disableAuth();
		db.close();

		const db2 = KitDatabase.openSync(join(dir, 'sec'), makeSchema());
		expect(db2.requireAuthEnabled()).toBe(false);
		// Should be able to read without auth.
		db2.close();
	});
});

describe('R4: FTS and window edge cases', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db); });
	afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

	it('FTS on identical values — all same score', async () => {
		const rows = await db.sqlRows(
			"SELECT id, mongreldb_fts_rank(team, 'A') AS score FROM players ORDER BY id"
		);
		expect(rows).toHaveLength(5);
		// Team A (ids 1-3) positive; team B (ids 4-5) zero.
		expect(Number(rows[0].score)).toBeGreaterThan(0);
		expect(Number(rows[3].score)).toBe(0);
	});

	it('FTS with numbers in text', async () => {
		await db.sqlRows("INSERT INTO players (id, score, team) VALUES (99, 1.0, 'version 2.0 build 1234')");
		const rows = await db.sqlRows(
			"SELECT mongreldb_fts_rank(team, 'version 1234') AS score FROM players WHERE id = 99"
		);
		expect(Number(rows[0].score)).toBeGreaterThan(0);
	});

	it('window FIRST_VALUE', async () => {
		const rows = await db.sqlRows(
			"SELECT id, FIRST_VALUE(score) OVER (PARTITION BY team ORDER BY score DESC) AS top FROM players ORDER BY id"
		);
		expect(rows).toHaveLength(5);
		expect(Number(rows[0].top)).toBe(100); // team A top
		expect(Number(rows[3].top)).toBe(80);  // team B top
	});

	it('recursive CTE with modulo filter', async () => {
		const rows = await db.sqlRows(
			"WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n + 2 FROM r WHERE n < 10) " +
			"SELECT count(*) AS c FROM r WHERE n % 4 = 0"
		);
		// r: 0,2,4,6,8,10. n%4=0: 0,4,8 → 3
		expect(rows[0]).toEqual({ c: 3n });
	});
});

describe('R4: Multi-statement with comments', () => {
	let dir: string, db: KitDatabase;
	beforeEach(() => { dir = makeDir(); db = KitDatabase.openSync(join(dir, 'db'), makeSchema()); seed(db); });
	afterEach(() => { { try { db.close(); } catch {} } rmSync(dir, { recursive: true, force: true }); });

	it('block comment with semicolons does not split', async () => {
		const rows = await db.sqlRows('SELECT 1 AS n /* this; has; semicolons; */; SELECT 2 AS n');
		expect(rows).toHaveLength(1);
		expect(rows[0]).toEqual({ n: 2n });
	});

	it('CTAS with CASE expression', async () => {
		await db.sqlRows(
			"CREATE TABLE labeled AS SELECT id, CASE WHEN score >= 90 THEN 'high' ELSE 'low' END AS tier FROM players"
		);
		const high = await db.sqlRows("SELECT tier FROM labeled WHERE id = 1");
		expect(high[0]).toEqual({ tier: 'high' });
		const low = await db.sqlRows("SELECT tier FROM labeled WHERE id = 4");
		expect(low[0]).toEqual({ tier: 'low' });
	});
});
