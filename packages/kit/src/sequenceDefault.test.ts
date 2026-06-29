import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase, Schema, table, int, text, sequenceDefault } from './index.js';

const widgets = table('widgets', {
	columns: [
		int('id', { primaryKey: true, default: sequenceDefault('widgets_id_seq') }),
		text('name', { nullable: false })
	],
	primaryKey: ['id']
});

const schema = new Schema([widgets]);
const migrations = [{ version: 1, name: 'init', up: () => {} }];

function fresh() {
	const dir = mkdtempSync(join(tmpdir(), 'kit-seqdefault-'));
	const db = KitDatabase.openSync(dir, schema);
	db.migrateSync(schema, migrations);
	return { db, dir };
}

describe('sequence default on insert (auto-increment)', () => {
	it('auto-assigns 1-based ids when the column is omitted', () => {
		const { db, dir } = fresh();
		try {
			const a = db.insertInto(widgets).values({ name: 'a' }).executeSync();
			const b = db.insertInto(widgets).values({ name: 'b' }).executeSync();
			const c = db.insertInto(widgets).values({ name: 'c' }).executeSync();

			expect(typeof a.id).toBe('bigint');
			// First id is 1, never 0 (0 is falsy and collides with the "unset FK"
			// sentinel that applications rely on).
			expect([a.id, b.id, c.id]).toEqual([1n, 2n, 3n]);

			const rows = db.selectFrom(widgets).executeSync();
			expect(rows.map((r) => r.id).sort()).toEqual([1n, 2n, 3n]);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('advances the counter past an explicitly supplied id (no future collision)', () => {
		const { db, dir } = fresh();
		try {
			const explicit = db.insertInto(widgets).values({ id: 100n, name: 'x' }).executeSync();
			expect(explicit.id).toBe(100n);
			// Engine-native AUTO_INCREMENT advances the counter past the explicit
			// id so a later auto-assign can never collide with it. (This replaces
			// the legacy Kit guarantee that explicit inserts left the sequence
			// untouched — engine ownership makes the collision-free rule primary.)
			const auto = db.insertInto(widgets).values({ name: 'y' }).executeSync();
			expect(auto.id).toBe(101n);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('survives reopen and never reuses ids', () => {
		const { db, dir } = fresh();
		try {
			db.insertInto(widgets).values({ name: 'a' }).executeSync();
			db.insertInto(widgets).values({ name: 'b' }).executeSync();
			db.close();
			const db2 = KitDatabase.openSync(dir, schema);
			db2.migrateSync(schema, migrations);
			const row = db2.insertInto(widgets).values({ name: 'c' }).executeSync();
			expect(row.id).toBe(3n);
			db2.close();
		} finally {
			try {
				rmSync(dir, { recursive: true, force: true });
			} catch {
				/* dir already removed if close path failed */
			}
		}
	});
});
