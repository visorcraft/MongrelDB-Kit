import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase, Schema, table, int, text, isNull, isNotNull, eq } from './index.js';

const t = table('t', {
	columns: [
		int('id', { primaryKey: true }),
		int('maybe_num', { nullable: true }),
		text('maybe_txt', { nullable: true }),
		text('label', { nullable: false })
	],
	primaryKey: 'id'
});
const schema = new Schema([t]);

function fresh() {
	const dir = mkdtempSync(join(tmpdir(), 'kit-null-'));
	const db = KitDatabase.openSync(dir, schema);
	db.migrateSync(schema, [{ version: 1, name: 'init', up: () => {} }]);
	return { db, dir };
}

describe('nullable columns store real null', () => {
	it('round-trips null (not zero/empty) and matches isNull/isNotNull', () => {
		const { db, dir } = fresh();
		try {
			// Row A: nullable columns omitted -> null.
			const a = db.insertInto(t).values({ id: 1n, label: 'a' }).executeSync();
			expect(a.maybe_num).toBeNull();
			expect(a.maybe_txt).toBeNull();

			// Row B: nullable columns set, including a legitimate zero / empty string.
			const b = db.insertInto(t).values({ id: 2n, maybe_num: 0n, maybe_txt: '', label: 'b' }).executeSync();
			expect(b.maybe_num).toBe(0n); // a real 0 is preserved, distinct from null
			expect(b.maybe_txt).toBe('');

			// Read back confirms persistence.
			const ra = db.selectFrom(t).where(eq(t.id, 1n)).executeSync()[0];
			expect(ra.maybe_num).toBeNull();
			expect(ra.maybe_txt).toBeNull();

			// isNull / isNotNull
			const nullNums = db.selectFrom(t).where(isNull(t.maybe_num)).executeSync();
			expect(nullNums.map((r) => r.id)).toEqual([1n]);
			const nonNullNums = db.selectFrom(t).where(isNotNull(t.maybe_num)).executeSync();
			expect(nonNullNums.map((r) => r.id)).toEqual([2n]);

			// Update to null and back.
			db.updateTable(t).set({ maybe_num: null }).where(eq(t.id, 2n)).executeSync();
			expect(db.selectFrom(t).where(eq(t.id, 2n)).executeSync()[0].maybe_num).toBeNull();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
