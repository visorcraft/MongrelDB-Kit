import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, real } from './schema.js';
import { eq, gte } from './query.js';
import { view } from './external.js';

function freshDb() {
	const dir = mkdtempSync(join(tmpdir(), 'kit-tier2-test-'));
	const schema = new Schema([
		table('users', {
			columns: [
				int('id', { primaryKey: true }),
				text('email'),
				real('score', { nullable: true })
			],
			primaryKey: ['id']
		})
	]);
	const db = KitDatabase.openSync(dir, schema);
	return { db, dir };
}

describe('Tier 2: views, updateWhere, deleteWhere', () => {
	it('createView / dropView round-trip via the SQL session', async () => {
		const { db, dir } = freshDb();
		try {
			db.insertInto(db.schema.table('users'))
				.valuesMany([{ id: 1n, email: 'a@x', score: 50 }, { id: 2n, email: 'b@x', score: 95 }])
				.executeSync();
			await db.createView(view('vip', 'SELECT id, email FROM users WHERE score >= 90'));
			const rows = await db.sqlRows('SELECT * FROM vip ORDER BY id');
			expect(rows).toEqual([{ id: 2n, email: 'b@x' }]);
			await db.dropView('vip');
			// The view is gone from the session.
			await expect(db.sqlRows('SELECT * FROM vip')).rejects.toThrow();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('updateWhere updates matched rows and returns them', () => {
		const { db, dir } = freshDb();
		try {
			const t = db.schema.table('users');
			db.insertInto(t)
				.valuesMany([
					{ id: 1n, email: 'a@x', score: 50 },
					{ id: 2n, email: 'b@x', score: 60 },
					{ id: 3n, email: 'c@x', score: 95 }
				])
				.executeSync();
			const scoreCol = t.columns.find((c) => c.name === 'score')!;
			const updated = db.updateWhere(t, { score: 100 }, gte(scoreCol, 60));
			expect(updated.length).toBe(2);
			expect(updated.every((r: any) => r.score === 100)).toBe(true);
			// Verify persistence.
			const all = db.selectFrom(t).executeSync() as any[];
			expect(all.filter((r) => r.score === 100).length).toBe(2);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('deleteWhere removes matched rows and returns the count', () => {
		const { db, dir } = freshDb();
		try {
			const t = db.schema.table('users');
			db.insertInto(t)
				.valuesMany([
					{ id: 1n, email: 'a@x', score: 50 },
					{ id: 2n, email: 'b@x', score: 60 },
					{ id: 3n, email: 'c@x', score: 95 }
				])
				.executeSync();
			const emailCol = t.columns.find((c) => c.name === 'email')!;
			const count = db.deleteWhere(t, eq(emailCol, 'b@x'));
			expect(count).toBe(1n);
			// Verify.
			const remaining = db.selectFrom(t).executeSync() as any[];
			expect(remaining.length).toBe(2);
			expect(remaining.find((r) => r.email === 'b@x')).toBeUndefined();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
