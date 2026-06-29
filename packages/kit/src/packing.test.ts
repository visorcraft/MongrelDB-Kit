import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { eq, asc } from './query.js'; // importing query.js also attaches the builder methods
import {
	Schema,
	table,
	int,
	text,
	real,
	bool,
	json,
	timestamp,
	blob,
	foreignKey
} from './schema.js';
import type { Insert } from './types.js';

// Exercises the packed bulk-write fast path (valuesMany -> putPacked, deleteFrom
// -> deletePacked). The round-trip must be byte-identical to the per-row path
// across every storage type, including nulls and multibyte text/raw bytes.

const things = table('things', {
	columns: [
		int('id', { primaryKey: true }),
		text('name'),
		json('meta'),
		timestamp('seen_at'),
		real('score', { nullable: true }),
		bool('active'),
		blob('payload', { nullable: true }),
		int('count', { nullable: true })
	],
	primaryKey: 'id'
});

// A referenced parent + cascading child: deleting a parent must NOT take the
// batched fast path (the table is an FK target) and must still cascade.
const parent = table('parent', {
	columns: [int('id', { primaryKey: true }), text('label')],
	primaryKey: 'id'
});
const child = table('child', {
	columns: [int('id', { primaryKey: true }), int('parent_id')],
	primaryKey: 'id',
	foreignKeys: [
		foreignKey(['parent_id'], { table: 'parent', columns: ['id'] }, {
			name: 'child_parent_fk',
			onDelete: 'cascade'
		})
	]
});

function open() {
	const dir = mkdtempSync(join(tmpdir(), 'kit-packing-'));
	const db = KitDatabase.openSync(dir, new Schema([things, parent, child]));
	return { db, close: () => { db.close(); rmSync(dir, { recursive: true, force: true }); } };
}

describe('packed bulk write', () => {
	it('valuesMany round-trips every storage type incl. nulls and raw bytes', () => {
		const { db, close } = open();
		try {
			const rows: Insert<typeof things>[] = [
				{
					id: 1n,
					name: 'café ☕ 北京',
					meta: '{"a":1}',
					seen_at: '2026-01-02T03:04:05.000Z',
					score: 3.5,
					active: true,
					payload: new Uint8Array([0, 1, 2, 255]),
					count: 42n
				},
				{
					id: 2n,
					name: 'second',
					meta: '[]',
					seen_at: '2026-06-28T00:00:00.000Z',
					score: null,
					active: false,
					payload: null,
					count: null
				}
			];
			db.insertInto(things).valuesMany(rows).executeSync();

			const got = db.selectFrom(things).orderBy(asc(things.id)).executeSync();
			expect(got).toHaveLength(2);

			expect(got[0].id).toBe(1n);
			expect(got[0].name).toBe('café ☕ 北京');
			expect(got[0].meta).toBe('{"a":1}');
			expect(got[0].seen_at).toBe('2026-01-02T03:04:05.000Z');
			expect(got[0].score).toBe(3.5);
			expect(got[0].active).toBe(true);
			expect(Array.from(got[0].payload as Uint8Array)).toEqual([0, 1, 2, 255]);
			expect(got[0].count).toBe(42n);

			expect(got[1].score).toBeNull();
			expect(got[1].active).toBe(false);
			expect(got[1].payload).toBeNull();
			expect(got[1].count).toBeNull();
		} finally {
			close();
		}
	});

	it('deleteFrom fast path clears a simple table', () => {
		const { db, close } = open();
		try {
			db.insertInto(things)
				.valuesMany(
					Array.from({ length: 500 }, (_, i) => ({
						id: BigInt(i + 1),
						name: `n${i}`,
						meta: '{}',
						seen_at: '2026-01-01T00:00:00.000Z',
						score: i,
						active: i % 2 === 0,
						payload: null,
						count: BigInt(i)
					})) as Insert<typeof things>[]
				)
				.executeSync();
			expect(db.selectFrom(things).selectCount().executeSync()).toBe(500n);

			const deleted = db.deleteFrom(things).executeSync();
			expect(deleted).toBe(500n);
			expect(db.selectFrom(things).selectCount().executeSync()).toBe(0n);
		} finally {
			close();
		}
	});

	it('deleteFrom on a referenced (FK-parent) table still cascades', () => {
		const { db, close } = open();
		try {
			db.insertInto(parent).valuesMany([
				{ id: 1n, label: 'a' },
				{ id: 2n, label: 'b' }
			] as Insert<typeof parent>[]).executeSync();
			db.insertInto(child).valuesMany([
				{ id: 10n, parent_id: 1n },
				{ id: 11n, parent_id: 1n },
				{ id: 12n, parent_id: 2n }
			] as Insert<typeof child>[]).executeSync();

			// Delete parent 1 — must cascade to children 10 and 11.
			const n = db.deleteFrom(parent).where(eq(parent.id, 1n)).executeSync();
			expect(n).toBe(1n);
			const remainingChildren = db.selectFrom(child).executeSync().map((r) => r.id).sort();
			expect(remainingChildren).toEqual([12n]);
		} finally {
			close();
		}
	});
});
