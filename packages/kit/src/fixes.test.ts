import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
	KitDatabase,
	Schema,
	table,
	int,
	text,
	timestamp,
	sequenceDefault,
	nowDefault,
	index,
	eq,
	KitDuplicateError,
	KitError
} from './index.js';

function open(schema: Schema) {
	const dir = mkdtempSync(join(tmpdir(), 'kit-fixes-'));
	const db = KitDatabase.openSync(dir, schema);
	db.migrateSync(schema, [{ version: 1, name: 'init', up: () => {} }]);
	return { db, dir };
}

describe('#2 name column-accessor collision', () => {
	const things = table('things', {
		columns: [int('id', { primaryKey: true }), text('name', { nullable: false })],
		primaryKey: 'id'
	});
	it('table.name is the table name; table.column("name") is the column', () => {
		expect(things.name).toBe('things');
		expect(things.column('name').name).toBe('name');
		expect(things.column('name').storageType).toBe('text');
	});
	it('a predicate on a shadowed accessor fails loudly', () => {
		// things.name is the string "things", not a column.
		expect(() => eq(things.name as never, 'x' as never)).toThrow(KitError);
		// The correct accessor works.
		expect(() => eq(things.column('name'), 'x')).not.toThrow();
	});
});

describe('#3 now-default not refreshed on update', () => {
	const t = table('t3', {
		columns: [
			int('id', { primaryKey: true, default: sequenceDefault('t3_id_seq') }),
			text('label', { nullable: false }),
			timestamp('created_at', { default: nowDefault() }), // insert-only
			timestamp('updated_at', { generated: 'now' }) // write-managed
		],
		primaryKey: 'id'
	});
	it('created_at stays put on update; updated_at is present', () => {
		const { db, dir } = open(new Schema([t]));
		try {
			const row = db.insertInto(t).values({ label: 'a' }).executeSync();
			const createdAt = row.created_at;
			expect(createdAt).toBeTruthy();
			const after = db.updateTable(t).set({ label: 'b' }).where(eq(t.id, row.id)).executeSync()[0];
			expect(after.created_at).toBe(createdAt); // unchanged
			expect(after.updated_at).toBeTruthy();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe('#4 single-column PK collisions throw', () => {
	const t = table('t4', {
		columns: [int('id', { primaryKey: true }), text('v', { nullable: false })],
		primaryKey: 'id'
	});
	it('duplicate id throws; re-insert after delete is allowed', () => {
		const { db, dir } = open(new Schema([t]));
		try {
			db.insertInto(t).values({ id: 1n, v: 'a' }).executeSync();
			expect(() => db.insertInto(t).values({ id: 1n, v: 'b' }).executeSync()).toThrow(KitDuplicateError);
			// Original row untouched (no last-writer-wins).
			expect(db.selectFrom(t).where(eq(t.id, 1n)).executeSync()[0].v).toBe('a');
			db.deleteFrom(t).where(eq(t.id, 1n)).executeSync();
			expect(() => db.insertInto(t).values({ id: 1n, v: 'c' }).executeSync()).not.toThrow();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe('#5 unique index enforces uniqueness', () => {
	const t = table('t5', {
		columns: [
			int('id', { primaryKey: true, default: sequenceDefault('t5_id_seq') }),
			text('code', { nullable: false })
		],
		primaryKey: 'id',
		indexes: [index(['code'], { unique: true })]
	});
	it('a duplicate unique-index value throws', () => {
		const { db, dir } = open(new Schema([t]));
		try {
			db.insertInto(t).values({ code: 'A' }).executeSync();
			expect(() => db.insertInto(t).values({ code: 'A' }).executeSync()).toThrow(KitDuplicateError);
			expect(() => db.insertInto(t).values({ code: 'B' }).executeSync()).not.toThrow();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
