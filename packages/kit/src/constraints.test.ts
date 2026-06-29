import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ConditionKind } from 'mongreldb/native.js';
import type { Database as NativeDatabase } from 'mongreldb/native.js';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, real, unique, foreignKey, index, check } from './schema.js';
import { validateRow } from './validation.js';
import type { TableSpec, PkValue } from './types.js';
import {
	stageUniqueGuards,
	stagePkGuard,
	deleteUniqueGuards,
	deletePkGuard,
	touchRowGuard,
	deleteRowGuard,
	parentExists,
	enforceForeignKeys,
	planDelete,
	toCells,
	pkValueFromRow,
	type ConstraintKit
} from './constraints.js';
import {
	KitDuplicateError,
	KitForeignKeyError,
	KitRestrictError,
	KitNotFoundError
} from './errors.js';
import { encodeUniqueKey, encodeRowGuardKey } from './keys.js';

function makeTempDir(): string {
	return mkdtempSync(join(tmpdir(), 'kit-constraints-'));
}

const users = table('users', {
	columns: [int('id', { primaryKey: true }), text('email', { nullable: false })],
	primaryKey: ['id'],
	unique: [unique(['email'], { name: 'users_email_uq' })]
});

const trips = table('trips', {
	columns: [int('id', { primaryKey: true }), text('name', { nullable: false })],
	primaryKey: ['id']
});

const shares = table('shares', {
	columns: [int('id', { primaryKey: true }), int('trip_id'), int('user_id')],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(
			['trip_id'],
			{ table: 'trips', columns: ['id'] },
			{ name: 'shares_trip_fk', onDelete: 'cascade' }
		)
	]
});

const invites = table('invites', {
	columns: [int('id', { primaryKey: true }), int('trip_id', { nullable: true })],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(
			['trip_id'],
			{ table: 'trips', columns: ['id'] },
			{ name: 'invites_trip_fk', onDelete: 'set null' }
		)
	]
});

const members = table('members', {
	columns: [int('id', { primaryKey: true }), int('trip_id', { nullable: false })],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(
			['trip_id'],
			{ table: 'trips', columns: ['id'] },
			{ name: 'members_trip_fk', onDelete: 'restrict' }
		)
	]
});

const tags = table('tags', {
	columns: [text('id', { primaryKey: true }), text('name', { nullable: false })],
	primaryKey: ['id']
});

const tagged = table('tagged', {
	columns: [int('id', { primaryKey: true }), text('tag_id', { nullable: true })],
	primaryKey: ['id'],
	indexes: [index(['tag_id'])],
	foreignKeys: [
		foreignKey(
			['tag_id'],
			{ table: 'tags', columns: ['id'] },
			{ name: 'tagged_tag_fk', onDelete: 'set null' }
		)
	]
});

const categories = table('categories', {
	columns: [text('id', { primaryKey: true }), text('name', { nullable: false })],
	primaryKey: ['id']
});

const items = table('items', {
	columns: [int('id', { primaryKey: true }), text('category_id', { nullable: false })],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(
			['category_id'],
			{ table: 'categories', columns: ['id'] },
			{ name: 'items_category_fk', onDelete: 'cascade' }
		)
	]
});

const a = table('a', {
	columns: [int('id', { primaryKey: true })],
	primaryKey: ['id']
});

const b = table('b', {
	columns: [int('id', { primaryKey: true }), int('a_id')],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(['a_id'], { table: 'a', columns: ['id'] }, { name: 'b_a_fk', onDelete: 'cascade' })
	]
});

const c = table('c', {
	columns: [int('id', { primaryKey: true }), int('a_id')],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(['a_id'], { table: 'a', columns: ['id'] }, { name: 'c_a_fk', onDelete: 'cascade' })
	]
});

const d = table('d', {
	columns: [int('id', { primaryKey: true }), int('b_id'), int('c_id')],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(['b_id'], { table: 'b', columns: ['id'] }, { name: 'd_b_fk', onDelete: 'cascade' }),
		foreignKey(['c_id'], { table: 'c', columns: ['id'] }, { name: 'd_c_fk', onDelete: 'cascade' })
	]
});

const groupMembers = table('group_members', {
	columns: [int('group_id'), int('user_id'), text('role', { nullable: false })],
	primaryKey: ['group_id', 'user_id']
});

const subscribers = table('subscribers', {
	columns: [int('id', { primaryKey: true }), text('email', { nullable: true })],
	primaryKey: ['id'],
	unique: [unique(['email'], { name: 'subscribers_email_uq' })]
});

const checkedOrders = table('checked_orders', {
	columns: [int('id', { primaryKey: true }), int('quantity'), real('price')],
	primaryKey: ['id'],
	checks: [
		check('positive_total', (row) =>
			Number(row.quantity) * (row.price as number) > 0 ? true : 'total must be positive'
		)
	]
});

const testSchema = new Schema([
	users,
	trips,
	shares,
	invites,
	members,
	tags,
	tagged,
	categories,
	items,
	a,
	b,
	c,
	d,
	groupMembers,
	subscribers,
	checkedOrders
]);

async function withDb(
	fn: (kit: ConstraintKit, db: KitDatabase) => Promise<void>
): Promise<void> {
	const dir = makeTempDir();
	const db = await KitDatabase.open(dir, testSchema);
	try {
		await fn({ db: db.nativeDb, schema: testSchema }, db);
	} finally {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	}
}

async function insertRow(
	kit: ConstraintKit,
	table: TableSpec,
	row: Record<string, unknown>
): Promise<void> {
	await kit.db.transaction(async (txn) => {
		validateRow(table, row);
		enforceForeignKeys(kit, txn, table, row);
		const pkValue = pkValueFromRow(table, row);
		stageUniqueGuards(kit, txn, table, row, pkValue);
		stagePkGuard(kit, txn, table, pkValue, true);
		txn.put(table.name, toCells(table, row));
	});
}

async function deleteRow(kit: ConstraintKit, table: TableSpec, pkValue: PkValue): Promise<void> {
	await kit.db.transaction(async (txn) => {
		planDelete(kit, txn, table, pkValue);
	});
}

function findGuard(db: NativeDatabase, encodedKey: string) {
	return db
		.table('__kit_unique_keys')
		.query([
			{ kind: ConditionKind.BitmapEq, columnId: 1, text: encodedKey }
		])[0] as { rowId: bigint; cells: { columnId: number; text?: string }[] } | undefined;
}

function findRowGuard(db: NativeDatabase, encodedKey: string) {
	return db
		.table('__kit_row_guards')
		.query([
			{ kind: ConditionKind.BitmapEq, columnId: 1, text: encodedKey }
		])[0] as { rowId: bigint; cells: { columnId: number; text?: string }[] } | undefined;
}

describe('key encoding', () => {
	it('encodes unique keys with typed components', () => {
		expect(encodeUniqueKey(1, 'users_email_uq', ['a@example.com'])).toBe(
			'uq:1:users_email_uq:s:a@example.com'
		);
		expect(encodeUniqueKey(1, 'shares_trip_user_uq', [42n, 7n])).toBe(
			'uq:1:shares_trip_user_uq:i:42:i:7'
		);
		expect(encodeUniqueKey(1, 'uq_esc', ['a:b\\c'])).toBe('uq:1:uq_esc:s:a\\:b\\\\c');
		expect(encodeUniqueKey(1, 'uq_null', [null])).toBe('uq:1:uq_null:n:null');
	});

	it('encodes row guard keys', () => {
		expect(encodeRowGuardKey('trips', 5n)).toBe('rg:trips:i:5');
		expect(encodeRowGuardKey('users', 'alpha')).toBe('rg:users:s:alpha');
	});
});

describe('unique constraints', () => {
	it('allows distinct values and rejects duplicates', async () => {
		await withDb(async (kit) => {
			await insertRow(kit, users, { id: 1n, email: 'one@example.com' });
			await insertRow(kit, users, { id: 2n, email: 'two@example.com' });
			await expect(insertRow(kit, users, { id: 3n, email: 'one@example.com' })).rejects.toBeInstanceOf(
				KitDuplicateError
			);
		});
	});

	it('cleans unique guards on delete', async () => {
		await withDb(async (kit, db) => {
			await insertRow(kit, users, { id: 1n, email: 'keep@example.com' });
			const key = encodeUniqueKey(1, 'users_email_uq', ['keep@example.com']);
			expect(findGuard(db.nativeDb, key)).toBeDefined();
			await deleteRow(kit, users, 1n);
			expect(findGuard(db.nativeDb, key)).toBeUndefined();
		});
	});

	it('allows two rows with null in a nullable unique column to coexist', async () => {
		await withDb(async (kit) => {
			await insertRow(kit, subscribers, { id: 1n, email: null });
			await insertRow(kit, subscribers, { id: 2n, email: null });
			await insertRow(kit, subscribers, { id: 3n, email: 'a@example.com' });
			await expect(
				insertRow(kit, subscribers, { id: 4n, email: 'a@example.com' })
			).rejects.toBeInstanceOf(KitDuplicateError);
		});
	});
});

describe('composite primary keys', () => {
	it('inserts, updates, and deletes rows with composite primary keys', async () => {
		await withDb(async (kit, db) => {
			await insertRow(kit, groupMembers, { group_id: 1n, user_id: 10n, role: 'member' });
			await insertRow(kit, groupMembers, { group_id: 1n, user_id: 11n, role: 'admin' });
			await insertRow(kit, groupMembers, { group_id: 2n, user_id: 10n, role: 'member' });

			await expect(
				insertRow(kit, groupMembers, { group_id: 1n, user_id: 10n, role: 'other' })
			).rejects.toThrow();

			await deleteRow(kit, groupMembers, [1n, 10n]);
			expect(db.nativeDb.table('group_members').count()).toBe(2n);
		});
	});

	it('cleans unique guards for composite primary key rows on delete', async () => {
		await withDb(async (kit, db) => {
			await insertRow(kit, subscribers, { id: 1n, email: 'keep@example.com' });
			const key = encodeUniqueKey(1, 'subscribers_email_uq', ['keep@example.com']);
			expect(findGuard(db.nativeDb, key)).toBeDefined();
			await deleteRow(kit, subscribers, 1n);
			expect(findGuard(db.nativeDb, key)).toBeUndefined();
		});
	});
});

describe('foreign keys', () => {
	it('rejects child insert with missing parent', async () => {
		await withDb(async (kit) => {
			await insertRow(kit, users, { id: 1n, email: 'a@example.com' });
			await insertRow(kit, trips, { id: 1n, name: 'Trip' });
			await expect(
				insertRow(kit, shares, { id: 1n, trip_id: 99n, user_id: 1n })
			).rejects.toBeInstanceOf(KitForeignKeyError);
		});
	});

	it('touches parent row guard on child insert and removes it on parent delete', async () => {
		await withDb(async (kit, db) => {
			await insertRow(kit, trips, { id: 1n, name: 'Trip' });
			await insertRow(kit, invites, { id: 1n, trip_id: 1n });
			const guardKey = encodeRowGuardKey('trips', 1n);
			expect(findRowGuard(db.nativeDb, guardKey)).toBeDefined();
			await deleteRow(kit, trips, 1n);
			expect(findRowGuard(db.nativeDb, guardKey)).toBeUndefined();
		});
	});

	it('cascade delete removes child rows', async () => {
		await withDb(async (kit, db) => {
			await insertRow(kit, trips, { id: 1n, name: 'Trip' });
			await insertRow(kit, users, { id: 1n, email: 'a@example.com' });
			await insertRow(kit, shares, { id: 1n, trip_id: 1n, user_id: 1n });
			await deleteRow(kit, trips, 1n);
			expect(db.nativeDb.table('trips').count()).toBe(0n);
			expect(db.nativeDb.table('shares').count()).toBe(0n);
		});
	});

	it('set-null delete clears nullable FK columns', async () => {
		await withDb(async (kit, db) => {
			await insertRow(kit, tags, { id: 't1', name: 'Tag' });
			await insertRow(kit, tagged, { id: 1n, tag_id: 't1' });
			await deleteRow(kit, tags, 't1');
			expect(db.nativeDb.table('tags').count()).toBe(0n);
			const taggedRows = db.nativeDb.table('tagged').query([
				{ kind: ConditionKind.RangeInt, columnId: 1, int64Lo: 1n, int64Hi: 1n }
			]);
			expect(taggedRows.length).toBe(1);
			const tagIdCell = taggedRows[0]!.cells.find((c) => c.columnId === 2);
			expect(tagIdCell).toBeDefined();
			expect(tagIdCell!.text).not.toBe('t1');
		});
	});

	it('restrict delete rejects when children exist', async () => {
		await withDb(async (kit) => {
			await insertRow(kit, trips, { id: 1n, name: 'Trip' });
			await insertRow(kit, members, { id: 1n, trip_id: 1n });
			await expect(deleteRow(kit, trips, 1n)).rejects.toBeInstanceOf(KitRestrictError);
		});
	});

	it('planDelete throws when target row does not exist', async () => {
		await withDb(async (kit) => {
			await expect(deleteRow(kit, trips, 99n)).rejects.toBeInstanceOf(KitNotFoundError);
		});
	});

	it('cascade delete works with text foreign keys', async () => {
		await withDb(async (kit, db) => {
			await insertRow(kit, categories, { id: 'cat1', name: 'Category 1' });
			await insertRow(kit, items, { id: 1n, category_id: 'cat1' });
			await deleteRow(kit, categories, 'cat1');
			expect(db.nativeDb.table('categories').count()).toBe(0n);
			expect(db.nativeDb.table('items').count()).toBe(0n);
		});
	});

	it('deletes diamond-shaped cascade without duplicate deletes or cycle errors', async () => {
		await withDb(async (kit, db) => {
			await insertRow(kit, a, { id: 1n });
			await insertRow(kit, b, { id: 1n, a_id: 1n });
			await insertRow(kit, c, { id: 1n, a_id: 1n });
			await insertRow(kit, d, { id: 1n, b_id: 1n, c_id: 1n });
			await deleteRow(kit, a, 1n);
			expect(db.nativeDb.table('a').count()).toBe(0n);
			expect(db.nativeDb.table('b').count()).toBe(0n);
			expect(db.nativeDb.table('c').count()).toBe(0n);
			expect(db.nativeDb.table('d').count()).toBe(0n);
		});
	});
});

describe('constraint helpers', () => {
	it('parentExists reflects committed rows', async () => {
		await withDb(async (kit) => {
			expect(parentExists(kit, 'trips', 1n)).toBe(false);
			await insertRow(kit, trips, { id: 1n, name: 'Trip' });
			expect(parentExists(kit, 'trips', 1n)).toBe(true);
		});
	});
});
