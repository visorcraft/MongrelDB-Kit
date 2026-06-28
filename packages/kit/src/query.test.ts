import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, unique, index, foreignKey, timestamp, date } from './schema.js';
import { nowDefault } from './defaults.js';
import { eq, asc, desc, and } from './query.js';
import { KitDuplicateError, KitForeignKeyError } from './errors.js';

function makeTempDir(): string {
	return mkdtempSync(join(tmpdir(), 'kit-query-'));
}

const users = table('users', {
	columns: [int('id', { primaryKey: true }), text('email', { nullable: false })],
	primaryKey: ['id'],
	unique: [unique(['email'], { name: 'users_email_uq' })],
	indexes: [index(['email'], { name: 'idx_users_email' })]
});

const posts = table('posts', {
	columns: [int('id', { primaryKey: true }), int('authorId', { nullable: false })],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(['authorId'], { table: 'users', columns: ['id'] }, { name: 'posts_author_fk' })
	]
});

const groupMembers = table('group_members', {
	columns: [int('group_id'), int('user_id'), text('role', { nullable: false })],
	primaryKey: ['group_id', 'user_id']
});

const events = table('events', {
	columns: [
		int('id', { primaryKey: true }),
		text('name', { nullable: false }),
		text('createdAt', { nullable: false, default: nowDefault() })
	],
	primaryKey: ['id']
});

const testSchema = new Schema([users, posts, groupMembers, events]);

async function withDb(fn: (db: KitDatabase) => Promise<void>): Promise<void> {
	const dir = makeTempDir();
	const db = await KitDatabase.open(dir, testSchema);
	try {
		await fn(db);
	} finally {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	}
}

function withDbSync(fn: (db: KitDatabase) => void): void {
	const dir = makeTempDir();
	const db = KitDatabase.openSync(dir, testSchema);
	try {
		fn(db);
	} finally {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	}
}

describe('query builder', () => {
	it('inserts and selects one row', async () => {
		await withDb(async (db) => {
			const inserted = await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();
			expect(inserted.email).toBe('a@example.com');
			expect(inserted.id).toBe(1n);

			const rows = await db.selectFrom(users).execute();
			expect(rows).toHaveLength(1);
			expect(rows[0].email).toBe('a@example.com');
		});
	});

	it('selects with where equality', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'one@example.com' }).execute();
			await db.insertInto(users).values({ id: 2n, email: 'two@example.com' }).execute();

			const rows = await db.selectFrom(users).where(eq(users.id, 1n)).execute();
			expect(rows).toHaveLength(1);
			expect(rows[0].email).toBe('one@example.com');
		});
	});

	it('selects with orderBy and limit', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'b@example.com' }).execute();
			await db.insertInto(users).values({ id: 2n, email: 'a@example.com' }).execute();
			await db.insertInto(users).values({ id: 3n, email: 'c@example.com' }).execute();

			const rows = await db.selectFrom(users).orderBy(asc(users.email)).limit(2).execute();
			expect(rows).toHaveLength(2);
			expect(rows[0].email).toBe('a@example.com');
			expect(rows[1].email).toBe('b@example.com');
		});
	});

	it('updates a row and verifies', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'old@example.com' }).execute();

			const updated = await db
				.updateTable(users)
				.set({ email: 'new@example.com' })
				.where(eq(users.id, 1n))
				.execute();
			expect(updated).toHaveLength(1);
			expect(updated[0].email).toBe('new@example.com');

			const rows = await db.selectFrom(users).where(eq(users.id, 1n)).execute();
			expect(rows[0].email).toBe('new@example.com');
		});
	});

	it('deletes a row and verifies', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'gone@example.com' }).execute();

			const deleted = await db.deleteFrom(users).where(eq(users.id, 1n)).execute();
			expect(deleted).toBe(1n);

			const rows = await db.selectFrom(users).execute();
			expect(rows).toHaveLength(0);
		});
	});

	it('counts rows', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();
			await db.insertInto(users).values({ id: 2n, email: 'b@example.com' }).execute();
			await db.insertInto(users).values({ id: 3n, email: 'c@example.com' }).execute();

			const count = await db.selectFrom(users).selectCount().execute();
			expect(count).toBe(3n);
		});
	});

	it('rejects unique constraint violations on insert', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'dup@example.com' }).execute();
			await expect(
				db.insertInto(users).values({ id: 2n, email: 'dup@example.com' }).execute()
			).rejects.toBeInstanceOf(KitDuplicateError);
		});
	});

	it('rejects unique constraint violations on update', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();
			await db.insertInto(users).values({ id: 2n, email: 'b@example.com' }).execute();

			await expect(
				db.updateTable(users).set({ email: 'b@example.com' }).where(eq(users.id, 1n)).execute()
			).rejects.toBeInstanceOf(KitDuplicateError);
		});
	});

	it('rejects foreign key violations on insert', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'author@example.com' }).execute();
			await expect(
				db.insertInto(posts).values({ id: 1n, authorId: 99n }).execute()
			).rejects.toBeInstanceOf(KitForeignKeyError);
		});
	});

	it('orders in descending direction', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();
			await db.insertInto(users).values({ id: 2n, email: 'b@example.com' }).execute();

			const rows = await db.selectFrom(users).orderBy(desc(users.id)).execute();
			expect(rows[0].email).toBe('b@example.com');
			expect(rows[1].email).toBe('a@example.com');
		});
	});

	it('handles composite primary keys', async () => {
		await withDb(async (db) => {
			await db
				.insertInto(groupMembers)
				.values({ group_id: 1n, user_id: 10n, role: 'member' })
				.execute();
			await db
				.insertInto(groupMembers)
				.values({ group_id: 1n, user_id: 11n, role: 'admin' })
				.execute();
			await db
				.insertInto(groupMembers)
				.values({ group_id: 2n, user_id: 10n, role: 'member' })
				.execute();

			const rows = await db
				.selectFrom(groupMembers)
				.where(eq(groupMembers.group_id, 1n))
				.orderBy(asc(groupMembers.user_id))
				.execute();
			expect(rows).toHaveLength(2);
			expect(rows[0].role).toBe('member');
			expect(rows[1].role).toBe('admin');

			const updated = await db
				.updateTable(groupMembers)
				.set({ role: 'owner' })
				.where(and(eq(groupMembers.group_id, 1n), eq(groupMembers.user_id, 10n)))
				.execute();
			expect(updated).toHaveLength(1);
			expect(updated[0].role).toBe('owner');

			const deleted = await db
				.deleteFrom(groupMembers)
				.where(and(eq(groupMembers.group_id, 1n), eq(groupMembers.user_id, 10n)))
				.execute();
			expect(deleted).toBe(1n);

			const remaining = await db.selectFrom(groupMembers).execute();
			expect(remaining).toHaveLength(2);
		});
	});

	it('stores timestamp defaults as ISO strings', async () => {
		await withDb(async (db) => {
			const inserted = await db.insertInto(events).values({ id: 1n, name: 'Launch' }).execute();
			expect(typeof inserted.createdAt).toBe('string');
			expect(inserted.createdAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);

			const rows = await db.selectFrom(events).where(eq(events.id, 1n)).execute();
			expect(rows[0].createdAt).toBe(inserted.createdAt);
		});
	});

	it('executes insert, select, update, and delete synchronously', () => {
		withDbSync((db) => {
			const inserted = db.insertInto(users).values({ id: 1n, email: 'sync@example.com' }).executeSync();
			expect(inserted.email).toBe('sync@example.com');
			expect(inserted.id).toBe(1n);

			const rows = db.selectFrom(users).where(eq(users.id, 1n)).executeSync();
			expect(rows).toHaveLength(1);
			expect(rows[0].email).toBe('sync@example.com');

			const updated = db
				.updateTable(users)
				.set({ email: 'updated@example.com' })
				.where(eq(users.id, 1n))
				.executeSync();
			expect(updated).toHaveLength(1);
			expect(updated[0].email).toBe('updated@example.com');

			const deleted = db.deleteFrom(users).where(eq(users.id, 1n)).executeSync();
			expect(deleted).toBe(1n);

			const remaining = db.selectFrom(users).executeSync();
			expect(remaining).toHaveLength(0);
		});
	});
});
