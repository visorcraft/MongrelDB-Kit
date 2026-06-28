import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, unique, index, foreignKey } from './schema.js';
import { eq, asc, desc } from './query.js';
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

const testSchema = new Schema([users, posts]);

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
});
