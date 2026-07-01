import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ConditionKind } from 'mongreldb/native.js';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, real, unique, index, foreignKey, timestamp, date } from './schema.js';
import { nowDefault } from './defaults.js';
import {
	eq,
	gt,
	asc,
	desc,
	and,
	inList,
	not,
	like,
	contains,
	notInList,
	inSubquery,
	exists,
	notExists,
	count,
	countColumn,
	countDistinct,
	sum,
	min,
	max,
	avg
} from './query.js';
import type { JoinRow } from './query.js';
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

const orders = table('orders', {
	columns: [
		int('id', { primaryKey: true }),
		int('userId', { nullable: false }),
		text('status', { nullable: false }),
		int('amount', { nullable: false }),
		real('total', { nullable: false })
	],
	primaryKey: ['id']
});

const tags = table('tags', {
	columns: [int('id', { primaryKey: true }), text('label', { nullable: false })],
	primaryKey: ['id']
});

const testSchema = new Schema([users, posts, groupMembers, events, orders, tags]);

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

function traceNativeTable(db: KitDatabase, tableName: string): {
	queryCalls: () => number;
	countWhereCalls: () => number;
	conditions: () => unknown[][];
	restore: () => void;
} {
	const native = db.nativeDb as any;
	const originalTable = native.table;
	const calls = { query: 0, countWhere: 0, conditions: [] as unknown[][] };
	native.table = function (name: string) {
		const handle = originalTable.call(this, name);
		if (name !== tableName) return handle;
		return new Proxy(handle, {
			get(target, prop, receiver) {
				if (prop === 'query') {
					return (conditions: unknown[]) => {
						calls.query++;
						calls.conditions.push(conditions);
						return target.query(conditions);
					};
				}
				if (prop === 'countWhere') {
					return (conditions: unknown[]) => {
						calls.countWhere++;
						calls.conditions.push(conditions);
						return target.countWhere(conditions);
					};
				}
				return Reflect.get(target, prop, receiver);
			}
		});
	};
	return {
		queryCalls: () => calls.query,
		countWhereCalls: () => calls.countWhere,
		conditions: () => calls.conditions,
		restore: () => {
			native.table = originalTable;
		}
	};
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

	it('pushes single-column integer primary-key equality through the native range path', () => {
		withDbSync((db) => {
			db.insertInto(users).values({ id: 1n, email: 'one@example.com' }).executeSync();
			db.insertInto(users).values({ id: 2n, email: 'two@example.com' }).executeSync();
			const trace = traceNativeTable(db, users.name);
			try {
				const rows = db.selectFrom(users).where(eq(users.id, 1n)).executeSync();
				expect(rows).toHaveLength(1);
				expect(rows[0].email).toBe('one@example.com');
				expect(trace.queryCalls()).toBe(1);
				expect(trace.conditions()[0]).toHaveLength(1);
				expect((trace.conditions()[0]![0] as { kind: ConditionKind }).kind).toBe(
					ConditionKind.RangeInt
				);
			} finally {
				trace.restore();
			}
		});
	});

	it('collapses pushable AND predicates into one native query', () => {
		withDbSync((db) => {
			db.insertInto(users).values({ id: 1n, email: 'one@example.com' }).executeSync();
			db.insertInto(users).values({ id: 2n, email: 'two@example.com' }).executeSync();
			const trace = traceNativeTable(db, users.name);
			try {
				const rows = db
					.selectFrom(users)
					.where(and(eq(users.id, 1n), eq(users.email, 'one@example.com')))
					.executeSync();
				expect(rows).toHaveLength(1);
				expect(rows[0].email).toBe('one@example.com');
				expect(trace.queryCalls()).toBe(1);
				expect(trace.conditions()[0]).toHaveLength(2);
			} finally {
				trace.restore();
			}
		});
	});

	it('uses native BitmapIn cardinality for filtered counts', () => {
		withDbSync((db) => {
			db.insertInto(users).values({ id: 1n, email: 'one@example.com' }).executeSync();
			db.insertInto(users).values({ id: 2n, email: 'two@example.com' }).executeSync();
			db.insertInto(users).values({ id: 3n, email: 'three@example.com' }).executeSync();
			const trace = traceNativeTable(db, users.name);
			try {
				const total = db
					.selectFrom(users)
					.where(inList(users.email, ['one@example.com', 'three@example.com']))
					.selectCount()
					.executeSync();
				expect(total).toBe(2n);
				expect(trace.queryCalls()).toBe(0);
				expect(trace.countWhereCalls()).toBe(1);
				expect(trace.conditions()[0]).toHaveLength(1);
				expect((trace.conditions()[0]![0] as { kind: ConditionKind }).kind).toBe(
					ConditionKind.BitmapIn
				);
			} finally {
				trace.restore();
			}
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

describe('query builder extensions', () => {
	async function seedOrders(db: KitDatabase): Promise<void> {
		await db.insertInto(users).values({ id: 1n, email: 'alice@example.com' }).execute();
		await db.insertInto(users).values({ id: 2n, email: 'bob@example.org' }).execute();
		await db.insertInto(users).values({ id: 3n, email: 'carol@example.com' }).execute();
		// user 1: two paid orders; user 2: one pending; user 3: no orders.
		await db
			.insertInto(orders)
			.values({ id: 1n, userId: 1n, status: 'paid', amount: 100n, total: 10.5 })
			.execute();
		await db
			.insertInto(orders)
			.values({ id: 2n, userId: 1n, status: 'paid', amount: 200n, total: 20.0 })
			.execute();
		await db
			.insertInto(orders)
			.values({ id: 3n, userId: 2n, status: 'pending', amount: 50n, total: 5.25 })
			.execute();
	}

	const at = (r: JoinRow, t: string, c: string): unknown => (r[t] as Record<string, unknown>)[c];

	it('computes sum/min/max/avg honoring where', async () => {
		await withDb(async (db) => {
			await seedOrders(db);

			expect(await db.selectFrom(orders).selectSum(orders.amount).execute()).toBe(350n);
			expect(
				await db
					.selectFrom(orders)
					.where(eq(orders.status, 'paid'))
					.selectSum(orders.amount)
					.execute()
			).toBe(300n);
			expect(await db.selectFrom(orders).selectMin(orders.amount).execute()).toBe(50n);
			expect(await db.selectFrom(orders).selectMax(orders.amount).execute()).toBe(200n);
			expect(await db.selectFrom(orders).selectAvg(orders.amount).execute()).toBeCloseTo(350 / 3);

			// a float column sums to a number, not a bigint
			const floatSum = await db.selectFrom(orders).selectSum(orders.total).execute();
			expect(typeof floatSum).toBe('number');
			expect(floatSum).toBeCloseTo(35.75);

			// aggregates over an empty set: null for max, 0 for sum
			expect(
				await db
					.selectFrom(orders)
					.where(eq(orders.status, 'void'))
					.selectMax(orders.amount)
					.execute()
			).toBeNull();
			expect(
				await db
					.selectFrom(orders)
					.where(eq(orders.status, 'void'))
					.selectSum(orders.amount)
					.execute()
			).toBe(0n);
		});
	});

	it('supports distinct', async () => {
		await withDb(async (db) => {
			await seedOrders(db);
			const statuses = await db
				.selectFrom(orders)
				.select([orders.status])
				.distinct()
				.orderBy(asc(orders.status))
				.execute();
			expect(statuses).toEqual([{ status: 'paid' }, { status: 'pending' }]);
		});
	});

	it('supports like, contains, notInList, and not', async () => {
		await withDb(async (db) => {
			await seedOrders(db);

			const dotCom = await db
				.selectFrom(users)
				.where(like(users.email, '%example.com'))
				.orderBy(asc(users.id))
				.execute();
			expect(dotCom.map((u) => u.id)).toEqual([1n, 3n]);

			const hasBob = await db.selectFrom(users).where(contains(users.email, 'bob')).execute();
			expect(hasBob.map((u) => u.id)).toEqual([2n]);

			const others = await db.selectFrom(users).where(notInList(users.id, [1n, 2n])).execute();
			expect(others.map((u) => u.id)).toEqual([3n]);

			const notFirst = await db
				.selectFrom(users)
				.where(not(eq(users.id, 1n)))
				.orderBy(asc(users.id))
				.execute();
			expect(notFirst.map((u) => u.id)).toEqual([2n, 3n]);
		});
	});

	it('performs inner, left, and cross joins', async () => {
		await withDb(async (db) => {
			await seedOrders(db);
			await db.insertInto(tags).values({ id: 1n, label: 'red' }).execute();
			await db.insertInto(tags).values({ id: 2n, label: 'blue' }).execute();

			const inner = db
				.selectFrom(users)
				.innerJoin(orders, (r) => at(r, 'orders', 'userId') === at(r, 'users', 'id'))
				.executeSync();
			expect(inner).toHaveLength(3); // user1 x2, user2 x1, user3 x0
			for (const row of inner) {
				expect(at(row, 'users', 'id')).toBe(at(row, 'orders', 'userId'));
			}

			const left = db
				.selectFrom(users)
				.leftJoin(orders, (r) => at(r, 'orders', 'userId') === at(r, 'users', 'id'))
				.where((r) => at(r, 'users', 'id') === 3n)
				.executeSync();
			expect(left).toHaveLength(1);
			expect(left[0].orders).toBeNull();
			expect(at(left[0], 'users', 'id')).toBe(3n);

			const cross = db.selectFrom(tags).crossJoin(orders).executeSync();
			expect(cross).toHaveLength(6); // 2 tags x 3 orders
		});
	});

	it('supports inSubquery, exists, and notExists', async () => {
		await withDb(async (db) => {
			await seedOrders(db);

			const bigSpenders = db
				.selectFrom(orders)
				.where(gt(orders.amount, 80n))
				.select([orders.userId]);
			const buyers = await db
				.selectFrom(users)
				.where(inSubquery(users.id, bigSpenders))
				.orderBy(asc(users.id))
				.execute();
			expect(buyers.map((u) => u.id)).toEqual([1n]);

			const pending = db.selectFrom(orders).where(eq(orders.status, 'pending'));
			expect(await db.selectFrom(users).where(exists(pending)).execute()).toHaveLength(3);

			const voids = db.selectFrom(orders).where(eq(orders.status, 'void'));
			expect(await db.selectFrom(users).where(exists(voids)).execute()).toHaveLength(0);
			expect(await db.selectFrom(users).where(notExists(voids)).execute()).toHaveLength(3);
		});
	});

	it('groups with aggregates and having', async () => {
		await withDb(async (db) => {
			await seedOrders(db);

			const byStatus = db
				.selectFrom(orders)
				.groupBy(orders.status)
				.aggregate({
					n: count(),
					total: sum(orders.amount),
					lo: min(orders.amount),
					hi: max(orders.amount),
					mean: avg(orders.amount)
				})
				.executeSync();

			const paid = byStatus.find((g) => g.status === 'paid')!;
			expect(paid.n).toBe(2n);
			expect(paid.total).toBe(300n);
			expect(paid.lo).toBe(100n);
			expect(paid.hi).toBe(200n);
			expect(paid.mean).toBeCloseTo(150);

			const pending = byStatus.find((g) => g.status === 'pending')!;
			expect(pending.n).toBe(1n);
			expect(pending.total).toBe(50n);

			const big = db
				.selectFrom(orders)
				.groupBy(orders.status)
				.aggregate({ total: sum(orders.amount) })
				.having((g) => (g.total as bigint) >= 100n)
				.executeSync();
			expect(big).toHaveLength(1);
			expect(big[0].status).toBe('paid');
		});
	});

	it('counts non-null and distinct column values per group', async () => {
		await withDb(async (db) => {
			await seedOrders(db);
			// paid: 2 orders, both userId 1, amounts {100,200}; pending: 1 order userId 2.
			const byStatus = db
				.selectFrom(orders)
				.groupBy(orders.status)
				.aggregate({
					users: countColumn(orders.userId),
					distinctUsers: countDistinct(orders.userId),
					distinctAmounts: countDistinct(orders.amount)
				})
				.executeSync();

			const paid = byStatus.find((g) => g.status === 'paid')!;
			expect(paid.users).toBe(2n); // COUNT(userId): 2 non-null
			expect(paid.distinctUsers).toBe(1n); // COUNT(DISTINCT userId): {1}
			expect(paid.distinctAmounts).toBe(2n); // {100, 200}

			const pending = byStatus.find((g) => g.status === 'pending')!;
			expect(pending.distinctUsers).toBe(1n); // {2}
		});
	});

	it('materializes a CTE for a later selectFrom', async () => {
		await withDb(async (db) => {
			await seedOrders(db);

			const scope = db.with('big_orders', db.selectFrom(orders).where(gt(orders.amount, 80n)));

			const all = await scope.selectFrom('big_orders').orderBy(asc(orders.id)).execute();
			expect(all.map((r) => r.id)).toEqual([1n, 2n]);

			// the materialized rows can be filtered again in memory
			const filtered = await scope
				.selectFrom('big_orders')
				.where(gt(orders.amount, 150n))
				.execute();
			expect(filtered).toHaveLength(1);
			expect(filtered[0].id).toBe(2n);

			// count over a CTE works too
			const paidCount = await db
				.with('paid', db.selectFrom(orders).where(eq(orders.status, 'paid')))
				.selectFrom('paid')
				.selectCount()
				.execute();
			expect(paidCount).toBe(2n);
		});
	});

	it('exposes the native database as a raw escape hatch', async () => {
		await withDb(async (db) => {
			await seedOrders(db);
			expect(db.nativeDb.table('orders').count()).toBe(3n);
		});
	});
});

describe('DML returning and upsert', () => {
	it('insert returning projects the row to requested columns', async () => {
		await withDb(async (db) => {
			const row = await db
				.insertInto(users)
				.values({ id: 1n, email: 'a@example.com' })
				.returning(users.email)
				.execute();
			expect(row).toEqual({ email: 'a@example.com' });

			const full = await db.insertInto(users).values({ id: 2n, email: 'b@example.com' }).execute();
			expect(full.id).toBe(2n);
			expect(full.email).toBe('b@example.com');
		});
	});

	it('insert on conflict do nothing returns the existing row', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();

			const row = await db
				.insertInto(users)
				.values({ id: 1n, email: 'b@example.com' })
				.onConflictDoNothing()
				.returning(users.email)
				.execute();
			expect(row).toEqual({ email: 'a@example.com' });

			const rows = await db.selectFrom(users).where(eq(users.id, 1n)).execute();
			expect(rows[0].email).toBe('a@example.com');
		});
	});

	it('insert on conflict do update merges the patch into the existing row', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();

			const row = await db
				.insertInto(users)
				.values({ id: 1n, email: 'ignored@example.com' })
				.onConflictDoUpdate({ email: 'updated@example.com' })
				.returning(users.id, users.email)
				.execute();
			expect(row).toEqual({ id: 1n, email: 'updated@example.com' });

			const rows = await db.selectFrom(users).where(eq(users.id, 1n)).execute();
			expect(rows[0].email).toBe('updated@example.com');
		});
	});

	it('update returning returns the post-image projected rows', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();
			await db.insertInto(users).values({ id: 2n, email: 'b@example.com' }).execute();

			const rows = await db
				.updateTable(users)
				.set({ email: 'changed@example.com' })
				.where(eq(users.id, 1n))
				.returning(users.id, users.email)
				.execute();
			expect(rows).toEqual([{ id: 1n, email: 'changed@example.com' }]);
		});
	});

	it('delete returning returns the pre-image projected rows', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();
			await db.insertInto(users).values({ id: 2n, email: 'b@example.com' }).execute();

			const rows = await db
				.deleteFrom(users)
				.where(eq(users.id, 1n))
				.returning(users.id, users.email)
				.execute();
			expect(rows).toEqual([{ id: 1n, email: 'a@example.com' }]);

			const remaining = await db.selectFrom(users).execute();
			expect(remaining).toHaveLength(1);
			expect(remaining[0].id).toBe(2n);
		});
	});

	it('truncateTable empties the table and clears unique guards', async () => {
		const categories = table('categories', {
			columns: [int('id', { primaryKey: true }), text('code', { nullable: false })],
			primaryKey: ['id'],
			unique: [unique(['code'], { name: 'categories_code_uq' })]
		});

		const dir = makeTempDir();
		const db = await KitDatabase.open(dir, new Schema([categories]));
		try {
			await db.insertInto(categories).values({ id: 1n, code: 'a' }).execute();
			await db.insertInto(categories).values({ id: 2n, code: 'b' }).execute();

			db.truncateTable(categories.name);

			expect(await db.selectFrom(categories).execute()).toHaveLength(0);

			// Reusing the same PK and unique value must succeed after truncate.
			const reused = await db
				.insertInto(categories)
				.values({ id: 1n, code: 'a' })
				.returning(categories.id, categories.code)
				.execute();
			expect(reused).toEqual({ id: 1n, code: 'a' });
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('update returning returns an empty array when no rows match', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();

			const rows = await db
				.updateTable(users)
				.set({ email: 'changed@example.com' })
				.where(eq(users.id, 99n))
				.returning(users.id, users.email)
				.execute();
			expect(rows).toEqual([]);
		});
	});

	it('delete returning returns an empty array when no rows match', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();

			const rows = await db
				.deleteFrom(users)
				.where(eq(users.id, 99n))
				.returning(users.id, users.email)
				.execute();
			expect(rows).toEqual([]);
		});
	});

	it('onConflictDoUpdate can change the primary key and clears the old guard', async () => {
		await withDb(async (db) => {
			await db
				.insertInto(groupMembers)
				.values({ group_id: 1n, user_id: 10n, role: 'member' })
				.execute();

			const row = await db
				.insertInto(groupMembers)
				.values({ group_id: 1n, user_id: 10n, role: 'ignored' })
				.onConflictDoUpdate({ user_id: 11n })
				.returning(groupMembers.group_id, groupMembers.user_id, groupMembers.role)
				.execute();
			expect(row).toEqual({ group_id: 1n, user_id: 11n, role: 'member' });

			const moved = await db
				.selectFrom(groupMembers)
				.where(and(eq(groupMembers.group_id, 1n), eq(groupMembers.user_id, 11n)))
				.execute();
			expect(moved).toHaveLength(1);

			const oldPk = await db
				.selectFrom(groupMembers)
				.where(and(eq(groupMembers.group_id, 1n), eq(groupMembers.user_id, 10n)))
				.execute();
			expect(oldPk).toEqual([]);

			// Reusing the old PK should now succeed.
			await db
				.insertInto(groupMembers)
				.values({ group_id: 1n, user_id: 10n, role: 'returned' })
				.execute();
		});
	});

	it('onConflictDoUpdate rejects a patch that violates a foreign key', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'author@example.com' }).execute();
			await db.insertInto(posts).values({ id: 1n, authorId: 1n }).execute();

			await expect(
				db
					.insertInto(posts)
					.values({ id: 1n, authorId: 1n })
					.onConflictDoUpdate({ authorId: 99n })
					.execute()
			).rejects.toBeInstanceOf(KitForeignKeyError);
		});
	});

	it('truncateTable throws when the table is referenced by a foreign key', async () => {
		await withDb(async (db) => {
			await db.insertInto(users).values({ id: 1n, email: 'a@example.com' }).execute();
			await expect(() => db.truncateTable(users.name)).toThrow(
				'table users is referenced by foreign key(s): posts.posts_author_fk'
			);
		});
	});
});
