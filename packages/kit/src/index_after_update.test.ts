/**
 * Regression: after updateTable.set, a row must remain listable by secondary
 * index on an unchanged FK-like column (PK still finds it either way).
 *
 * Also locks product patch semantics on the live path:
 *  - partial set
 *  - full-row-style spread with complete values
 *  - undefined keys omitted (do not NULL columns)
 *  - explicit null clears nullable columns
 */
import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import { Schema, table, int, text, index } from './schema.js';
import { eq } from './query.js';

function makeTempDir(): string {
	return mkdtempSync(join(tmpdir(), 'kit-idx-after-upd-'));
}

const segments = table('segments', {
	columns: [
		int('id', { primaryKey: true }),
		int('trip_id', { nullable: false }),
		text('title', { nullable: true }),
		text('details', { nullable: true })
	],
	primaryKey: ['id'],
	indexes: [index(['trip_id'], { name: 'segments_trip_idx' })]
});

const schema = new Schema([segments]);

function withDb(fn: (db: KitDatabase) => void): void {
	const dir = makeTempDir();
	const db = KitDatabase.openSync(dir, schema);
	try {
		fn(db);
	} finally {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	}
}

function listByTrip(db: KitDatabase, tripId: bigint) {
	return db.selectFrom(segments).where(eq(segments.trip_id, tripId)).executeSync();
}

function seedThree(db: KitDatabase) {
	db.insertInto(segments)
		.values({ id: 10n, trip_id: 42n, title: 'Golden Jade', details: null })
		.executeSync();
	db.insertInto(segments)
		.values({ id: 11n, trip_id: 42n, title: 'Outbound', details: null })
		.executeSync();
	db.insertInto(segments)
		.values({ id: 12n, trip_id: 99n, title: 'Other trip', details: null })
		.executeSync();
}

const trips = table('trips', {
	columns: [
		int('id', { primaryKey: true }),
		int('owner_id', { nullable: false }),
		text('name', { nullable: true })
	],
	primaryKey: ['id'],
	indexes: [index(['owner_id'], { name: 'trips_owner_idx' })]
});

const tripsSchema = new Schema([trips]);

function withTripsDb(fn: (db: KitDatabase) => void): void {
	const dir = makeTempDir();
	const db = KitDatabase.openSync(dir, tripsSchema);
	try {
		fn(db);
	} finally {
		db.close();
		rmSync(dir, { recursive: true, force: true });
	}
}

describe('secondary index after updateTable.set', () => {
	it('partial set keeps the row listable by the unchanged trip_id index', () => {
		withDb((db) => {
			seedThree(db);
			expect(listByTrip(db, 42n).map((r) => r.id).sort()).toEqual([10n, 11n]);

			const updated = db
				.updateTable(segments)
				.set({ title: 'Golden Jade Suvarnabhumi' })
				.where(eq(segments.id, 10n))
				.executeSync();
			expect(updated).toHaveLength(1);
			expect(updated[0].title).toBe('Golden Jade Suvarnabhumi');
			expect(updated[0].trip_id).toBe(42n);

			const byPk = db.selectFrom(segments).where(eq(segments.id, 10n)).executeSync();
			expect(byPk).toHaveLength(1);
			expect(byPk[0].title).toBe('Golden Jade Suvarnabhumi');

			const listed = listByTrip(db, 42n);
			expect(listed.map((r) => r.id).sort()).toEqual([10n, 11n]);
			expect(listed.find((r) => r.id === 10n)?.title).toBe('Golden Jade Suvarnabhumi');
			expect(listByTrip(db, 99n).map((r) => r.id)).toEqual([12n]);

			db.updateTable(segments)
				.set({ details: '{"room":"Family Quadruple"}' })
				.where(eq(segments.id, 10n))
				.executeSync();
			expect(listByTrip(db, 42n).map((r) => r.id).sort()).toEqual([10n, 11n]);
			const again = db.selectFrom(segments).where(eq(segments.id, 10n)).executeSync()[0];
			expect(again.title).toBe('Golden Jade Suvarnabhumi');
			expect(again.details).toBe('{"room":"Family Quadruple"}');
		});
	});

	it('full-row spread set keeps the trip_id secondary index when values are complete', () => {
		withDb((db) => {
			seedThree(db);
			const existing = db.selectFrom(segments).where(eq(segments.id, 10n)).executeSync()[0];
			const patch = {
				...existing,
				title: 'Hotel renamed',
				details: 'via full-row spread'
			};
			db.updateTable(segments)
				.set(patch)
				.where(eq(segments.id, 10n))
				.executeSync();

			const byPk = db.selectFrom(segments).where(eq(segments.id, 10n)).executeSync()[0];
			expect(byPk.title).toBe('Hotel renamed');
			expect(byPk.trip_id).toBe(42n);
			expect(listByTrip(db, 42n).map((r) => r.id).sort()).toEqual([10n, 11n]);
		});
	});

	it('undefined in set() omits the key (does not write SQL NULL)', () => {
		withDb((db) => {
			db.insertInto(segments)
				.values({
					id: 1n,
					trip_id: 7n,
					title: 'keep me',
					details: 'present'
				})
				.executeSync();

			// Old behavior nulled title; product pipeline now treats undefined as omit.
			db.updateTable(segments)
				.set({ title: undefined as unknown as string })
				.where(eq(segments.id, 1n))
				.executeSync();

			const row = db.selectFrom(segments).where(eq(segments.id, 1n)).executeSync()[0];
			expect(row.title).toBe('keep me');
			expect(row.details).toBe('present');
			expect(listByTrip(db, 7n).map((r) => r.id)).toEqual([1n]);
		});
	});

	it('full-row-style spread with undefined fields does not wipe columns or lose index', () => {
		withDb((db) => {
			seedThree(db);
			const existing = db.selectFrom(segments).where(eq(segments.id, 10n)).executeSync()[0];
			// Sparse / partial object mixed into a spread — previously could NULL
			// `details` via toCells(undefined).
			const badSpread = {
				...existing,
				title: 'patched title',
				details: undefined as unknown as string | null
			};
			db.updateTable(segments)
				.set(badSpread)
				.where(eq(segments.id, 10n))
				.executeSync();

			const row = db.selectFrom(segments).where(eq(segments.id, 10n)).executeSync()[0];
			expect(row.title).toBe('patched title');
			expect(row.details).toBeNull(); // was already null on seed row for id 10
			expect(row.trip_id).toBe(42n);
			expect(listByTrip(db, 42n).map((r) => r.id).sort()).toEqual([10n, 11n]);

			// Non-null details must survive undefined in a later spread.
			db.updateTable(segments)
				.set({ details: 'solid details' })
				.where(eq(segments.id, 10n))
				.executeSync();
			db.updateTable(segments)
				.set({
					title: 'again',
					details: undefined as unknown as string
				})
				.where(eq(segments.id, 10n))
				.executeSync();
			const again = db.selectFrom(segments).where(eq(segments.id, 10n)).executeSync()[0];
			expect(again.title).toBe('again');
			expect(again.details).toBe('solid details');
			expect(listByTrip(db, 42n).map((r) => r.id).sort()).toEqual([10n, 11n]);
		});
	});

	it('explicit null clears a nullable column while index membership holds', () => {
		withDb((db) => {
			db.insertInto(segments)
				.values({
					id: 2n,
					trip_id: 8n,
					title: 'wipe me',
					details: 'stay'
				})
				.executeSync();
			db.updateTable(segments)
				.set({ title: null })
				.where(eq(segments.id, 2n))
				.executeSync();
			const row = db.selectFrom(segments).where(eq(segments.id, 2n)).executeSync()[0];
			expect(row.title).toBeNull();
			expect(row.details).toBe('stay');
			expect(listByTrip(db, 8n).map((r) => r.id)).toEqual([2n]);
		});
	});

	it('partial set index membership survives reopen', () => {
		const dir = makeTempDir();
		try {
			{
				const db = KitDatabase.openSync(dir, schema);
				seedThree(db);
				db.updateTable(segments)
					.set({ title: 'after' })
					.where(eq(segments.id, 10n))
					.executeSync();
				db.close();
			}
			{
				const db = KitDatabase.openSync(dir, schema);
				expect(
					db.selectFrom(segments).where(eq(segments.id, 10n)).executeSync()[0].title
				).toBe('after');
				expect(listByTrip(db, 42n).map((r) => r.id).sort()).toEqual([10n, 11n]);
				db.close();
			}
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe('trips owner list after updates (Roamarr regression)', () => {
	it('lists both trips by owner_id after many title-only updates', () => {
		withTripsDb((db) => {
			db.insertInto(trips)
				.values({ id: 1n, owner_id: 1n, name: 'August' })
				.executeSync();
			db.insertInto(trips)
				.values({ id: 2n, owner_id: 1n, name: 'December' })
				.executeSync();

			const listOwner = () =>
				db
					.selectFrom(trips)
					.where(eq(trips.owner_id, 1n))
					.executeSync()
					.map((r) => r.id)
					.sort();

			expect(listOwner()).toEqual([1n, 2n]);

			for (const name of ['August v2', 'August v3', 'August final']) {
				db.updateTable(trips).set({ name }).where(eq(trips.id, 1n)).executeSync();
			}

			expect(listOwner()).toEqual([1n, 2n]);
			expect(
				db.selectFrom(trips).where(eq(trips.id, 1n)).executeSync()[0].name
			).toBe('August final');
			expect(db.selectFrom(trips).where(eq(trips.id, 2n)).executeSync()).toHaveLength(1);
		});
	});

	it('int primary key eq uses PkInt64 (HOT), not only RangeInt', () => {
		withTripsDb((db) => {
			db.insertInto(trips)
				.values({ id: 42n, owner_id: 9n, name: 'solo' })
				.executeSync();
			const plan = db.selectFrom(trips).where(eq(trips.id, 42n)).explain();
			expect(plan.pushedConditions).toContain('PkInt64');
			expect(db.selectFrom(trips).where(eq(trips.id, 42n)).executeSync()).toHaveLength(1);
		});
	});
});
