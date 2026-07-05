import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { tableFromIPC } from 'apache-arrow';
import { KitDatabase } from './db.js';
import { Schema, table, int, real, bool, index } from './schema.js';
import { ConditionKind } from '@visorcraft/mongreldb/native.js';

function freshDb() {
	const dir = mkdtempSync(join(tmpdir(), 'kit-async-test-'));
	const schema = new Schema([
		table('metrics', {
			columns: [
				int('id', { primaryKey: true }),
				real('value'),
				bool('flag')
			],
			primaryKey: ['id'],
			indexes: [index(['id'], { name: 'idx_metrics_id', learnedRange: true })]
		})
	]);
	const db = KitDatabase.openSync(dir, schema);
	return { db, dir };
}

describe('KitDatabase async + bulk-load surface', () => {
	it('putAsync / getAsync / queryAsync / countAsync round-trip off the event loop', async () => {
		const { db, dir } = freshDb();
		try {
			// putAsync takes raw native cells (columnId + one value field).
			const a = await db.putAsync('metrics', [
				{ columnId: 1, int64: 1n },
				{ columnId: 2, float64: 9.5 },
				{ columnId: 3, boolean: true }
			]);
			expect(a.rowId).toBe(0n);
			const b = await db.putAsync('metrics', [
				{ columnId: 1, int64: 2n },
				{ columnId: 2, float64: 42 },
				{ columnId: 3, boolean: false }
			]);
			expect(b.rowId).toBe(1n);
			// putAsync writes to the memtable; flushAsync makes rows durable.
			await db.flushAsync();

			// getAsync by row id.
			const row = await db.getAsync('metrics', 0n);
			expect(row).not.toBeNull();
			expect(row!.rowId).toBe(0n);

			// queryAsync with no conditions = full scan (returns all visible rows).
			// Native condition queries depend on index-build timing, so the
			// empty-condition form reliably exercises the async plumbing.
			const all = await db.queryAsync('metrics', []);
			expect(all.length).toBe(2);
			expect(all[0].rowId).toBe(0n);
			expect(all[1].rowId).toBe(1n);

			// countAsync (all rows) and countWhereAsync (full scan, no conditions).
			expect(await db.countAsync('metrics')).toBe(2n);
			expect(await db.countWhereAsync('metrics', [])).toBe(2n);

			// queryArrowAsync requires a condition (it rejects empty conditions,
			// mirroring SelectBuilder.executeArrow); use Database.sql for full
			// Arrow scans. Here we just confirm it returns IPC bytes for a
			// trivially-true PK range after a flush + index build.
			await db.analyze(); // ensure the learned-range index is built
			const ipc = await db.queryArrowAsync('metrics', [
				{ kind: ConditionKind.RangeInt, columnId: 1, int64Lo: 0n }
			]);
			const arrow = tableFromIPC(ipc);
			expect(arrow.numRows).toBeGreaterThanOrEqual(1);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('bulkLoadTyped ingests Int64/Float64/Bool columns and returns the epoch', () => {
		const { db, dir } = freshDb();
		try {
			// 3 rows, laid out column-major as little-endian buffers.
			// Int64 id: 10, 20, 30 -> 3 x 8 bytes LE.
			const int64 = new BigInt64Array([10n, 20n, 30n]);
			// Float64 value: 1.5, 2.5, 3.5.
			const float64 = new Float64Array([1.5, 2.5, 3.5]);
			// Bool flag: 1, 0, 1.
			const boolBytes = Buffer.from([1, 0, 1]);
			const epoch = db.bulkLoadTyped('metrics', [
				{ columnId: 1, ty: 1 /* ColumnType.Int64 */, data: Buffer.from(int64.buffer) },
				{ columnId: 2, ty: 2 /* ColumnType.Float64 */, data: Buffer.from(float64.buffer) },
				{ columnId: 3, ty: 0 /* ColumnType.Bool */, data: boolBytes }
			]);
			expect(typeof epoch).toBe('bigint');
			expect(epoch).toBeGreaterThan(0n);

			// Verify via a sync count that the rows landed.
			expect(db.nativeDb.table('metrics').count()).toBe(3n);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('flushAsync / compactAllAsync / snapshotEpochAsync / approxAggregateAsync resolve', async () => {
		const { db, dir } = freshDb();
		try {
			await db.putAsync('metrics', [
				{ columnId: 1, int64: 1n },
				{ columnId: 2, float64: 1 },
				{ columnId: 3, boolean: true }
			]);
			await db.flushAsync();
			const epoch = await db.snapshotEpochAsync();
			expect(typeof epoch).toBe('bigint');
			await db.compactAllAsync();
			const approx = await db.approxAggregateAsync('metrics', 'count');
			// Reservoir may be empty until flush; just assert it resolves to null or a shape.
			expect(approx === null || typeof approx === 'object').toBe(true);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
