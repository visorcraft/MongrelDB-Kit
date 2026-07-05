import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase } from './db.js';
import './query.js'; // imports the prototype patches (insertInto, selectFrom, etc.)
import { Schema, table, int, text } from './schema.js';
import { IndexBuildPolicyJs, type CacheStatsJs, type TriggerConfigJs } from '@visorcraft/mongreldb/native.js';

function freshDb() {
	const dir = mkdtempSync(join(tmpdir(), 'kit-tier3-test-'));
	const schema = new Schema([
		table('widgets', {
			columns: [int('id', { primaryKey: true }), text('name', { nullable: true })],
			primaryKey: ['id']
		})
	]);
	const db = KitDatabase.openSync(dir, schema);
	return { db, dir };
}

describe('Tier 3: storage tuning, introspection, WriteBuffer', () => {
	it('trigger config round-trips (set then read)', () => {
		const { db, dir } = freshDb();
		try {
			db.setRecursiveTriggers(true);
			let cfg: TriggerConfigJs = db.triggerConfig();
			expect(cfg.recursiveTriggers).toBe(true);

			db.setRecursiveTriggers(false);
			cfg = db.triggerConfig();
			expect(cfg.recursiveTriggers).toBe(false);
			expect(cfg.maxDepth).toBeGreaterThan(0);

			// Full config set.
			db.setTriggerConfig({ recursiveTriggers: true, maxDepth: 16, maxLoopIterations: 5000 });
			cfg = db.triggerConfig();
			expect(cfg.recursiveTriggers).toBe(true);
			expect(cfg.maxDepth).toBe(16);
			expect(cfg.maxLoopIterations).toBe(5000);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('introspection: run_count, memtable_len, page_cache_stats resolve', () => {
		const { db, dir } = freshDb();
		try {
			// Insert a row so the table has state.
			db.insertInto(db.schema.table('widgets')).values({ id: 1n, name: 'w1' }).executeSync();

			const runCount = db.tableRunCount('widgets');
			expect(typeof runCount).toBe('number');
			expect(runCount).toBeGreaterThanOrEqual(0);

			const memtableLen = db.tableMemtableLen('widgets');
			expect(typeof memtableLen).toBe('number');

			const stats: CacheStatsJs = db.tablePageCacheStats('widgets');
			expect(stats).toHaveProperty('hits');
			expect(stats).toHaveProperty('misses');
			expect(stats).toHaveProperty('hitRate');
			expect(typeof stats.hitRate).toBe('number');
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('storage tuning setters do not throw', () => {
		const { db, dir } = freshDb();
		try {
			db.setSpillThreshold(1_000_000);
			db.setTableCompactionZstdLevel('widgets', 3);
			db.setTableResultCacheMaxBytes('widgets', 64_000_000);
			db.setTableMutableRunSpillBytes('widgets', 4_000_000);
			db.setTableSyncByteThreshold('widgets', 1_000_000);
			db.setTableIndexBuildPolicy('widgets', IndexBuildPolicyJs.Deferred);
			db.setTableIndexBuildPolicy('widgets', IndexBuildPolicyJs.Eager);
			// If none threw, the test passes.
			expect(true).toBe(true);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('WriteBuffer batches writes and flushes', () => {
		const { db, dir } = freshDb();
		try {
			const wb = db.writeBuffer('widgets', 100);
			// Buffer two writes (below threshold — no auto-flush).
			wb.put([{ columnId: 1, int64: 1n }, { columnId: 2, text: 'a' }]);
			const auto = wb.put([{ columnId: 1, int64: 2n }, { columnId: 2, text: 'b' }]);
			expect(auto).toBeNull(); // no auto-flush at 2 rows (threshold 100)

			const epoch = wb.flush();
			expect(typeof epoch).toBe('bigint');
			expect(epoch).toBeGreaterThan(0n);

			// Verify the rows landed.
			const count = db.nativeDb.table('widgets').count();
			expect(count).toBe(2n);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
