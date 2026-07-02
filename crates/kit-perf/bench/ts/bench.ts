/**
 * Cross-language benchmark: TypeScript Kit.
 *
 * Same workload as the Rust `kit-perf` runner: seed N rows, then measure
 * single-row insert/update/delete + bulk-ingest throughput. Run from the
 * packages/kit directory after `npm run build`.
 *
 *   npx tsx ../../crates/kit-perf/bench/ts/bench.ts
 */
import { Database, table, column } from '../../src/index.js';

function usersSchema() {
	return table('users', {
		id: column.int64().primaryKey(),
		name: column.text(),
		cost: column.float64(),
	});
}

function median(arr: number[]): number {
	const s = [...arr].sort((a, b) => a - b);
	return s[Math.floor(s.length / 2)];
}

function us(ns: number): string {
	const s = ns / 1e9;
	if (s >= 1) return `${s.toFixed(2)} s`;
	if (s >= 1e-3) return `${(s * 1e3).toFixed(2)} ms`;
	return `${(s * 1e6).toFixed(1)} us`;
}

function bench(n: number) {
	const dir = `${import.meta.dirname ?? '.'}/_bench_ts_${n}`;
	const { rmSync } = require('fs');
	try { rmSync(dir, { recursive: true }); } catch {}
	const db = Database.create(dir, [usersSchema()]);
	const t = usersSchema();

	// Seed via valuesMany (one transaction).
	const seed: Record<string, unknown>[] = [];
	for (let i = 1; i <= n; i++) seed.push({ id: BigInt(i), name: 'City', cost: 199.99 + i });
	db.insertInto(t).valuesMany(seed).executeSync();

	// Single insert + commit.
	const inserts: number[] = [];
	for (let i = 0; i < 7; i++) {
		const start = process.hrtime.bigint();
		db.insertInto(t).values({ id: BigInt(n + 1 + i), name: 'CityX', cost: 1.0 }).executeSync();
		inserts.push(Number(process.hrtime.bigint() - start));
	}

	// Single update + commit.
	const updates: number[] = [];
	for (let i = 0; i < 7; i++) {
		const pk = BigInt(i + 1);
		const start = process.hrtime.bigint();
		db.updateTable(t).set({ cost: 99.0 + i }).where(pk).executeSync();
		updates.push(Number(process.hrtime.bigint() - start));
	}

	// Single delete + commit.
	const deletes: number[] = [];
	for (let i = 0; i < 7; i++) {
		const pk = BigInt(n - 6 + i);
		const start = process.hrtime.bigint();
		db.deleteFrom(t).where(pk).executeSync();
		deletes.push(Number(process.hrtime.bigint() - start));
	}

	console.log(`### TS Kit — N = ${n}`);
	console.log(`| single_insert | single_update | delete_one |`);
	console.log(`|---|---|---|`);
	console.log(`| ${us(median(inserts))} | ${us(median(updates))} | ${us(median(deletes))} |`);
	console.log();
}

function bulk(n: number) {
	const dir = `${import.meta.dirname ?? '.'}/_bench_ts_bulk_${n}`;
	const { rmSync } = require('fs');
	try { rmSync(dir, { recursive: true }); } catch {}
	const db = Database.create(dir, [usersSchema()]);
	const t = usersSchema();
	const seed: Record<string, unknown>[] = [];
	for (let i = 1; i <= n; i++) seed.push({ id: BigInt(i), name: 'City', cost: 199.99 + i });
	const start = process.hrtime.bigint();
	db.insertInto(t).valuesMany(seed).executeSync();
	const secs = Number(process.hrtime.bigint() - start) / 1e9;
	console.log(`### TS Kit bulk — N = ${n}`);
	console.log(`| Melem/s |`);
	console.log(`|---|`);
	console.log(`| ${(n / secs / 1e6).toFixed(1)} |`);
	console.log();
}

const N = Number(process.argv[2] ?? 100000);
bench(100);
bulk(N);
