/**
 * Live integration tests for the TypeScript RemoteDatabase against a real
 * mongreldb-server daemon.
 *
 * Boots the daemon as a child process (if the binary is available), exercises
 * every RemoteDatabase method with real engine behavior, then gracefully
 * shuts down.
 *
 * Skips automatically if no mongreldb-server binary is found.
 */

import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, chmodSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir as osTmpdir } from 'node:os';
import { join } from 'node:path';
import * as net from 'node:net';

// ── Daemon management ──────────────────────────────────────────────────────

const SERVER_VERSION = 'v0.52.0';
const DOWNLOAD_URL = `https://github.com/visorcraft/MongrelDB/releases/download/${SERVER_VERSION}/mongreldb-server-linux-x64`;

async function findServerBinary(): Promise<string | null> {
	const env = process.env.MONGRELDB_SERVER;
	if (env && existsSync(env)) return env;

	const candidates = [
		join(process.env.HOME || '', '.cargo/bin/mongreldb-server'),
		join(__dirname, '../../../../mongreldb/crates/mongreldb-server/target/release/mongreldb-server'),
	];
	for (const c of candidates) {
		if (existsSync(c)) return c;
	}

	// Download from GitHub releases (Linux x64 only)
	const cacheDir = join(osTmpdir(), 'mdb-test-server');
	const binaryPath = join(cacheDir, 'mongreldb-server');
	if (existsSync(binaryPath)) return binaryPath;

	try {
		mkdirSync(cacheDir, { recursive: true });
		console.log(`Downloading mongreldb-server from ${DOWNLOAD_URL}...`);
		const resp = await fetch(DOWNLOAD_URL);
		if (!resp.ok) return null;
		const buf = Buffer.from(await resp.arrayBuffer());
		writeFileSync(binaryPath, buf);
		chmodSync(binaryPath, 0o755);
		return binaryPath;
	} catch {
		return null;
	}
}

function freePort(): Promise<number> {
	return new Promise((resolve, reject) => {
		const srv = net.createServer();
		srv.listen(0, '127.0.0.1', () => {
			const addr = srv.address();
			if (addr && typeof addr === 'object') {
				const port = addr.port;
				srv.close(() => resolve(port));
			} else {
				reject(new Error('failed to get port'));
			}
		});
		srv.on('error', reject);
	});
}

function waitForHealth(url: string, timeoutMs = 15000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	return new Promise((resolve, reject) => {
		function poll() {
			if (Date.now() > deadline) {
				reject(new Error('daemon did not become healthy'));
				return;
			}
			fetch(`${url}/health`)
				.then((r) => {
					if (r.ok) resolve();
					else setTimeout(poll, 500);
				})
				.catch(() => setTimeout(poll, 500));
		}
		poll();
	});
}

let daemonProcess: ChildProcess | null = null;
let daemonUrl = '';

beforeAll(async () => {
	const binary = await findServerBinary();
	if (!binary) {
		console.warn('mongreldb-server binary not found; skipping live tests');
		return;
	}

	const port = await freePort();
	daemonUrl = `http://127.0.0.1:${port}`;
	const dbDir = mkdtempSync(join(osTmpdir(), 'mdb_live_ts_'));

	daemonProcess = spawn(binary, [dbDir, '--port', String(port)], {
		stdio: ['ignore', 'pipe', 'pipe'],
	});

	daemonProcess.on('error', (err) => {
		console.error('daemon spawn error:', err);
	});

	await waitForHealth(daemonUrl);
}, 30000);

afterAll(async () => {
	if (daemonProcess) {
		daemonProcess.kill('SIGTERM');
		await new Promise<void>((resolve) => {
			daemonProcess!.on('exit', () => resolve());
			setTimeout(() => {
				daemonProcess!.kill('SIGKILL');
				resolve();
			}, 10000);
		});
	}
});

// ── Tests ──────────────────────────────────────────────────────────────────

function canFindServerBinarySync(): boolean {
	if (process.env.MONGRELDB_SERVER && existsSync(process.env.MONGRELDB_SERVER)) return true;
	const candidates = [
		join(process.env.HOME || '', '.cargo/bin/mongreldb-server'),
		join(__dirname, '../../../../mongreldb/crates/mongreldb-server/target/release/mongreldb-server'),
		// Include the GitHub release download cache so the suite doesn't skip
		// when findServerBinary() downloaded the binary on a prior run.
		join(osTmpdir(), 'mdb-test-server', 'mongreldb-server'),
	];
	return candidates.some((c) => existsSync(c));
}

// Skip the suite if no daemon binary can be found.
const hasDaemon = canFindServerBinarySync();
describe.skipIf(!hasDaemon)('RemoteDatabase live tests', () => {
	test('health() returns ok', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		const result = remote.health();
		expect(result).toBeDefined();
	});

	test('tableNames() returns empty initially', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		// Create a table via SQL
		remote.sql('CREATE TABLE test_items (id BIGINT PRIMARY KEY, name VARCHAR(50))');
		const names = remote.tableNames();
		expect(names).toContain('test_items');
	});

	test('sql INSERT + count', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		remote.sql("INSERT INTO test_items (id, name) VALUES (1, 'widget')");
		remote.sql("INSERT INTO test_items (id, name) VALUES (2, 'gadget')");
		expect(remote.count('test_items')).toBe(2n);
	});

	test('sqlRows returns decoded rows', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		const rows = remote.sqlRows('SELECT * FROM test_items ORDER BY id');
		expect(rows.length).toBe(2);
		expect(rows[0].name).toBe('widget');
		expect(rows[1].name).toBe('gadget');
	});

	test('sql UPDATE + verify', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		remote.sql("UPDATE test_items SET name = 'updated' WHERE id = 1");
		const rows = remote.sqlRows('SELECT name FROM test_items WHERE id = 1');
		expect(rows[0].name).toBe('updated');
	});

	test('sql DELETE + verify count drops', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		remote.sql('DELETE FROM test_items WHERE id = 2');
		expect(remote.count('test_items')).toBe(1n);
	});

	test('compact() succeeds', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		const result = remote.compact();
		expect(result).toBeDefined();
		expect(typeof result.compacted).toBe('number');
	});

	test('compactTable() succeeds', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		const result = remote.compactTable('test_items');
		expect(typeof result).toBe('boolean');
	});

	test('retention settings and AS OF EPOCH time-travel reads', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);

		remote.setHistoryRetentionEpochs(100n);
		expect(remote.historyRetentionEpochs()).toBe(100n);
		expect(remote.earliestRetainedEpoch()).toBeGreaterThanOrEqual(0n);

		remote.sql(
			'CREATE TABLE time_travel (id BIGINT PRIMARY KEY, name VARCHAR(50))'
		);
		remote.sql("INSERT INTO time_travel (id, name) VALUES (1, 'orig')");
		const e1 = remote.commit('time_travel');

		remote.sql("UPDATE time_travel SET name = 'updated' WHERE id = 1");

		const past = remote.sqlRows(
			`SELECT name FROM time_travel AS OF EPOCH ${e1} WHERE id = 1`
		);
		expect(past).toHaveLength(1);
		expect(past[0].name).toBe('orig');
	});

	test('sql DROP TABLE', async () => {
		const { RemoteDatabase } = await import('./remote.js');
		const remote = new RemoteDatabase(daemonUrl);
		remote.sql('DROP TABLE test_items');
		expect(remote.tableNames()).not.toContain('test_items');
	});
});
