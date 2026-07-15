import { describe, it, expect } from 'vitest';
import { Worker } from 'node:worker_threads';
import type { IncomingHttpHeaders } from 'node:http';
import { RemoteDatabase } from './remote.js';
import { KitUnsupportedError, QueryCancelledError, QueryTimeoutError } from './errors.js';

type RequestRecord = {
	method?: string;
	path?: string;
	headers: IncomingHttpHeaders;
	body: string;
};

const WORKER_SCRIPT = `
const { parentPort, workerData } = require('node:worker_threads');
const http = require('http');

const mode = workerData || 'success';
const requests = [];

const server = http.createServer((req, res) => {
	let body = '';
	req.setEncoding('utf8');
	req.on('data', (chunk) => {
		body += chunk;
	});
	req.on('end', () => {
		requests.push({ method: req.method, path: req.url, headers: req.headers, body });
		if (mode === 'error') {
			res.writeHead(503, { 'content-type': 'application/json' });
			res.end(JSON.stringify({ error: 'service unavailable' }));
			return;
		}
		if (req.url === '/history/retention') {
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({ history_retention_epochs: 42, earliest_retained_epoch: 5 }));
		} else if (req.url === '/capabilities' && mode !== 'old') {
			res.writeHead(200, { 'content-type': 'application/json' });
			res.end(JSON.stringify({ sql_cancellation: { version: 1, client_query_ids: true, cancel_endpoint: true, query_status: true, stream_disconnect_cancels: true } }));
		} else if (req.url === '/sql') {
			const request = JSON.parse(body);
			if (request.sql === 'TIMEOUT') {
				res.writeHead(504, { 'content-type': 'application/json' });
				res.end(JSON.stringify({ error: { code: 'DEADLINE_EXCEEDED', message: 'timed out', query_id: request.query_id } }));
			} else if (request.sql === 'SLOW') {
				setTimeout(() => {
					if (!res.writableEnded) {
						res.writeHead(200, { 'content-type': 'application/octet-stream' });
						res.end();
					}
				}, 1000);
			} else {
				res.writeHead(200, { 'content-type': 'application/octet-stream' });
				res.end();
			}
		} else if (req.url.endsWith('/cancel')) {
			res.writeHead(202, { 'content-type': 'application/json' });
			res.end(JSON.stringify({ state: 'cancellation_requested' }));
		} else {
			res.writeHead(404, { 'content-type': 'text/plain' });
			res.end('not found');
		}
	});
});

server.listen(0, '127.0.0.1', () => {
	const { port } = server.address();
	parentPort.postMessage({ type: 'ready', port });
});

parentPort.on('message', (msg) => {
	if (msg.type === 'getRequests') {
		parentPort.postMessage({ type: 'requests', requests });
	}
});
`;

function startMockServer(
	mode: 'success' | 'error' | 'old' = 'success'
): Promise<{ url: string; worker: Worker }> {
	const worker = new Worker(WORKER_SCRIPT, { eval: true, workerData: mode });
	return new Promise((resolve, reject) => {
		worker.once('message', (msg: { type: string; port?: number }) => {
			if (msg.type === 'ready' && msg.port !== undefined) {
				resolve({ url: `http://127.0.0.1:${msg.port}`, worker });
			} else {
				reject(new Error(`unexpected worker message: ${JSON.stringify(msg)}`));
			}
		});
		worker.once('error', reject);
	});
}

function getRequests(worker: Worker): Promise<RequestRecord[]> {
	return new Promise((resolve) => {
		worker.once('message', (msg: { type: string; requests?: RequestRecord[] }) => {
			if (msg.type === 'requests') {
				resolve(msg.requests ?? []);
			}
		});
		worker.postMessage({ type: 'getRequests' });
	});
}

function stopMockServer(worker: Worker): Promise<void> {
	return worker.terminate().then(() => {});
}

describe('RemoteDatabase', () => {
	it('setHistoryRetentionEpochs sends PUT /history/retention with a number body', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			remote.setHistoryRetentionEpochs(42n);
			const requests = await getRequests(worker);
			expect(requests).toHaveLength(1);
			expect(requests[0].method).toBe('PUT');
			expect(requests[0].path).toBe('/history/retention');
			expect(requests[0].headers['content-type']).toBe('application/json');
			expect(JSON.parse(requests[0].body)).toEqual({ history_retention_epochs: 42 });
		} finally {
			await stopMockServer(worker);
		}
	});

	it('historyRetentionEpochs sends GET /history/retention and returns the value', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			expect(remote.historyRetentionEpochs()).toBe(42n);
			const requests = await getRequests(worker);
			expect(requests).toHaveLength(1);
			expect(requests[0].method).toBe('GET');
			expect(requests[0].path).toBe('/history/retention');
		} finally {
			await stopMockServer(worker);
		}
	});

	it('earliestRetainedEpoch sends GET /history/retention and returns the value', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			expect(remote.earliestRetainedEpoch()).toBe(5n);
			const requests = await getRequests(worker);
			expect(requests).toHaveLength(1);
			expect(requests[0].method).toBe('GET');
			expect(requests[0].path).toBe('/history/retention');
		} finally {
			await stopMockServer(worker);
		}
	});

	it('propagates 503 errors from the daemon', async () => {
		const { url, worker } = await startMockServer('error');
		try {
			const remote = new RemoteDatabase(url);
			expect(() => remote.historyRetentionEpochs()).toThrow();
			expect(() => remote.earliestRetainedEpoch()).toThrow();
			expect(() => remote.setHistoryRetentionEpochs(7n)).toThrow();
		} finally {
			await stopMockServer(worker);
		}
	});

	it('sends query ID and server timeout and maps timeout error', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.sql('TIMEOUT', {
				queryId: '11112222333344445555666677778888',
				timeoutMs: 250
			})).rejects.toBeInstanceOf(QueryTimeoutError);
			const requests = await getRequests(worker);
			const sqlRequest = requests.find((request) => request.path === '/sql');
			expect(JSON.parse(sqlRequest!.body)).toMatchObject({
				format: 'arrow',
				query_id: '11112222333344445555666677778888',
				timeout_ms: 250
			});
		} finally {
			await stopMockServer(worker);
		}
	});

	it('AbortSignal sends remote cancellation and rejects typed', async () => {
		const { url, worker } = await startMockServer();
		try {
			const remote = new RemoteDatabase(url);
			const controller = new AbortController();
			const result = remote.sql('SLOW', {
				queryId: 'aaaabbbbccccddddeeeeffff00001111',
				signal: controller.signal
			});
			await new Promise((resolve) => setTimeout(resolve, 20));
			controller.abort();
			await expect(result).rejects.toBeInstanceOf(QueryCancelledError);
			await new Promise((resolve) => setTimeout(resolve, 20));
			const requests = await getRequests(worker);
			expect(requests.some((request) => request.path === '/queries/aaaabbbbccccddddeeeeffff00001111/cancel')).toBe(true);
		} finally {
			await stopMockServer(worker);
		}
	});

	it('old server permits uncontrolled SQL and rejects controlled SQL', async () => {
		const { url, worker } = await startMockServer('old');
		try {
			const remote = new RemoteDatabase(url);
			await expect(remote.sql('SELECT 1')).resolves.toBeDefined();
			await expect(remote.sql('SELECT 1', { timeoutMs: 10 })).rejects.toBeInstanceOf(KitUnsupportedError);
		} finally {
			await stopMockServer(worker);
		}
	});
});
