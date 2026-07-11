import { describe, it, expect } from 'vitest';
import { Worker } from 'node:worker_threads';
import type { IncomingHttpHeaders } from 'node:http';
import { RemoteDatabase } from './remote.js';

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
	mode: 'success' | 'error' = 'success'
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
});
