import { RemoteDatabase as NativeRemoteDatabase } from '@visorcraft/mongreldb/native.js';
import { tableFromIPC, tableFromJSON, type Table as ArrowTable } from 'apache-arrow';
import { randomBytes } from 'node:crypto';
import type { SqlOptions, SqlQuery } from './db.js';
import {
	KitError,
	KitUnsupportedError,
	QueryCancelledError,
	QueryIdConflictError,
	QueryTimeoutError,
	TransactionAbortedError
} from './errors.js';
import { procedureJson, type ProcedureCallOptions, type ProcedureSpec } from './procedure.js';
import { triggerJson, type TriggerSpec } from './trigger.js';
import {
	createVirtualTableSql,
	dropVirtualTableSql,
	type VirtualTableSpec
} from './external.js';

type RemoteRetention = {
	setHistoryRetentionEpochs(epochs: bigint): void;
	historyRetentionEpochs(): bigint;
	earliestRetainedEpoch(): bigint;
};

/**
 * A thin Kit client for a running `mongreldb-server` daemon.
 *
 * Unlike the embedded {@link KitDatabase}, the daemon speaks SQL + native query
 * over HTTP, not the Kit's typed/validated object model — so this surface is
 * SQL-oriented: run SQL (returning Arrow), read table names / counts, force a
 * commit, and health-check. Schema, validation, and constraints are the local
 * KitDatabase's job; connect a remote for cross-process reads/queries.
 *
 * TypeScript-only: the addon exposes the daemon client; the Rust/Python kit
 * would need the `mongreldb-client` crate for an equivalent.
 */
export class RemoteDatabase {
	private readonly inner: NativeRemoteDatabase & RemoteRetention;
	private readonly url: string;
	private capabilities?: Promise<SqlCancellationCapabilities | null>;

	/** Connect (lazily) to a daemon at `url`, e.g. `http://127.0.0.1:8453`. */
	constructor(url: string) {
		this.url = url.replace(/\/$/, '');
		this.inner = new NativeRemoteDatabase(this.url) as NativeRemoteDatabase & RemoteRetention;
	}

	/** Liveness check; returns the server's health string (throws if down). */
	health(): string {
		return this.inner.health();
	}

	/** Live table names on the server. */
	tableNames(): string[] {
		return this.inner.tableNames();
	}

	/** Row count of `table`. */
	count(table: string): bigint {
		return this.inner.count(table);
	}

	setHistoryRetentionEpochs(epochs: bigint): void {
		this.inner.setHistoryRetentionEpochs(epochs);
	}

	historyRetentionEpochs(): bigint {
		return this.inner.historyRetentionEpochs();
	}

	earliestRetainedEpoch(): bigint {
		return this.inner.earliestRetainedEpoch();
	}

	/** Run a SQL query without blocking the Node event loop. */
	async sql(sql: string, options?: SqlOptions): Promise<ArrowTable> {
		if (options?.timeoutMs !== undefined || options?.signal !== undefined || options?.queryId !== undefined) {
			return this.startSql(sql, options).result;
		}
		return this.executeSql(sql);
	}

	async sqlRows(sql: string, options?: SqlOptions): Promise<Record<string, unknown>[]> {
		return [...(await this.sql(sql, options))].map((row) => ({ ...row }));
	}

	startSql(sql: string, options: SqlOptions = {}): SqlQuery<ArrowTable> {
		const queryId = options.queryId ?? randomBytes(16).toString('hex');
		if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0)) {
			throw new RangeError('timeoutMs must be a positive safe integer');
		}
		if (options.signal?.aborted) {
			return {
				id: queryId,
				result: Promise.reject(new QueryCancelledError(queryId)),
				cancel: async () => {}
			};
		}

		const controller = new AbortController();
		let cancelled = false;
		const cancel = async () => {
			if (cancelled) return;
			cancelled = true;
			const request = this.cancelSql(queryId);
			controller.abort();
			await request;
		};
		const onAbort = () => {
			void cancel();
		};
		options.signal?.addEventListener('abort', onAbort, { once: true });
		const result = this.requireSqlCancellation()
			.then(() => this.executeSql(sql, queryId, options.timeoutMs, controller.signal))
			.catch((error: unknown) => {
				if (cancelled || options.signal?.aborted) {
					throw new QueryCancelledError(queryId);
				}
				throw error;
			})
			.finally(() => options.signal?.removeEventListener('abort', onAbort));
		return { id: queryId, result, cancel };
	}

	async cancelSql(queryId: string): Promise<void> {
		await this.requireSqlCancellation();
		const response = await fetch(`${this.url}/queries/${queryId}/cancel`, { method: 'POST' });
		if (response.ok || response.status === 409 || response.status === 404) return;
		throw new KitError(`SQL cancellation failed with HTTP ${response.status}`);
	}

	private async executeSql(
		sql: string,
		queryId?: string,
		timeoutMs?: number,
		signal?: AbortSignal
	): Promise<ArrowTable> {
		const response = await fetch(`${this.url}/sql`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ sql, format: 'arrow', query_id: queryId, timeout_ms: timeoutMs }),
			signal
		});
		if (!response.ok) {
			throw await remoteSqlError(response, queryId ?? 'unknown');
		}
		const bytes = new Uint8Array(await response.arrayBuffer());
		if (bytes.length === 0) {
			return tableFromJSON([]) as unknown as ArrowTable;
		}
		return tableFromIPC(bytes);
	}

	private async requireSqlCancellation(): Promise<SqlCancellationCapabilities> {
		this.capabilities ??= fetch(`${this.url}/capabilities`).then(async (response) => {
			if (response.status === 404) return null;
			if (!response.ok) throw new KitError(`capability request failed with HTTP ${response.status}`);
			const body = await response.json() as { sql_cancellation?: SqlCancellationCapabilities };
			return body.sql_cancellation ?? null;
		});
		const capability = await this.capabilities;
		if (capability?.version !== 1 || !capability.client_query_ids || !capability.cancel_endpoint) {
			throw new KitUnsupportedError('server does not support SQL cancellation capability version 1');
		}
		return capability;
	}

	/** Flush/commit `table` on the server; returns the new epoch. */
	commit(table: string): bigint {
		return this.inner.commit(table);
	}

	createProcedure(spec: ProcedureSpec): unknown {
		return JSON.parse(this.inner.createProcedure({ json: procedureJson(spec) }));
	}

	dropProcedure(name: string): void {
		this.inner.dropProcedure(name);
	}

	callProcedure(name: string, opts: ProcedureCallOptions = {}): unknown {
		return JSON.parse(
			this.inner.callProcedure(name, {
				argsJson: JSON.stringify(opts.args ?? {}),
				idempotencyKey: opts.idempotencyKey
			})
		);
	}

	createTrigger(spec: TriggerSpec): unknown {
		return JSON.parse(this.inner.createTrigger({ json: triggerJson(spec) }));
	}

	replaceTrigger(name: string, spec: TriggerSpec): unknown {
		return JSON.parse(this.inner.replaceTrigger(name, { json: triggerJson(spec) }));
	}

	dropTrigger(name: string): void {
		this.inner.dropTrigger(name);
	}

	triggers(): unknown {
		return JSON.parse(this.inner.triggers());
	}

	trigger(name: string): unknown {
		return JSON.parse(this.inner.trigger(name));
	}

	createVirtualTable(spec: VirtualTableSpec): Promise<ArrowTable> {
		return this.sql(createVirtualTableSql(spec));
	}

	dropVirtualTable(name: string): Promise<ArrowTable> {
		return this.sql(dropVirtualTableSql(name));
	}

	/** Compact every table on the daemon (POST /compact). Returns
	 * `{ compacted, skipped }`. */
	compact(): { compacted: number; skipped: number } {
		return this.inner.compact();
	}

	/** Compact a single table on the daemon (POST /tables/{name}/compact).
	 * Returns `true` if compacted, `false` if skipped. */
	compactTable(table: string): boolean {
		return this.inner.compactTable(table);
	}
}

type SqlCancellationCapabilities = {
	version: number;
	client_query_ids: boolean;
	cancel_endpoint: boolean;
	query_status: boolean;
	stream_disconnect_cancels: boolean;
};

async function remoteSqlError(response: Response, fallbackQueryId: string): Promise<Error> {
	let body: { error?: { code?: string; message?: string; query_id?: string } } = {};
	try {
		body = await response.json() as typeof body;
	} catch {
		return new KitError(`SQL request failed with HTTP ${response.status}`);
	}
	const queryId = body.error?.query_id ?? fallbackQueryId;
	const message = body.error?.message;
	switch (body.error?.code) {
		case 'QUERY_CANCELLED':
			return new QueryCancelledError(queryId, message);
		case 'DEADLINE_EXCEEDED':
			return new QueryTimeoutError(queryId, message);
		case 'QUERY_ID_CONFLICT':
			return new QueryIdConflictError(queryId, message);
		case 'TRANSACTION_ABORTED':
			return new TransactionAbortedError(message);
		default:
			return new KitError(message ?? `SQL request failed with HTTP ${response.status}`);
	}
}
