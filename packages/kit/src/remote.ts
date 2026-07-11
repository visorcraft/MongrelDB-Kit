import { RemoteDatabase as NativeRemoteDatabase } from '@visorcraft/mongreldb/native.js';
import { tableFromIPC, tableFromJSON, type Table as ArrowTable } from 'apache-arrow';
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

	/** Connect (lazily) to a daemon at `url`, e.g. `http://127.0.0.1:8453`. */
	constructor(url: string) {
		this.inner = new NativeRemoteDatabase(url) as NativeRemoteDatabase & RemoteRetention;
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

	/** Run a SQL query; returns the result as an Arrow (columnar) table. */
	sql(sql: string): ArrowTable {
		const bytes = this.inner.sql(sql);
		// DDL/DML commands return an empty body; produce an empty Arrow table.
		if (bytes.length === 0) {
			return tableFromJSON([]) as unknown as ArrowTable;
		}
		return tableFromIPC(bytes);
	}

	sqlRows(sql: string): Record<string, unknown>[] {
		return [...this.sql(sql)].map((row) => ({ ...row }));
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

	createVirtualTable(spec: VirtualTableSpec): ArrowTable {
		return this.sql(createVirtualTableSql(spec));
	}

	dropVirtualTable(name: string): ArrowTable {
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
