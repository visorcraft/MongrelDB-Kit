import { RemoteDatabase as NativeRemoteDatabase } from '@visorcraft/mongreldb/native.js';
import { tableFromIPC, type Table as ArrowTable } from 'apache-arrow';

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
	private readonly inner: NativeRemoteDatabase;

	/** Connect (lazily) to a daemon at `url`, e.g. `http://127.0.0.1:8453`. */
	constructor(url: string) {
		this.inner = new NativeRemoteDatabase(url);
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
	count(table: string): number {
		return this.inner.count(table);
	}

	/** Run a SQL query; returns the result as an Arrow (columnar) table. */
	sql(sql: string): ArrowTable {
		return tableFromIPC(this.inner.sql(sql));
	}

	/** Flush/commit `table` on the server; returns the new epoch. */
	commit(table: string): bigint {
		return this.inner.commit(table);
	}
}
