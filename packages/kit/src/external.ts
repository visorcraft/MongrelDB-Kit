export interface VirtualTableSpec {
	name: string;
	module: string;
	args?: string[];
}

export function virtualTable(
	name: string,
	module: string,
	args: string[] = []
): VirtualTableSpec {
	return { name, module, args };
}

export function quoteIdent(name: string): string {
	return `"${name.replaceAll('"', '""')}"`;
}

export function createVirtualTableSql(spec: VirtualTableSpec): string {
	const args = spec.args?.join(', ') ?? '';
	const suffix = args.length > 0 ? `(${args})` : '';
	return `CREATE VIRTUAL TABLE ${quoteIdent(spec.name)} USING ${quoteIdent(spec.module)}${suffix}`;
}

export function dropVirtualTableSql(name: string): string {
	return `DROP TABLE ${quoteIdent(name)}`;
}

/** A SQL view definition (`CREATE VIEW <name> AS <select>`). Views are
 * session-scoped in the engine (not persisted to the catalog); a view created
 * via a migration lives in the kit's long-lived SQL session. */
export interface ViewSpec {
	name: string;
	/** The `AS <select>` body — the full `SELECT ...` statement the view
	 * resolves to. */
	sql: string;
}

export function view(name: string, sql: string): ViewSpec {
	return { name, sql };
}

/** `CREATE VIEW <name> AS <select>`. The engine overwrites any existing view
 * with the same name, so this is also used for `replaceView`. */
export function createViewSql(spec: ViewSpec): string {
	return `CREATE VIEW ${quoteIdent(spec.name)} AS ${spec.sql}`;
}

export function dropViewSql(name: string): string {
	return `DROP VIEW IF EXISTS ${quoteIdent(name)}`;
}
