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
