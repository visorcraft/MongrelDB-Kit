import type { TableSpec, ColumnSpec } from './types.js';

export type DefaultValue =
	| { kind: 'static'; value: unknown }
	| { kind: 'now' }
	| { kind: 'uuid' }
	| { kind: 'sequence'; name: string }
	| { kind: 'custom'; fn: () => unknown };

export interface DefaultContext {
	now: string;
	uuid: () => string;
	allocateSequence: (name: string, count?: number) => bigint;
}

export function staticDefault(value: unknown): DefaultValue {
	return { kind: 'static', value };
}

export function nowDefault(): DefaultValue {
	return { kind: 'now' };
}

export function uuidDefault(): DefaultValue {
	return { kind: 'uuid' };
}

export function sequenceDefault(name: string): DefaultValue {
	return { kind: 'sequence', name };
}

export function customDefault(fn: () => unknown): DefaultValue {
	return { kind: 'custom', fn };
}

function generatedDefault(generated: 'uuid' | 'now' | null): DefaultValue | null {
	if (generated === 'uuid') return uuidDefault();
	if (generated === 'now') return nowDefault();
	return null;
}

export function applyDefaults(
	table: TableSpec,
	row: Record<string, unknown>,
	ctx: DefaultContext
): Record<string, unknown> {
	const out: Record<string, unknown> = { ...row };
	for (const col of table.columns) {
		const value = out[col.name];
		if (value !== undefined && value !== null) continue;

		const source = col.default ?? generatedDefault(col.generated);
		if (!source) continue;

		switch (source.kind) {
			case 'static':
				out[col.name] = source.value;
				break;
			case 'now':
				out[col.name] = ctx.now;
				break;
			case 'uuid':
				out[col.name] = ctx.uuid();
				break;
			case 'sequence':
				out[col.name] = ctx.allocateSequence(source.name, 1);
				break;
			case 'custom':
				out[col.name] = source.fn();
				break;
		}
	}
	return out;
}
