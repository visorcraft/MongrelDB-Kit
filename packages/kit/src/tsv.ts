import type { TableSpec } from './types.js';

// TSV codec matching the Rust/Python kit: header row of column names,
// tab-separated cells, NULL = empty field, `\t \n \r \\` backslash-escaped.
// Numbers/bools render as literal text; arrays/objects render as escaped JSON.
// (An empty string round-trips as null — the documented limitation.)

function escape(s: string): string {
	let o = '';
	for (const c of s) {
		if (c === '\\') o += '\\\\';
		else if (c === '\t') o += '\\t';
		else if (c === '\n') o += '\\n';
		else if (c === '\r') o += '\\r';
		else o += c;
	}
	return o;
}

function unescape(s: string): string {
	let o = '';
	for (let i = 0; i < s.length; i++) {
		const c = s[i];
		if (c === '\\' && i + 1 < s.length) {
			const n = s[++i];
			o += n === 't' ? '\t' : n === 'n' ? '\n' : n === 'r' ? '\r' : n === '\\' ? '\\' : '\\' + n;
		} else {
			o += c;
		}
	}
	return o;
}

function cellToTsv(v: unknown): string {
	if (v === null || v === undefined) return '';
	if (typeof v === 'string') return escape(v);
	if (typeof v === 'boolean') return v ? 'true' : 'false';
	if (typeof v === 'number' || typeof v === 'bigint') return String(v);
	return escape(JSON.stringify(v));
}

export function rowsToTsv(table: TableSpec, rows: Record<string, unknown>[]): string {
	const cols = table.columns.map((c) => c.name);
	const lines = [cols.join('\t')];
	for (const row of rows) {
		lines.push(cols.map((n) => cellToTsv(row[n] ?? null)).join('\t'));
	}
	return lines.join('\n') + '\n';
}

function parseCell(raw: string, ty: string): unknown {
	if (raw === '') return null;
	const text = unescape(raw);
	switch (ty) {
		case 'bool':
			return text === 'true';
		case 'int64':
			return BigInt(text);
		case 'float64':
			return Number(text);
		case 'embedding':
		case 'sparse':
		case 'bytes':
			// These insert as arrays/buffers; json is carried as a JSON string.
			try {
				return JSON.parse(text);
			} catch {
				return text;
			}
		default:
			return text; // text, date, timestamp, json (JSON string)
	}
}

export function tsvToRows(table: TableSpec, text: string): Record<string, unknown>[] {
	const lines = text.split('\n');
	if (lines.length === 0 || lines[0] === '') return [];
	const names = lines[0].split('\t');
	const types = names.map((n) => table.columns.find((c) => c.name === n)?.applicationType);
	const rows: Record<string, unknown>[] = [];
	for (let i = 1; i < lines.length; i++) {
		if (lines[i] === '') continue;
		const fields = lines[i].split('\t');
		const row: Record<string, unknown> = {};
		fields.forEach((f, j) => {
			const name = names[j];
			const ty = types[j];
			if (name === undefined || ty === undefined) return;
			row[name] = parseCell(f, ty);
		});
		rows.push(row);
	}
	return rows;
}
