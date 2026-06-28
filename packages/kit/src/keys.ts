import type { PkValue } from './types.js';

export const KIT_KEY_VERSION = 1;

function escapeString(value: string): string {
	return value.replace(/\\/g, '\\\\').replace(/:/g, '\\:');
}

function unescapeString(value: string): string {
	return value.replace(/\\:/g, ':').replace(/\\\\/g, '\\');
}

function encodeKeyComponent(value: string | bigint | null): string {
	if (value === null || value === undefined) {
		return 'n:null';
	}
	if (typeof value === 'bigint') {
		return `i:${value.toString()}`;
	}
	return `s:${escapeString(value)}`;
}

function decodeKeyComponent(token: string): string | bigint | null {
	const prefix = token.slice(0, 2);
	const body = token.slice(2);
	switch (prefix) {
		case 's:':
			return unescapeString(body);
		case 'i:':
			return BigInt(body);
		case 'n:':
			return null;
		default:
			throw new Error(`Unexpected primary key token prefix: ${prefix}`);
	}
}

function decodeKeyComponents(encoded: string): (string | bigint | null)[] {
	const tokens: string[] = [];
	let current = '';
	let escaped = false;
	for (const ch of encoded) {
		if (escaped) {
			current += ch;
			escaped = false;
			continue;
		}
		if (ch === '\\') {
			current += ch;
			escaped = true;
			continue;
		}
		if (ch === ':') {
			// A token is a typed component: "s:", "i:", or "n:" followed by a body.
			if (current.length >= 2 && /^[sin]:/.test(current)) {
				tokens.push(current);
				current = '';
				continue;
			}
		}
		current += ch;
	}
	if (current.length >= 2) {
		tokens.push(current);
	}
	return tokens.map(decodeKeyComponent);
}

export function encodedPk(pkValue: PkValue): string {
	if (Array.isArray(pkValue)) {
		return pkValue.map(encodeKeyComponent).join(':');
	}
	return encodeKeyComponent(pkValue);
}

export function decodePk(encoded: string): PkValue {
	const parts = decodeKeyComponents(encoded);
	if (parts.length === 1) {
		const value = parts[0];
		if (value === null) {
			throw new Error('Single-column primary key cannot be decoded as null');
		}
		return value;
	}
	return parts;
}

export function encodeUniqueKey(
	kitVersion: number,
	constraintName: string,
	values: (string | bigint | null)[]
): string {
	return `uq:${kitVersion}:${constraintName}:${values.map(encodeKeyComponent).join(':')}`;
}

export function encodeRowGuardKey(tableName: string, pkValue: PkValue): string {
	return `rg:${tableName}:${encodedPk(pkValue)}`;
}
