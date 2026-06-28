export const KIT_KEY_VERSION = 1;

function escapeString(value: string): string {
	return value.replace(/\\/g, '\\\\').replace(/:/g, '\\:');
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

export function encodePkValue(pkValue: string | bigint): string {
	if (typeof pkValue === 'bigint') {
		return `i:${pkValue.toString()}`;
	}
	return `s:${escapeString(pkValue)}`;
}

export function encodeUniqueKey(
	kitVersion: number,
	constraintName: string,
	values: (string | bigint | null)[]
): string {
	return `uq:${kitVersion}:${constraintName}:${values.map(encodeKeyComponent).join(':')}`;
}

export function encodeRowGuardKey(tableName: string, pkValue: string | bigint): string {
	return `rg:${tableName}:${encodePkValue(pkValue)}`;
}
