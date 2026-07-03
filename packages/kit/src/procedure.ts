export type ProcedureMode = 'read_only' | 'read_write';

export interface ProcedureSpec {
	name: string;
	version?: number;
	mode: ProcedureMode;
	params?: unknown[];
	body: unknown;
	checksum?: string;
	created_epoch?: number;
	updated_epoch?: number;
}

export interface ProcedureCallOptions {
	args?: Record<string, unknown>;
	idempotencyKey?: string;
}

export interface ProcedureCallResult {
	epoch?: bigint;
	result: unknown;
}

export function procedure(spec: ProcedureSpec): ProcedureSpec {
	return {
		version: 1,
		params: [],
		checksum: '',
		created_epoch: 0,
		updated_epoch: 0,
		...spec
	};
}

export function procedureJson(spec: ProcedureSpec): string {
	return JSON.stringify(procedure(spec));
}
