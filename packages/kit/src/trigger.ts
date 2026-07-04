export type TriggerTiming = 'before' | 'after' | 'instead_of';
export type TriggerEvent = 'insert' | 'update' | 'delete';
export type TriggerRaiseAction = 'abort' | 'fail' | 'rollback' | 'ignore';

export type MongrelValue =
	| 'Null'
	| { Bool: boolean }
	| { Int64: number }
	| { Float64: number }
	| { Bytes: number[] }
	| { Embedding: number[] };

export type TriggerTarget =
	| { kind: 'table'; name: string }
	| { kind: 'view'; name: string };

export type TriggerValue =
	| { kind: 'literal'; value: MongrelValue }
	| { kind: 'new_column'; value: number }
	| { kind: 'old_column'; value: number };

export type TriggerExpr =
	| { kind: 'value'; value: TriggerValue }
	| { kind: 'eq'; left: TriggerValue; right: TriggerValue }
	| { kind: 'not_eq'; left: TriggerValue; right: TriggerValue }
	| { kind: 'is_null'; value: TriggerValue }
	| { kind: 'is_not_null'; value: TriggerValue };

export interface TriggerCell {
	column_id: number;
	value: TriggerValue;
}

export type TriggerCondition =
	| { kind: 'pk'; value: TriggerValue }
	| { kind: 'eq'; column_id: number; value: TriggerValue }
	| { kind: 'is_null'; column_id: number }
	| { kind: 'is_not_null'; column_id: number };

export type TriggerStep =
	| { kind: 'set_new'; cells: TriggerCell[] }
	| { kind: 'insert'; table: string; cells: TriggerCell[] }
	| { kind: 'update_by_pk'; table: string; pk: TriggerValue; cells: TriggerCell[] }
	| { kind: 'delete_by_pk'; table: string; pk: TriggerValue }
	| { kind: 'select'; id: string; table: string; conditions?: TriggerCondition[] }
	| { kind: 'raise'; action: TriggerRaiseAction; message: TriggerValue };

export interface TriggerProgram {
	steps: TriggerStep[];
}

export interface TriggerSpec {
	name: string;
	version?: number;
	target: TriggerTarget;
	timing: TriggerTiming;
	event: TriggerEvent;
	update_of?: string[];
	target_columns?: unknown[];
	when?: TriggerExpr;
	program: TriggerProgram;
	enabled?: boolean;
	checksum?: string;
	created_epoch?: number;
	updated_epoch?: number;
}

export function trigger(spec: TriggerSpec): TriggerSpec {
	return {
		version: 1,
		update_of: [],
		target_columns: [],
		enabled: true,
		checksum: '',
		created_epoch: 0,
		updated_epoch: 0,
		...spec
	};
}

export function triggerJson(spec: TriggerSpec): string {
	return JSON.stringify(trigger(spec));
}

export const literal = (value: MongrelValue): TriggerValue => ({ kind: 'literal', value });
export const nullValue = (): TriggerValue => literal('Null');
export const boolValue = (value: boolean): TriggerValue => literal({ Bool: value });
export const int64Value = (value: number): TriggerValue => literal({ Int64: value });
export const float64Value = (value: number): TriggerValue => literal({ Float64: value });
export const textValue = (value: string): TriggerValue =>
	literal({ Bytes: [...Buffer.from(value, 'utf8')] });
export const bytesValue = (value: Uint8Array | number[]): TriggerValue =>
	literal({ Bytes: Array.from(value) });
export const embeddingValue = (value: number[]): TriggerValue => literal({ Embedding: value });
export const newColumn = (columnId: number): TriggerValue => ({ kind: 'new_column', value: columnId });
export const oldColumn = (columnId: number): TriggerValue => ({ kind: 'old_column', value: columnId });
