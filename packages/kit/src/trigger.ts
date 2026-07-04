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
	| { kind: 'old_column'; value: number }
	| { kind: 'selected_column'; value: number };

export type TriggerExpr =
	| { kind: 'value'; value: TriggerValue }
	| { kind: 'eq'; left: TriggerValue; right: TriggerValue }
	| { kind: 'not_eq'; left: TriggerValue; right: TriggerValue }
	| { kind: 'lt'; left: TriggerValue; right: TriggerValue }
	| { kind: 'lte'; left: TriggerValue; right: TriggerValue }
	| { kind: 'gt'; left: TriggerValue; right: TriggerValue }
	| { kind: 'gte'; left: TriggerValue; right: TriggerValue }
	| { kind: 'is_null'; value: TriggerValue }
	| { kind: 'is_not_null'; value: TriggerValue }
	| { kind: 'and'; left: TriggerExpr; right: TriggerExpr }
	| { kind: 'or'; left: TriggerExpr; right: TriggerExpr }
	| { kind: 'not'; value: TriggerExpr };

export interface TriggerCell {
	column_id: number;
	value: TriggerValue;
}

export type TriggerCondition =
	| { kind: 'pk'; value: TriggerValue }
	| { kind: 'eq'; column_id: number; value: TriggerValue }
	| { kind: 'not_eq'; column_id: number; value: TriggerValue }
	| { kind: 'lt'; column_id: number; value: TriggerValue }
	| { kind: 'lte'; column_id: number; value: TriggerValue }
	| { kind: 'gt'; column_id: number; value: TriggerValue }
	| { kind: 'gte'; column_id: number; value: TriggerValue }
	| { kind: 'is_null'; column_id: number }
	| { kind: 'is_not_null'; column_id: number }
	| { kind: 'and'; left: TriggerCondition; right: TriggerCondition }
	| { kind: 'or'; left: TriggerCondition; right: TriggerCondition }
	| { kind: 'not'; value: TriggerCondition };

export type TriggerStep =
	| { kind: 'set_new'; cells: TriggerCell[] }
	| { kind: 'insert'; table: string; cells: TriggerCell[] }
	| { kind: 'update_by_pk'; table: string; pk: TriggerValue; cells: TriggerCell[] }
	| { kind: 'delete_by_pk'; table: string; pk: TriggerValue }
	| { kind: 'select'; id: string; table: string; conditions?: TriggerCondition[] }
	| { kind: 'raise'; action: TriggerRaiseAction; message: TriggerValue }
	| { kind: 'foreach'; id: string; steps: TriggerStep[] }
	| { kind: 'delete_where'; table: string; conditions?: TriggerCondition[] }
	| { kind: 'update_where'; table: string; conditions?: TriggerCondition[]; cells: TriggerCell[] };

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
export const selectedColumn = (columnId: number): TriggerValue => ({
	kind: 'selected_column',
	value: columnId
});

export const exprValue = (value: TriggerValue): TriggerExpr => ({ kind: 'value', value });
export const exprEq = (left: TriggerValue, right: TriggerValue): TriggerExpr => ({
	kind: 'eq',
	left,
	right
});
export const exprNotEq = (left: TriggerValue, right: TriggerValue): TriggerExpr => ({
	kind: 'not_eq',
	left,
	right
});
export const exprLt = (left: TriggerValue, right: TriggerValue): TriggerExpr => ({
	kind: 'lt',
	left,
	right
});
export const exprLte = (left: TriggerValue, right: TriggerValue): TriggerExpr => ({
	kind: 'lte',
	left,
	right
});
export const exprGt = (left: TriggerValue, right: TriggerValue): TriggerExpr => ({
	kind: 'gt',
	left,
	right
});
export const exprGte = (left: TriggerValue, right: TriggerValue): TriggerExpr => ({
	kind: 'gte',
	left,
	right
});
export const exprIsNull = (value: TriggerValue): TriggerExpr => ({ kind: 'is_null', value });
export const exprIsNotNull = (value: TriggerValue): TriggerExpr => ({ kind: 'is_not_null', value });
export const exprAnd = (left: TriggerExpr, right: TriggerExpr): TriggerExpr => ({
	kind: 'and',
	left,
	right
});
export const exprOr = (left: TriggerExpr, right: TriggerExpr): TriggerExpr => ({
	kind: 'or',
	left,
	right
});
export const exprNot = (value: TriggerExpr): TriggerExpr => ({ kind: 'not', value });

export const condPk = (value: TriggerValue): TriggerCondition => ({ kind: 'pk', value });
export const condEq = (columnId: number, value: TriggerValue): TriggerCondition => ({
	kind: 'eq',
	column_id: columnId,
	value
});
export const condNotEq = (columnId: number, value: TriggerValue): TriggerCondition => ({
	kind: 'not_eq',
	column_id: columnId,
	value
});
export const condLt = (columnId: number, value: TriggerValue): TriggerCondition => ({
	kind: 'lt',
	column_id: columnId,
	value
});
export const condLte = (columnId: number, value: TriggerValue): TriggerCondition => ({
	kind: 'lte',
	column_id: columnId,
	value
});
export const condGt = (columnId: number, value: TriggerValue): TriggerCondition => ({
	kind: 'gt',
	column_id: columnId,
	value
});
export const condGte = (columnId: number, value: TriggerValue): TriggerCondition => ({
	kind: 'gte',
	column_id: columnId,
	value
});
export const condIsNull = (columnId: number): TriggerCondition => ({ kind: 'is_null', column_id: columnId });
export const condIsNotNull = (columnId: number): TriggerCondition => ({
	kind: 'is_not_null',
	column_id: columnId
});
export const condAnd = (left: TriggerCondition, right: TriggerCondition): TriggerCondition => ({
	kind: 'and',
	left,
	right
});
export const condOr = (left: TriggerCondition, right: TriggerCondition): TriggerCondition => ({
	kind: 'or',
	left,
	right
});
export const condNot = (value: TriggerCondition): TriggerCondition => ({ kind: 'not', value });

export const cell = (columnId: number, value: TriggerValue): TriggerCell => ({
	column_id: columnId,
	value
});

export const stepSelect = (
	id: string,
	table: string,
	conditions: TriggerCondition[] = []
): TriggerStep => ({ kind: 'select', id, table, conditions });

export const stepForeach = (id: string, steps: TriggerStep[]): TriggerStep => ({
	kind: 'foreach',
	id,
	steps
});

export const stepDeleteWhere = (
	table: string,
	conditions: TriggerCondition[] = []
): TriggerStep => ({ kind: 'delete_where', table, conditions });

export const stepUpdateWhere = (
	table: string,
	cells: TriggerCell[],
	conditions: TriggerCondition[] = []
): TriggerStep => ({ kind: 'update_where', table, conditions, cells });

export const stepSetNew = (cells: TriggerCell[]): TriggerStep => ({
	kind: 'set_new',
	cells
});

export const stepInsert = (table: string, cells: TriggerCell[]): TriggerStep => ({
	kind: 'insert',
	table,
	cells
});

export const stepUpdateByPk = (
	table: string,
	pk: TriggerValue,
	cells: TriggerCell[]
): TriggerStep => ({ kind: 'update_by_pk', table, pk, cells });

export const stepDeleteByPk = (table: string, pk: TriggerValue): TriggerStep => ({
	kind: 'delete_by_pk',
	table,
	pk
});

export const stepRaise = (action: TriggerRaiseAction, message: TriggerValue): TriggerStep => ({
	kind: 'raise',
	action,
	message
});
