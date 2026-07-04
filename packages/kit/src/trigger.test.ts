import { describe, expect, it } from 'vitest';
import {
	boolValue,
	bytesValue,
	cell,
	condAnd,
	condEq,
	condGt,
	condGte,
	condIsNotNull,
	condIsNull,
	condLt,
	condLte,
	condNot,
	condNotEq,
	condOr,
	condPk,
	embeddingValue,
	exprAnd,
	exprEq,
	exprGt,
	exprGte,
	exprIsNotNull,
	exprIsNull,
	exprLt,
	exprLte,
	exprNot,
	exprNotEq,
	exprOr,
	exprValue,
	float64Value,
	int64Value,
	newColumn,
	nullValue,
	oldColumn,
	selectedColumn,
	stepDeleteByPk,
	stepDeleteWhere,
	stepForeach,
	stepInsert,
	stepRaise,
	stepSelect,
	stepSetNew,
	stepUpdateByPk,
	stepUpdateWhere,
	textValue,
	trigger,
	triggerJson,
	type TriggerCell,
	type TriggerSpec
} from './trigger.js';

const makeCell = (column_id: number, value: number): TriggerCell => cell(column_id, int64Value(value));

describe('trigger value builders', () => {
	it('nullValue produces a Null literal', () => {
		expect(nullValue()).toEqual({ kind: 'literal', value: 'Null' });
	});

	it('boolValue produces a Bool literal', () => {
		expect(boolValue(true)).toEqual({ kind: 'literal', value: { Bool: true } });
		expect(boolValue(false)).toEqual({ kind: 'literal', value: { Bool: false } });
	});

	it('int64Value produces an Int64 literal', () => {
		expect(int64Value(42)).toEqual({ kind: 'literal', value: { Int64: 42 } });
	});

	it('float64Value produces a Float64 literal', () => {
		expect(float64Value(3.14)).toEqual({ kind: 'literal', value: { Float64: 3.14 } });
	});

	it('textValue produces a Bytes literal from UTF-8 text', () => {
		expect(textValue('hi')).toEqual({ kind: 'literal', value: { Bytes: [104, 105] } });
	});

	it('bytesValue produces a Bytes literal from a Uint8Array or number array', () => {
		expect(bytesValue(new Uint8Array([1, 2, 3]))).toEqual({
			kind: 'literal',
			value: { Bytes: [1, 2, 3] }
		});
		expect(bytesValue([4, 5, 6])).toEqual({ kind: 'literal', value: { Bytes: [4, 5, 6] } });
	});

	it('embeddingValue produces an Embedding literal', () => {
		expect(embeddingValue([0.1, 0.2])).toEqual({
			kind: 'literal',
			value: { Embedding: [0.1, 0.2] }
		});
	});

	it('newColumn / oldColumn / selectedColumn produce column references', () => {
		expect(newColumn(1)).toEqual({ kind: 'new_column', value: 1 });
		expect(oldColumn(2)).toEqual({ kind: 'old_column', value: 2 });
		expect(selectedColumn(7)).toEqual({ kind: 'selected_column', value: 7 });
	});

	it('cell produces a TriggerCell with snake_case column_id', () => {
		expect(cell(5, int64Value(10))).toEqual({ column_id: 5, value: { kind: 'literal', value: { Int64: 10 } } });
	});
});

describe('trigger expression builders', () => {
	it('exprValue wraps a trigger value', () => {
		const v = int64Value(42);
		expect(exprValue(v)).toEqual({ kind: 'value', value: v });
	});

	it('exprEq / exprNotEq / exprLt / exprLte / exprGt / exprGte compare two values', () => {
		const left = newColumn(1);
		const right = int64Value(5);
		expect(exprEq(left, right)).toEqual({ kind: 'eq', left, right });
		expect(exprNotEq(left, right)).toEqual({ kind: 'not_eq', left, right });
		expect(exprLt(left, right)).toEqual({ kind: 'lt', left, right });
		expect(exprLte(left, right)).toEqual({ kind: 'lte', left, right });
		expect(exprGt(left, right)).toEqual({ kind: 'gt', left, right });
		expect(exprGte(left, right)).toEqual({ kind: 'gte', left, right });
	});

	it('exprIsNull / exprIsNotNull test nullity of a value', () => {
		const v = oldColumn(2);
		expect(exprIsNull(v)).toEqual({ kind: 'is_null', value: v });
		expect(exprIsNotNull(v)).toEqual({ kind: 'is_not_null', value: v });
	});

	it('exprAnd / exprOr / exprNot combine expressions recursively', () => {
		const a = exprGt(newColumn(3), int64Value(0));
		const b = exprIsNotNull(newColumn(4));
		const c = exprLt(newColumn(5), int64Value(100));

		expect(exprAnd(a, b)).toEqual({ kind: 'and', left: a, right: b });
		expect(exprOr(b, c)).toEqual({ kind: 'or', left: b, right: c });
		expect(exprNot(a)).toEqual({ kind: 'not', value: a });

		const nested = exprAnd(exprNot(a), exprOr(b, c));
		expect(nested).toEqual({
			kind: 'and',
			left: { kind: 'not', value: a },
			right: { kind: 'or', left: b, right: c }
		});
	});

	it('produces the engine-expected JSON shape for a complex when clause', () => {
		const when = exprAnd(
			exprIsNotNull(newColumn(1)),
			exprOr(exprGt(newColumn(2), int64Value(0)), exprLte(newColumn(2), int64Value(-1)))
		);
		expect(JSON.parse(JSON.stringify(when))).toEqual({
			kind: 'and',
			left: { kind: 'is_not_null', value: { kind: 'new_column', value: 1 } },
			right: {
				kind: 'or',
				left: {
					kind: 'gt',
					left: { kind: 'new_column', value: 2 },
					right: { kind: 'literal', value: { Int64: 0 } }
				},
				right: {
					kind: 'lte',
					left: { kind: 'new_column', value: 2 },
					right: { kind: 'literal', value: { Int64: -1 } }
				}
			}
		});
	});
});

describe('trigger condition builders', () => {
	it('condPk references a value as the primary key', () => {
		const v = selectedColumn(9);
		expect(condPk(v)).toEqual({ kind: 'pk', value: v });
	});

	it('condEq / condNotEq / condLt / condLte / condGt / condGte compare a column to a value', () => {
		const value = textValue('ok');
		expect(condEq(1, value)).toEqual({ kind: 'eq', column_id: 1, value });
		expect(condNotEq(1, value)).toEqual({ kind: 'not_eq', column_id: 1, value });
		expect(condLt(1, value)).toEqual({ kind: 'lt', column_id: 1, value });
		expect(condLte(1, value)).toEqual({ kind: 'lte', column_id: 1, value });
		expect(condGt(1, value)).toEqual({ kind: 'gt', column_id: 1, value });
		expect(condGte(1, value)).toEqual({ kind: 'gte', column_id: 1, value });
	});

	it('condIsNull / condIsNotNull test a column for nullity', () => {
		expect(condIsNull(7)).toEqual({ kind: 'is_null', column_id: 7 });
		expect(condIsNotNull(7)).toEqual({ kind: 'is_not_null', column_id: 7 });
	});

	it('condAnd / condOr / condNot combine conditions recursively', () => {
		const a = condGt(1, int64Value(0));
		const b = condIsNotNull(2);
		const c = condEq(3, boolValue(true));

		expect(condAnd(a, b)).toEqual({ kind: 'and', left: a, right: b });
		expect(condOr(b, c)).toEqual({ kind: 'or', left: b, right: c });
		expect(condNot(a)).toEqual({ kind: 'not', value: a });

		expect(condAnd(condNot(a), condOr(b, c))).toEqual({
			kind: 'and',
			left: { kind: 'not', value: a },
			right: { kind: 'or', left: b, right: c }
		});
	});
});

describe('trigger step builders', () => {
	it('stepForeach builds a foreach step', () => {
		const inner = stepSelect('rows', 'details');
		expect(stepForeach('rows', [inner])).toEqual({
			kind: 'foreach',
			id: 'rows',
			steps: [inner]
		});
	});

	it('stepDeleteWhere builds a delete_where step with optional conditions', () => {
		const conditions = [condEq(1, oldColumn(3))];
		expect(stepDeleteWhere('stale', conditions)).toEqual({
			kind: 'delete_where',
			table: 'stale',
			conditions
		});
		expect(stepDeleteWhere('stale')).toEqual({
			kind: 'delete_where',
			table: 'stale',
			conditions: []
		});
	});

	it('stepUpdateWhere builds an update_where step with optional conditions', () => {
		const cells: TriggerCell[] = [makeCell(2, 1)];
		const conditions = [condPk(newColumn(0))];
		expect(stepUpdateWhere('totals', cells, conditions)).toEqual({
			kind: 'update_where',
			table: 'totals',
			conditions,
			cells
		});
		expect(stepUpdateWhere('totals', cells)).toEqual({
			kind: 'update_where',
			table: 'totals',
			conditions: [],
			cells
		});
	});

	it('stepSelect builds a select step with optional conditions', () => {
		const conditions = [condGt(4, int64Value(10))];
		expect(stepSelect('found', 'orders', conditions)).toEqual({
			kind: 'select',
			id: 'found',
			table: 'orders',
			conditions
		});
	});

	it('stepSetNew builds a set_new step', () => {
		const cells: TriggerCell[] = [cell(1, int64Value(2))];
		expect(stepSetNew(cells)).toEqual({ kind: 'set_new', cells });
	});

	it('stepInsert builds an insert step', () => {
		const cells: TriggerCell[] = [cell(3, textValue('x'))];
		expect(stepInsert('logs', cells)).toEqual({ kind: 'insert', table: 'logs', cells });
	});

	it('stepUpdateByPk builds an update_by_pk step', () => {
		const pk = newColumn(0);
		const cells: TriggerCell[] = [cell(2, boolValue(true))];
		expect(stepUpdateByPk('totals', pk, cells)).toEqual({
			kind: 'update_by_pk',
			table: 'totals',
			pk,
			cells
		});
	});

	it('stepDeleteByPk builds a delete_by_pk step', () => {
		const pk = oldColumn(0);
		expect(stepDeleteByPk('logs', pk)).toEqual({ kind: 'delete_by_pk', table: 'logs', pk });
	});

	it('stepRaise builds a raise step', () => {
		const message = textValue('constraint violated');
		expect(stepRaise('abort', message)).toEqual({
			kind: 'raise',
			action: 'abort',
			message
		});
	});
});

describe('trigger() and triggerJson()', () => {
	it('serializes a full trigger spec using a when clause and mixed steps', () => {
		const spec = trigger({
			name: 'audit_after_insert',
			target: { kind: 'table', name: 'orders' },
			timing: 'after',
			event: 'insert',
			when: exprAnd(
				exprIsNotNull(newColumn(1)),
				exprGt(newColumn(2), int64Value(0))
			),
			program: {
				steps: [
					stepDeleteWhere('stale_rows', [condEq(1, oldColumn(3))]),
					stepUpdateWhere('totals', [makeCell(2, 1)], [condPk(newColumn(0))]),
					stepForeach('found', [stepSelect('s', 'details', [condGt(4, int64Value(10))])])
				]
			}
		});

		const json = JSON.parse(triggerJson(spec));
		expect(json.name).toBe('audit_after_insert');
		expect(json.target).toEqual({ kind: 'table', name: 'orders' });
		expect(json.timing).toBe('after');
		expect(json.event).toBe('insert');
		expect(json.when).toEqual({
			kind: 'and',
			left: { kind: 'is_not_null', value: { kind: 'new_column', value: 1 } },
			right: {
				kind: 'gt',
				left: { kind: 'new_column', value: 2 },
				right: { kind: 'literal', value: { Int64: 0 } }
			}
		});
		expect(json.program.steps).toHaveLength(3);
		expect(json.program.steps[0]).toEqual({
			kind: 'delete_where',
			table: 'stale_rows',
			conditions: [
				{
					kind: 'eq',
					column_id: 1,
					value: { kind: 'old_column', value: 3 }
				}
			]
		});
		expect(json.program.steps[1]).toEqual({
			kind: 'update_where',
			table: 'totals',
			conditions: [{ kind: 'pk', value: { kind: 'new_column', value: 0 } }],
			cells: [{ column_id: 2, value: { kind: 'literal', value: { Int64: 1 } } }]
		});
		expect(json.program.steps[2]).toEqual({
			kind: 'foreach',
			id: 'found',
			steps: [
				{
					kind: 'select',
					id: 's',
					table: 'details',
					conditions: [
						{
							kind: 'gt',
							column_id: 4,
							value: { kind: 'literal', value: { Int64: 10 } }
						}
					]
				}
			]
		});
	});

	it('round-trips builder output through trigger() and triggerJson() without type errors', () => {
		const when = exprNot(exprOr(exprLt(newColumn(1), int64Value(0)), exprIsNull(oldColumn(2))));
		const steps = [
			stepForeach('items', [
				stepUpdateWhere(
					'items',
					[makeCell(5, 99)],
					[condAnd(condGte(1, int64Value(1)), condIsNotNull(2))]
				),
				stepDeleteWhere('shadows', [condNot(condEq(0, selectedColumn(0)))])
			])
		];

		const built: TriggerSpec = {
			name: 'round_trip',
			target: { kind: 'table', name: 'events' },
			timing: 'before',
			event: 'update',
			when,
			program: { steps }
		};

		const spec = trigger(built);
		const json = triggerJson(spec);
		expect(JSON.parse(json).program.steps).toHaveLength(1);
		expect(JSON.parse(json).when.kind).toBe('not');
	});
});

describe('trigger() defaults', () => {
	it('applies defaults for optional fields', () => {
		const spec = trigger({
			name: 'defaults',
			target: { kind: 'table', name: 'orders' },
			timing: 'after',
			event: 'insert',
			program: { steps: [] }
		});

		expect(spec.version).toBe(1);
		expect(spec.update_of).toEqual([]);
		expect(spec.target_columns).toEqual([]);
		expect(spec.enabled).toBe(true);
		expect(spec.checksum).toBe('');
		expect(spec.created_epoch).toBe(0);
		expect(spec.updated_epoch).toBe(0);
	});

	it('lets explicit values override defaults', () => {
		const spec = trigger({
			name: 'overrides',
			version: 3,
			target: { kind: 'table', name: 'orders' },
			timing: 'before',
			event: 'update',
			update_of: ['status'],
			target_columns: [1, 2],
			enabled: false,
			checksum: 'abc',
			created_epoch: 100,
			updated_epoch: 200,
			program: { steps: [] }
		});

		expect(spec.version).toBe(3);
		expect(spec.update_of).toEqual(['status']);
		expect(spec.target_columns).toEqual([1, 2]);
		expect(spec.enabled).toBe(false);
		expect(spec.checksum).toBe('abc');
		expect(spec.created_epoch).toBe(100);
		expect(spec.updated_epoch).toBe(200);
	});
});
