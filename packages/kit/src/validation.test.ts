import { describe, it, expect } from 'vitest';
import { table, int, text, real, bool, json } from './schema.js';
import { staticDefault, nowDefault, applyDefaults, type DefaultContext } from './defaults.js';
import { validateRow } from './validation.js';
import { KitValidationError } from './errors.js';

describe('validateRow', () => {
	const users = table('users', {
		columns: [
			int('id', { primaryKey: true }),
			text('email', { nullable: false }),
			text('role', { enumValues: ['user', 'admin'] }),
			real('score', { min: 0, max: 100 }),
			bool('active'),
			json('meta'),
			text('code', { minLength: 2, maxLength: 6, regex: /^[A-Z]+$/ }),
			text('note', { nullable: true, check: (v: unknown) => typeof v === 'string' && v.length > 0 })
		],
		primaryKey: ['id']
	});

	it('valid row passes', () => {
		expect(() =>
			validateRow(users, {
				id: 1n,
				email: 'a@b.com',
				role: 'user',
				score: 50,
				active: true,
				meta: { tags: ['x'] },
				code: 'ABC',
				note: null
			})
		).not.toThrow();
	});

	it('missing not-null column throws', () => {
		expect(() => validateRow(users, { id: 1n, email: 'a@b.com' })).toThrow(KitValidationError);
		expect(() => validateRow(users, { id: 1n, email: 'a@b.com' })).toThrow(/role/);
	});

	it('wrong type throws', () => {
		expect(() => validateRow(users, { id: 1, email: 'a@b.com', role: 'user' })).toThrow(KitValidationError);
		expect(() => validateRow(users, { id: 1, email: 'a@b.com', role: 'user' })).toThrow(/id/);
	});

	it('enum violation throws', () => {
		expect(() =>
			validateRow(users, { id: 1n, email: 'a@b.com', role: 'superuser' })
		).toThrow(KitValidationError);
		expect(() =>
			validateRow(users, { id: 1n, email: 'a@b.com', role: 'superuser' })
		).toThrow(/superuser/);
	});

	it('range violation throws', () => {
		expect(() =>
			validateRow(users, { id: 1n, email: 'a@b.com', role: 'user', score: -1 })
		).toThrow(KitValidationError);
		expect(() =>
			validateRow(users, { id: 1n, email: 'a@b.com', role: 'user', score: 101 })
		).toThrow(KitValidationError);
	});

	it('length violation throws', () => {
		expect(() =>
			validateRow(users, { id: 1n, email: 'a@b.com', role: 'user', code: 'A' })
		).toThrow(KitValidationError);
		expect(() =>
			validateRow(users, { id: 1n, email: 'a@b.com', role: 'user', code: 'ABCDEFG' })
		).toThrow(KitValidationError);
	});

	it('regex violation throws', () => {
		expect(() =>
			validateRow(users, { id: 1n, email: 'a@b.com', role: 'user', code: 'abc' })
		).toThrow(KitValidationError);
	});

	it('custom check violation throws', () => {
		expect(() =>
			validateRow(users, { id: 1n, email: 'a@b.com', role: 'user', note: '' })
		).toThrow(KitValidationError);
	});

	it('defaults applied before validation', () => {
		const items = table('items', {
			columns: [
				int('id', { primaryKey: true }),
				text('name', { nullable: false, default: staticDefault('unnamed') }),
				text('createdAt', { nullable: false, default: nowDefault() })
			],
			primaryKey: ['id']
		});
		const ctx: DefaultContext = {
			now: '2024-01-01T00:00:00Z',
			uuid: () => '00000000-0000-0000-0000-000000000000',
			allocateSequence: () => 1n
		};
		const row = applyDefaults(items, { id: 1n }, ctx);
		expect(() => validateRow(items, row)).not.toThrow();
		expect(row.name).toBe('unnamed');
		expect(row.createdAt).toBe('2024-01-01T00:00:00Z');
	});
});
