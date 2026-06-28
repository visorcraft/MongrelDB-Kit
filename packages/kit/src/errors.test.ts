import { describe, it, expect } from 'vitest';
import {
	KitError,
	KitValidationError,
	KitNotFoundError,
	KitDuplicateError,
	KitForeignKeyError,
	KitRestrictError,
	KitConflictError,
	KitMigrationError,
	KitSchemaDriftError,
	KitTimeoutError,
	KitUnsupportedError,
	isRetryableConflict
} from './errors.js';

describe('error taxonomy', () => {
	it('every error class exposes a stable uppercase .code property', () => {
		const cases: [Error, string][] = [
			[new KitError('msg'), 'STORAGE'],
			[new KitValidationError('msg'), 'VALIDATION'],
			[new KitNotFoundError('users', 1n), 'NOT_FOUND'],
			[new KitDuplicateError('users', 'uq_email'), 'DUPLICATE'],
			[new KitForeignKeyError('orders', 'fk_user'), 'FOREIGN_KEY'],
			[new KitRestrictError('users', 'fk_user'), 'RESTRICT'],
			[new KitConflictError(), 'CONFLICT'],
			[new KitMigrationError('boom'), 'MIGRATION'],
			[new KitSchemaDriftError('mismatch'), 'SCHEMA_DRIFT'],
			[new KitTimeoutError(), 'TIMEOUT'],
			[new KitUnsupportedError('nope'), 'UNSUPPORTED']
		];

		for (const [err, expectedCode] of cases) {
			expect((err as unknown as { code: string }).code).toBe(expectedCode);
		}
	});

	it('validation error preserves table and column context', () => {
		const err = new KitValidationError('bad', 'users', 'email');
		expect(err.table).toBe('users');
		expect(err.column).toBe('email');
		expect(err.code).toBe('VALIDATION');
	});

	it('conflict error is flagged retryable', () => {
		const err = new KitConflictError();
		expect(err.retryable).toBe(true);
		expect(err.code).toBe('CONFLICT');
	});

	it('isRetryableConflict recognises KitConflictError', () => {
		expect(isRetryableConflict(new KitConflictError())).toBe(true);
	});

	it('isRetryableConflict recognises native __CONFLICT__ messages', () => {
		const nativeLike = new Error('__CONFLICT__: write-write on row 42');
		expect(isRetryableConflict(nativeLike)).toBe(true);
	});

	it('isRetryableConflict rejects ordinary errors', () => {
		expect(isRetryableConflict(new Error('not a conflict'))).toBe(false);
		expect(isRetryableConflict(null)).toBe(false);
		expect(isRetryableConflict(undefined)).toBe(false);
	});
});
