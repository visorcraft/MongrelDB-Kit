/**
 * Unit tests for the pure update-patch pipeline (sanitize → merge → defaults).
 * These drive the shipped helpers in updatePatch.ts — the same functions
 * applyUpdateInTxn uses on the live write path.
 */
import { describe, it, expect } from 'vitest';
import { table, int, text, timestamp } from './schema.js';
import {
	sanitizeUpdatePatch,
	mergeUpdateOntoRow,
	applyUpdateTimestampDefaults,
	prepareMergedUpdateRow,
	changedUpdateColumns,
	patchTouchesForeignKeys,
	DEFAULT_UPDATE_PATCH_POLICY
} from './updatePatch.js';
import { foreignKey } from './schema.js';

const notes = table('notes', {
	columns: [
		int('id', { primaryKey: true }),
		text('title', { nullable: true }),
		text('body', { nullable: true }),
		timestamp('updated_at', { generated: 'now' })
	],
	primaryKey: ['id']
});

const children = table('children', {
	columns: [
		int('id', { primaryKey: true }),
		int('parent_id', { nullable: false }),
		text('label', { nullable: true })
	],
	primaryKey: ['id'],
	foreignKeys: [
		foreignKey(['parent_id'], { table: 'parents', columns: ['id'] }, { name: 'fk_parent' })
	]
});

describe('sanitizeUpdatePatch', () => {
	it('drops undefined keys (undefined means omit)', () => {
		const sanitized = sanitizeUpdatePatch({
			title: 'kept',
			body: undefined,
			extra: null
		});
		expect(sanitized).toEqual({ title: 'kept', extra: null });
		expect(Object.prototype.hasOwnProperty.call(sanitized, 'body')).toBe(false);
	});

	it('preserves explicit null', () => {
		expect(sanitizeUpdatePatch({ title: null })).toEqual({ title: null });
	});

	it('preserves undefined when policy.undefinedMeansOmit is false', () => {
		const sanitized = sanitizeUpdatePatch(
			{ title: undefined },
			{ undefinedMeansOmit: false }
		);
		expect(Object.prototype.hasOwnProperty.call(sanitized, 'title')).toBe(true);
		expect(sanitized.title).toBeUndefined();
	});

	it('default policy is undefinedMeansOmit true', () => {
		expect(DEFAULT_UPDATE_PATCH_POLICY.undefinedMeansOmit).toBe(true);
	});
});

describe('mergeUpdateOntoRow', () => {
	it('overwrites only sanitized keys; omitted stay as stored', () => {
		const existing = { id: 1n, title: 'old', body: 'keep' };
		const merged = mergeUpdateOntoRow(existing, { title: 'new' });
		expect(merged).toEqual({ id: 1n, title: 'new', body: 'keep' });
	});

	it('applies null to clear a column', () => {
		const existing = { id: 1n, title: 'old', body: 'x' };
		const merged = mergeUpdateOntoRow(existing, { title: null });
		expect(merged.title).toBeNull();
		expect(merged.body).toBe('x');
	});
});

describe('prepareMergedUpdateRow', () => {
	const ctx = { now: '2026-07-23T12:00:00.000Z', uuid: () => 'u' };

	it('undefined in raw patch does not clear the stored column', () => {
		const existing = {
			id: 1n,
			title: 'keep me',
			body: 'present',
			updated_at: '2020-01-01T00:00:00.000Z'
		};
		const { sanitizedPatch, merged } = prepareMergedUpdateRow(
			notes,
			existing,
			{ title: undefined, body: 'changed' },
			ctx
		);
		expect(sanitizedPatch).toEqual({ body: 'changed' });
		expect(merged.title).toBe('keep me');
		expect(merged.body).toBe('changed');
		// generated:now refreshes when not in the sanitized patch
		expect(merged.updated_at).toBe(ctx.now);
	});

	it('explicit null clears a nullable column', () => {
		const existing = {
			id: 1n,
			title: 'wipe',
			body: 'stay',
			updated_at: '2020-01-01T00:00:00.000Z'
		};
		const { merged } = prepareMergedUpdateRow(notes, existing, { title: null }, ctx);
		expect(merged.title).toBeNull();
		expect(merged.body).toBe('stay');
	});

	it('does not refresh generated:now when the caller patches that column', () => {
		const existing = {
			id: 1n,
			title: 't',
			body: null,
			updated_at: '2020-01-01T00:00:00.000Z'
		};
		const { merged } = prepareMergedUpdateRow(
			notes,
			existing,
			{ updated_at: '2021-06-01T00:00:00.000Z' },
			ctx
		);
		expect(merged.updated_at).toBe('2021-06-01T00:00:00.000Z');
	});

	it('full-row-style sparse spread with undefined fields is safe after sanitize', () => {
		const existing = {
			id: 1n,
			title: 'hotel',
			body: 'details',
			updated_at: '2020-01-01T00:00:00.000Z'
		};
		// Simulate a bad app spread: some fields undefined
		const sparse = {
			...existing,
			title: 'hotel renamed',
			body: undefined as unknown as string
		};
		const { merged } = prepareMergedUpdateRow(notes, existing, sparse, ctx);
		expect(merged.title).toBe('hotel renamed');
		expect(merged.body).toBe('details');
	});
});

describe('applyUpdateTimestampDefaults', () => {
	it('skips columns present in the sanitized patch', () => {
		const merged = {
			id: 1n,
			title: 't',
			body: null,
			updated_at: 'fixed'
		};
		applyUpdateTimestampDefaults(
			notes,
			merged,
			{ updated_at: 'fixed' },
			{ now: 'new', uuid: () => 'u' }
		);
		expect(merged.updated_at).toBe('fixed');
	});
});

describe('changedUpdateColumns / patchTouchesForeignKeys', () => {
	it('lists only columns whose values differ', () => {
		const existing = { id: 1n, title: 'a', body: 'b' };
		const merged = { id: 1n, title: 'a', body: 'c' };
		expect(changedUpdateColumns(existing, merged, ['id', 'title', 'body'])).toEqual(['body']);
	});

	it('detects FK column presence in sanitized patch', () => {
		expect(patchTouchesForeignKeys(children, { label: 'x' })).toBe(false);
		expect(patchTouchesForeignKeys(children, { parent_id: 2n })).toBe(true);
	});
});
