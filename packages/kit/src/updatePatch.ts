/**
 * Modular update-patch pipeline for Kit mutations.
 *
 * Every product update (`updateTable.set`, `onConflictDoUpdate`, …) runs:
 *
 *   1. {@link sanitizeUpdatePatch}  — normalize caller intent
 *   2. {@link mergeUpdateOntoRow}   — apply onto the stored row
 *   3. {@link applyUpdateTimestampDefaults} — refresh `generated: 'now'` when not patched
 *   4. write phase (delete + put + guards) — remains in the query/txn layer
 *
 * Pure steps are exported so unit tests and future bindings share one policy
 * without re-implementing merge rules inside transaction helpers.
 */

import type { TableSpec } from './types.js';
import type { DefaultContext } from './defaults.js';

/** Policy knobs for {@link sanitizeUpdatePatch}. Defaults match product semantics. */
export interface UpdatePatchPolicy {
	/**
	 * When true (default), keys whose value is `undefined` are dropped from the
	 * patch (**omit**). Callers must pass explicit `null` to write SQL NULL.
	 *
	 * When false, `undefined` is left in the object for a custom merge step
	 * (not used by the product path).
	 */
	undefinedMeansOmit?: boolean;
}

export const DEFAULT_UPDATE_PATCH_POLICY: Readonly<Required<UpdatePatchPolicy>> = {
	undefinedMeansOmit: true
};

/**
 * Stage 1 — sanitize: drop non-intent keys so merge cannot mis-interpret them.
 *
 * Product rule: **`undefined` means omit** (safe for sparse objects and
 * accidental `{ ...row, field: maybeUndefined }` spreads). Explicit `null`
 * is preserved so callers can clear nullable columns.
 */
export function sanitizeUpdatePatch(
	patch: Record<string, unknown>,
	policy: UpdatePatchPolicy = DEFAULT_UPDATE_PATCH_POLICY
): Record<string, unknown> {
	const undefinedMeansOmit = policy.undefinedMeansOmit ?? true;
	const out: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(patch)) {
		if (undefinedMeansOmit && value === undefined) continue;
		out[key] = value;
	}
	return out;
}

/**
 * Stage 2 — merge a **sanitized** patch onto the on-disk row.
 *
 * Keys present in `sanitizedPatch` overwrite (including `null` → SQL NULL later).
 * Keys absent leave the stored value alone.
 */
export function mergeUpdateOntoRow(
	existingRow: Record<string, unknown>,
	sanitizedPatch: Record<string, unknown>
): Record<string, unknown> {
	return { ...existingRow, ...sanitizedPatch };
}

/**
 * Stage 3 — refresh write-managed timestamps that the caller did not patch.
 *
 * Only `generated: 'now'` columns refresh on update. Plain `default: nowDefault()`
 * is insert-time only and is not touched here.
 */
export function applyUpdateTimestampDefaults(
	table: TableSpec,
	merged: Record<string, unknown>,
	sanitizedPatch: Record<string, unknown>,
	ctx: DefaultContext
): void {
	for (const col of table.columns) {
		if (Object.prototype.hasOwnProperty.call(sanitizedPatch, col.name)) continue;
		if (col.generated === 'now') {
			merged[col.name] = ctx.now;
		}
	}
}

/**
 * Full pure pipeline through merge (no I/O): sanitize → merge → timestamp defaults.
 * The live write path uses this then validate + delete + put.
 */
export function prepareMergedUpdateRow(
	table: TableSpec,
	existingRow: Record<string, unknown>,
	patch: Record<string, unknown>,
	ctx: DefaultContext,
	policy: UpdatePatchPolicy = DEFAULT_UPDATE_PATCH_POLICY
): { sanitizedPatch: Record<string, unknown>; merged: Record<string, unknown> } {
	const sanitizedPatch = sanitizeUpdatePatch(patch, policy);
	const merged = mergeUpdateOntoRow(existingRow, sanitizedPatch);
	applyUpdateTimestampDefaults(table, merged, sanitizedPatch, ctx);
	return { sanitizedPatch, merged };
}

/**
 * Column names whose values differ between existing and merged rows.
 * Useful for future index-delta maintenance and guard narrowing.
 */
export function changedUpdateColumns(
	existingRow: Record<string, unknown>,
	merged: Record<string, unknown>,
	columnNames: readonly string[]
): string[] {
	return columnNames.filter((name) => existingRow[name] !== merged[name]);
}

/**
 * True when the sanitized patch mentions any foreign-key column of `table`.
 */
export function patchTouchesForeignKeys(
	table: TableSpec,
	sanitizedPatch: Record<string, unknown>
): boolean {
	return table.foreignKeys.some((fk) =>
		fk.columns.some((colName) => Object.prototype.hasOwnProperty.call(sanitizedPatch, colName))
	);
}
