import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase, Schema, table, int, text } from './index.js';

const widgets = table('widgets', {
	columns: [
		int('id', { primaryKey: true }),
		text('name', { nullable: false })
	],
	primaryKey: ['id']
});

const gadgets = table('gadgets', {
	columns: [
		int('id', { primaryKey: true }),
		text('name', { nullable: false })
	],
	primaryKey: ['id']
});

const schema = new Schema([widgets, gadgets]);
const migrations = [{ version: 1, name: 'init', up: () => {} }];

function fresh() {
	const dir = mkdtempSync(join(tmpdir(), 'kit-rename-'));
	const db = KitDatabase.openSync(dir, schema);
	db.migrateSync(schema, migrations);
	return { db, dir };
}

describe('KitDatabase.renameTable', () => {
	it('renames a live table and updates tableNames', () => {
		const { db, dir } = fresh();
		try {
			expect(db.tableNames().sort()).toEqual(['gadgets', 'widgets']);
			db.renameTable('widgets', 'things');
			expect(db.tableNames().sort()).toEqual(['gadgets', 'things']);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('rejects renaming onto an existing table name', () => {
		const { db, dir } = fresh();
		try {
			expect(() => db.renameTable('widgets', 'gadgets')).toThrow();
			// Neither table changed.
			expect(db.tableNames().sort()).toEqual(['gadgets', 'widgets']);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('rejects a missing source table', () => {
		const { db, dir } = fresh();
		try {
			expect(() => db.renameTable('ghost', 'x')).toThrow();
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('forbids __kit_-prefixed names in either direction', () => {
		const { db, dir } = fresh();
		try {
			// Renaming an app table TO a __kit_ name would hide it from
			// tableNames() (which filters that prefix) — rejected.
			expect(() => db.renameTable('widgets', '__kit_evil')).toThrow();
			// Renaming an internal table AWAY from its expected name would break
			// the Kit's by-name lookups — also rejected.
			expect(() => db.renameTable('__kit_schema_catalog', 'widgets')).toThrow();
			// Nothing changed.
			expect(db.tableNames().sort()).toEqual(['gadgets', 'widgets']);
		} finally {
			db.close();
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('survives reopen when the schema is updated to match', () => {
		// A durable rename in a Kit app pairs the runtime rename with an update
		// to the code-defined schema (typically inside a migration's up()): the
		// TableSpec is renamed to match, so on reopen ensureAppTable finds the
		// already-renamed table and does not re-create the old name. This test
		// mirrors that pattern.
		const { db, dir } = fresh();
		db.renameTable('widgets', 'things');
		db.close();
		try {
			const things = table('things', {
				columns: [
					int('id', { primaryKey: true }),
					text('name', { nullable: false })
				],
				primaryKey: ['id']
			});
			const renamedSchema = new Schema([things, gadgets]);
			const db2 = KitDatabase.openSync(dir, renamedSchema);
			db2.migrateSync(renamedSchema, migrations);
			// 'things' persisted from the rename; 'widgets' is NOT re-created
			// because the updated schema no longer declares it.
			expect(db2.tableNames().sort()).toEqual(['gadgets', 'things']);
			db2.close();
		} finally {
			try {
				rmSync(dir, { recursive: true, force: true });
			} catch {
				/* dir may already be removed */
			}
		}
	});
});
