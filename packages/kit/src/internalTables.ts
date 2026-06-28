import { table, int, text, index } from './schema.js';

export const kitSchemaMigrations = table('__kit_schema_migrations', {
	columns: [
		int('version', { primaryKey: true }),
		text('name', { nullable: false }),
		text('checksum', { nullable: false }),
		text('applied_at', { nullable: false }),
		text('kit_version', { nullable: false }),
		text('status', { nullable: false })
	],
	primaryKey: ['version']
});

export const kitSchemaCatalog = table('__kit_schema_catalog', {
	columns: [
		int('schema_version', { primaryKey: true }),
		text('schema_json', { nullable: false }),
		text('checksum', { nullable: false }),
		text('written_at', { nullable: false })
	],
	primaryKey: ['schema_version']
});

export const kitSequences = table('__kit_sequences', {
	columns: [
		text('sequence_name', { primaryKey: true }),
		int('next_value', { nullable: false }),
		text('updated_at', { nullable: false })
	],
	primaryKey: ['sequence_name'],
	indexes: [index(['sequence_name'])]
});

export const kitUniqueKeys = table('__kit_unique_keys', {
	columns: [
		text('encoded_key', { primaryKey: true }),
		text('constraint_name', { nullable: false }),
		text('owner_table', { nullable: false }),
		text('owner_pk', { nullable: false }),
		text('created_at', { nullable: false })
	],
	primaryKey: ['encoded_key'],
	indexes: [index(['owner_table'])]
});

export const kitRowGuards = table('__kit_row_guards', {
	columns: [
		text('encoded_guard_key', { primaryKey: true }),
		text('table_name', { nullable: false }),
		text('primary_key', { nullable: false }),
		int('version', { nullable: false }),
		text('updated_at', { nullable: false })
	],
	primaryKey: ['encoded_guard_key'],
	indexes: [index(['table_name'])]
});

export const kitMigrationLocks = table('__kit_migration_locks', {
	columns: [
		text('lock_name', { primaryKey: true }),
		text('holder', { nullable: false }),
		text('acquired_at', { nullable: false }),
		text('expires_at', { nullable: false })
	],
	primaryKey: ['lock_name']
});

export const internalTables = [
	kitSchemaMigrations,
	kitSchemaCatalog,
	kitSequences,
	kitUniqueKeys,
	kitRowGuards,
	kitMigrationLocks
];
