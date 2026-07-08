import { describe, it, expect } from 'vitest';
import { table, int, text, timestamp, unique, index, foreignKey, check, Schema } from './schema.js';

describe('schema DSL', () => {
	it('builds a users table spec', () => {
		const users = table('users', {
			columns: [
				int('id', { primaryKey: true }),
				text('email', { nullable: false }),
				text('role', { enumValues: ['user', 'admin'] }),
				timestamp('createdAt', { default: { kind: 'now' } })
			],
			primaryKey: ['id'],
			unique: [unique(['email'])],
			indexes: [index(['email'], { name: 'idx_users_email' })]
		});

		expect(users.name).toBe('users');
		expect(users.primaryKey).toEqual(['id']);
		expect(users.columns.map((c) => c.name)).toEqual(['id', 'email', 'role', 'createdAt']);

		const idCol = users.columns.find((c) => c.name === 'id');
		expect(idCol).toMatchObject({ storageType: 'int64', primaryKey: true, nullable: false });

		const emailCol = users.columns.find((c) => c.name === 'email');
		expect(emailCol).toMatchObject({ storageType: 'text', nullable: false });

		const roleCol = users.columns.find((c) => c.name === 'role');
		expect(roleCol?.enumValues).toEqual(['user', 'admin']);

		const createdAtCol = users.columns.find((c) => c.name === 'createdAt');
		expect(createdAtCol?.default).toEqual({ kind: 'now' });

		expect(users.unique).toHaveLength(1);
		expect(users.unique[0]).toMatchObject({ name: 'uq_email', columns: ['email'] });
	});

	it('rejects duplicate column names', () => {
		expect(() =>
			table('bad', {
				columns: [int('id'), int('id')],
				primaryKey: ['id']
			})
		).toThrow('Duplicate column name');
	});

	it('honors explicit column ids and assigns auto ids without collisions', () => {
		const users = table('users', {
			columns: [int('id', { id: 10, primaryKey: true }), text('email', { id: 1 }), text('name')],
			primaryKey: ['id']
		});

		expect(users.columns.map((c) => [c.name, c.id])).toEqual([
			['id', 10],
			['email', 1],
			['name', 2]
		]);
	});

	it('rejects invalid explicit column ids', () => {
		expect(() =>
			table('bad', {
				columns: [int('id', { id: 0, primaryKey: true })],
				primaryKey: ['id']
			})
		).toThrow('invalid id');
	});

	it('rejects missing primary key columns', () => {
		expect(() =>
			table('bad', {
				columns: [int('id')],
				primaryKey: ['missing']
			})
		).toThrow('Primary key column');
	});

	it('supports foreign keys and checks', () => {
		const posts = table('posts', {
			columns: [int('id', { primaryKey: true }), int('authorId')],
			primaryKey: ['id'],
			foreignKeys: [
				foreignKey(['authorId'], { table: 'users', columns: ['id'] }, { onDelete: 'cascade' })
			],
			checks: [
				check('positive_id', (row) => typeof row.id === 'number' && row.id > 0)
			]
		});

		expect(posts.foreignKeys[0]).toMatchObject({
			name: 'fk_authorId_users',
			columns: ['authorId'],
			referencesTable: 'users',
			referencesColumns: ['id'],
			onDelete: 'cascade'
		});
		expect(posts.checks[0].name).toBe('positive_id');
	});

	it('Schema holds tables and provides lookup', () => {
		const users = table('users', {
			columns: [int('id', { primaryKey: true })],
			primaryKey: ['id']
		});
		const posts = table('posts', {
			columns: [int('id', { primaryKey: true })],
			primaryKey: ['id']
		});

		const schema = new Schema([users, posts]);
		expect(schema.table('users').name).toBe('users');
		expect(schema.table('posts').name).toBe('posts');
		expect(schema.hasTable('comments')).toBe(false);
		expect(schema.tablesList()).toHaveLength(2);
	});

	it('Schema rejects duplicate table ids', () => {
		const a = table('a', { id: 1, columns: [int('id')], primaryKey: ['id'] });
		const b = table('b', { id: 1, columns: [int('id')], primaryKey: ['id'] });
		expect(() => new Schema([a, b])).toThrow('Duplicate table id');
	});
});
