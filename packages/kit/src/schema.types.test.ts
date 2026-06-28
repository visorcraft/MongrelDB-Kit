import { describe, it, expectTypeOf } from 'vitest';
import { table, int, text, real, bool, json, timestamp } from './schema.js';
import type { Row, Insert, Update, TableSpec } from './types.js';

describe('schema type inference', () => {
	const users = table('users', {
		columns: [
			int('id', { primaryKey: true }),
			text('email', { nullable: false }),
			text('role', { enumValues: ['user', 'admin'] }),
			real('score'),
			bool('active'),
			json('meta'),
			timestamp('createdAt', { default: { kind: 'now' } }),
			text('nickname', { nullable: true })
		],
		primaryKey: ['id']
	});

	type UsersRow = Row<typeof users>;
	type UsersInsert = Insert<typeof users>;
	type UsersUpdate = Update<typeof users>;

	it('infers Row types', () => {
		expectTypeOf<UsersRow['id']>().toEqualTypeOf<bigint>();
		expectTypeOf<UsersRow['email']>().toEqualTypeOf<string>();
		expectTypeOf<UsersRow['role']>().toEqualTypeOf<string>();
		expectTypeOf<UsersRow['score']>().toEqualTypeOf<number>();
		expectTypeOf<UsersRow['active']>().toEqualTypeOf<boolean>();
		expectTypeOf<UsersRow['meta']>().toEqualTypeOf<unknown>();
		expectTypeOf<UsersRow['createdAt']>().toEqualTypeOf<unknown>();
		expectTypeOf<UsersRow['nickname']>().toEqualTypeOf<string | null>();
	});

	it('infers Insert types', () => {
		expectTypeOf<UsersInsert>().toHaveProperty('id');
		expectTypeOf<UsersInsert>().toHaveProperty('email');
		expectTypeOf<UsersInsert>().not.toHaveProperty('createdAt');
		expectTypeOf<UsersInsert['nickname']>().toEqualTypeOf<string | null>();
	});

	it('infers Update types', () => {
		expectTypeOf<UsersUpdate>().toHaveProperty('id');
		expectTypeOf<UsersUpdate['id']>().toEqualTypeOf<bigint | undefined>();
		expectTypeOf<UsersUpdate['nickname']>().toEqualTypeOf<string | null | undefined>();
	});

	it('works with generic TableSpec parameter', () => {
		function acceptRow<T extends TableSpec>(_row: Row<T>) {
			return _row;
		}
		const row: UsersRow = {
			id: 1n,
			email: 'a@b.com',
			role: 'user',
			score: 0,
			active: true,
			meta: null,
			createdAt: null,
			nickname: null
		};
		acceptRow<typeof users>(row);
	});
});
