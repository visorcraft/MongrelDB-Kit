import { KitValidationError } from './errors.js';
import type { TableSpec, ColumnSpec, ColumnStorageType } from './types.js';

function typeError(value: unknown, storageType: ColumnStorageType): string | undefined {
	switch (storageType) {
		case 'bool':
			return typeof value === 'boolean' ? undefined : 'must be a boolean';
		case 'int64':
			return typeof value === 'bigint' ? undefined : 'must be a bigint';
		case 'float64':
			return typeof value === 'number' ? undefined : 'must be a number';
		case 'text':
			return typeof value === 'string' ? undefined : 'must be a string';
		case 'bytes':
			return value instanceof Uint8Array ? undefined : 'must be a Uint8Array';
		case 'json': {
			try {
				JSON.stringify(value);
				return undefined;
			} catch {
				return 'must be JSON serializable';
			}
		}
		case 'timestamp':
			return typeof value === 'string' ? undefined : 'must be an ISO timestamp string';
		case 'date':
			return typeof value === 'string' ? undefined : 'must be a date string';
	}
}

function validateColumn(tableName: string, column: ColumnSpec, value: unknown): void {
	if (value === null || value === undefined) {
		if (!column.nullable) {
			throw new KitValidationError(
				`Column "${column.name}" cannot be null`,
				tableName,
				column.name
			);
		}
		return;
	}

	const typeErr = typeError(value, column.storageType);
	if (typeErr) {
		throw new KitValidationError(`Column "${column.name}" ${typeErr}`, tableName, column.name);
	}

	if (column.enumValues && typeof value === 'string' && !column.enumValues.includes(value)) {
		throw new KitValidationError(
			`Value "${value}" for "${column.name}" must be one of ${column.enumValues.join(', ')}`,
			tableName,
			column.name
		);
	}

	if (column.min !== undefined) {
		if (typeof value === 'bigint') {
			const min = BigInt(column.min);
			if (value < min) {
				throw new KitValidationError(
					`Value for "${column.name}" must be at least ${column.min}`,
					tableName,
					column.name
				);
			}
		} else if (typeof value === 'number' && value < column.min) {
			throw new KitValidationError(
				`Value for "${column.name}" must be at least ${column.min}`,
				tableName,
				column.name
			);
		}
	}

	if (column.max !== undefined) {
		if (typeof value === 'bigint') {
			const max = BigInt(column.max);
			if (value > max) {
				throw new KitValidationError(
					`Value for "${column.name}" must be at most ${column.max}`,
					tableName,
					column.name
				);
			}
		} else if (typeof value === 'number' && value > column.max) {
			throw new KitValidationError(
				`Value for "${column.name}" must be at most ${column.max}`,
				tableName,
				column.name
			);
		}
	}

	if (column.minLength !== undefined) {
		const length = typeof value === 'string' ? value.length : value instanceof Uint8Array ? value.length : null;
		if (length !== null && length < column.minLength) {
			throw new KitValidationError(
				`Value for "${column.name}" must have length at least ${column.minLength}`,
				tableName,
				column.name
			);
		}
	}

	if (column.maxLength !== undefined) {
		const length = typeof value === 'string' ? value.length : value instanceof Uint8Array ? value.length : null;
		if (length !== null && length > column.maxLength) {
			throw new KitValidationError(
				`Value for "${column.name}" must have length at most ${column.maxLength}`,
				tableName,
				column.name
			);
		}
	}

	if (column.regex && typeof value === 'string' && !column.regex.test(value)) {
		throw new KitValidationError(
			`Value for "${column.name}" does not match required pattern`,
			tableName,
			column.name
		);
	}

	if (column.check) {
		const result = column.check(value);
		if (result !== true) {
			throw new KitValidationError(
				typeof result === 'string' ? result : `Value for "${column.name}" failed custom check`,
				tableName,
				column.name
			);
		}
	}
}

export function validateRow(table: TableSpec, row: Record<string, unknown>): void {
	for (const column of table.columns) {
		validateColumn(table.name, column, row[column.name]);
	}

	for (const check of table.checks) {
		const result = check.expr(row);
		if (result !== true) {
			throw new KitValidationError(
				typeof result === 'string' ? result : `Table check "${check.name}" failed`,
				table.name
			);
		}
	}
}
