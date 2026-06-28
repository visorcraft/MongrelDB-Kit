export class KitError extends Error {
	constructor(message: string) {
		super(message);
		this.name = 'KitError';
	}
}

export class KitValidationError extends KitError {
	table?: string;
	column?: string;

	constructor(message: string, table?: string, column?: string) {
		super(message);
		this.name = 'KitValidationError';
		this.table = table;
		this.column = column;
	}
}

export class KitNotFoundError extends KitError {
	table: string;
	pk: unknown;

	constructor(table: string, pk: unknown) {
		super(`${table}(${String(pk)}) not found`);
		this.name = 'KitNotFoundError';
		this.table = table;
		this.pk = pk;
	}
}

export class KitDuplicateError extends KitError {
	table: string;
	constraint: string;

	constructor(table: string, constraint: string) {
		super(`Duplicate in ${table} for ${constraint}`);
		this.name = 'KitDuplicateError';
		this.table = table;
		this.constraint = constraint;
	}
}

export class KitForeignKeyError extends KitError {
	table: string;
	constraint: string;

	constructor(table: string, constraint: string) {
		super(`Foreign key violation in ${table} for ${constraint}`);
		this.name = 'KitForeignKeyError';
		this.table = table;
		this.constraint = constraint;
	}
}

export class KitConflictError extends KitError {
	constructor(message = 'Conflict') {
		super(message);
		this.name = 'KitConflictError';
	}
}

export class KitMigrationError extends KitError {
	constructor(message: string) {
		super(message);
		this.name = 'KitMigrationError';
	}
}
