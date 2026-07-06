export type KitErrorCode =
	| 'STORAGE'
	| 'VALIDATION'
	| 'NOT_FOUND'
	| 'DUPLICATE'
	| 'FOREIGN_KEY'
	| 'RESTRICT'
	| 'CONFLICT'
	| 'TRIGGER_VALIDATION'
	| 'MIGRATION'
	| 'SCHEMA_DRIFT'
	| 'TIMEOUT'
	| 'UNSUPPORTED'
	| 'INTEGRITY'
	| 'AUTH_REQUIRED'
	| 'AUTH_NOT_REQUIRED'
	| 'INVALID_CREDENTIALS'
	| 'PERMISSION_DENIED';

export class KitError extends Error {
	readonly code: KitErrorCode;

	constructor(message: string, code: KitErrorCode = 'STORAGE') {
		super(message);
		this.name = 'KitError';
		this.code = code;
	}
}

export class KitValidationError extends KitError {
	table?: string;
	column?: string;

	constructor(message: string, table?: string, column?: string) {
		super(message, 'VALIDATION');
		this.name = 'KitValidationError';
		this.table = table;
		this.column = column;
	}
}

export class KitNotFoundError extends KitError {
	table: string;
	pk: unknown;

	constructor(table: string, pk: unknown) {
		super(`${table}(${String(pk)}) not found`, 'NOT_FOUND');
		this.name = 'KitNotFoundError';
		this.table = table;
		this.pk = pk;
	}
}

export class KitDuplicateError extends KitError {
	table: string;
	constraint: string;

	constructor(table: string, constraint: string) {
		super(`Duplicate in ${table} for ${constraint}`, 'DUPLICATE');
		this.name = 'KitDuplicateError';
		this.table = table;
		this.constraint = constraint;
	}
}

export class KitForeignKeyError extends KitError {
	table: string;
	constraint: string;

	constructor(table: string, constraint: string) {
		super(`Foreign key violation in ${table} for ${constraint}`, 'FOREIGN_KEY');
		this.name = 'KitForeignKeyError';
		this.table = table;
		this.constraint = constraint;
	}
}

export class KitRestrictError extends KitError {
	table: string;
	constraint: string;

	constructor(table: string, constraint: string) {
		super(`Restrict violation in ${table} for ${constraint}`, 'RESTRICT');
		this.name = 'KitRestrictError';
		this.table = table;
		this.constraint = constraint;
	}
}

export class KitConflictError extends KitError {
	retryable = true;

	constructor(message = 'Conflict') {
		super(message, 'CONFLICT');
		this.name = 'KitConflictError';
	}
}

export class KitTriggerValidationError extends KitError {
	constructor(message: string) {
		super(message, 'TRIGGER_VALIDATION');
		this.name = 'KitTriggerValidationError';
	}
}

export class KitMigrationError extends KitError {
	constructor(message: string) {
		super(message, 'MIGRATION');
		this.name = 'KitMigrationError';
	}
}

export class KitSchemaDriftError extends KitError {
	constructor(message: string) {
		super(message, 'SCHEMA_DRIFT');
		this.name = 'KitSchemaDriftError';
	}
}

export class KitTimeoutError extends KitError {
	constructor(message = 'Transaction timed out') {
		super(message, 'TIMEOUT');
		this.name = 'KitTimeoutError';
	}
}

export class KitUnsupportedError extends KitError {
	constructor(message: string) {
		super(message, 'UNSUPPORTED');
		this.name = 'KitUnsupportedError';
	}
}

/** Thrown when a `require_auth` database is opened without credentials, or an
 * operation runs on a handle with no cached principal. HTTP 401 equivalent. */
export class KitAuthRequiredError extends KitError {
	constructor(message: string) {
		super(message, 'AUTH_REQUIRED');
		this.name = 'KitAuthRequiredError';
	}
}

/** Thrown when a credentialed constructor is used on a credentialless database
 * (the caller picked the wrong constructor). */
export class KitAuthNotRequiredError extends KitError {
	constructor(message: string) {
		super(message, 'AUTH_NOT_REQUIRED');
		this.name = 'KitAuthNotRequiredError';
	}
}

/** Thrown when `openWithCredentials` verification fails (bad username/password).
 * HTTP 401 equivalent. */
export class KitInvalidCredentialsError extends KitError {
	readonly username: string;

	constructor(username: string) {
		super(`invalid credentials for user "${username}"`, 'INVALID_CREDENTIALS');
		this.name = 'KitInvalidCredentialsError';
		this.username = username;
	}
}

/** Thrown when an operation's required permission is not satisfied by the
 * cached principal. HTTP 403 equivalent. */
export class KitPermissionDeniedError extends KitError {
	constructor(message: string) {
		super(message, 'PERMISSION_DENIED');
		this.name = 'KitPermissionDeniedError';
	}
}

/**
 * Returns true when the underlying error is a retryable MongrelDB write-write
 * conflict. The native addon prefixes commit-time conflict errors with
 * `__CONFLICT__:` so callers can detect them without instanceof checks against
 * addon-owned classes that may not cross the require boundary cleanly.
 */
export function isRetryableConflict(err: unknown): boolean {
	if (err instanceof KitConflictError) return true;
	if (err == null || typeof err !== 'object' || !('message' in err)) return false;
	const msg = (err as { message?: unknown }).message;
	return typeof msg === 'string' && msg.startsWith('__CONFLICT__:');
}
