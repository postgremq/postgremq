/**
 * PostgreMQ typed errors.
 *
 * The SQL functions raise exceptions with custom SQLSTATE codes (PMQ01,
 * PMQ02, PMQ03) so application code can match on type, not error text.
 * The classes below mirror the Go client's sentinels (ErrLeaseLost,
 * ErrQueueNotFound, ErrValidation).
 *
 * Use `instanceof LeaseLostError` (or check `err.code === 'PMQ01'`) to
 * branch on the failure mode. Some admin methods (e.g. deleteQueue,
 * cleanUpTopic, cleanupCompletedMessages) pass the database error through
 * unmapped; match those on `err.code`.
 */

import { formatSchemaVersion } from './schema-version';

/** SQLSTATE code for "lease lost" (PMQ01). */
export const ErrCodeLeaseLost = 'PMQ01';
/** SQLSTATE code for "queue or topic not found" (PMQ02). */
export const ErrCodeQueueNotFound = 'PMQ02';
/** SQLSTATE code for "validation error" (PMQ03). */
export const ErrCodeValidation = 'PMQ03';

/**
 * LeaseLostError is thrown by ack/ackWithTransaction/nack/release/setVt when
 * this consumer does not hold the delivery: it was settled, re-claimed by
 * another consumer (token mismatch) or removed (e.g. cleanUpQueue,
 * deleteQueueMessage, queue deletion). Settlement is fenced by the delivery
 * token only: an expired visibility timeout alone does not fail
 * ack/nack/release, but setVt also fails once it has expired. Handlers must
 * be idempotent — receiving this error means another consumer may have
 * processed the same message.
 */
export class LeaseLostError extends Error {
  readonly code = ErrCodeLeaseLost;
  constructor(message: string) {
    super(message);
    this.name = 'LeaseLostError';
  }
}

/**
 * QueueNotFoundError is thrown when a referenced queue or topic does not
 * exist (or is an expired exclusive queue). Usually a misconfiguration
 * — the operation will fail until the queue/topic is created (an expired
 * exclusive queue must be deleted first). A consumer whose queue is gone
 * reports QueueFatalError instead.
 */
export class QueueNotFoundError extends Error {
  readonly code = ErrCodeQueueNotFound;
  constructor(message: string) {
    super(message);
    this.name = 'QueueNotFoundError';
  }
}

/**
 * ValidationError is thrown on caller-rejection paths: input validation
 * failures (negative timeouts, mismatched batch arrays, names containing
 * characters outside `[A-Za-z0-9_:.\-]`) AND state-precondition failures
 * (a queue re-created with different parameters, deleting a topic that
 * still has messages). The common thread is "the request was rejected
 * before any state change." Preconditions checked by deleteQueue (DLQ
 * entries) and cleanUpTopic (messages in the DLQ) arrive as a database
 * error with code 'PMQ03' instead; the client-side option checks of the
 * connection options, createQueue and maxInFlight throw a plain Error.
 */
export class ValidationError extends Error {
  readonly code = ErrCodeValidation;
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

/**
 * QueueFatalError is the reason a consumer was torn down: the queue it depends
 * on is gone — deleted out-of-band (a consume returned PMQ02), or an exclusive
 * queue whose keep-alive permanently failed. It is delivered to
 * Consumer.onClose / HandlerConsumer.onClose and to the connection-level
 * `onQueueFatal` handler / 'queueFatal' event. Mirrors the Go client's
 * QueueFatalError + ErrQueueGone. Check `err instanceof QueueFatalError`.
 */
export class QueueFatalError extends Error {
  /** The queue that is gone. */
  readonly queue: string;
  /** The underlying cause (e.g. a QueueNotFoundError when deleted out-of-band). */
  readonly cause?: Error;
  constructor(queue: string, cause?: Error) {
    super(`postgremq: queue "${queue}" is gone${cause ? `: ${cause.message}` : ''}`);
    this.name = 'QueueFatalError';
    this.queue = queue;
    this.cause = cause;
  }
}

/**
 * ConnectionClosedError is thrown by `connect()` after close(), and by
 * operations on a connection that is not connected (before connect() or
 * after close(); publish, publishWithTransaction, consume, consumeHandler,
 * createTopic and createQueue also once close() has begun). close() is terminal: a connection cannot be
 * resurrected — use the package-level `connect()` to get a new one. Mirrors
 * the Go client's ErrConnectionClosed sentinel.
 *
 * Pure client-side error (no SQLSTATE), so it carries no `code`.
 */
export class ConnectionClosedError extends Error {
  constructor(message: string = 'Connection has been closed; construct a new Connection to reconnect') {
    super(message);
    this.name = 'ConnectionClosedError';
  }
}

/**
 * CompatibilityError is thrown by `Connection.connect()` when the database's
 * PostgreMQ schema is on an mq line this client does not support (see
 * `SUPPORTED_MQ_VERSIONS`), reports no schema version, or has no discovery
 * function (`postgremq.info()`) and needs an installation or upgrade. Mirrors
 * the Go client's CompatibilityError / ErrIncompatibleSchema.
 *
 * Pure client-side error (no SQLSTATE), so it carries no `code`; when
 * discovery is missing, `cause` is the database error.
 */
export class CompatibilityError extends Error {
  /**
   * The installed schema version (the last migration applied;
   * `formatSchemaVersion()` gives its mq version); undefined when discovery is
   * missing or reports none.
   */
  readonly schemaVersion?: number;
  /** The mq lines this client supports (`SUPPORTED_MQ_VERSIONS`). */
  readonly supportedVersions: readonly string[];
  /** The database error when discovery is missing. */
  readonly cause?: Error;
  constructor(details: { schemaVersion?: number; supportedVersions: readonly string[]; cause?: Error }) {
    const supported = `[${details.supportedVersions.join(', ')}]`;
    super(
      details.cause
        ? `postgremq: postgremq.info() is unavailable; the database needs a PostgreMQ installation or upgrade (client supports mq ${supported}): ${details.cause.message}`
        : details.schemaVersion === undefined
          ? `postgremq: the database's PostgreMQ installation reports no schema version; it needs an installation or upgrade (client supports mq ${supported})`
          : `postgremq: the database's PostgreMQ schema is mq ${formatSchemaVersion(details.schemaVersion)} (schema version ${details.schemaVersion}); this client supports mq ${supported}`
    );
    this.name = 'CompatibilityError';
    this.schemaVersion = details.schemaVersion;
    this.supportedVersions = details.supportedVersions;
    this.cause = details.cause;
  }
}

/**
 * DirtySchemaError is thrown by `migrate()` when the recorded schema version
 * is marked dirty: a migration failed partway, so the schema is in an unknown
 * state. Fix the schema by hand, then clear the flag in
 * `postgremq.postgremq_migrations`. Mirrors the Go client's
 * `*DirtySchemaError` (`ErrDirtySchema`).
 *
 * Pure client-side error (no SQLSTATE), so it carries no `code`.
 */
export class DirtySchemaError extends Error {
  /** The version whose migration did not finish. */
  readonly version: number;
  constructor(version: number) {
    super(`postgremq: database schema is dirty at version ${version}; a migration failed partway and needs manual repair`);
    this.name = 'DirtySchemaError';
    this.version = version;
  }
}

/**
 * UnknownSchemaVersionError is thrown by `migrate()` when the recorded schema
 * version is below this package's latest migration but is none of its
 * migrations, so no upgrade from it is known. Nothing is applied. Mirrors the
 * Go client's `*UnknownSchemaVersionError` (`ErrUnknownSchemaVersion`).
 */
export class UnknownSchemaVersionError extends Error {
  /** The recorded schema version. */
  readonly version: number;
  constructor(version: number) {
    super(
      `postgremq: database schema version ${version} (mq ${formatSchemaVersion(version)}) is not one of this client's migrations, so no upgrade from it is known`,
    );
    this.name = 'UnknownSchemaVersionError';
    this.version = version;
  }
}

/** The fields of a node-postgres database error a MigrationError reports. */
interface DatabaseErrorFields {
  severity?: string;
  code?: string;
  detail?: string;
  hint?: string;
  internalQuery?: string;
  where?: string;
}

/**
 * MigrationError is thrown by `migrate()` when a migration fails. Its version
 * is left dirty (`migrate()` then throws DirtySchemaError until it is
 * repaired). The migration runs in one transaction: when the database
 * reported the error (`cause.code` is its SQLSTATE), it was rolled back; after
 * a connection failure or a query timeout its outcome is unknown, so inspect
 * the schema before repairing.
 * The database error is `cause` (its SQLSTATE is `cause.code`). The message
 * adds the server's detail, hint, the failing query inside a function and its
 * context. Mirrors the Go client's `*MigrationError` (`ErrMigrationFailed`).
 */
export class MigrationError extends Error {
  /** The schema version of the migration that failed. */
  readonly version: number;
  /**
   * The line and column of the error in the migration's file, from 1, when
   * PostgreSQL reports a position (it does for errors in the file's
   * statements, including a function's definition). Absent too when the
   * position cannot be mapped to the file: with a multibyte database or client
   * encoding other than UTF8 (such as EUC_JP) the server counts other
   * characters.
   */
  readonly line?: number;
  readonly column?: number;
  /** The database error. */
  readonly cause: Error;
  constructor(details: { version: number; line?: number; column?: number; cause: Error }) {
    const db = details.cause as Error & DatabaseErrorFields;
    const at = details.line !== undefined ? ` at line ${details.line}, column ${details.column}` : '';
    const error = db.severity && db.code ? `${db.severity}: ${db.message} (SQLSTATE ${db.code})` : db.message;
    const extra = (
      [
        ['detail', db.detail],
        ['hint', db.hint],
        ['query', db.internalQuery],
        ['context', db.where],
      ] as const
    )
      .filter(([, value]) => value)
      .map(([name, value]) => `; ${name}: ${value}`)
      .join('');
    super(
      `postgremq: migration ${formatSchemaVersion(details.version)} (schema version ${details.version}) failed${at}: ${error}${extra}`,
    );
    this.name = 'MigrationError';
    this.version = details.version;
    this.line = details.line;
    this.column = details.column;
    this.cause = details.cause;
  }
}

/**
 * Map a database error to one of the typed errors, if its SQLSTATE matches
 * a PostgreMQ code. Otherwise returns the input unchanged. nil pass-through.
 *
 * @internal — used by the Connection wrappers.
 */
export function mapDbError(err: any): Error {
  if (err == null) return err;
  // node-pg surfaces the SQLSTATE on `code`. Some drivers may use
  // `sqlState`. Both are safe to inspect.
  const code: string | undefined = err.code ?? err.sqlState;
  if (typeof code !== 'string') return err;
  switch (code) {
    case ErrCodeLeaseLost:
      return new LeaseLostError(err.message ?? 'lease lost');
    case ErrCodeQueueNotFound:
      return new QueueNotFoundError(err.message ?? 'queue or topic not found');
    case ErrCodeValidation:
      return new ValidationError(err.message ?? 'validation error');
  }
  return err;
}
