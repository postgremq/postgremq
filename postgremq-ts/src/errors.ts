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
 * PostgreMQ schema has an mq major this client does not support (see
 * `SUPPORTED_MQ_MAJORS`), reports no schema version, or has no discovery
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
  /** The mq majors this client supports. */
  readonly supportedMajors: readonly number[];
  /** The database error when discovery is missing. */
  readonly cause?: Error;
  constructor(details: { schemaVersion?: number; supportedMajors: readonly number[]; cause?: Error }) {
    const supported = `[${details.supportedMajors.join(', ')}]`;
    super(
      details.cause
        ? `postgremq: postgremq.info() is unavailable; the database needs a PostgreMQ installation or upgrade (client supports mq majors ${supported}): ${details.cause.message}`
        : details.schemaVersion === undefined
          ? `postgremq: the database's PostgreMQ installation reports no schema version; it needs an installation or upgrade (client supports mq majors ${supported})`
          : `postgremq: the database's PostgreMQ schema is mq ${formatSchemaVersion(details.schemaVersion)} (schema version ${details.schemaVersion}); this client supports mq majors ${supported}`
    );
    this.name = 'CompatibilityError';
    this.schemaVersion = details.schemaVersion;
    this.supportedMajors = details.supportedMajors;
    this.cause = details.cause;
  }
}

/**
 * DirtySchemaError is thrown by `migrate()` when the recorded schema version
 * is marked dirty: a migration failed partway, so the schema is in an unknown
 * state. Fix the schema by hand, then clear the flag in
 * `postgremq.postgremq_migrations`. Mirrors golang-migrate's ErrDirty, which
 * the Go client returns.
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
