/**
 * PostgreMQ typed errors.
 *
 * The SQL functions raise exceptions with custom SQLSTATE codes (PMQ01,
 * PMQ02, PMQ03) so application code can match on type, not error text.
 * The classes below mirror the Go client's sentinels (ErrLeaseLost,
 * ErrQueueNotFound, ErrValidation).
 *
 * Use `instanceof LeaseLostError` (or check `err.code === 'PMQ01'`) to
 * branch on the failure mode.
 */

/** SQLSTATE code for "lease lost" (PMQ01). */
export const ErrCodeLeaseLost = 'PMQ01';
/** SQLSTATE code for "queue or topic not found" (PMQ02). */
export const ErrCodeQueueNotFound = 'PMQ02';
/** SQLSTATE code for "validation error" (PMQ03). */
export const ErrCodeValidation = 'PMQ03';

/**
 * LeaseLostError is thrown by ack/nack/release/setVt/setVtBatch when the
 * consumer's lease is no longer valid: the visibility timeout expired,
 * the consumer token doesn't match (another consumer re-consumed), or
 * the message is no longer in 'processing' state. Handlers must be
 * idempotent — receiving this error means another consumer may have
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
 * — the operation will fail until the queue/topic is created.
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
 * (exclusive-queue duplicate, deleting a topic that still has messages).
 * The common thread is "the request was rejected before any state change."
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
 * ConnectionClosedError is thrown by `connect()` when the connection has
 * already been closed. close() is terminal: a connection cannot be
 * resurrected — callers must construct a new Connection. Mirrors the Go
 * client's ErrConnectionClosed sentinel.
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
 * PostgreMQ installation uses a protocol major this client does not support
 * (see `SUPPORTED_PROTOCOL_MAJORS`), or has no discovery function
 * (`postgremq.info()`) and needs an installation or upgrade. Mirrors the Go
 * client's CompatibilityError / ErrIncompatibleSchema.
 *
 * Pure client-side error (no SQLSTATE), so it carries no `code`; when
 * discovery is missing, `cause` is the database error.
 */
export class CompatibilityError extends Error {
  /** The installed schema version (the last migration applied); undefined when discovery is missing. */
  readonly schemaVersion?: number;
  /** The installation's protocol major; undefined when discovery is missing. */
  readonly protocolMajor?: number;
  /** The protocol majors this client supports. */
  readonly supportedMajors: readonly number[];
  /** The database error when discovery is missing. */
  readonly cause?: Error;
  constructor(details: {
    schemaVersion?: number;
    protocolMajor?: number;
    supportedMajors: readonly number[];
    cause?: Error;
  }) {
    const supported = `[${details.supportedMajors.join(', ')}]`;
    super(
      details.cause
        ? `postgremq: postgremq.info() is unavailable; the database needs a PostgreMQ installation or upgrade (client supports protocol majors ${supported}): ${details.cause.message}`
        : `postgremq: the database's PostgreMQ schema (version ${details.schemaVersion ?? 'unknown'}) uses protocol major ${details.protocolMajor}; this client supports ${supported}`
    );
    this.name = 'CompatibilityError';
    this.schemaVersion = details.schemaVersion;
    this.protocolMajor = details.protocolMajor;
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
