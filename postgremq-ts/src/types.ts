/**
 * PostgreMQ TypeScript Client
 * Types and interfaces
 */

import type { MeterProvider } from '@opentelemetry/api';
import { Pool, PoolConfig } from 'pg';
// Type-only import (erased at compile) so MessageHandler refers to the same
// concrete Message class users import from the package — avoids the
// contravariance mismatch a handler typed with the concrete class would hit
// against the lighter forward `Message` interface below. No runtime cycle.
import type { Message as MessageClass } from './message';

/** 
 * Forward declaration of Consumer interface
 * Full implementation is in consumer.ts
 */
export interface Consumer {
  /** Stop the consumer: release buffered messages, fire in-flight messages'
   *  `signal`, and wait up to shutdownTimeoutMs for them to settle. */
  stop(): Promise<void>;
  /** Start the consumer (on first call) and iterate its messages; leaving a
   *  `for await` loop early stops it. Throws if the queue's topic is unknown. */
  messages(): AsyncIterableIterator<Message>;
}

/** 
 * Forward declaration of Message interface
 * Full implementation is in message.ts
 */
export interface Message {
  readonly id: number;
  readonly queueName: string;
  readonly payload: any;
  readonly consumerToken: string;
  readonly deliveryAttempts: number;
  readonly publishedAt: Date;
  /** Message group (see PublishOptions.groupKey); null if ungrouped. */
  readonly groupKey: string | null;
  /** Dense 1-based position in the group, in publish commit order; null if ungrouped. */
  readonly groupSeq: number | null;
  /** Current lease end; updated by auto-extension and setVt. */
  vt: Date;
  /** Fires when the consumer stops (stop, close, queue-fatal) or a lost lease
   *  is detected (auto-extension, or a setVt rejected as lease lost). */
  readonly signal: AbortSignal;
  /** True once ack/nack/release has been attempted (whether or not it succeeded). */
  readonly isSettled: boolean;
  /** Mark this delivery completed so it is never redelivered. */
  ack(): Promise<void>;
  /** Ack inside the caller's open transaction; takes effect only if it commits. */
  ackWithTransaction(tx: Transaction): Promise<void>;
  /** Return the message for redelivery (optionally delayed), or move it to
   *  the DLQ on the queue's last allowed attempt. */
  nack(options?: MessageOptions): Promise<void>;
  /** Return the message, visible immediately, without counting this attempt. */
  release(): Promise<void>;
  /** Set the lease to end newVt seconds from now; resolves to the new lease end. */
  setVt(newVt: number): Promise<Date>;
}

/**
 * Forward declaration of HandlerConsumer interface.
 * Full implementation is in handler-consumer.ts
 */
export interface HandlerConsumer {
  /** Stop dispatching, fire in-flight messages' `signal`, and wait (up to
   *  shutdownTimeoutMs) for running handlers to finish. */
  stop(): Promise<void>;
}

/**
 * A message handler dispatched to by a HandlerConsumer
 * (Connection.consumeHandler).
 *
 * Settlement, applied when the handler finishes:
 * - it returns normally while `msg.signal` is not aborted: the message is
 *   auto-acked;
 * - it throws (or rejects), or returns after `msg.signal` fired (shutdown or
 *   lease loss): the message is auto-nacked, without delay (on the queue's
 *   last allowed attempt that moves it to the DLQ);
 * - it already started settlement itself (ack, ackWithTransaction, nack or
 *   release attempted, so `msg.isSettled` is true): no automatic action.
 *
 * A failed auto-ack or auto-nack is logged, not rethrown.
 */
export type MessageHandler = (msg: MessageClass) => void | Promise<void>;

/**
 * Connection options for PostgreMQ client
 */
export interface ConnectionOptions {
  /** Opt-in metrics. The application owns SDK/exporters and provider shutdown.
   * Omitted means no-op, even when a global provider exists. To use the global
   * provider, explicitly pass metrics.getMeterProvider() from the OTel API. */
  meterProvider?: MeterProvider;
  /** PostgreSQL connection string; sets config.connectionString (other
   *  config fields still apply). Ignored when pool is given. */
  connectionString?: string;
  /** Connection pool configuration (creates new pool; ignored when pool is given) */
  config?: PoolConfig;
  /** Existing pool instance (reuses existing pool; close() leaves it open) */
  pool?: Pool;
  /** Upper bound (ms) on close(): draining consumers and their in-flight
   *  messages, stopping the listener and ending an owned pool; also bounds
   *  each consumer's stop(). Default 30000. Must be positive and at most
   *  2147483647. */
  shutdownTimeoutMs?: number;
  /**
   * Retry policy for transient errors (connection loss, 08xxx, 40001, 40P01,
   * 55P03, 57P01-03) on single-statement calls: ack/nack/release, setVt and
   * the admin methods. publish retries only 40001/40P01. consume and the
   * *WithTransaction calls are never retried. The background keep-alive and
   * extension bypass this policy and reschedule failed attempts themselves
   * until the last confirmed deadline. Default: 5 attempts, 100 ms initial backoff, x2, capped at
   * 5000 ms.
   */
  retry?: RetryPolicy;
  /**
   * Called when a queue becomes fatal for this connection — it is gone and
   * teardown of its consumers has started (each consumer's onClose fires when
   * its drain finishes). Fires when a consume finds the queue gone (PMQ02:
   * deleted, recreated, or an expired exclusive queue) or when an exclusive
   * queue's keep-alive permanently fails. The error is a QueueFatalError. This
   * is the queue-level signal; consumers also learn via Consumer.onClose, and
   * it is the only signal for a producer-only exclusive queue with no
   * consumer. The same event is emitted on the connection's emitter as
   * `'queueFatal'`. Optional — if omitted, the queue loss is logged with
   * console.error (the `'queueFatal'` event is still emitted).
   */
  onQueueFatal?: (queue: string, error: unknown) => void;
  /**
   * Maximum number of in-flight messages the connection-level auto-extension
   * actor extends in a single set_vt_batch_multi call per tick. Auto-extension
   * is connection-level (one actor coalesces every consumer's due extensions),
   * so this is a connection option, not a per-consumer one. Bounds statement
   * size only — correctness does not depend on it. Default 100. Must be > 0.
   */
  extenderBatchSize?: number;
}

/**
 * Public interface for the PostgreMQ Connection
 * Only methods intended for direct use by library users are exposed here
 */
export interface IConnection {
  /**
   * Connect to the database and check the installation's schema version.
   * Throws ConnectionClosedError after close() (close() is terminal) and
   * CompatibilityError for an unsupported or missing schema version.
   */
  connect(): Promise<void>;
  
  /**
   * Close the connection and clean up resources. Idempotent and terminal: it
   * stops every consumer and waits up to shutdownTimeoutMs for in-flight
   * messages to settle. Awaiting it inside a message handler therefore waits
   * the full shutdownTimeoutMs; start it without awaiting there.
   */
  close(): Promise<void>;
  
  /**
   * Publish a message to a topic. Retries only 40001/40P01. Throws
   * QueueNotFoundError if the topic does not exist.
   * @returns Message ID
   */
  publish(topic: string, payload: any, options?: PublishOptions): Promise<number>;

  /**
   * Publish a message within an existing transaction, atomically with the
   * caller's other writes. Does not apply the internal retry policy — the
   * caller owns the transaction boundaries.
   * @returns Message ID
   */
  publishWithTransaction(
    tx: Transaction,
    topic: string,
    payload: any,
    options?: PublishOptions
  ): Promise<number>;

  /**
   * Create a consumer for a queue. It does nothing until messages() is
   * called, which throws if the queue's topic is unknown (no `options.topic`
   * is given and this connection has not learned it from createQueue() or
   * an earlier consumer's `topic`).
   */
  consume(queue: string, options?: Partial<ConsumerOptions>): Consumer;

  /**
   * Create a handler-based consumer for a queue. Each message is dispatched
   * to `handler` with optional concurrency limiting (options.maxInFlight).
   * The push-based counterpart to consume()'s async iteration. Settlement
   * rules: see {@link MessageHandler}. Throws synchronously if the queue's
   * topic is unknown.
   */
  consumeHandler(
    queue: string,
    handler: MessageHandler,
    options?: Partial<HandlerConsumerOptions>
  ): HandlerConsumer;
  
  /** Create a new topic (idempotent). Throws ValidationError for an invalid name. */
  createTopic(topic: string): Promise<void>;
  
  /**
   * Create a new queue. Idempotent when the parameters match; throws
   * ValidationError if the queue exists with different parameters, and
   * QueueNotFoundError if the topic does not exist or the queue is an expired
   * exclusive queue. An exclusive queue is kept alive by this connection.
   */
  createQueue(
    name: string, 
    topic: string, 
    exclusive?: boolean, 
    options?: QueueOptions
  ): Promise<void>;
  
  /**
   * Delete a topic and, by cascade, its queues. Throws ValidationError while
   * any message of the topic exists; run cleanUpTopic() first.
   */
  deleteTopic(topic: string): Promise<void>;
  
  /**
   * Delete a queue and all its deliveries. Rejects with a database error
   * with code 'PMQ03' while the queue has DLQ entries.
   */
  deleteQueue(queue: string): Promise<void>;
  
  /** List all topics */
  listTopics(): Promise<string[]>;
  
  /** List all queues */
  listQueues(): Promise<QueueInfo[]>;
  
  /** Get statistics for a queue, or totals across all queues */
  getQueueStatistics(queue?: string): Promise<QueueStatistics>;

  /**
   * Bundled fast maintenance: retire crashed-final-attempt rows to DLQ + reap
   * expired exclusive queues. Run it every 1–10 seconds, and at least once
   * every 60 seconds.
   */
  maintenanceFast(): Promise<{ retiredToDlq: number; inactiveQueuesDropped: number }>;

  /** List all messages in the Dead Letter Queue */
  listDLQMessages(): Promise<DLQMessage[]>;
  
  /** Move this queue's DLQ entries back into it as pending deliveries (delivery attempts reset to 0) */
  requeueDLQMessages(queue: string): Promise<void>;
  
  /** Purge the Dead Letter Queue, in every queue */
  purgeDLQ(): Promise<void>;
  
  /** Delete every message in the system: all DLQ entries, deliveries and payloads; pruned groups restart at group_seq 1 */
  purgeAllMessages(): Promise<void>;
  
  /** Delete one message's delivery from a queue, in any state (in-flight included) */
  deleteQueueMessage(queue: string, messageID: number): Promise<void>;
  
  /** List all messages in a queue, in every status (completed included until cleanup), without payloads */
  listMessages(queue: string): Promise<QueueMessage[]>;
  
  /** Get a specific message by ID */
  getMessage(messageID: number): Promise<PublishedMessage | null>;
  
  /** Clean up a queue: delete all of the queue's deliveries, in every state */
  cleanUpQueue(queue: string): Promise<void>;
  
  /**
   * Clean up a topic: delete all of the topic's messages. Rejects with a
   * database error with code 'PMQ03' while any of them is in the DLQ.
   */
  cleanUpTopic(topic: string): Promise<void>;
  
  /** Delete expired exclusive queues (keep-alive lapsed), skipping any that still have DLQ entries */
  deleteInactiveQueues(): Promise<void>;

  /**
   * Delete up to 1000 completed deliveries processed more than olderThanHours
   * (whole hours, default 24) ago, then up to 1000 unreferenced payloads older
   * than that; returns the number of deliveries deleted (payloads are not
   * counted, so 0 does not mean the payload backlog is empty). Call it
   * periodically to drain a backlog.
   */
  cleanupCompletedMessages(olderThanHours?: number): Promise<number>;
  
  /** True once close() has been called (including after it has finished) */
  isClientShuttingDown(): boolean;
}

/**
 * Retry policy for database operations
 */
export interface RetryPolicy {
  /** Maximum number of attempts in total, the first one included (1 = no retries). Default 5. */
  maxAttempts: number;
  /** Initial backoff delay in milliseconds */
  initialBackoffMs: number;
  /** Backoff multiplier for each subsequent retry */
  backoffMultiplier: number;
  /** Maximum backoff delay in milliseconds */
  maxBackoffMs: number;
}

/**
 * Options for publishing a message
 */
export interface PublishOptions {
  /** Time at which the message should be delivered (defaults to immediate),
   *  compared with the database's clock */
  deliverAfter?: Date;
  /**
   * Message group (1..255 characters; empty is rejected with ValidationError).
   * Omit for an ungrouped message.
   *
   * Within one queue, deliveries of one group are claimed strictly in publish
   * commit order, one at a time: a message is not delivered while an earlier
   * message of its group is pending or being processed (head-of-line
   * blocking, as in SQS FIFO). A head nacked with a delay blocks its group
   * until the delay passes; a head that exhausts its delivery attempts moves
   * to the dead letter queue, which unblocks the group. A consume batch holds
   * at most one message per group.
   *
   * Publishers of one group are serialised by the database until their
   * transaction commits, so a group should be a session, an order or an
   * account — never a hot key shared by unrelated work. A transaction that
   * publishes to several groups should take them in a consistent order to
   * avoid deadlocks.
   */
  groupKey?: string;
}

/**
 * Options for creating a queue
 */
export interface QueueOptions {
  /** Maximum number of delivery attempts before moving to DLQ (0 for unlimited) */
  maxDeliveryAttempts?: number;
  /**
   * Keep-alive interval in seconds for exclusive queues. Consuming does not
   * refresh keep_alive_until; the client runs a background timer that
   * extends it about every half interval (after each confirmed extension it
   * schedules the next one halfway to the confirmed deadline). Default 300s
   * (5 minutes) — matches the SQL function default and the Go client. Only
   * exclusive queues created by this connection's createQueue() are kept
   * alive. Must be >= 0.001; re-creating any queue with a different value
   * fails (ValidationError).
   */
  keepAliveInterval?: number;
}

/**
 * Options for consuming messages
 */
export interface ConsumerOptions {
  /** Maximum number of messages to fetch in a single batch, and to hold
   *  buffered. Default 10. */
  batchSize?: number;
  /** Visibility timeout (lease) for fetched messages, in whole seconds; also
   *  the amount auto-extension re-extends by. Default 30 (0 means the
   *  default). */
  visibilityTimeoutSec?: number;
  /** Auto-extension settings for visibility timeout */
  autoExtension?: {
    /** Whether to automatically extend message visibility. Default true. */
    enabled: boolean;
    /**
     * Fraction of the remaining visibility timeout that must elapse before
     * auto-extension fires, matching the Go client's `WithExtensionThreshold`.
     * Must be in (0, 1). Default 0.5 (extend at the halfway point). Lower
     * values extend earlier (more headroom, more DB calls); higher values
     * extend later (e.g. 0.9 = extend at 90% of the lease).
     */
    extensionThreshold?: number;
    /**
     * @deprecated Ignored. Auto-extension always re-extends by
     * `visibilityTimeoutSec` (matching the Go client) so the granted lease
     * and the next-extension schedule stay consistent.
     */
    extensionSec?: number;
    /**
     * @deprecated Ignored. The extension batch cap is a connection option
     * (`ConnectionOptions.extenderBatchSize`) because auto-extension is
     * connection-level — one actor batches across all consumers, so a
     * per-consumer cap is meaningless.
     */
    maxBatchSize?: number;
  };
  /** Polling interval in milliseconds (when no messages and no notifications). Default 1000. */
  pollingIntervalMs?: number;
  /**
   * Topic of the consumed queue, used to LISTEN on its publish channel.
   * Required unless this connection already knows the queue's topic, from
   * createQueue() or an earlier consumer's `topic`: the client never looks
   * the topic up in the database, and messages()/consumeHandler() throw if
   * it is unknown. The value is cached for the queue.
   */
  topic?: string;
}

/**
 * Options for a handler-based consumer (Connection.consumeHandler).
 * Extends ConsumerOptions with handler-specific settings.
 */
export interface HandlerConsumerOptions extends ConsumerOptions {
  /**
   * Maximum number of handler invocations running concurrently. 0 (the
   * default) means unlimited — a handler is started as soon as a message
   * is available. Mirrors the Go client's WithMaxInFlight.
   */
  maxInFlight?: number;
}

/**
 * Options for negative acknowledgment (nack)
 */
export interface MessageOptions {
  /** Delay before the message becomes visible again (in seconds; omitted or
   *  0 = immediately). Ignored when the nack moves the message to the DLQ. */
  delaySeconds?: number;
}

/**
 * Information about a queue
 */
export interface QueueInfo {
  /** Queue name */
  queueName: string;
  /** Associated topic name */
  topicName: string;
  /** Maximum delivery attempts (0 for unlimited) */
  maxDeliveryAttempts: number;
  /** Whether the queue is exclusive (temporary, kept alive by its owner) */
  exclusive: boolean;
  /** Expiration timestamp for exclusive queues */
  keepAliveUntil: Date | null;
}

/**
 * Queue statistics
 */
export interface QueueStatistics {
  /** Number of pending messages */
  pendingCount: number;
  /** Number of messages being processed */
  processingCount: number;
  /** Number of completed messages (retained until cleanupCompletedMessages() deletes them) */
  completedCount: number;
  /** Total number of messages */
  totalCount: number;
}

/**
 * Information about a message in the Dead Letter Queue
 */
export interface DLQMessage {
  /** Queue name */
  queueName: string;
  /** Message ID */
  messageId: number;
  /** Retry count (number of delivery attempts made) */
  retryCount: number;
  /** Timestamp when the message was published to DLQ */
  publishedAt: Date;
}

/**
 * Information about a message in a queue
 */
export interface QueueMessage {
  /** Message ID */
  messageId: number;
  /** Message status (pending, processing, completed) */
  status: string;
  /** Timestamp when the message was published */
  publishedAt: Date;
  /** Number of delivery attempts */
  deliveryAttempts: number;
  /** Visibility timeout expiration */
  vt: Date;
  /** Timestamp when the message was processed (if completed) */
  processedAt: Date | null;
  /** Message group; null if ungrouped */
  groupKey: string | null;
  /** Position in the group; null if ungrouped */
  groupSeq: number | null;
}

/**
 * Information about a published message
 */
export interface PublishedMessage {
  /** Message ID */
  messageId: number;
  /** Topic name */
  topicName: string;
  /** Message payload */
  payload: any;
  /** Timestamp when the message was published */
  publishedAt: Date;
  /** Message group; null if ungrouped */
  groupKey: string | null;
  /** Position in the group; null if ungrouped */
  groupSeq: number | null;
}

/**
 * Unused: PostgreMQ NOTIFYs carry an empty payload (the channel name is the
 * signal). Not exported from the package.
 */
export enum EventType {
  MESSAGE_PUBLISHED = 'message_published',
  MESSAGE_NACKED = 'message_nacked',
  MESSAGE_RELEASED = 'message_released'
}

/**
 * Unused: describes a JSON NOTIFY payload PostgreMQ does not send. Not
 * exported from the package.
 */
export interface NotificationEvent {
  /** Event type */
  event: EventType;
  /** Affected queues */
  queues: string[];
  /** Visibility timestamp */
  vt?: string | Date;
}

/**
 * Message processing status
 */
export enum MessageStatus {
  PENDING = 'pending',
  PROCESSING = 'processing',
  COMPLETED = 'completed'
}

/**
 * Transaction interface for transaction support
 */
export interface Transaction {
  /** Execute query within transaction */
  query(text: string, values?: any[]): Promise<any>;
} 
