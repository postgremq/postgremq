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
  stop(): Promise<void>;
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
  vt: Date;
  /** Fires when the lease is lost or the consumer is shutting down. */
  readonly signal: AbortSignal;
  /** True once ack/nack/release has been attempted (whether or not it succeeded). */
  readonly isSettled: boolean;
  ack(): Promise<void>;
  ackWithTransaction(tx: Transaction): Promise<void>;
  nack(options?: MessageOptions): Promise<void>;
  release(): Promise<void>;
  setVt(newVt: number): Promise<Date>;
}

/**
 * Forward declaration of HandlerConsumer interface.
 * Full implementation is in handler-consumer.ts
 */
export interface HandlerConsumer {
  stop(): Promise<void>;
}

/**
 * A message handler dispatched to by a HandlerConsumer. See
 * handler-consumer.ts for the auto-ack / auto-nack settlement rules.
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
  /** PostgreSQL connection string (overrides config if provided) */
  connectionString?: string;
  /** Connection pool configuration (creates new pool) */
  config?: PoolConfig;
  /** Existing pool instance (reuses existing pool) */
  pool?: Pool;
  /** Maximum time (ms) to wait for in-flight messages during shutdown */
  shutdownTimeoutMs?: number;
  /** Retry policy for transient errors */
  retry?: RetryPolicy;
  /**
   * Called when a queue becomes fatal for this connection — it is gone and any
   * consumers on it have been torn down. Fires when the queue is deleted
   * out-of-band (a consume returns PMQ02) or when an exclusive queue's
   * keep-alive permanently fails. The error is a QueueFatalError. This is the
   * queue-level signal; consumers also learn via Consumer.onClose, and it is the
   * only signal for a producer-only exclusive queue with no consumer. The same
   * event is emitted on the connection's emitter as `'queueFatal'`. Optional —
   * omit it to fall back to logging only.
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
  /** Connect to the database */
  connect(): Promise<void>;
  
  /** Close the connection and clean up resources */
  close(): Promise<void>;
  
  /**
   * Publish a message to a topic
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

  /** Create a consumer for a queue */
  consume(queue: string, options?: Partial<ConsumerOptions>): Consumer;

  /**
   * Create a handler-based consumer for a queue. Each message is dispatched
   * to `handler` with optional concurrency limiting (options.maxInFlight).
   * The push-based counterpart to consume()'s async iteration.
   */
  consumeHandler(
    queue: string,
    handler: MessageHandler,
    options?: Partial<HandlerConsumerOptions>
  ): HandlerConsumer;
  
  /** Create a new topic */
  createTopic(topic: string): Promise<void>;
  
  /** Create a new queue */
  createQueue(
    name: string, 
    topic: string, 
    exclusive?: boolean, 
    options?: QueueOptions
  ): Promise<void>;
  
  /** Delete a topic */
  deleteTopic(topic: string): Promise<void>;
  
  /** Delete a queue */
  deleteQueue(queue: string): Promise<void>;
  
  /** List all topics */
  listTopics(): Promise<string[]>;
  
  /** List all queues */
  listQueues(): Promise<QueueInfo[]>;
  
  /** Get statistics for a queue or all queues */
  getQueueStatistics(queue?: string): Promise<QueueStatistics>;

  /** Bundled fast maintenance: retire crashed-final-attempt rows to DLQ + reap expired exclusive queues. */
  maintenanceFast(): Promise<{ retiredToDlq: number; inactiveQueuesDropped: number }>;

  /** List all messages in the Dead Letter Queue */
  listDLQMessages(): Promise<DLQMessage[]>;
  
  /** Requeue messages from the Dead Letter Queue to a specific queue */
  requeueDLQMessages(queue: string): Promise<void>;
  
  /** Purge the Dead Letter Queue */
  purgeDLQ(): Promise<void>;
  
  /** Purge all messages from all queues */
  purgeAllMessages(): Promise<void>;
  
  /** Delete a specific message from a queue */
  deleteQueueMessage(queue: string, messageID: number): Promise<void>;
  
  /** List all messages in a queue */
  listMessages(queue: string): Promise<QueueMessage[]>;
  
  /** Get a specific message by ID */
  getMessage(messageID: number): Promise<PublishedMessage | null>;
  
  /** Clean up a queue (remove completed messages) */
  cleanUpQueue(queue: string): Promise<void>;
  
  /** Clean up a topic (remove messages with no subscribers) */
  cleanUpTopic(topic: string): Promise<void>;
  
  /** Delete inactive queues */
  deleteInactiveQueues(): Promise<void>;

  /** Remove completed messages older than the specified retention window */
  cleanupCompletedMessages(olderThanHours?: number): Promise<number>;
  
  /** Check if the client is shutting down */
  isClientShuttingDown(): boolean;
}

/**
 * Retry policy for database operations
 */
export interface RetryPolicy {
  /** Maximum number of retry attempts */
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
  /** Time at which the message should be delivered (defaults to immediate) */
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
   * Keep-alive interval in seconds for exclusive queues. consume_message
   * refreshes keep_alive_until on every call, and the client also runs a
   * background timer that extends it every (interval / 2). Default 300s
   * (5 minutes) — matches the SQL function default and the Go client.
   */
  keepAliveInterval?: number;
}

/**
 * Options for consuming messages
 */
export interface ConsumerOptions {
  /** Maximum number of messages to fetch in a single batch */
  batchSize?: number;
  /** Default visibility timeout for messages in seconds */
  visibilityTimeoutSec?: number;
  /** Auto-extension settings for visibility timeout */
  autoExtension?: {
    /** Whether to automatically extend message visibility */
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
     * @deprecated Ignored. The extension batch cap is now a connection option
     * (`ConnectionOptions.extenderBatchSize`) because auto-extension is
     * connection-level — one actor batches across all consumers, so a
     * per-consumer cap is meaningless.
     */
    maxBatchSize?: number;
  };
  /** Polling interval in milliseconds (when no messages and no notifications) */
  pollingIntervalMs?: number;
  /**
   * Topic name for the consumed queue. When set, bypasses the internal
   * queue->topic cache and any database lookup. Use when the caller already
   * knows the topic (e.g. consuming a queue created out-of-band).
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
 * Options for negative acknowledgment (nack) or release of a message
 */
export interface MessageOptions {
  /** Delay before the message becomes visible again (in seconds) */
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
  /** Number of completed messages */
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
 * Event types for the notification system
 */
export enum EventType {
  MESSAGE_PUBLISHED = 'message_published',
  MESSAGE_NACKED = 'message_nacked',
  MESSAGE_RELEASED = 'message_released'
}

/**
 * Event notification from PostgreSQL
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
