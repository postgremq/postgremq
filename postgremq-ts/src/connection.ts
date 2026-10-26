/**
 * PostgreMQ TypeScript Client
 * Connection management
 */

import { Pool, PoolClient, PoolConfig } from 'pg';
import { ClientMetrics } from './metrics';
import {
  ConnectionOptions,
  Consumer,
  ConsumerOptions,
  DLQMessage,
  EventType,
  HandlerConsumerOptions,
  IConnection,
  MessageHandler,
  MessageOptions,
  NotificationEvent,
  PublishOptions,
  PublishedMessage,
  QueueInfo,
  QueueMessage,
  QueueOptions,
  QueueStatistics,
  RetryPolicy,
  Transaction,
} from './types';
import { Consumer as ConsumerImpl } from './consumer';
import { HandlerConsumer } from './handler-consumer';
import {
  EventEmitter,
  DEFAULT_RETRY_POLICY,
  shouldRetry,
  sleep,
  withRetry,
  messageId as checkedMessageId,
  groupSeq as checkedGroupSeq,
  untilDeadline,
  createDeferred,
} from './utils';
import { mapDbError, ConnectionClosedError, QueueFatalError, ValidationError } from './errors';
import { checkSchema } from './compatibility';

/** The longest delay nack_message accepts (100 years); a later delayUntil is
 *  capped to it rather than failing the settlement. */
const MAX_NACK_DELAY_MS = 3_155_760_000_000;

/** Anything the connection can shut down during close(). Both ConsumerImpl
 *  and HandlerConsumer satisfy this. */
interface Stoppable {
  stop(): Promise<void>;
}

/** A consumer the connection can tear down when its queue becomes fatal (gone).
 *  Both ConsumerImpl and HandlerConsumer implement it. */
interface FatalConsumer extends Stoppable {
  getQueueName(): string;
  getQueueGeneration(): string | undefined;
  fatal(err: QueueFatalError): void;
}

function isFatalConsumer(s: Stoppable): s is FatalConsumer {
  return (
    typeof (s as Partial<FatalConsumer>).getQueueName === 'function' &&
    typeof (s as Partial<FatalConsumer>).fatal === 'function'
  );
}

/** Subscriber callback fires whenever a NOTIFY arrives on the subscribed
 *  channel. NOTIFYs from PostgreMQ carry no payload — the channel name is
 *  the wake-up signal, and the consumer re-fetches via consume_message. */
type SubscriberCallback = () => void;

interface ChannelState {
  refCount: number;
  /** True when LISTEN has been issued on the active notify client. */
  listening: boolean;
  subscribers: Set<SubscriberCallback>;
}

function quoteIdent(s: string): string {
  return '"' + s.replace(/"/g, '""') + '"';
}

/** One exclusive queue tracked by the connection-level keep-alive actor. */
interface KeepAliveEntry {
  generation: string;
  intervalSec: number;
  nextAt: Date; // when to extend next: halfway to the confirmed deadline, or ~100ms while retrying
  expiresAt: Date;
}

/** One in-flight message tracked by the connection-level extender actor. */
interface ExtEntry {
  queue: string;
  id: number;
  token: string;
  vtSec: number;
  threshold: number;
  nextExtensionTime: Date;
  expiresAt: Date;
  cancel: () => void; // advisory abort on lease loss (message._cancel)
  onExtended: (vt: Date) => void; // keep message.vt current after an extension
}

/** Delivery-identity key (queue, message_id, consumer_token). message_id alone
 *  collides across queues because distribute_message fans one message into
 *  every queue on the topic, and each claim mints a new token. */
function extKey(queue: string, id: number, token: string): string {
  return queue + '\0' + id + '\0' + token;
}

/** Bounds a heartbeat call by the earliest confirmed deadline in its batch,
 *  between one and five seconds: an entry about to expire must not fail the call
 *  for the rest of the batch. The SQL leaves out a row whose deadline has passed, so
 *  it is reported lost from the result. */
export function heartbeatBudgetMs(deadlines: Date[]): number {
  const now = Date.now();
  let budget = 5000;
  for (const deadline of deadlines) {
    budget = Math.min(budget, deadline.getTime() - now);
  }
  return Math.max(1000, budget);
}

/**
 * Connection class for PostgreMQ client: the object `connect()` returns.
 * Manages the PostgreSQL pool and provides publish, consume and admin
 * operations. It owns the connection-level keep-alive and vt-extension
 * actors and the LISTEN client its consumers share. close() is terminal.
 */
export class Connection implements IConnection {
  /** @internal Shared instrumentation for the connection and its handlers. */
  readonly telemetry: ClientMetrics;
  /** PostgreSQL connection pool */
  private pool: Pool;

  /** Whether this connection owns the pool (should close it) */
  private ownsPool: boolean;

  /** Client for notifications */
  private notifyClient: PoolClient | null = null;

  /** Flag indicating if client is shutting down */
  private isShuttingDown: boolean = false;
  private connectPromise: Promise<void> | null = null;
  private closePromise: Promise<void> | null = null;
  private closeDeadline = Infinity;
  private activeIO = new Set<() => void>();

  /** Set of active consumers. Holds both iterator-based consumers
   *  (ConsumerImpl) and handler-based ones (HandlerConsumer); both expose
   *  stop() so close() can shut them all down uniformly. */
  private consumers: Set<Stoppable> = new Set();

  // ---- Connection-level keep-alive actor ----
  // One timer per connection batches every exclusive queue's keep-alive into a
  // single extend_queue_keep_alive_multi call per tick. Mirrors the Go
  // client's keep-alive actor.
  /** Per-queue keep-alive schedule, keyed by queue name. */
  private keepAliveEntries: Map<string, KeepAliveEntry> = new Map();
  /** Single timer that fires at the earliest pending keep-alive. */
  private keepAliveTimer: NodeJS.Timeout | null = null;
  /** True while a keep-alive flush is awaiting the DB (prevents overlap). */
  private keepAliveFlushing = false;

  // ---- Connection-level visibility-timeout extender actor ----
  // One timer per connection coalesces up to `extenderBatchSize` of every
  // consumer's due extensions into one set_vt_batch_multi call per tick.
  // Keyed by (queue, id, token): the same message_id in two queues, or
  // re-claimed with a new token, is a separate entry.
  /** In-flight messages awaiting extension, keyed by `extKey(queue, id, token)`. */
  private extenderEntries: Map<string, ExtEntry> = new Map();
  /** Single timer that fires at the earliest pending extension. */
  private extenderTimer: NodeJS.Timeout | null = null;
  /** True while an extension flush is awaiting the DB (prevents overlap). */
  private extenderFlushing = false;
  /** Unused: only ever reset to null. A transient extension failure defers
   *  each entry instead (see flushExtender). */
  private extenderTryAfter: Date | null = null;
  /** Per-tick cap on messages extended in one call (ConnectionOptions.extenderBatchSize). */
  private readonly extenderBatchCap: number;

  /** Set once close() has drained consumers and stopped the background actors
   *  (keep-alive + extender). Gates the actors instead of isShuttingDown so
   *  they keep running THROUGH the consumer drain and stop only after,
   *  mirroring the Go client, which stops its keep-alive and extender actors
   *  after the consumer drain. */
  private backgroundStopped = false;

  /** Emitter for connection-level events the application can observe.
   *  Emits 'queueFatal' (queue, QueueFatalError) when a queue becomes gone and
   *  teardown of its consumers has begun. */
  private readonly events: EventEmitter = new EventEmitter();

  /** Optional callback invoked alongside the 'queueFatal' event. */
  private readonly onQueueFatal?: (queue: string, error: unknown) => void;

  /** Queues already declared fatal (dedupe queueFatal). Cleared on createQueue
   *  so a recreated queue can go fatal again. */
  private readonly fatalQueues: Set<string> = new Set();

  /** Retry policy for operations */
  private retryPolicy: RetryPolicy;

  /** Shutdown timeout in milliseconds */
  private shutdownTimeoutMs: number;

  /** Flag indicating if client is connected */
  private connected: boolean = false;

  /** Abort controller of the running notification loop; null when none runs. */
  private notifyController: AbortController | null = null;
  private notifyWake = createDeferred<void>();

  /**
   * Refcounted state for each NOTIFY channel we want to receive events on.
   * The first subscriber to a channel triggers a LISTEN; the last unsubscribe
   * triggers UNLISTEN while other channels remain (when the last channel goes,
   * the notification loop is stopped instead). Multiple consumers on the same
   * topic share one physical LISTEN.
   */
  private channelStates: Map<string, ChannelState> = new Map();

  /**
   * Cache of queue name -> topic name. Populated by createQueue, and by
   * resolveTopic when a consumer starts (messages() or consumeHandler) with
   * an explicit `topic` option, so a consumer knows which per-topic channel
   * to subscribe to without querying the database.
   */
  private topicCache: Map<string, string> = new Map();
  private queueGenerations = new Map<string, string>();

  /**
   * Promise of the running notification loop; non-null while it runs, so all
   * subscribers share one notify client.
   */
  private notifyStartupPromise: Promise<void> | null = null;

  private static readonly NOTIFY_RECONNECT_BASE_MS = 500;
  private static readonly NOTIFY_RECONNECT_MAX_MS = 30000;
  /** A session that had been listening this long starts the backoff again. */
  private static readonly NOTIFY_RECONNECT_STABLE_MS = 5000;

  /**
   * Create a new connection manager
   * @param options - Connection options
   * @throws Error if shutdownTimeoutMs or a retry policy value is not a
   *   positive finite number <= 2147483647, retry.maxAttempts is not an
   *   integer, or extenderBatchSize is not a positive integer
   */
  constructor(options: ConnectionOptions = {}) {
    this.telemetry = new ClientMetrics(options.meterProvider);
    // Set up the connection pool
    if (options.pool) {
      // Use existing pool
      this.pool = options.pool;
      this.ownsPool = false;
    } else {
      // Create new pool from connection string or config
      const poolConfig: PoolConfig = { ...options.config };

      if (options.connectionString) {
        poolConfig.connectionString = options.connectionString;
      }

      this.pool = new Pool(poolConfig);
      this.ownsPool = true;
    }

    // Set up retry policy
    this.retryPolicy = options.retry || DEFAULT_RETRY_POLICY;

    // Set shutdown timeout
    this.shutdownTimeoutMs = options.shutdownTimeoutMs ?? 30000;
    for (const [name, value] of Object.entries({
      shutdownTimeoutMs: this.shutdownTimeoutMs,
      ...this.retryPolicy,
    })) {
      if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0 || value > 2147483647)
        throw new Error(`${name} must be positive and finite`);
    }
    if (!Number.isSafeInteger(this.retryPolicy.maxAttempts))
      throw new Error('maxAttempts must be an integer');

    // Optional queue-fatal callback (also surfaced via the emitter).
    this.onQueueFatal = options.onQueueFatal;

    // Connection-level extension batch cap (statement-size guard).
    const extenderBatchSize = options.extenderBatchSize ?? 100;
    if (!Number.isSafeInteger(extenderBatchSize) || extenderBatchSize <= 0) {
      throw new Error('extenderBatchSize must be positive');
    }
    this.extenderBatchCap = extenderBatchSize;

    // Install a single pool-level error handler to avoid unhandled errors from idle clients
    try {
      const anyPool: any = this.pool as any;
      if (typeof anyPool.on === 'function' && !anyPool.__postgremqErrorHookInstalled) {
        const errorHandler = (err: any) => {
          const msg = err?.message ?? String(err);
          const expected =
            msg.includes('terminating connection') ||
            msg.includes('Connection terminated unexpectedly');
          if (expected) {
            console.debug('Pool client error during shutdown (ignored):', msg);
          } else {
            console.error('Pool client error:', err);
          }
        };
        anyPool.on('error', errorHandler);
        // Also attach to acquired clients so in-flight clients don't cause unhandled errors
        if (typeof anyPool.on === 'function') {
          anyPool.on('acquire', (client: any) => {
            try {
              if (!client.__postgremqClientErrorHookInstalled) {
                client.on('error', errorHandler);
                client.__postgremqClientErrorHookInstalled = true;
              }
            } catch {}
          });
        }
        anyPool.__postgremqErrorHookInstalled = true;
      }
    } catch {
      // best-effort; do not fail constructor
    }
  }

  /**
   * Connect to the database and check the installation's schema version.
   * The notification listener starts later, when the first consumer starts
   * consuming.
   *
   * close() is terminal — once a connection has been closed, calling
   * connect() again throws ConnectionClosedError rather than silently
   * succeeding into a half-broken state (the pool may have been .end()'d,
   * notification machinery torn down, etc.). Use the package-level
   * `connect()` to get a new connection.
   *
   * It reads `postgremq.info()` and rejects a database whose schema version
   * is on an mq line not in `SUPPORTED_MQ_VERSIONS`, that reports no schema
   * version, or that has no discovery function (it needs a PostgreMQ
   * installation or upgrade).
   *
   * @returns Promise that resolves when connected
   * @throws ConnectionClosedError if close() has already been called.
   * @throws CompatibilityError if the installation is not compatible.
   */
  connect(): Promise<void> {
    if (this.isShuttingDown) return Promise.reject(new ConnectionClosedError());
    if (this.connected) return Promise.resolve();
    if (!this.connectPromise)
      this.connectPromise = this.runDatabase(async (client) => {
        await checkSchema(client);
        if (this.isShuttingDown) throw new ConnectionClosedError();
        this.connected = true;
      }).finally(() => {
        this.connectPromise = null;
      });
    return this.connectPromise;
  }

  /**
   * Close the connection and clean up resources. Idempotent and terminal.
   *
   * Every consumer stops: fetching ends, buffered (undelivered) messages are
   * released, and each in-flight message's `signal` fires. Close then waits
   * up to `shutdownTimeoutMs` for in-flight messages to settle, with
   * keep-alive and auto-extension still running, and then stops those
   * actors. The notification listener stops once the last consumer
   * unsubscribes, which happens as consumers start draining. It ends the pool only if this
   * connection created it; a `pool` passed in the options stays open.
   *
   * Close waits for delivered messages, so awaiting it inside a message
   * handler, or in a `for await` loop while holding an unsettled message,
   * waits on itself until `shutdownTimeoutMs` passes. Start it without
   * awaiting there (`void client.close()`), or call it from outside the
   * handler.
   * @returns Promise that resolves when disconnected
   */
  close(): Promise<void> {
    if (!this.closePromise) {
      this.isShuttingDown = true;
      this.closeDeadline = Date.now() + this.shutdownTimeoutMs;
      this.closePromise = this.drain();
    }
    return this.closePromise;
  }

  private async drain(): Promise<void> {
    const timer = setTimeout(() => {
      for (const abort of this.activeIO) abort();
    }, this.getShutdownTimeoutMs());
    try {
      await untilDeadline(
        Promise.allSettled(Array.from(this.consumers, (c) => c.stop())),
        this.closeDeadline
      );
      this.backgroundStopped = true;
      if (this.keepAliveTimer) clearTimeout(this.keepAliveTimer);
      if (this.extenderTimer) clearTimeout(this.extenderTimer);
      this.keepAliveTimer = this.extenderTimer = null;
      this.keepAliveEntries.clear();
      this.extenderEntries.clear();
      await untilDeadline(this.stopNotificationListener(), this.closeDeadline);
      this.connected = false;
      this.consumers.clear();
      this.channelStates.clear();
      this.topicCache.clear();
      for (const abort of this.activeIO) abort();
      if (this.ownsPool) await untilDeadline(this.pool.end(), this.closeDeadline);
    } finally {
      clearTimeout(timer);
    }
  }

  /** Remaining drain budget; consumers use the same deadline during Close. @internal */
  getShutdownTimeoutMs(): number {
    return Math.max(0, Math.min(this.shutdownTimeoutMs, this.closeDeadline - Date.now()));
  }

  /**
   * Start the notification loop in the background unless one is running, the
   * connection is closing, or no channel has subscribers. The loop restarts
   * itself if a subscriber arrives while the previous loop is tearing down.
   * @internal
   */
  private startNotificationListener(): void {
    if (this.notifyStartupPromise || this.isShuttingDown || !this.channelStates.size) return;
    const controller = new AbortController();
    this.notifyController = controller;
    this.notifyStartupPromise = this.notificationLoop(controller.signal).finally(() => {
      this.notifyStartupPromise = null;
      this.notifyController = null;
      // A subscriber may have arrived during teardown of the previous session.
      this.startNotificationListener();
    });
  }

  private async notificationLoop(signal: AbortSignal): Promise<void> {
    let backoff = Connection.NOTIFY_RECONNECT_BASE_MS;
    while (!signal.aborted && !this.isShuttingDown && this.channelStates.size) {
      // When this session's LISTENs were all in place (performance.now();
      // undefined: never).
      let listeningSince: number | undefined;
      try {
        await this.runDatabase(
          async (client) => {
            this.notifyClient = client;
            const actual = new Set<string>();
            let failed: Error | undefined;
            const onError = (error: Error) => {
              failed = error;
              this.notifyWake[1]();
            };
            const wake = (callbacks: Iterable<SubscriberCallback>) => {
              for (const callback of callbacks) {
                try {
                  callback();
                } catch (error) {
                  console.error('Notification callback failed:', error);
                }
              }
            };
            const onNotification = (msg: { channel: string }) => {
              wake(this.channelStates.get(msg.channel)?.subscribers ?? []);
            };
            client.on('error', onError);
            client.on('notification', onNotification);
            try {
              while (!signal.aborted && this.channelStates.size) {
                const changed = (this.notifyWake = createDeferred<void>());
                if (failed) throw failed;
                for (const channel of actual) {
                  if (!this.channelStates.has(channel)) {
                    await client.query('UNLISTEN ' + quoteIdent(channel));
                    actual.delete(channel);
                  }
                }
                const added: string[] = [];
                for (const [channel, state] of this.channelStates) {
                  if (!actual.has(channel)) {
                    await client.query('LISTEN ' + quoteIdent(channel));
                    actual.add(channel);
                    added.push(channel);
                  }
                  state.listening = true;
                }
                if (failed) throw failed;
                if (listeningSince === undefined) listeningSince = performance.now();
                // Wake the subscribers of the channels newly listened to, once
                // per subscriber: a message published before a channel's
                // LISTEN is never notified (on a new session that is every
                // channel, since notifications sent while no session was
                // listening are lost). The wake comes only once the LISTENs
                // are in place: a consumer woken earlier could fetch before
                // them and miss a message published in between.
                const callbacks = new Set<SubscriberCallback>();
                for (const channel of added)
                  for (const callback of this.channelStates.get(channel)?.subscribers ?? [])
                    callbacks.add(callback);
                wake(callbacks);
                await changed[0];
              }
            } finally {
              client.removeListener('notification', onNotification);
              client.removeListener('error', onError);
              if (this.notifyClient === client) this.notifyClient = null;
              for (const state of this.channelStates.values()) state.listening = false;
            }
          },
          Infinity,
          signal
        );
      } catch (error) {
        if (!signal.aborted && !this.isShuttingDown) {
          if (
            listeningSince !== undefined &&
            performance.now() - listeningSince >= Connection.NOTIFY_RECONNECT_STABLE_MS
          )
            backoff = Connection.NOTIFY_RECONNECT_BASE_MS;
          // Jittered within [backoff/2, backoff] so instances that lost their
          // sessions together do not reconnect in lockstep.
          const delay = backoff / 2 + Math.random() * (backoff / 2);
          await new Promise<void>((resolve) => {
            const done = () => {
              clearTimeout(timer);
              signal.removeEventListener('abort', done);
              resolve();
            };
            const timer = setTimeout(done, delay);
            signal.addEventListener('abort', done, { once: true });
          });
          backoff = Math.min(backoff * 2, Connection.NOTIFY_RECONNECT_MAX_MS);
        }
      }
    }
  }

  /**
   * Subscribe to NOTIFY events on a single channel. Refcounts the channel so
   * the first subscriber issues LISTEN and the last unsubscribe issues
   * UNLISTEN (or, when no channel remains, stops the notification loop).
   * Returns an unsubscribe function.
   * @internal
   */
  private subscribeChannel(channel: string, callback: SubscriberCallback): () => void {
    let state = this.channelStates.get(channel);
    if (!state) {
      state = { refCount: 0, listening: false, subscribers: new Set() };
      this.channelStates.set(channel, state);
    }
    state.subscribers.add(callback);
    state.refCount = state.subscribers.size;
    this.notifyWake[1]();
    this.startNotificationListener();
    return () => {
      const current = this.channelStates.get(channel);
      if (current !== state) return;
      current.subscribers.delete(callback);
      current.refCount = current.subscribers.size;
      if (!current.refCount) this.channelStates.delete(channel);
      this.notifyWake[1]();
      if (!this.channelStates.size) void this.stopNotificationListener();
    };
  }

  /**
   * Subscribe a consumer to both the per-topic publish channel and the
   * per-queue wake channel `pmq:q:<queue>` (nack, release, DLQ requeue, and
   * group-successor wakeups). Returns a single unsubscribe function
   * that drops both subscriptions.
   * @internal
   */
  subscribeForConsumer(queue: string, topic: string, callback: SubscriberCallback): () => void {
    const offTopic = this.subscribeChannel('pmq:t:' + topic, callback);
    const offQueue = this.subscribeChannel('pmq:q:' + queue, callback);
    return () => {
      offTopic();
      offQueue();
    };
  }

  /**
   * Resolve a queue's topic. Lookup order: explicit override > in-memory
   * cache. If neither is available, throws — callers must either pass
   * `topic` to consume() or have populated the cache via createQueue.
   * @internal
   */
  resolveTopic(queue: string, explicit?: string): string {
    if (explicit) {
      this.topicCache.set(queue, explicit);
      return explicit;
    }
    const cached = this.topicCache.get(queue);
    if (cached) return cached;
    throw new Error(
      `Queue "${queue}" topic unknown — pass options.topic to consume() or call createQueue() first`
    );
  }

  /**
   * Register a consumer with this connection
   * @internal
   * @param consumer - The consumer to register
   */
  registerConsumer(consumer: Stoppable): void {
    if (this.isShuttingDown) throw new ConnectionClosedError();
    this.consumers.add(consumer);
  }

  /**
   * Unregister a consumer from this connection
   * @internal
   * @param consumer - The consumer to unregister
   */
  unregisterConsumer(consumer: Stoppable): void {
    this.consumers.delete(consumer);
    // If there are no more consumers, proactively stop the notification listener
    // to avoid lingering clients and open handles under heavy load.
    if (this.consumers.size === 0) {
      this.stopNotificationListener().catch((err) => {
        // Safe to ignore during normal shutdown
        console.debug('Error stopping notification listener (ignored):', err);
      });
    }
  }

  /**
   * Abort the notification loop and wait for it to exit; its client is
   * destroyed (dropping all LISTENs) and its listeners removed.
   */
  private async stopNotificationListener(): Promise<void> {
    this.notifyController?.abort();
    this.notifyWake[1]();
    await this.notifyStartupPromise;
  }

  /**
   * Execute an operation with retry logic
   * @internal
   * @param operation - The operation to execute
   * @param retryPolicy - The retry policy to use
   * @param retryable - Which errors are retried (default shouldRetry)
   * @returns The result of the operation
   */
  async executeWithRetry<T>(
    operation: (client: PoolClient) => Promise<T>,
    retryPolicy: RetryPolicy = this.retryPolicy,
    retryable: (error: any) => boolean = shouldRetry
  ): Promise<T> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return withRetry(() => this.runDatabase(operation), retryPolicy, retryable);
  }

  /** A pooled operation owns acquisition, its deadline, and release. On timeout
   * destroy the socket: a rejected Promise alone does not cancel PostgreSQL I/O. */
  private async runDatabase<T>(
    operation: (client: PoolClient) => Promise<T>,
    budgetMs = 30000,
    signal?: AbortSignal
  ): Promise<T> {
    let client: PoolClient | undefined;
    let ended = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let abort!: () => void;
    const timeout = new Promise<never>((_, reject) => {
      abort = () => {
        if (ended) return;
        ended = true;
        if (client) client.release(true);
        reject(new Error('Database operation deadline exceeded'));
      };
      const budget = Math.min(budgetMs, this.closeDeadline - Date.now());
      if (Number.isFinite(budget)) timer = setTimeout(abort, Math.max(0, budget));
    });
    this.activeIO.add(abort);
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    const work = (async () => {
      const acquired = await this.pool.connect();
      if (ended) {
        acquired.release();
        throw new Error('Database acquisition deadline exceeded');
      }
      client = acquired;
      return operation(acquired);
    })();
    try {
      return await Promise.race([work, timeout]);
    } catch (error) {
      if (signal && !ended) {
        ended = true;
        client?.release(true);
      }
      throw error;
    } finally {
      if (timer) clearTimeout(timer);
      this.activeIO.delete(abort);
      signal?.removeEventListener('abort', abort);
      if (!ended) {
        ended = true;
        client?.release();
      }
    }
  }

  /**
   * Run publish_message against any queryable (a pooled client or a
   * caller-supplied transaction) and return the new message ID. Shared by
   * publish() and publishWithTransaction() so the SQL, parameter shape, and
   * BIGINT conversion live in exactly one place.
   */
  private async publishOn(
    queryable: Transaction,
    topic: string,
    payload: any,
    options: PublishOptions,
    transaction: boolean
  ): Promise<number> {
    // Optional arguments use named notation so each keeps its SQL default
    // when omitted.
    const params: any[] = [topic, JSON.stringify(payload)];
    let args = '$1, $2';
    if (options.deliverAfter) {
      params.push(options.deliverAfter);
      args += `, p_deliver_after => $${params.length}`;
    }
    if (options.groupKey !== undefined && options.groupKey !== null) {
      params.push(options.groupKey);
      args += `, p_group_key => $${params.length}`;
    }
    const query = `SELECT postgremq.publish_message(${args}) as publish_message`;

    return this.telemetry.send(topic, transaction, async () => {
      const result = await queryable.query(query, params);
      // BIGINT columns arrive as strings from node-pg by default; convert at
      // the read boundary to keep the rest of the client on `number`.
      return checkedMessageId(result.rows[0].publish_message);
    });
  }

  /**
   * Publish a message to a topic. Under the retry policy it retries only
   * serialization failures and deadlocks (40001 / 40P01).
   * @param topic - The topic name
   * @param payload - The message payload
   * @param options - Publishing options
   * @returns Promise resolving to the message ID
   * @throws QueueNotFoundError if the topic does not exist
   * @throws ValidationError for an invalid group key
   * @throws ConnectionClosedError if not connected, or once close() has begun
   */
  async publish(topic: string, payload: any, options: PublishOptions = {}): Promise<number> {
    return this.telemetry.operation('publish', topic, false, async () => {
      if (!this.connected || this.isShuttingDown) {
        throw new ConnectionClosedError('Client is not connected');
      }

      try {
        // executeWithRetry hands us a pooled client, which satisfies the
        // Transaction (query-bearing) shape publishOn expects.
        return await this.executeWithRetry(
          (client) => this.publishOn(client, topic, payload, options, false),
          this.retryPolicy,
          (error) => ['40001', '40P01'].includes(error?.code ?? error?.sqlState)
        );
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }

  /**
   * Publish a message within an existing transaction.
   *
   * Runs publish_message on the caller-supplied transaction so the publish
   * commits (or rolls back) atomically with the caller's other writes — e.g.
   * insert an order row and enqueue its "order.created" event in one unit.
   * The distribution trigger fires inside the transaction, so the message
   * only reaches queues if the transaction commits.
   *
   * Unlike publish(), this does NOT apply the internal retry policy: retry
   * boundaries belong to the caller, who owns BEGIN / COMMIT / ROLLBACK. A
   * retry here could replay the INSERT against an already-aborted or
   * already-committed transaction. The caller is responsible for retrying
   * the whole transaction on a serialization failure.
   *
   * @param tx - An object exposing `query(text, values)` bound to an open
   *             transaction — a pg PoolClient/Client after `BEGIN` satisfies
   *             this. Mirrors the tx accepted by Message.ackWithTransaction.
   * @param topic - The topic name (must already exist)
   * @param payload - The message payload
   * @param options - Publishing options (e.g. deliverAfter for delayed delivery)
   * @returns Promise resolving to the message ID
   *
   * @example
   *   const client = await pool.connect();
   *   try {
   *     await client.query('BEGIN');
   *     await client.query('INSERT INTO orders ...');
   *     const id = await connection.publishWithTransaction(
   *       client, 'orders', { orderId: 42 });
   *     await client.query('COMMIT');
   *   } catch (err) {
   *     await client.query('ROLLBACK');
   *     throw err;
   *   } finally {
   *     client.release();
   *   }
   */
  async publishWithTransaction(
    tx: Transaction,
    topic: string,
    payload: any,
    options: PublishOptions = {}
  ): Promise<number> {
    return this.telemetry.operation('publish', topic, true, async () => {
      if (!this.connected || this.isShuttingDown) {
        throw new ConnectionClosedError('Client is not connected');
      }

      try {
        return await this.publishOn(tx, topic, payload, options, true);
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }

  /**
   * Create a consumer for a queue. The consumer does nothing until
   * messages() is called. An unknown topic (no `options.topic` is given and
   * this connection has not learned it from createQueue() or an earlier
   * consumer's `topic`) throws from messages(), not from here.
   * @param queue - The queue name
   * @param options - Consumer options
   * @returns A new consumer instance
   * @throws ValidationError for invalid consumer options
   * @throws ConnectionClosedError if not connected, or once close() has begun
   */
  consume(queue: string, options: Partial<ConsumerOptions> = {}): ConsumerImpl {
    if (!this.connected || this.isShuttingDown) {
      throw new ConnectionClosedError('Client is not connected');
    }

    const consumer = new ConsumerImpl(queue, this, options);
    this.registerConsumer(consumer);
    return consumer;
  }

  /**
   * Create a handler-based consumer for a queue.
   *
   * Each message is dispatched to `handler`. The push-based counterpart to
   * consume()'s `for await` iteration, mirroring the Go client's
   * ConsumeHandler. The auto-ack / auto-nack settlement rules are documented
   * on {@link MessageHandler}.
   *
   * `options.maxInFlight` bounds how many handlers run concurrently (0 =
   * unlimited, the default). Handlers receive a message whose `signal`
   * fires on shutdown, or on lease loss detected by auto-extension or a
   * failed setVt; long-running handlers should check it.
   *
   * @param queue - The queue name
   * @param handler - Per-message handler
   * @param options - Consumer options plus maxInFlight
   * @returns A started HandlerConsumer; call stop() to shut it down
   * @throws Error synchronously if the queue's topic is unknown (no
   *   `options.topic` is given and this connection has not learned it from
   *   createQueue() or an earlier consumer's `topic`), or if maxInFlight is not a non-negative integer
   * @throws ValidationError for invalid consumer options
   * @throws ConnectionClosedError if not connected, or once close() has begun
   *
   * @example
   *   const hc = connection.consumeHandler('orders', async (msg) => {
   *     await processOrder(msg.payload);   // auto-acked on return
   *   }, { maxInFlight: 10 });
   *   // ... later ...
   *   await hc.stop();
   */
  consumeHandler(
    queue: string,
    handler: MessageHandler,
    options: Partial<HandlerConsumerOptions> = {}
  ): HandlerConsumer {
    if (!this.connected || this.isShuttingDown) {
      throw new ConnectionClosedError('Client is not connected');
    }

    const { maxInFlight = 0, ...consumerOptions } = options;
    consumerOptions.topic = this.resolveTopic(queue, consumerOptions.topic);
    // The underlying consumer is NOT registered with the connection — the
    // HandlerConsumer is, so close() drives the full handler-aware shutdown
    // (wait for in-flight handlers) rather than just stopping the consumer.
    const consumer = new ConsumerImpl(queue, this, consumerOptions);
    const handlerConsumer = new HandlerConsumer(this, consumer, handler, maxInFlight);
    this.registerConsumer(handlerConsumer);
    handlerConsumer.start();
    return handlerConsumer;
  }

  /**
   * Create a new topic. Idempotent: an existing topic is left as is.
   * @param topic - The topic name: `^[A-Za-z0-9_:.\-]+$`, at most 57 bytes
   * @returns Promise that resolves when the topic is created
   * @throws ValidationError for an invalid or too long name
   */
  async createTopic(topic: string): Promise<void> {
    if (!this.connected || this.isShuttingDown) {
      throw new ConnectionClosedError('Client is not connected');
    }

    try {
      await this.executeWithRetry(async (client) => {
        await client.query('SELECT postgremq.create_topic($1)', [topic]);
      });
    } catch (err) {
      throw mapDbError(err);
    }
  }

  /**
   * Create a new queue. Idempotent when the parameters match an existing
   * queue (re-creating a live exclusive queue also refreshes its
   * keep-alive). An exclusive queue is registered with this connection's
   * keep-alive actor, which extends it until deleteQueue() is called, the
   * queue goes fatal, or the connection closes.
   * @param name - The queue name: `^[A-Za-z0-9_:.\-]+$`, at most 57 bytes
   * @param topic - The topic name
   * @param exclusive - Whether the queue is exclusive (temporary, kept alive by this connection)
   * @param options - Queue options (keepAliveInterval defaults to 300 s)
   * @returns Promise that resolves when the queue is created
   * @throws QueueNotFoundError if the topic does not exist, or the queue is an
   *   expired exclusive queue (delete it first)
   * @throws ValidationError for an invalid name, or if the queue already
   *   exists with different parameters
   * @throws Error if keepAliveInterval is below 0.001 or maxDeliveryAttempts
   *   is not an integer in [0, 2147483647]
   */
  async createQueue(
    name: string,
    topic: string,
    exclusive: boolean = false,
    options: QueueOptions = {}
  ): Promise<void> {
    if (!this.connected || this.isShuttingDown) {
      throw new ConnectionClosedError('Client is not connected');
    }

    try {
      await this.executeWithRetry(async (client) => {
        // Always pass all parameters to avoid parameter index misalignment
        const maxDeliveryAttempts = options.maxDeliveryAttempts ?? 0;
        // 5 minutes — matches the SQL function default and the Go client.
        const keepAliveSeconds = options.keepAliveInterval ?? 300;
        if (!Number.isFinite(keepAliveSeconds) || keepAliveSeconds < 0.001)
          throw new Error('keepAliveInterval must be at least 0.001 seconds');
        if (
          !Number.isSafeInteger(maxDeliveryAttempts) ||
          maxDeliveryAttempts < 0 ||
          maxDeliveryAttempts > 2147483647
        )
          throw new Error('maxDeliveryAttempts must be >= 0 and a finite integer <= 2147483647');

        // Scale seconds to INTERVAL via "$5 * interval '1 sec'" so the SQL
        // function can stay typed as INTERVAL without us needing a per-driver
        // serializer for it.
        const sent = Date.now(); // the keep-alive deadline is counted from it
        const created = await client.query(
          "SELECT postgremq.create_queue($1, $2, $3, $4, $5 * interval '1 sec')",
          [name, topic, maxDeliveryAttempts, exclusive, keepAliveSeconds]
        );

        // Exclusive queues expire unless the client keeps extending
        // keep_alive_until; consuming does not refresh it. Register every
        // exclusive queue with the keep-alive actor (matching Go), using the
        // interval passed to create_queue (default 300s, matching SQL and Go).
        const generation = created.rows[0].create_queue as string;
        this.queueGenerations.set(name, generation);
        if (exclusive) {
          this.keepAliveRegister(name, keepAliveSeconds, generation, sent);
        }
      });
    } catch (err) {
      throw mapDbError(err);
    }
    // Cache the queue->topic mapping so consume() can subscribe to the
    // per-topic publish channel without an extra DB lookup.
    this.topicCache.set(name, topic);
    // Recreating a queue clears any prior fatal mark so it can be consumed (and,
    // if it dies again, declared fatal) anew.
    this.fatalQueues.delete(name);
  }

  /**
   * Delete a topic and, by cascade, every queue subscribed to it. A no-op if
   * the topic does not exist.
   * @param topic - The topic name
   * @returns Promise that resolves when the topic is deleted
   * @throws ValidationError while any message of the topic exists (including
   *   completed, retained ones); run cleanUpTopic() first
   */
  async deleteTopic(topic: string): Promise<void> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    try {
      await this.executeWithRetry(async (client) => {
        await client.query('SELECT postgremq.delete_topic($1)', [topic]);
      });
    } catch (err) {
      throw mapDbError(err);
    }
  }

  /**
   * Delete a queue and all its deliveries, in-flight ones included. A no-op
   * if the queue does not exist. Running consumers on the queue are not
   * stopped here: their next fetch finds it gone and they go queue-fatal.
   * @param queue - The queue name
   * @returns Promise that resolves when the queue is deleted
   * @throws a database error with code 'PMQ03' while the queue has DLQ
   *   entries (requeueDLQMessages() or purgeDLQ() first)
   */
  async deleteQueue(queue: string): Promise<void> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      await client.query('SELECT postgremq.delete_queue($1)', [queue]);
    });

    // Invalidate the cached topic mapping. If the queue is recreated under
    // a different topic later, the next consume() call must re-resolve from
    // a fresh source (explicit option or createQueue) instead of reading a
    // stale entry.
    this.keepAliveDeregister(queue);
    this.queueGenerations.delete(queue);
    this.topicCache.delete(queue);
  }

  /**
   * List all topics
   * @returns Promise that resolves to a list of topic names
   */
  async listTopics(): Promise<string[]> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM postgremq.list_topics()');
      return result.rows.map((row) => row.topic);
    });
  }

  /**
   * List all queues
   * @returns Promise that resolves to a list of queue information
   */
  async listQueues(): Promise<QueueInfo[]> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM postgremq.list_queues()');

      return result.rows.map((row) => ({
        queueName: row.queue_name,
        topicName: row.topic_name,
        maxDeliveryAttempts: row.max_delivery_attempts,
        exclusive: row.exclusive,
        keepAliveUntil: row.keep_alive_until,
      }));
    });
  }

  /**
   * Get statistics for a queue
   * @param queue - Optional queue name (if omitted, totals across all
   *   queues; an unknown queue gives zeros). Completed deliveries count until
   *   cleanupCompletedMessages() deletes them; DLQ entries are not counted.
   * @returns Promise that resolves to queue statistics
   */
  async getQueueStatistics(queue?: string): Promise<QueueStatistics> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM postgremq.get_queue_statistics($1)', [
        queue || null,
      ]);

      if (result.rows.length === 0) {
        return {
          pendingCount: 0,
          processingCount: 0,
          completedCount: 0,
          totalCount: 0,
        };
      }

      const row = result.rows[0];
      return {
        pendingCount: parseInt(row.pending_count, 10),
        processingCount: parseInt(row.processing_count, 10),
        completedCount: parseInt(row.completed_count, 10),
        totalCount: parseInt(row.total_count, 10),
      };
    });
  }

  /**
   * Bundle the latency-sensitive maintenance routines into one call: retire
   * crashed-final-attempt rows to DLQ + reap expired exclusive queues. Run
   * it every 1–10 seconds, and at least once every 60 seconds (see the mq
   * README, "Maintenance and retention").
   *
   * nack_message already retires the final attempt inline; this catches every
   * final-attempt delivery still 'processing' after its lease expired,
   * whatever the cause (e.g. a consumer that crashed mid-handler).
   * cleanup_completed_messages stays separate — it's a latency-tolerant bulk
   * DELETE governed by retention policy.
   *
   * @returns `retiredToDlq` (deliveries moved to the DLQ) and
   *   `inactiveQueuesDropped` (expired exclusive queues deleted; queues with
   *   DLQ entries are skipped)
   */
  async maintenanceFast(): Promise<{ retiredToDlq: number; inactiveQueuesDropped: number }> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT retired_to_dlq, inactive_queues_dropped FROM postgremq.pmq_maintenance_fast()'
      );
      const row = result.rows[0];
      return {
        retiredToDlq: Number(row.retired_to_dlq),
        inactiveQueuesDropped: Number(row.inactive_queues_dropped),
      };
    });
  }

  /**
   * List messages in the Dead Letter Queue
   * @returns Promise that resolves to a list of DLQ messages
   */
  async listDLQMessages(): Promise<DLQMessage[]> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM postgremq.list_dlq_messages()');

      return result.rows.map((row) => ({
        queueName: row.queue_name,
        messageId: checkedMessageId(row.message_id),
        retryCount: row.retry_count,
        publishedAt: row.published_at,
      }));
    });
  }

  /**
   * Move this queue's DLQ entries back into it as pending deliveries
   * (delivery attempts reset to 0) and wake its consumers.
   * @param queue - The queue name
   * @returns Promise that resolves when messages are requeued
   */
  async requeueDLQMessages(queue: string): Promise<void> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      await client.query('SELECT postgremq.requeue_dlq_messages($1)', [queue]);
    });
  }

  /**
   * Purge all messages from the Dead Letter Queue, in every queue. Payloads
   * are left for retention cleanup.
   * @returns Promise that resolves when DLQ is purged
   */
  async purgeDLQ(): Promise<void> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      await client.query('SELECT postgremq.purge_dlq()');
    });
  }

  /**
   * Delete every message in the system: all DLQ entries, deliveries
   * (in-flight ones included) and payloads; the emptied groups are pruned
   * (best-effort), so their next publish restarts at group_seq 1. Topics
   * and queues remain.
   * @returns Promise that resolves when all messages are purged
   */
  async purgeAllMessages(): Promise<void> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      await client.query('SELECT postgremq.purge_all_messages()');
    });
  }

  /**
   * Delete one message's delivery from a queue, in any state, in-flight
   * included (its holder's ack/nack then fails with LeaseLostError). A no-op
   * if absent. Deleting an unsettled grouped delivery wakes its group's
   * successor. The payload and other queues' deliveries remain.
   * @param queue - The queue name
   * @param messageID - The message ID
   * @returns Promise that resolves when the message is deleted
   */
  async deleteQueueMessage(queue: string, messageID: number): Promise<void> {
    checkedMessageId(messageID);
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      await client.query('SELECT postgremq.delete_queue_message($1, $2)', [queue, messageID]);
    });
  }

  /**
   * List all messages in a queue, in every status (completed deliveries
   * included until cleanupCompletedMessages() deletes them), without
   * payloads, ordered by when they entered the queue.
   * @param queue - The queue name
   * @returns Promise that resolves to a list of queue messages
   */
  async listMessages(queue: string): Promise<QueueMessage[]> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM postgremq.list_messages($1)', [queue]);

      return result.rows.map((row) => ({
        messageId: checkedMessageId(row.message_id),
        status: row.status,
        publishedAt: row.published_at,
        deliveryAttempts: row.delivery_attempts,
        vt: row.vt,
        processedAt: row.processed_at,
        groupKey: row.group_key,
        groupSeq: checkedGroupSeq(row.group_seq),
      }));
    });
  }

  /**
   * Get a specific message by ID
   * @param messageID - The message ID
   * @returns Promise that resolves to the message information or null if not found
   */
  async getMessage(messageID: number): Promise<PublishedMessage | null> {
    checkedMessageId(messageID);
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM postgremq.get_message($1)', [messageID]);

      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      return {
        messageId: checkedMessageId(row.message_id),
        topicName: row.topic_name,
        payload: row.payload,
        publishedAt: row.published_at,
        groupKey: row.group_key,
        groupSeq: checkedGroupSeq(row.group_seq),
      };
    });
  }

  /**
   * Return the queue's generation (UUID), cached from createQueue or read
   * from postgremq.queues.
   * @throws QueueNotFoundError if the queue does not exist
   * @internal
   */
  async resolveQueueGeneration(queue: string): Promise<string> {
    const cached = this.queueGenerations.get(queue);
    if (cached) return cached;
    const generation = await this.runDatabase(async (client) => {
      const result = await client.query('SELECT generation FROM postgremq.queues WHERE name=$1', [
        queue,
      ]);
      if (!result.rows.length)
        throw mapDbError({ code: 'PMQ02', message: `Queue "${queue}" does not exist` });
      return result.rows[0].generation as string;
    });
    if (!this.queueGenerations.has(queue)) this.queueGenerations.set(queue, generation);
    return generation;
  }

  /**
   * Consume messages from a queue (one consume_message call, not retried;
   * see the inline comment).
   * @internal
   * @param queueName - The queue name
   * @param visibilityTimeout - Visibility timeout in seconds
   * @param limit - Maximum number of messages to fetch
   * @param generation - Pins the claim to that queue incarnation; a missing,
   *   expired or replaced queue raises PMQ02, which maps to QueueNotFoundError
   * @returns Promise resolving to array of raw messages
   */
  async consumeMessages(
    queueName: string,
    visibilityTimeout: number,
    limit: number = 1,
    generation?: string
  ): Promise<any[]> {
    return this.telemetry.operation('consume', queueName, false, async () => {
      if (
        !Number.isSafeInteger(visibilityTimeout) ||
        visibilityTimeout < 0 ||
        visibilityTimeout > 2147483647 ||
        !Number.isSafeInteger(limit) ||
        limit <= 0 ||
        limit > 2147483647
      )
        throw new ValidationError('invalid visibility timeout or batch size');
      if (!this.connected || this.isShuttingDown) {
        throw new ConnectionClosedError('Client is not connected or shutting down');
      }

      // Deliberately a DIRECT, un-retried call — not via executeWithRetry.
      // consume_message is not idempotent: it flips matched rows to 'processing',
      // increments delivery_attempts, mints a new consumer_token, and pushes vt
      // forward. If the statement commits server-side but the response is lost on
      // the wire (e.g. an 08-class drop while reading the result), a retry would
      // skip the just-claimed rows and claim a SECOND disjoint batch — orphaning
      // the first batch in 'processing' with delivery_attempts already burned.
      // Recovery is the next fetch tick + vt expiry, not a retry. Mirrors Go's
      // consumeMessages, which is also deliberately un-retried. Publication
      // retries only confirmed transaction aborts; settlement uses token fencing.
      try {
        const messages = await this.runDatabase(
          async (client) => {
            const sent = Date.now(); // vt is sent plus the time left by the database clock
            const result = await client.query(
              'SELECT queue_name, message_id, payload, consumer_token, delivery_attempts, vt_remaining_ms, published_at, group_key, group_seq, first_delivery_wait_ms FROM postgremq.consume_message($1, $2, $3, $4)',
              [queueName, visibilityTimeout, limit, generation ?? null]
            );
            // message_id is BIGINT; convert at the read boundary so callers see a
            // number (matches Message.id and the rest of the public surface).
            return result.rows.map((row) => ({
              ...row,
              message_id: checkedMessageId(row.message_id),
              group_seq: checkedGroupSeq(row.group_seq),
              vt: new Date(sent + Number(row.vt_remaining_ms)),
            }));
          },
          Math.max(1000, visibilityTimeout * 500)
        );
        this.telemetry.recordConsumed(queueName, messages);
        return messages;
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }

  /**
   * Acknowledge a message
   * @internal
   * @param queueName - The queue name
   * @param messageId - The message ID
   * @param consumerToken - The consumer token
   * @param tx - Optional transaction object
   * @returns Promise that resolves when the message is acknowledged
   */
  async ackMessage(
    queueName: string,
    messageId: number,
    consumerToken: string,
    tx?: Transaction
  ): Promise<void> {
    return this.telemetry.operation('ack', queueName, !!tx, async () => {
      checkedMessageId(messageId);
      if (!this.connected) {
        throw new ConnectionClosedError('Client is not connected');
      }

      if (tx) {
        try {
          await tx.query('SELECT postgremq.ack_message($1, $2, $3)', [
            queueName,
            messageId,
            consumerToken,
          ]);
        } catch (err) {
          throw mapDbError(err);
        }
        return;
      }

      try {
        await this.executeWithRetry(async (client) => {
          await client.query('SELECT postgremq.ack_message($1, $2, $3)', [
            queueName,
            messageId,
            consumerToken,
          ]);
        });
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }

  /**
   * Negatively acknowledge a message
   * @internal
   * @param queueName - The queue name
   * @param messageId - The message ID
   * @param consumerToken - The consumer token
   * @param delayUntil - Optional absolute time at which the message becomes
   *   visible again (default now). On the queue's last allowed attempt the
   *   message moves to the DLQ instead.
   * @returns Promise that resolves when the message is negative acknowledged
   */
  async nackMessage(
    queueName: string,
    messageId: number,
    consumerToken: string,
    delayUntil?: Date
  ): Promise<void> {
    return this.telemetry.operation('nack', queueName, false, async () => {
      checkedMessageId(messageId);
      if (!this.connected) {
        throw new ConnectionClosedError('Client is not connected');
      }

      try {
        await this.executeWithRetry(async (client) => {
          // Sent as the time remaining on the local clock, which the database
          // adds to its own, so the clocks need not agree.
          const delayMs = delayUntil
            ? Math.min(Math.max(delayUntil.getTime() - Date.now(), 0), MAX_NACK_DELAY_MS)
            : 0;
          await client.query('SELECT postgremq.nack_message($1, $2, $3, $4)', [
            queueName,
            messageId,
            consumerToken,
            delayMs,
          ]);
        });
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }

  /**
   * Release a message back to the queue
   * @internal
   * @param queueName - The queue name
   * @param messageId - The message ID
   * @param consumerToken - The consumer token
   * @returns Promise that resolves when the message is released
   */
  async releaseMessage(queueName: string, messageId: number, consumerToken: string): Promise<void> {
    return this.telemetry.operation('release', queueName, false, async () => {
      checkedMessageId(messageId);
      if (!this.connected) {
        throw new ConnectionClosedError('Client is not connected');
      }

      try {
        await this.executeWithRetry(async (client) => {
          await client.query('SELECT postgremq.release_message($1, $2, $3)', [
            queueName,
            messageId,
            consumerToken,
          ]);
        });
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }

  /**
   * Set the visibility timeout for a message
   * @internal
   * @param queueName - The queue name
   * @param messageId - The message ID
   * @param consumerToken - The consumer token
   * @param visibilityTimeout - The new lease duration, in seconds from now;
   *   raises PMQ01 (LeaseLostError) if the lease has already expired
   * @returns Promise that resolves to the new expiration date
   */
  async setMessageVt(
    queueName: string,
    messageId: number,
    consumerToken: string,
    visibilityTimeout: number
  ): Promise<Date> {
    return this.telemetry.operation('extend', queueName, false, async () => {
      checkedMessageId(messageId);
      if (!this.connected) {
        throw new ConnectionClosedError('Client is not connected');
      }

      try {
        return await this.executeWithRetry(async (client) => {
          // set_vt returns the time left by the database clock; vt is sent
          // plus it, as for consumed messages.
          const sent = Date.now();
          const result = await client.query(
            'SELECT postgremq.set_vt($1, $2, $3, $4) AS remaining_ms',
            [queueName, messageId, consumerToken, visibilityTimeout]
          );
          return new Date(sent + Number(result.rows[0].remaining_ms));
        });
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }

  /**
   * How long until the queue's next row becomes visible, measured by the
   * database's clock
   * @internal
   * @param queueName - The queue name
   * @returns Promise that resolves to the milliseconds until then (zero or
   *   negative if one is already due), or null if there is none
   */
  async getNextVisibleInMs(queueName: string): Promise<number | null> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT postgremq.get_next_visible_in_ms($1) AS next_ms',
        [queueName]
      );

      const nextMs = result.rows[0].next_ms;
      return nextMs === null ? null : Number(nextMs);
    });
  }

  /**
   * Delete every delivery of the queue, in every state, including in-flight
   * ones (their holders lose the lease). Payloads remain until retention
   * cleanup.
   * @param queue - The queue name
   * @returns Promise that resolves when the queue is cleaned up
   */
  async cleanUpQueue(queue: string): Promise<void> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      await client.query('SELECT postgremq.clean_up_queue($1)', [queue]);
    });
  }

  /**
   * Delete every message of the topic (and so all their deliveries, in every
   * state) and prune its group rows. Queues remain.
   * @param topic - The topic name
   * @returns Promise that resolves when the topic is cleaned up
   * @throws a database error with code 'PMQ03' while any of the topic's
   *   messages is in the DLQ; requeue or purge the DLQ first
   */
  async cleanUpTopic(topic: string): Promise<void> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      await client.query('SELECT postgremq.clean_up_topic($1)', [topic]);
    });
  }

  /**
   * Delete inactive (expired) exclusive queues, skipping any that still have
   * DLQ entries
   * @returns Promise that resolves when inactive queues are deleted
   */
  async deleteInactiveQueues(): Promise<void> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      await client.query('SELECT postgremq.delete_inactive_queues()');
    });
  }

  /**
   * Delete up to 1000 completed deliveries processed more than
   * `olderThanHours` hours ago, then up to 1000 payloads no delivery or DLQ
   * entry references and published more than `olderThanHours` hours ago.
   * Call repeatedly to drain a backlog.
   * @param olderThanHours - Optional retention threshold in whole hours (default 24).
   * @returns Number of completed deliveries deleted (payloads not counted).
   * @throws a database error with code 'PMQ03' if olderThanHours is negative
   */
  async cleanupCompletedMessages(olderThanHours?: number): Promise<number> {
    if (!this.connected) {
      throw new ConnectionClosedError('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result =
        olderThanHours !== undefined
          ? await client.query('SELECT postgremq.cleanup_completed_messages($1) AS deleted', [
              olderThanHours,
            ])
          : await client.query('SELECT postgremq.cleanup_completed_messages() AS deleted');

      return Number(result.rows[0].deleted);
    });
  }

  /**
   * Check if client is shutting down
   * @returns True once close() has been called (including after it has finished)
   */
  isClientShuttingDown(): boolean {
    return this.isShuttingDown;
  }

  /**
   * Subscribe to a connection-level event. Currently emits:
   *   - 'queueFatal' (queue: string, error: QueueFatalError): the queue is gone
   *     (deleted out-of-band, or an exclusive queue whose keep-alive permanently
   *     failed) and teardown of every consumer on it has begun (each
   *     consumer's onClose fires when it has drained). Consumers also learn
   *     via Consumer.onClose; this is the only signal for a producer-only
   *     exclusive queue with no consumer. Not part of the exported Connection
   *     interface.
   *
   * @param event - The event name
   * @param listener - The callback
   */
  on(event: 'queueFatal', listener: (queue: string, error: unknown) => void): void;
  on(event: string, listener: (...args: any[]) => void): void {
    this.events.on(event, listener);
  }

  /**
   * Unsubscribe a previously registered connection-level event listener.
   * @param event - The event name
   * @param listener - The callback to remove
   */
  off(event: string, listener: (...args: any[]) => void): void {
    this.events.off(event, listener);
  }

  // ===================================================================
  // Connection-level keep-alive actor
  // ===================================================================

  /**
   * Upsert an exclusive queue into the keep-alive schedule. Called by
   * createQueue. Dedupes by queue name (createQueue may be called twice).
   * `sent` is the Date.now() read before the create_queue statement that set
   * keep_alive_until.
   * @internal
   */
  private keepAliveRegister(
    queueName: string,
    intervalSec: number,
    generation: string,
    sent: number
  ): void {
    const nextAt = new Date(sent + Math.floor((intervalSec * 1000) / 2));
    this.keepAliveEntries.set(queueName, {
      generation,
      intervalSec,
      nextAt,
      expiresAt: new Date(sent + intervalSec * 1000),
    });
    this.armKeepAlive();
  }

  /**
   * Remove a queue from the keep-alive schedule. No-op if absent. Called by
   * deleteQueue so an intentional delete doesn't fire a spurious failure.
   * @internal
   */
  private keepAliveDeregister(queueName: string): void {
    this.keepAliveEntries.delete(queueName);
    this.armKeepAlive();
  }

  /** (Re)arm the single keep-alive timer to fire at the earliest pending
   *  nextAt. No-op while a flush is in flight (the flush re-arms when done) or
   *  after the background actors have been stopped. @internal */
  private armKeepAlive(): void {
    if (this.keepAliveTimer) {
      clearTimeout(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }
    if (this.keepAliveFlushing || this.backgroundStopped) return;
    let earliest: number | null = null;
    for (const e of this.keepAliveEntries.values()) {
      const t = e.nextAt.getTime();
      if (earliest === null || t < earliest) earliest = t;
    }
    if (earliest === null) return;
    const wait = Math.max(0, earliest - Date.now());
    this.keepAliveTimer = setTimeout(() => {
      void this.flushKeepAlive();
    }, wait);
  }

  /** Extend every due queue in one extend_queue_keep_alive_multi call, then
   *  reschedule kept queues / drop+notify permanently-failed ones / bounded-
   *  retry transient failures. @internal */
  private async flushKeepAlive(): Promise<void> {
    if (this.keepAliveFlushing || this.backgroundStopped) return;
    const due = Array.from(this.keepAliveEntries).filter(
      ([, e]) => e.nextAt.getTime() <= Date.now()
    );
    if (!due.length) {
      this.armKeepAlive();
      return;
    }
    this.keepAliveFlushing = true;
    try {
      let leases: Map<string, { until: Date; busy: boolean }> | undefined;
      try {
        leases = await this.runDatabase(
          async (client) => {
            // Deadlines are the time left by the database clock, counted from
            // before the call, so they never depend on the two clocks agreeing.
            const sent = Date.now();
            const result = await client.query(
              'SELECT queue_name, keep_alive_remaining_ms, outcome FROM postgremq.extend_queue_keep_alive_multi($1, $2, $3)',
              [
                due.map(([n]) => n),
                due.map(([, e]) => Math.floor(e.intervalSec * 1000)),
                due.map(([, e]) => e.generation),
              ]
            );
            return new Map(
              result.rows.map((r) => [
                r.queue_name,
                {
                  until: new Date(sent + Number(r.keep_alive_remaining_ms)),
                  busy: r.outcome === 'busy',
                },
              ])
            );
          },
          heartbeatBudgetMs(due.map(([, e]) => e.expiresAt))
        );
      } catch {
        /* Retry only while the last confirmed lease remains live. */
      }
      for (const [name, e] of due) {
        if (this.keepAliveEntries.get(name) !== e) continue;
        const lease = leases?.get(name);
        if (lease && !lease.busy) {
          e.expiresAt = lease.until;
          e.nextAt = new Date(Date.now() + (lease.until.getTime() - Date.now()) / 2);
        } else if ((!leases || lease?.busy) && e.expiresAt.getTime() > Date.now())
          e.nextAt = new Date(Math.min(Date.now() + 100, e.expiresAt.getTime()));
        else {
          this.keepAliveEntries.delete(name);
          this.queueFatal(name, undefined, e.generation);
        }
      }
    } finally {
      this.keepAliveFlushing = false;
      this.armKeepAlive();
    }
  }

  /**
   * queueFatal handles a queue that has become unrecoverably gone — deleted out
   * of band (a consume returns PMQ02), or an exclusive queue whose keep-alive
   * permanently failed. Idempotent per queue. It stops keeping the queue alive,
   * tears down every consumer bound to it (their normal stop() cancels handlers
   * and deregisters in-flight messages from the extender), then signals: each
   * consumer's onClose, plus the connection-level 'queueFatal' event /
   * onQueueFatal handler — the only signal for a producer-only exclusive queue
   * with no consumer. consumer.fatal is non-blocking, so this returns promptly;
   * each consumer's onClose fires once it finishes draining.
   * @internal — called by the keep-alive actor and by consumers on a PMQ02 fetch.
   */
  queueFatal(queue: string, cause?: Error, generation?: string): void {
    if (
      generation &&
      this.queueGenerations.has(queue) &&
      this.queueGenerations.get(queue) !== generation
    )
      return;
    if (this.fatalQueues.has(queue)) return;
    this.fatalQueues.add(queue);
    const err = new QueueFatalError(queue, cause);

    // Stop keeping a dead queue alive (no-op if not exclusive / not registered).
    this.keepAliveDeregister(queue);

    // Tear down every consumer on this queue (consumers can share a queue).
    for (const cons of Array.from(this.consumers)) {
      if (
        isFatalConsumer(cons) &&
        cons.getQueueName() === queue &&
        (!generation || cons.getQueueGeneration() === generation)
      ) {
        cons.fatal(err);
      }
    }

    // Queue-level signal (also the only signal for producer-only queues).
    this.events.emit('queueFatal', queue, err);
    if (this.onQueueFatal) {
      try {
        this.onQueueFatal(queue, err);
      } catch (cbErr) {
        console.error(`onQueueFatal callback threw for queue ${queue}:`, cbErr);
      }
    } else {
      console.error(`Queue ${queue} is gone:`, err);
    }
  }

  /**
   * Batch keep-alive extension (uses the retry policy). Returns one row per
   * requested queue that was extended (`until` = new deadline, `busy: false`)
   * or lock-contended (`until: null`, `busy: true`). A queue omitted from the
   * result is gone, non-exclusive, already expired, or (with `generations`) a
   * different incarnation. Not used by the keep-alive actor.
   * @internal
   */
  async extendQueueKeepAliveMulti(
    names: string[],
    intervalsMs: number[],
    generations?: string[]
  ): Promise<Array<{ queue: string; until: Date | null; busy: boolean }>> {
    return this.telemetry.operation('keep_alive', '', false, async () => {
      if (!this.connected) {
        throw new ConnectionClosedError('Client is not connected');
      }
      if (names.length === 0) return [];
      try {
        return await this.executeWithRetry(async (client) => {
          const sent = Date.now(); // until is sent plus the time left by the database clock
          const result = await client.query(
            'SELECT queue_name, keep_alive_remaining_ms, outcome FROM postgremq.extend_queue_keep_alive_multi($1, $2, $3)',
            [names, intervalsMs, generations ?? null]
          );
          return result.rows.map((row: any) => ({
            queue: row.queue_name as string,
            until:
              row.keep_alive_remaining_ms === null
                ? null
                : new Date(sent + Number(row.keep_alive_remaining_ms)),
            busy: row.outcome === 'busy',
          }));
        });
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }

  // ===================================================================
  // Connection-level visibility-timeout extender actor
  // ===================================================================

  /**
   * Register (upsert) an in-flight delivery for auto-extension; each
   * (queue, id, token) is independent. No-op once close() has stopped the
   * background actors.
   * @internal
   */
  extenderRegister(e: {
    queue: string;
    id: number;
    token: string;
    vtSec: number;
    threshold: number;
    vt: Date;
    cancel: () => void;
    onExtended: (vt: Date) => void;
  }): void {
    if (this.backgroundStopped) return;
    const now = Date.now();
    const remaining = Math.max(0, e.vt.getTime() - now);
    const nextExtensionTime = new Date(now + remaining * e.threshold);
    this.extenderEntries.set(extKey(e.queue, e.id, e.token), {
      queue: e.queue,
      id: e.id,
      token: e.token,
      vtSec: e.vtSec,
      threshold: e.threshold,
      nextExtensionTime,
      expiresAt: e.vt,
      cancel: e.cancel,
      onExtended: e.onExtended,
    });
    this.armExtender();
  }

  /** Deregister a settled message from auto-extension. No-op if absent. @internal */
  extenderDeregister(queue: string, id: number, token: string): void {
    this.extenderEntries.delete(extKey(queue, id, token));
    this.armExtender();
  }

  /** (Re)arm the single extension timer to fire at the earliest pending
   *  nextExtensionTime. No-op while a flush is in flight (the flush re-arms
   *  when done) or after the background actors have been stopped. @internal */
  private armExtender(): void {
    if (this.extenderTimer) {
      clearTimeout(this.extenderTimer);
      this.extenderTimer = null;
    }
    if (this.extenderFlushing || this.backgroundStopped) return;
    let earliest: number | null = null;
    for (const e of this.extenderEntries.values()) {
      const t = e.nextExtensionTime.getTime();
      if (earliest === null || t < earliest) earliest = t;
    }
    if (earliest === null) {
      if (this.extenderTryAfter === null) return;
      earliest = this.extenderTryAfter.getTime();
    } else if (this.extenderTryAfter && this.extenderTryAfter.getTime() > earliest) {
      earliest = this.extenderTryAfter.getTime();
    }
    const wait = Math.max(0, earliest - Date.now());
    this.extenderTimer = setTimeout(() => {
      void this.flushExtender();
    }, wait);
  }

  /** Extend up to `extenderBatchSize` due messages in one set_vt_batch_multi
   *  call (correlated by the composite (queue, id, token) key), reschedule the
   *  extended ones, retry busy rows within ~100 ms until their confirmed vt,
   *  and cancel + drop lease-lost ones. Transient failures back off ~1s.
   *  @internal */
  private async flushExtender(): Promise<void> {
    if (this.extenderFlushing || this.backgroundStopped) return;
    const now = Date.now();
    const due: ExtEntry[] = [];
    for (const e of this.extenderEntries.values()) {
      if (e.nextExtensionTime.getTime() <= now) {
        due.push(e);
        if (due.length >= this.extenderBatchCap) break;
      }
    }
    if (due.length === 0) {
      this.extenderTryAfter = null;
      this.armExtender();
      return;
    }
    this.extenderFlushing = true;
    try {
      this.extenderTryAfter = null;
      const queues = due.map((e) => e.queue);
      const ids = due.map((e) => e.id);
      const tokens = due.map((e) => e.token);
      const vts = due.map((e) => e.vtSec);

      let results: Array<{ queue: string; id: number; vt: Date; token: string; busy: boolean }>;
      try {
        results = await this.setVtBatchMulti(
          queues,
          ids,
          tokens,
          vts,
          heartbeatBudgetMs(due.map((e) => e.expiresAt))
        );
      } catch (error) {
        // Transient failure (setVtBatchMulti makes a single attempt, with no
        // retry policy): cancel and drop entries whose confirmed lease has
        // expired; retry the rest in ~1s, never later than expiresAt.
        // nextExtensionTime sits at `threshold` of the remaining lease, which
        // leaves headroom before the server-side lease lapses.
        for (const e of due) {
          if (this.extenderEntries.get(extKey(e.queue, e.id, e.token)) !== e) continue;
          if (e.expiresAt.getTime() <= Date.now()) {
            this.extenderEntries.delete(extKey(e.queue, e.id, e.token));
            this.telemetry.recordRenewalLost(e.queue);
            e.cancel();
          } else e.nextExtensionTime = new Date(Math.min(Date.now() + 1000, e.expiresAt.getTime()));
        }
        if (!this.isShuttingDown) {
          console.warn(`Transient error extending ${due.length} messages: ${error}; will retry`);
        }
        return;
      }

      const extended = new Map(results.map((r) => [extKey(r.queue, r.id, r.token), r]));

      const after = Date.now();
      for (const e of due) {
        const k = extKey(e.queue, e.id, e.token);
        const current = this.extenderEntries.get(k);
        const result = extended.get(k);
        if (current !== e) continue;
        if (result?.busy && e.expiresAt.getTime() > after) {
          e.nextExtensionTime = new Date(Math.min(after + 100, e.expiresAt.getTime()));
          continue;
        }
        const newVt = result?.busy ? undefined : result?.vt;
        if (newVt) {
          e.expiresAt = newVt;
          if (current === e) {
            const remaining = Math.max(0, newVt.getTime() - after);
            e.nextExtensionTime = new Date(after + remaining * e.threshold);
            e.onExtended(newVt);
          }
        } else {
          // Omitted, or still busy at the confirmed vt: lease lost. Advise the
          // handler and drop the entry.
          if (current === e) this.extenderEntries.delete(k);
          this.telemetry.recordRenewalLost(e.queue);
            e.cancel();
        }
      }
    } finally {
      this.extenderFlushing = false;
      this.armExtender();
    }
  }

  /**
   * Cross-queue batch visibility-timeout extension: one attempt, no retry
   * policy, bounded by `budgetMs`. Returns one row per requested delivery
   * that was extended (`busy: false`, `vt` = confirmed lease end) or was
   * lock-contended (`busy: true`, `vt` meaningless). A delivery omitted from
   * the result has lost its lease (not owned, not processing, or expired).
   * Correlate by (queue, id, token).
   * @internal
   */
  async setVtBatchMulti(
    queues: string[],
    ids: number[],
    tokens: string[],
    vts: number[],
    budgetMs = 1000
  ): Promise<Array<{ queue: string; id: number; vt: Date; token: string; busy: boolean }>> {
    return this.telemetry.operation('extend_batch', '', false, async () => {
      if (!this.connected) {
        throw new ConnectionClosedError('Client is not connected');
      }
      ids.forEach(checkedMessageId);
      if (queues.length === 0) return [];
      try {
        return await this.runDatabase(async (client) => {
          const sent = Date.now(); // vt is sent plus the time left by the database clock
          const result = await client.query(
            'SELECT queue_name, message_id, vt_remaining_ms, consumer_token, outcome FROM postgremq.set_vt_batch_multi($1, $2, $3, $4)',
            [queues, ids, tokens, vts]
          );
          return result.rows.map((row: any) => ({
            queue: row.queue_name as string,
            id: checkedMessageId(row.message_id),
            vt: new Date(sent + Number(row.vt_remaining_ms)),
            token: row.consumer_token,
            busy: row.outcome === 'busy',
          }));
        }, budgetMs);
      } catch (err) {
        throw mapDbError(err);
      }
    });
  }
}
