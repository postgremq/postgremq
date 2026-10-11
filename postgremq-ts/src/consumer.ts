/**
 * PostgreMQ TypeScript Client
 * Consumer implementation
 */

import { ConsumerOptions } from './types';
import { Connection } from './connection';
import { Message } from './message';
import { createDeferred, sleep, untilDeadline } from './utils';
import { ValidationError, QueueNotFoundError, QueueFatalError } from './errors';

/**
 * Validate consumer options at construction, mirroring the Go client's
 * validateConsumeOptions. Throws ValidationError synchronously so misuse
 * surfaces at consume()/consumeHandler() rather than as a silent hang
 * (e.g. batchSize 0 never fetches) or a never-extended lease (e.g.
 * extensionThreshold outside (0, 1) puts every extension time in the past).
 *
 * visibilityTimeoutSec === 0 is treated as "use the default" (Go maps
 * 0 -> 30) rather than rejected, so it is validated by the caller after the
 * default has been applied.
 */
export function validateConsumerOptions(options: Partial<ConsumerOptions>): void {
  for (const [name, value] of Object.entries({
    batchSize: options.batchSize,
    visibilityTimeoutSec: options.visibilityTimeoutSec,
    pollingIntervalMs: options.pollingIntervalMs,
    extensionSec: options.autoExtension?.extensionSec,
    maxBatchSize: options.autoExtension?.maxBatchSize,
  })) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value > 2147483647))
      throw new ValidationError(`${name} must be a finite integer <= 2147483647`);
  }
  if (options.batchSize !== undefined && options.batchSize <= 0) {
    throw new ValidationError('batchSize must be positive');
  }
  if (options.visibilityTimeoutSec !== undefined && options.visibilityTimeoutSec < 0) {
    throw new ValidationError('visibilityTimeoutSec must be non-negative');
  }
  if (options.pollingIntervalMs !== undefined && options.pollingIntervalMs <= 0) {
    throw new ValidationError('pollingIntervalMs must be positive');
  }
  const ext = options.autoExtension;
  if (ext) {
    if (
      ext.extensionThreshold !== undefined &&
      (!Number.isFinite(ext.extensionThreshold) ||
        ext.extensionThreshold <= 0 ||
        ext.extensionThreshold >= 1)
    ) {
      throw new ValidationError('extensionThreshold must be in (0, 1)');
    }
    if (ext.extensionSec !== undefined && ext.extensionSec <= 0) {
      throw new ValidationError('extensionSec must be positive');
    }
    if (ext.maxBatchSize !== undefined && ext.maxBatchSize <= 0) {
      throw new ValidationError('autoExtension.maxBatchSize must be positive');
    }
  }
}

/** How soon an empty fetch is retried when get_next_visible_time reports a
 *  row that is already due (see scheduleNextFetch). */
const DUE_HINT_BACKOFF_MS = 100;

/**
 * Consumer class for consuming messages from a queue.
 *
 * - Implements an asynchronous iterator pattern for message consumption
 *   via `messages()`.
 * - Supports automatic visibility timeout extension (autoExtension) that
 *   extends in‑flight messages before they expire to prevent redelivery
 *   while processing.
 */
export class Consumer {
  /** Queue name */
  private readonly queueName: string;
  private generation?: string;

  /** Connection manager reference */
  private readonly connection: Connection;

  /** Consumer options */
  private readonly options: Required<ConsumerOptions>;

  /** Default options for the consumer */
  private static readonly DEFAULT_OPTIONS: Required<ConsumerOptions> = {
    batchSize: 10,
    visibilityTimeoutSec: 30,
    autoExtension: {
      enabled: true,
      extensionThreshold: 0.5,
      extensionSec: 30,
      maxBatchSize: 100,
    },
    pollingIntervalMs: 1000,
    topic: '',
  };

  /** Map of in-flight messages by ID */
  private inFlightMessages: Map<string, Message> = new Map();

  /** Buffer of fetched messages waiting to be consumed */
  private messageBuffer: Message[] = [];

  /** Flag indicating if the consumer is actively running */
  private running: boolean = false;
  private stopPromise: Promise<void> | null = null;
  private drained = createDeferred<void>();

  /** The reason this consumer closed: a QueueFatalError on a fatal teardown
   *  (the queue is gone), or undefined on a normal stop. Delivered to onClose
   *  listeners once stop() completes. */
  private fatalErr?: QueueFatalError;
  /** Registered onClose listeners, fired once when the consumer closes. */
  private closeListeners: Array<(err?: Error) => void> = [];
  /** True once the close outcome has been delivered to listeners. */
  private closeSignalled: boolean = false;

  /** Flag indicating if the consumer is actively fetching messages */
  private fetching: boolean = false;

  /** A NOTIFY arrived while a fetch was in flight. That fetch's snapshot may
   *  predate the event (e.g. the ack of a group's head, which is what makes
   *  its successor claimable), so one more fetch follows it. */
  private wokeDuringFetch: boolean = false;

  /** Promise that resolves when the currently in-flight fetch completes
   *  (success or error), or null when no fetch is running. Set
   *  synchronously at triggerFetch() — before any await — so stop() can
   *  reliably wait for messages a fetch already claimed server-side to
   *  reach the buffer/inFlight tracking, even if the SQL call takes
   *  longer than the old 1s spin cap. Without this, a fetch in progress
   *  during stop() leaks: consume_message has already moved rows to
   *  'processing', but the consumer is gone before the messages get
   *  released, so the rows sit until vt expires. */
  private fetchInFlight: Promise<void> | null = null;

  /** Notification unsubscribe function */
  private unsubscribe: (() => void) | null = null;

  /** Reference to the current iterator for signaling availability */
  private iteratorSignal: [Promise<void>, () => void] | null = null;

  /** Timer for the next scheduled fetch */
  private nextFetchTimer: NodeJS.Timeout | null = null;

  /** Flag indicating if the last fetch returned a full batch */
  private lastFetchWasFull: boolean = false;

  /** Threshold for triggering new fetches (1/3 of batchSize) */
  private readonly fetchThreshold: number;

  /**
   * Create a new Consumer
   * @param queueName - Name of the queue to consume from
   * @param connection - Connection manager instance
   * @param options - Consumer options
   */
  constructor(queueName: string, connection: Connection, options: Partial<ConsumerOptions> = {}) {
    this.queueName = queueName;
    this.connection = connection;

    // Reject invalid options up front (mirrors Go's validateConsumeOptions)
    // so misuse fails loudly here instead of as a silent hang / never-extend.
    validateConsumerOptions(options);

    // Treat an explicit visibilityTimeoutSec of 0 as "use the default"
    // (matches Go mapping 0 -> 30) rather than letting it override.
    const normalized: Partial<ConsumerOptions> = { ...options };
    if (normalized.visibilityTimeoutSec === 0) {
      delete normalized.visibilityTimeoutSec;
    }

    // Merge default options with provided options
    this.options = {
      ...Consumer.DEFAULT_OPTIONS,
      ...normalized,
      autoExtension: {
        ...Consumer.DEFAULT_OPTIONS.autoExtension,
        ...normalized.autoExtension,
      },
    };

    // Calculate the fetch threshold once (1/3 of batch size)
    this.fetchThreshold = Math.max(1, Math.floor(this.options.batchSize / 3));
  }

  /**
   * Start the consumer.
   * Called internally when messages() is called. resolveTopic is
   * synchronous (explicit option > cache > throw), so subscription and the
   * initial fetch happen on the same tick.
   */
  private start(): void {
    if (this.running || this.stopPromise) {
      return;
    }

    const topic = this.connection.resolveTopic(this.queueName, this.options.topic || undefined);

    this.running = true;
    this.unsubscribe = this.connection.subscribeForConsumer(
      this.queueName,
      topic,
      this.handleNotification.bind(this)
    );

    // Auto-extension is connection-level: in-flight messages are
    // registered with the connection-level extender actor as they are fetched
    // (addToQueue) and deregistered when they settle (handleMessageComplete).

    // Start with an initial fetch
    this.triggerFetch();
  }

  /**
   * Handle a notification event from the connection's per-topic or
   * per-queue NOTIFY listener. PostgreMQ NOTIFYs carry no payload — the
   * arrival itself is the signal to re-fetch.
   */
  private handleNotification(): void {
    if (!this.running) return;
    if (this.fetching) this.wokeDuringFetch = true;
    this.triggerFetch();
  }

  /** Register `message` with the connection-level extender. No-op if
   *  auto-extension is disabled. The extender holds an advisory cancel
   *  (message._cancel) for lease loss and updates message.vt on each
   *  extension via onExtended. */
  private addToQueue(message: Message): void {
    if (!this.options.autoExtension.enabled) return;
    this.connection.extenderRegister({
      queue: this.queueName,
      id: message.id,
      token: message.consumerToken,
      // Re-extend by the configured consume VT (matching Go). The extender's
      // schedule is derived from the returned vt, so lease and next-extension
      // time stay consistent even after a large VT.
      vtSec: this.options.visibilityTimeoutSec,
      threshold: this.options.autoExtension.extensionThreshold ?? 0.5,
      vt: message.vt,
      cancel: () => message._cancel(),
      onExtended: (vt: Date) => {
        message.vt = vt;
      },
    });
  }

  /** Deregister `messageId` from the connection-level extender. No-op if
   *  auto-extension is disabled. */
  private handleMessageComplete(message: Message): void {
    this.inFlightMessages.delete(message.consumerToken);
    this.connection.extenderDeregister(this.queueName, message.id, message.consumerToken);
    if (this.inFlightMessages.size === 0) this.drained[1]();
  }

  /**
   * Trigger a fetch operation based on the rules
   */
  private triggerFetch(): void {
    // Cancel any pending fetch timer
    if (this.nextFetchTimer) {
      clearTimeout(this.nextFetchTimer);
      this.nextFetchTimer = null;
    }

    // If already fetching or not running, don't start another fetch
    if (this.fetching || !this.running) {
      return;
    }

    // If buffer is full, don't fetch now (will trigger after consumption)
    if (this.messageBuffer.length >= this.options.batchSize) {
      return;
    }

    // Execute fetch with inline error handling. Capture the promise
    // synchronously so stop() can await it before tearing down — see the
    // fetchInFlight comment.
    this.fetchInFlight = (async () => {
      try {
        await this.fetchMessages();
      } catch (error) {
        console.error(`Error fetching messages: ${error}`);

        // On error, retry in one second
        if (this.running) {
          if (this.nextFetchTimer) clearTimeout(this.nextFetchTimer);
          this.nextFetchTimer = setTimeout(() => this.triggerFetch(), 1000);
        }
      } finally {
        this.fetchInFlight = null;
      }
    })();
  }

  /**
   * Fetch messages from the queue
   */
  private async fetchMessages(): Promise<void> {
    // If already fetching or not running, return
    if (this.fetching || !this.running) {
      return;
    }

    this.fetching = true;

    try {
      // Calculate how many messages to fetch
      const fetchCount = Math.min(
        this.options.batchSize - this.messageBuffer.length,
        this.options.batchSize
      );

      if (fetchCount <= 0) {
        this.fetching = false;
        return;
      }

      // Fetch messages. A PMQ02 here means the queue was deleted out-of-band:
      // it's fatal for this consumer (it can never get messages again). Escalate
      // to the connection's queueFatal — which tears this consumer (and any
      // siblings on the queue) down and fires the queue-fatal signal — instead
      // of letting the error fall through to the 1s fetch-retry loop forever.
      let rawMessages;
      try {
        this.generation ??= await this.connection.resolveQueueGeneration(this.queueName);
        if (!this.running) return;
        rawMessages = await this.connection.consumeMessages(
          this.queueName,
          this.options.visibilityTimeoutSec,
          fetchCount,
          this.generation
        );
      } catch (err) {
        if (err instanceof QueueNotFoundError) {
          this.connection.queueFatal(this.queueName, err, this.generation);
          return;
        }
        throw err;
      }

      // A fetch result belongs to the consumer lifetime that started it.
      // After stop it can only be released, never delivered or auto-extended.
      if (!this.running) {
        await Promise.allSettled(
          rawMessages.map((m) =>
            this.connection.releaseMessage(this.queueName, m.message_id, m.consumer_token)
          )
        );
        return;
      }
      // Process fetched messages
      for (const rawMessage of rawMessages) {
        // Create Message object
        const message = new Message(
          rawMessage.message_id,
          this.queueName,
          rawMessage.payload,
          rawMessage.consumer_token,
          rawMessage.delivery_attempts,
          new Date(rawMessage.vt),
          new Date(rawMessage.published_at),
          () => this.handleMessageComplete(message),
          {
            ack: this.connection.ackMessage.bind(this.connection),
            nack: this.connection.nackMessage.bind(this.connection),
            release: this.connection.releaseMessage.bind(this.connection),
            setVt: this.connection.setMessageVt.bind(this.connection),
          },
          rawMessage.group_key === null || rawMessage.group_key === undefined
            ? null
            : { key: rawMessage.group_key, seq: rawMessage.group_seq }
        );

        // Add to buffer and in-flight tracking
        this.messageBuffer.push(message);
        if (this.inFlightMessages.size === 0) this.drained = createDeferred<void>();
        this.inFlightMessages.set(message.consumerToken, message);
        this.addToQueue(message);
      }

      // Check if we received a full batch
      this.lastFetchWasFull = rawMessages.length === fetchCount;

      // Signal iterator if waiting
      if (this.iteratorSignal && this.messageBuffer.length > 0) {
        this.iteratorSignal[1]();
        this.iteratorSignal = null;
      }

      // If we got a partial batch, schedule next fetch based on visibility times
      if (rawMessages.length < fetchCount && this.running) {
        await this.scheduleNextFetch();
      }
    } finally {
      this.fetching = false;
      if (this.wokeDuringFetch) {
        this.wokeDuringFetch = false;
        if (this.running) {
          // After the current fetchInFlight settles (a timer, not a direct
          // call, so triggerFetch's own bookkeeping is not clobbered).
          if (this.nextFetchTimer) clearTimeout(this.nextFetchTimer);
          this.nextFetchTimer = setTimeout(() => this.triggerFetch(), 0);
        }
      }
    }
  }

  /**
   * Schedule the next fetch based on message visibility times
   */
  private async scheduleNextFetch(): Promise<void> {
    try {
      // Get the next visible time from the database
      const nextVisibleTime = await this.connection.getNextVisibleTime(this.queueName);

      if (!this.running) return;

      let waitTime = this.options.pollingIntervalMs;

      if (nextVisibleTime) {
        const now = new Date();
        const timeUntilNextVisible = Math.max(0, nextVisibleTime.getTime() - now.getTime());

        // Use the shorter of the two wait times
        waitTime = Math.min(timeUntilNextVisible, this.options.pollingIntervalMs);
        if (timeUntilNextVisible === 0) {
          // A visible-but-unclaimable row: locked by another transaction (a
          // claim in progress, or an ackWithTransaction whose transaction is
          // still open past the lease). Refetching at once would spin; back
          // off briefly. NOTIFYs still wake the consumer immediately.
          waitTime = Math.min(DUE_HINT_BACKOFF_MS, this.options.pollingIntervalMs);
        }
      }

      // Schedule the next fetch
      this.nextFetchTimer = setTimeout(() => this.triggerFetch(), waitTime);
    } catch (error) {
      if (!this.running) return;
      console.error(`Error scheduling next fetch: ${error}`);

      // On error, retry in one second
      if (this.running) {
        this.nextFetchTimer = setTimeout(() => this.triggerFetch(), 1000);
      }
    }
  }

  /**
   * Stop the consumer
   * @returns Promise that resolves when consumer is stopped
   */
  stop(): Promise<void> {
    if (!this.stopPromise) {
      this.running = false;
      this.stopPromise = this.drain();
    }
    return this.stopPromise;
  }

  private async drain(): Promise<void> {
    const deadline = Date.now() + this.connection.getShutdownTimeoutMs();
    if (this.nextFetchTimer) clearTimeout(this.nextFetchTimer);
    this.nextFetchTimer = null;
    this.unsubscribe?.();
    this.unsubscribe = null;
    this.iteratorSignal?.[1]();
    this.iteratorSignal = null;
    for (const message of this.inFlightMessages.values()) message._cancel();
    // Only buffered messages are known to be unattempted. A forced drain
    // abandons running leases without resetting delivery attempts.
    const buffered = this.messageBuffer.splice(0);
    const releases = Promise.allSettled(buffered.map((m) => m.release()));
    await untilDeadline(Promise.all([releases, this.fetchInFlight]), deadline);
    if (this.inFlightMessages.size) await untilDeadline(this.drained[0], deadline);
    for (const m of this.inFlightMessages.values()) {
      this.connection.extenderDeregister(this.queueName, m.id, m.consumerToken);
    }
    this.inFlightMessages.clear();
    this.connection.unregisterConsumer(this);
    this.signalClose();
  }

  /**
   * getQueueName reports the queue this consumer is bound to. Used by the
   * connection's queueFatal routing to find consumers on a gone queue.
   * @internal
   */
  getQueueGeneration(): string | undefined {
    return this.generation;
  }

  getQueueName(): string {
    return this.queueName;
  }

  /**
   * fatal tears the consumer down because its queue is gone. It records the
   * reason (delivered to onClose) and runs the normal stop() teardown
   * (handlers cancelled via their AbortSignal, in-flight deregistered from the
   * extender). Non-blocking and idempotent. @internal
   */
  fatal(err: QueueFatalError): void {
    if (!this.fatalErr) this.fatalErr = err;
    void this.stop();
  }

  /**
   * onClose registers a listener for when this consumer closes. It fires once
   * with a QueueFatalError if the consumer was torn down because its queue is
   * gone, or with no argument on a normal stop(). If the consumer has already
   * closed, the listener fires on the next microtask. Mirrors the RabbitMQ Go
   * client's Channel.NotifyClose.
   */
  onClose(listener: (err?: Error) => void): void {
    if (this.closeSignalled) {
      const err = this.fatalErr;
      queueMicrotask(() => listener(err));
      return;
    }
    this.closeListeners.push(listener);
  }

  /** Fire the close listeners once with the recorded outcome. */
  private signalClose(): void {
    if (this.closeSignalled) return;
    this.closeSignalled = true;
    const listeners = this.closeListeners;
    this.closeListeners = [];
    for (const listener of listeners) {
      try {
        listener(this.fatalErr);
      } catch (err) {
        console.error(`Consumer onClose listener threw for queue ${this.queueName}:`, err);
      }
    }
  }

  /**
   * Create an asynchronous iterator for consuming messages
   * @returns Asynchronous iterator that yields messages
   */
  messages(): AsyncIterableIterator<Message> {
    // Start the consumer if not already running
    this.start();

    // Create a message iterator
    const messageIterator: AsyncIterableIterator<Message> = {
      // Implementation of Symbol.asyncIterator
      [Symbol.asyncIterator]: () => messageIterator,

      // The next method for the iterator
      next: async (): Promise<IteratorResult<Message>> => {
        // If not running, return done
        if (!this.running) {
          return { done: true, value: undefined as any };
        }

        // If buffer has messages, return the next one
        if (this.messageBuffer.length > 0) {
          const message = this.messageBuffer.shift()!;

          // Only fetch more if buffer is below threshold AND last fetch was full
          const shouldFetch =
            this.messageBuffer.length < this.fetchThreshold && this.lastFetchWasFull;

          if (shouldFetch) {
            this.triggerFetch();
          }

          return {
            done: false,
            value: message,
          };
        }

        // No messages available, wait for more
        const [promise, resolve] = createDeferred<void>();
        this.iteratorSignal = [promise, resolve];

        // Trigger a fetch if not already fetching
        this.triggerFetch();

        // Wait for signal or shutdown
        await promise;

        // Check if we've been stopped while waiting
        if (!this.running) {
          return { done: true, value: undefined as any };
        }

        // If buffer now has messages, return the next one
        if (this.messageBuffer.length > 0) {
          const message = this.messageBuffer.shift()!;

          // Only fetch more if buffer is below threshold AND last fetch was full
          const shouldFetch =
            this.messageBuffer.length < this.fetchThreshold && this.lastFetchWasFull;

          if (shouldFetch) {
            this.triggerFetch();
          }

          return {
            done: false,
            value: message,
          };
        }

        // This shouldn't happen, but just in case
        return messageIterator.next();
      },

      // Called by the runtime when the consumer of `for await ... of` exits
      // early — break, return, throw, an outer-scope catch. Without this, the
      // auto-extension timer, LISTEN refcount, and any buffered/in-flight
      // messages would leak until the GC eventually finalized the iterator.
      return: async (): Promise<IteratorResult<Message>> => {
        await this.stop();
        return { done: true, value: undefined as any };
      },
    };

    return messageIterator;
  }
}
