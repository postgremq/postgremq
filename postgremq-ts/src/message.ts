import { messageId } from './utils';
/**
 * PostgreMQ TypeScript Client
 * Message class implementation
 */

import { MessageOptions, Transaction } from './types';
import { LeaseLostError, QueueNotFoundError, ValidationError } from './errors';

/**
 * Message class representing a message retrieved from a queue.
 * Provides methods for acknowledging, negatively acknowledging, or
 * releasing the message, as well as extending its visibility timeout.
 */
export class Message {
  /** Message ID */
  public readonly id: number;

  /** Queue name */
  public readonly queueName: string;

  /** Message payload */
  public readonly payload: any;

  /** Consumer token for this message */
  public readonly consumerToken: string;

  /** Number of delivery attempts */
  public readonly deliveryAttempts: number;

  /** Published timestamp */
  public readonly publishedAt: Date;

  /** Message group (see PublishOptions.groupKey); null if ungrouped */
  public readonly groupKey: string | null;

  /** Dense 1-based position in the group, in publish commit order; null if ungrouped */
  public readonly groupSeq: number | null;

  /** Visibility timeout expiration */
  public vt: Date;

  /** Internal flag to track if message has been processed */
  private processed: boolean = false;
  private terminalStarted = false;

  /** Internal flag to track if onComplete has been called */
  private completeCalled: boolean = false;

  /** AbortController used by the Consumer to signal that this message
   *  can no longer be acked successfully — the lease was lost server-side
   *  (auto-extension found the row missing from set_vt_batch_multi's result, or
   *  set_vt raised PMQ01). Handlers can check `message.signal.aborted` /
   *  listen for 'abort' to short-circuit work that would otherwise be
   *  wasted (and may commit non-idempotent side-effects). Mirrors the Go
   *  client's `msg.StoppedCtx`. */
  private readonly _abortController: AbortController = new AbortController();

  /** Callback to invoke when message is completed */
  private onComplete: (messageId: number) => void;

  /** Database operations */
  private dbOperations: {
    ack: (
      queueName: string,
      messageId: number,
      consumerToken: string,
      tx?: Transaction
    ) => Promise<void>;
    nack: (
      queueName: string,
      messageId: number,
      consumerToken: string,
      delayUntil?: Date
    ) => Promise<void>;
    release: (queueName: string, messageId: number, consumerToken: string) => Promise<void>;
    setVt: (
      queueName: string,
      messageId: number,
      consumerToken: string,
      vt: number
    ) => Promise<Date>;
  };

  /**
   * Create a new Message
   * @param id - Message ID
   * @param queueName - Queue name
   * @param payload - Message payload
   * @param consumerToken - Consumer token
   * @param deliveryAttempts - Number of delivery attempts
   * @param vt - Visibility timeout expiration
   * @param publishedAt - Published timestamp
   * @param onComplete - Callback when message is completed
   * @param dbOperations - Database operations for this message
   * @param group - Message group key and sequence; null if ungrouped
   */
  constructor(
    id: number,
    queueName: string,
    payload: any,
    consumerToken: string,
    deliveryAttempts: number,
    vt: Date,
    publishedAt: Date,
    onComplete: (messageId: number) => void,
    dbOperations: {
      ack: (
        queueName: string,
        messageId: number,
        consumerToken: string,
        tx?: Transaction
      ) => Promise<void>;
      nack: (
        queueName: string,
        messageId: number,
        consumerToken: string,
        delayUntil?: Date
      ) => Promise<void>;
      release: (queueName: string, messageId: number, consumerToken: string) => Promise<void>;
      setVt: (
        queueName: string,
        messageId: number,
        consumerToken: string,
        vt: number
      ) => Promise<Date>;
    },
    group: { key: string; seq: number } | null = null
  ) {
    this.id = messageId(id);
    this.groupKey = group?.key ?? null;
    this.groupSeq = group?.seq ?? null;
    this.queueName = queueName;
    this.payload = payload;
    this.consumerToken = consumerToken;
    this.deliveryAttempts = deliveryAttempts;
    this.vt = vt;
    this.publishedAt = publishedAt;
    this.onComplete = onComplete;
    this.dbOperations = dbOperations;
  }

  /**
   * AbortSignal that fires when the message's lease is lost — auto-extension
   * could not extend the visibility timeout server-side, so any further ack /
   * nack / setVt call from this consumer will fail with LeaseLostError.
   * Handlers should check `message.signal.aborted` (or listen for 'abort')
   * and short-circuit work that would otherwise be wasted or commit
   * non-idempotent side effects after the lease has already been lost.
   *
   *   for await (const msg of consumer) {
   *     for (let i = 0; i < workItems.length; i++) {
   *       if (msg.signal.aborted) break;
   *       await doWork(workItems[i]);
   *     }
   *     if (!msg.signal.aborted) await msg.ack();
   *   }
   *
   * Mirrors the Go client's `msg.StoppedCtx`.
   */
  get signal(): AbortSignal {
    return this._abortController.signal;
  }

  /**
   * True once a terminal operation (ack / nack / release) has been attempted
   * on this message, whether or not it succeeded. HandlerConsumer uses this
   * to decide whether to auto-ack a message the handler returned without
   * settling. Mirrors the "completed" tracking in the Go HandlerConsumer.
   */
  get isSettled(): boolean {
    return this.terminalStarted;
  }

  /**
   * Cancel this message's signal — invoked by the Consumer when it
   * determines the lease can no longer be extended.
   * @internal — for Consumer use only.
   */
  _cancel(): void {
    if (!this._abortController.signal.aborted) {
      this._abortController.abort();
    }
  }

  /**
   * Acknowledge the message as successfully processed.
   * This removes the message from the queue.
   *
   * @returns Promise that resolves when acknowledgment is complete
   * @throws Error if message has already been processed or if acknowledgment fails
   */
  async ack(): Promise<void> {
    this.checkAlreadyProcessed();
    this.terminalStarted = true;

    try {
      await this.dbOperations.ack(this.queueName, this.id, this.consumerToken);
      // Only mark as processed if the operation succeeded
      this.processed = true;
    } catch (error) {
      throw this.wrapError('Failed to acknowledge message', error);
    } finally {
      // Always call onComplete to remove from tracking, even if operation failed
      // Like Go's sync.Once, this ensures the callback runs only once
      this.callOnCompleteOnce();
    }
  }

  /**
   * Acknowledge the message within an existing transaction.
   *
   * @param tx - The transaction object
   * @returns Promise that resolves when acknowledgment is complete
   * @throws Error if message has already been processed or if acknowledgment fails
   */
  async ackWithTransaction(tx: Transaction): Promise<void> {
    this.checkAlreadyProcessed();
    this.terminalStarted = true;

    try {
      await this.dbOperations.ack(this.queueName, this.id, this.consumerToken, tx);
      // Only mark as processed if the operation succeeded
      this.processed = true;
    } catch (error) {
      throw this.wrapError('Failed to acknowledge message with transaction', error);
    } finally {
      // Always call onComplete to remove from tracking, even if operation failed
      this.callOnCompleteOnce();
    }
  }

  /**
   * Negatively acknowledge the message, returning it to the queue
   * for reprocessing after an optional delay.
   *
   * @param options - Options for negative acknowledgment
   * @returns Promise that resolves when negative acknowledgment is complete
   * @throws Error if message has already been processed or if nack fails
   */
  async nack(options?: MessageOptions): Promise<void> {
    this.checkAlreadyProcessed();
    this.terminalStarted = true;

    try {
      const delayUntil = options?.delaySeconds
        ? new Date(Date.now() + options.delaySeconds * 1000)
        : undefined;

      await this.dbOperations.nack(this.queueName, this.id, this.consumerToken, delayUntil);
      // Only mark as processed if the operation succeeded
      this.processed = true;
    } catch (error) {
      throw this.wrapError('Failed to negatively acknowledge message', error);
    } finally {
      // Always call onComplete to remove from tracking, even if operation failed
      this.callOnCompleteOnce();
    }
  }

  /**
   * Release the message back to the queue without incrementing the
   * delivery attempt counter. Use this when you haven't actually
   * attempted to process the message.
   *
   * @returns Promise that resolves when release is complete
   * @throws Error if message has already been processed or if release fails
   */
  async release(): Promise<void> {
    this.checkAlreadyProcessed();
    this.terminalStarted = true;

    try {
      await this.dbOperations.release(this.queueName, this.id, this.consumerToken);
      // Mark as processed to prevent multiple releases
      this.processed = true;
    } catch (error) {
      throw this.wrapError('Failed to release message', error);
    } finally {
      // Always call onComplete to remove from tracking, even if operation failed
      this.callOnCompleteOnce();
    }
  }

  /**
   * Extend the visibility timeout for this message.
   *
   * @param newVt - New visibility timeout in seconds
   * @returns Promise that resolves to the new expiration date
   * @throws Error if message has already been processed or if extension fails
   */
  async setVt(newVt: number): Promise<Date> {
    this.checkAlreadyProcessed();

    try {
      const newExpiration = await this.dbOperations.setVt(
        this.queueName,
        this.id,
        this.consumerToken,
        newVt
      );
      this.vt = newExpiration;
      return newExpiration;
    } catch (error) {
      if (error instanceof LeaseLostError) this._cancel();
      throw this.wrapError('Failed to extend visibility timeout', error);
    }
  }

  /**
   * Check if the message has already been processed
   * @throws Error if message has already been processed
   */
  private checkAlreadyProcessed(): void {
    if (this.terminalStarted) {
      throw new Error(`Message ${this.id} has already been processed`);
    }
  }

  /**
   * Call the onComplete callback exactly once.
   * Like Go's sync.Once, this ensures the callback runs only once
   * even if called multiple times.
   */
  private callOnCompleteOnce(): void {
    if (!this.completeCalled) {
      this.completeCalled = true;
      this.onComplete(this.id);
    }
  }

  /**
   * Wrap an error with additional context
   * @param message - Error message prefix
   * @param error - Original error
   * @returns Wrapped error
   */
  private wrapError(message: string, error: any): Error {
    // Preserve typed PostgreMQ errors (LeaseLostError / QueueNotFoundError /
    // ValidationError) so callers can `instanceof`-match. Wrapping these in
    // a generic Error would erase the type — and the `code` field — that the
    // application relies on to distinguish failure modes.
    if (
      error instanceof LeaseLostError ||
      error instanceof QueueNotFoundError ||
      error instanceof ValidationError
    ) {
      return error;
    }
    const wrappedError = new Error(`${message}: ${error.message}`);
    wrappedError.stack = error.stack;
    return wrappedError;
  }
}
