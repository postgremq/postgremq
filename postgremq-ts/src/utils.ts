/**
 * PostgreMQ TypeScript Client
 * Utility functions
 */

import { RetryPolicy } from './types';
import { PoolConfig, Pool } from 'pg';

// Define transient error codes for Node errors
const transientNodeErrorCodes = new Set([
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ECONNREFUSED',
  'ENOTFOUND',
  'EAI_AGAIN',
]);

// Transient SQLSTATE codes for PostgreSQL errors that should trigger a retry:
//   - 40001 (serialization_failure), 40P01 (deadlock_detected): transaction
//     conflicts that may resolve on retry.
//   - 57P01 (admin_shutdown), 57P02 (crash_shutdown), 57P03
//     (cannot_connect_now): server-side restart / failover; retries succeed
//     once the server is available.
// Class 08 (connection_exception) codes are matched by prefix below.
const transientSQLStateCodes = new Set(['40001', '40P01', '55P03', '57P01', '57P02', '57P03']);

function isTransientSQLState(code: unknown): boolean {
  if (typeof code !== 'string') return false;
  return transientSQLStateCodes.has(code) || code.startsWith('08');
}

/**
 * Determines if an error should trigger a retry.
 * @param {Error} error - The error object returned by node-postgres.
 * @returns {boolean} - True if the error is transient and can be retried.
 */
export function shouldRetry(error: any): boolean {
  // Network-level error from Node (no SQLSTATE).
  if (error?.code && transientNodeErrorCodes.has(error.code)) {
    return true;
  }

  // PostgreSQL errors include a SQLSTATE code. Depending on node-postgres
  // version and the error class, it may be on `error.code` or
  // `error.sqlState`. Try both.
  if (isTransientSQLState(error?.sqlState)) {
    return true;
  }
  if (isTransientSQLState(error?.code)) {
    return true;
  }

  return false;
}

/**
 * Implements an exponential backoff retry mechanism for database operations.
 * @param operation - The async function to retry
 * @param retryPolicy - The retry policy configuration
 * @returns The result of the operation if successful
 * @throws The last error encountered if all retries fail
 */
export async function withRetry<T>(
  operation: () => Promise<T>,
  retryPolicy: RetryPolicy,
  retryable: (error: any) => boolean = shouldRetry
): Promise<T> {
  let lastError: Error | null = null;
  let attempt = 0;
  let backoffMs = retryPolicy.initialBackoffMs;

  while (attempt < retryPolicy.maxAttempts) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;

      // Only retry if the error is transient
      if (!retryable(error)) {
        throw error;
      }

      attempt++;

      // If we've reached maximum attempts, throw the last error
      if (attempt >= retryPolicy.maxAttempts) {
        break;
      }

      // Calculate backoff time for next attempt
      await sleep(backoffMs);

      // Increase backoff for next attempt, up to the maximum
      backoffMs = Math.min(backoffMs * retryPolicy.backoffMultiplier, retryPolicy.maxBackoffMs);
    }
  }

  // If we've exhausted all retries, throw the last error
  throw lastError;
}

/**
 * Sleep for a specified duration
 * @param ms - Time in milliseconds to sleep
 * @returns A promise that resolves after the specified delay
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Default retry policy settings
 */
export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 5,
  initialBackoffMs: 100,
  backoffMultiplier: 2,
  maxBackoffMs: 5000,
};

/**
 * Create a deferred promise that can be resolved/rejected from outside
 * @returns A tuple with the promise and functions to resolve/reject it
 */
export function createDeferred<T>(): [Promise<T>, (value: T) => void, (reason?: any) => void] {
  let resolve!: (value: T) => void;
  let reject!: (reason?: any) => void;

  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return [promise, resolve, reject];
}

/**
 * Event emitter class for internal event handling
 */
export class EventEmitter {
  private listeners: Map<string, Set<(...args: any[]) => void>> = new Map();

  /**
   * Register an event listener
   * @param event - The event name
   * @param listener - The callback function
   */
  on(event: string, listener: (...args: any[]) => void): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(listener);
  }

  /**
   * Remove an event listener
   * @param event - The event name
   * @param listener - The callback function to remove
   */
  off(event: string, listener: (...args: any[]) => void): void {
    const listeners = this.listeners.get(event);
    if (listeners) {
      listeners.delete(listener);
      if (listeners.size === 0) {
        this.listeners.delete(event);
      }
    }
  }

  /**
   * Emit an event with arguments
   * @param event - The event name
   * @param args - Arguments to pass to listeners
   */
  emit(event: string, ...args: any[]): void {
    const listeners = this.listeners.get(event);
    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(...args);
        } catch (error) {
          console.error(`Error in event listener for ${event}:`, error);
        }
      }
    }
  }

  /**
   * Remove all listeners for an event
   * @param event - The event name (optional - if omitted, removes all listeners)
   */
  removeAllListeners(event?: string): void {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
  }
}

export interface ConnectionOptions {
  // PostgreSQL connection string (overrides config if provided)
  connectionString?: string;
  // Connection pool configuration (creates new pool)
  config?: PoolConfig;
  // Existing pool instance (reuses existing pool)
  pool?: Pool;
  // Client-specific options
  shutdownTimeoutMs?: number;
  retry?: RetryPolicy;
}
/** IDs remain numbers in this API; reject lossy BIGINT conversions. */
export function messageId(value: unknown): number {
  const id = Number(value);
  if (!Number.isSafeInteger(id) || id < 1)
    throw new RangeError('message ID must be a positive safe integer');
  return id;
}

/** Converts a nullable BIGINT group sequence (a string from node-pg) to a
 *  number, or null for an ungrouped message. */
export function groupSeq(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const seq = Number(value);
  if (!Number.isSafeInteger(seq) || seq < 1)
    throw new RangeError('group sequence must be a positive safe integer');
  return seq;
}

/** One deadline shared by all phases of a drain. The losing work is observed. */
export async function untilDeadline(work: Promise<unknown>, deadline: number): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      work,
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, Math.max(0, deadline - Date.now()));
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
