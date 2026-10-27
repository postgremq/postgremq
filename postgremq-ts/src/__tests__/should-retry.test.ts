/**
 * Unit tests for shouldRetry — the SQLSTATE / Node-error classifier used by
 * withRetry. Pre-fix the retryable list was just 40001/40P01; this test
 * pins the expanded coverage (class 08 + 57Pxx) so a regression can't
 * silently shrink it.
 *
 * Pure function under test — no database needed.
 */

import { describe, test, expect, jest } from '@jest/globals';
import { shouldRetry, withRetry } from '../utils';

describe('shouldRetry', () => {
  describe('Node-level network errors', () => {
    test.each([
      'ECONNRESET',
      'ETIMEDOUT',
      'EPIPE',
      'ECONNREFUSED',
      'ENOTFOUND',
      'EAI_AGAIN',
    ])('%s is retryable', (code) => {
      expect(shouldRetry({ code })).toBe(true);
    });

    test('unknown Node code is NOT retryable', () => {
      expect(shouldRetry({ code: 'EUNKNOWN' })).toBe(false);
    });
  });

  describe('Postgres SQLSTATE on err.code', () => {
    test.each([
      ['40001', 'serialization_failure'],
      ['40P01', 'deadlock_detected'],
      ['57P01', 'admin_shutdown'],
      ['57P02', 'crash_shutdown'],
      ['57P03', 'cannot_connect_now'],
      ['08000', 'connection_exception (class 08)'],
      ['08006', 'connection_failure'],
      ['08P01', 'protocol_violation'],
    ])('%s (%s) is retryable', (code) => {
      expect(shouldRetry({ code })).toBe(true);
    });

    test.each([
      ['PMQ01', 'lease_lost'],
      ['PMQ02', 'queue_not_found'],
      ['PMQ03', 'validation'],
      ['23505', 'unique_violation'],
      ['42P01', 'undefined_table'],
    ])('%s (%s) is NOT retryable', (code) => {
      expect(shouldRetry({ code })).toBe(false);
    });
  });

  describe('Postgres SQLSTATE on err.sqlState', () => {
    // Some node-postgres versions/error types put the SQLSTATE on
    // `sqlState` instead of `code` — both must work.
    test('40001 on sqlState is retryable', () => {
      expect(shouldRetry({ sqlState: '40001' })).toBe(true);
    });
    test('08006 on sqlState is retryable', () => {
      expect(shouldRetry({ sqlState: '08006' })).toBe(true);
    });
    test('57P01 on sqlState is retryable', () => {
      expect(shouldRetry({ sqlState: '57P01' })).toBe(true);
    });
    test('PMQ01 on sqlState is NOT retryable', () => {
      expect(shouldRetry({ sqlState: 'PMQ01' })).toBe(false);
    });
  });

  describe('non-error inputs', () => {
    test('plain string error is not retryable', () => {
      expect(shouldRetry({ message: 'something went wrong' })).toBe(false);
    });
    test('null is not retryable', () => {
      expect(shouldRetry(null)).toBe(false);
    });
    test('undefined is not retryable', () => {
      expect(shouldRetry(undefined)).toBe(false);
    });
    test('error with non-string code is not retryable', () => {
      expect(shouldRetry({ code: 12345 })).toBe(false);
    });
  });
});

// The bound used by the background keep-alive and renewal calls: retries stop
// at the deadline, or when the signal aborts a backoff, with the last error.
describe('withRetry bound', () => {
  const transient = Object.assign(new Error('serialization failure'), { code: '40001' });
  const policy = { maxAttempts: 10, initialBackoffMs: 100, backoffMultiplier: 1, maxBackoffMs: 100 };

  // Fake timers: the clock moves only when the test advances it.
  test('no attempt starts at or after the deadline', async () => {
    jest.useFakeTimers();
    try {
      let attempts = 0;
      const result = withRetry(
        async () => {
          attempts++;
          throw transient;
        },
        policy,
        shouldRetry,
        { deadline: Date.now() + 250 }
      ).catch((e) => e);
      // Attempts at 0, 100 and 200 ms; a backoff to 300 ms would end past it.
      await jest.advanceTimersByTimeAsync(1000);
      expect(await result).toBe(transient);
      expect(attempts).toBe(3);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a backoff that wakes past the deadline starts no attempt', async () => {
    jest.useFakeTimers();
    try {
      let attempts = 0;
      const result = withRetry(
        async () => {
          attempts++;
          throw transient;
        },
        policy,
        shouldRetry,
        { deadline: Date.now() + 250 }
      ).catch((e) => e);
      // The first backoff (to 100 ms) is pending. The event loop stalls: the
      // clock jumps 200 ms without firing it, so it wakes at 300 ms.
      await jest.advanceTimersByTimeAsync(0);
      jest.setSystemTime(Date.now() + 200);
      await jest.advanceTimersByTimeAsync(1000);
      expect(await result).toBe(transient);
      expect(attempts).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('an abort ends the backoff with the last error', async () => {
    let attempts = 0;
    const stop = new AbortController();
    const started = Date.now();
    setTimeout(() => stop.abort(), 50);
    await expect(
      withRetry(
        async () => {
          attempts++;
          throw transient;
        },
        { ...policy, initialBackoffMs: 10000, maxBackoffMs: 10000 },
        shouldRetry,
        { signal: stop.signal }
      )
    ).rejects.toBe(transient);
    expect(attempts).toBe(1);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  test('without a bound, every attempt of the policy runs', async () => {
    let attempts = 0;
    await expect(
      withRetry(
        async () => {
          attempts++;
          throw transient;
        },
        { ...policy, maxAttempts: 3, initialBackoffMs: 1, maxBackoffMs: 1 }
      )
    ).rejects.toBe(transient);
    expect(attempts).toBe(3);
  });
});
