/**
 * Unit tests for shouldRetry — the SQLSTATE / Node-error classifier used by
 * withRetry. Pre-fix the retryable list was just 40001/40P01; this test
 * pins the expanded coverage (class 08 + 57Pxx) so a regression can't
 * silently shrink it.
 *
 * Pure function under test — no database needed.
 */

import { describe, test, expect } from '@jest/globals';
import { shouldRetry } from '../utils';

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
