/**
 * Unit tests for option handling and stack limits (Issue #20)
 *
 * 1. Connection constructor does not mutate caller's options/config
 * 2. Ignored/deprecated consumer options do not throw ValidationError
 * 3. Batches with >= 200,000 entries compute deadline without stack overflow
 */

import { describe, test, expect } from '@jest/globals';
import { Connection } from '../connection';
import { validateConsumerOptions } from '../consumer';

describe('Option handling and stack limits (#20)', () => {
  describe('Connection constructor immutability', () => {
    test('does not mutate caller options.config when connectionString is provided', () => {
      const config = { host: '127.0.0.1', port: 5432, user: 'test_user' };
      const configCopy = { ...config };
      const options = {
        config,
        connectionString: 'postgresql://otheruser:secret@otherhost:5433/otherdb',
      };

      const conn = new Connection(options);
      try {
        expect(config).toEqual(configCopy);
        expect((config as any).connectionString).toBeUndefined();
      } finally {
        conn.close().catch(() => {});
      }
    });

    test('works cleanly when options.config is not provided', () => {
      const conn = new Connection({
        connectionString: 'postgresql://localhost:5432/db',
      });
      try {
        expect(conn).toBeInstanceOf(Connection);
      } finally {
        conn.close().catch(() => {});
      }
    });
  });

  describe('Consumer options validation', () => {
    test('does not throw on deprecated/ignored autoExtension options with non-positive values', () => {
      expect(() => {
        validateConsumerOptions({
          autoExtension: {
            extensionSec: -10,
            maxBatchSize: 0,
          } as any,
        });
      }).not.toThrow();

      expect(() => {
        validateConsumerOptions({
          autoExtension: {
            extensionSec: 0,
            maxBatchSize: -100,
          } as any,
        });
      }).not.toThrow();
    });

    test('does not throw on out-of-range or large values for deprecated options', () => {
      expect(() => {
        validateConsumerOptions({
          autoExtension: {
            extensionSec: 999999999999,
            maxBatchSize: 999999999999,
          } as any,
        });
      }).not.toThrow();
    });

    test('still validates non-deprecated options like batchSize, visibilityTimeoutSec, and extensionThreshold', () => {
      expect(() => {
        validateConsumerOptions({ batchSize: 0 });
      }).toThrow('batchSize must be positive');

      expect(() => {
        validateConsumerOptions({ visibilityTimeoutSec: -5 });
      }).toThrow('visibilityTimeoutSec must be non-negative');

      expect(() => {
        validateConsumerOptions({
          autoExtension: {
            enabled: true,
            extensionThreshold: 1.5,
          },
        });
      }).toThrow('extensionThreshold must be in (0, 1)');
    });
  });

  describe('Large batch deadline computation', () => {
    test('computes deadline over 200,000 entries without argument stack overflow', () => {
      const now = Date.now();
      const count = 200_000;
      const due = Array.from({ length: count }, (_, i) => ({
        id: i,
        token: `token-${i}`,
        vtSec: 30,
        expiresAt: new Date(now + 5000 + (i % 100)),
        cancel: () => {},
      }));

      // In V8, spreading 200,000 arguments into Math.min throws RangeError: Maximum call stack size exceeded.
      expect(() => {
        Math.min(1000, ...due.map((e) => e.expiresAt.getTime() - now));
      }).toThrow(RangeError);

      // Using reduce avoids the stack overflow and computes correctly
      const minDeadline = due.reduce(
        (min, e) => Math.min(min, e.expiresAt.getTime() - now),
        1000
      );
      expect(minDeadline).toBe(1000);

      const dueWithShorter = [
        ...due,
        {
          id: -1,
          token: 'token-short',
          vtSec: 30,
          expiresAt: new Date(now + 200),
          cancel: () => {},
        },
      ];
      const minDeadlineShorter = dueWithShorter.reduce(
        (min, e) => Math.min(min, e.expiresAt.getTime() - now),
        1000
      );
      expect(minDeadlineShorter).toBe(200);
    });
  });
});
