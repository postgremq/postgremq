import { describe, expect, test } from '@jest/globals';
import { PoolConfig } from 'pg';
import { Connection, heartbeatBudgetMs } from '../connection';
import { validateConsumerOptions } from '../consumer';

describe('TypeScript option handling', () => {
  test('does not mutate the supplied pool config when overriding its connection string', async () => {
    const config: PoolConfig = { application_name: 'postgremq-test' };
    const originalConfig = { ...config };
    const connection = new Connection({
      config,
      connectionString: 'postgres://example.invalid/postgremq',
    });

    try {
      expect(config).toEqual(originalConfig);
    } finally {
      await connection.close();
    }
  });

  test('does not validate deprecated auto-extension options that are ignored', () => {
    expect(() =>
      validateConsumerOptions({
        autoExtension: {
          enabled: true,
          extensionThreshold: 0.5,
          extensionSec: Number.NaN,
          maxBatchSize: -1,
        },
      })
    ).not.toThrow();
  });

  test('continues validating active consumer options', () => {
    expect(() => validateConsumerOptions({ batchSize: 0 })).toThrow('batchSize must be positive');
    expect(() =>
      validateConsumerOptions({
        autoExtension: { enabled: true, extensionThreshold: 1.5 },
      })
    ).toThrow('extensionThreshold must be in (0, 1)');
  });

  test('computes a heartbeat budget for a batch with 200,000 entries', () => {
    const deadline = new Date(Date.now() + 10_000);
    const deadlines = new Array<Date>(200_000).fill(deadline);

    expect(heartbeatBudgetMs(deadlines)).toBe(5000);
  });
});
