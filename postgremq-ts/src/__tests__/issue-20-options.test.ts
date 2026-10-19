import { describe, expect, test } from '@jest/globals';
import { PoolConfig } from 'pg';
import { Connection } from '../connection';
import { validateConsumerOptions } from '../consumer';
import { minDeadlineDelay } from '../utils';

describe('issue #20 option handling', () => {
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

  test('computes a deadline delay for a batch with 200,000 entries', () => {
    const deadline = Date.now() + 10_000;
    const deadlines = new Array<number>(200_000).fill(deadline);

    expect(minDeadlineDelay(deadlines, (entry) => entry, 1000)).toBe(1000);
  });
});
