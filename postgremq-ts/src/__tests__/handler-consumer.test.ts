/**
 * HandlerConsumer integration tests — push-based dispatch, settlement rules
 * (auto-ack on return, auto-nack on throw, explicit settle respected),
 * maxInFlight concurrency limiting, and clean shutdown.
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  generateTestPayload,
  sleep,
  waitFor,
} from './helpers';
import { Connection } from '../connection';
import { Message } from '../message';

describe('HandlerConsumer', () => {
  let testDb: TestDatabase;
  let connection: Connection;
  let dropDb: (() => Promise<void>) | null = null;

  beforeAll(async () => {
    testDb = await getSharedTestDatabase();
  });

  beforeEach(async () => {
    const iso = await createIsolatedTestConnection(testDb);
    connection = iso.connection;
    dropDb = iso.dropDatabase;
    await connection.createTopic('hc-topic');
    await connection.createQueue('hc-queue', 'hc-topic', false);
  });

  afterEach(async () => {
    if (connection) {
      await connection.close();
    }
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
  }, 20000);

  afterAll(async () => {
    if (testDb) {
      // shared container is stopped once on process exit (getSharedTestDatabase)
    }
  });

  test('dispatches each message to the handler and auto-acks on return', async () => {
    const total = 5;
    for (let i = 0; i < total; i++) {
      await connection.publish('hc-topic', generateTestPayload(i));
    }

    const seen: number[] = [];
    const hc = connection.consumeHandler('hc-queue', (msg: Message) => {
      seen.push(msg.payload.id);
      // No explicit ack — the handler returning should auto-ack.
    });

    await waitFor(() => seen.length === total, 10000);
    // All auto-acked -> queue drains to zero completed-or-removed pending.
    await waitFor(async () => {
      const stats = await connection.getQueueStatistics('hc-queue');
      return stats.completedCount === total && stats.pendingCount === 0;
    }, 10000);

    await hc.stop();
  });

  test('auto-nacks (redelivers) when the handler throws', async () => {
    await connection.publish('hc-topic', generateTestPayload(1));

    let attempts = 0;
    const hc = connection.consumeHandler('hc-queue', (msg: Message) => {
      attempts++;
      if (attempts === 1) {
        // First delivery throws -> auto-nack -> redelivery.
        throw new Error('boom');
      }
      // Second delivery returns -> auto-ack.
    });

    // The same message must be redelivered after the throw, then acked.
    await waitFor(() => attempts >= 2, 10000);
    await waitFor(async () => {
      const stats = await connection.getQueueStatistics('hc-queue');
      return stats.completedCount === 1;
    }, 10000);

    await hc.stop();
    expect(attempts).toBeGreaterThanOrEqual(2);
  });

  test('respects an explicit nack from the handler (no auto-ack)', async () => {
    await connection.publish('hc-topic', generateTestPayload(1));

    let calls = 0;
    const hc = connection.consumeHandler('hc-queue', async (msg: Message) => {
      calls++;
      if (calls === 1) {
        await msg.nack();   // explicit nack -> redelivery, no auto-ack
        return;
      }
      await msg.ack();
    });

    await waitFor(() => calls >= 2, 10000);
    await waitFor(async () => {
      const stats = await connection.getQueueStatistics('hc-queue');
      return stats.completedCount === 1;
    }, 10000);

    await hc.stop();
  });

  test('limits concurrency to maxInFlight', async () => {
    const total = 10;
    for (let i = 0; i < total; i++) {
      await connection.publish('hc-topic', generateTestPayload(i));
    }

    let active = 0;
    let peak = 0;
    let processed = 0;

    const hc = connection.consumeHandler(
      'hc-queue',
      async () => {
        active++;
        peak = Math.max(peak, active);
        await sleep(150); // hold the slot so concurrency can build up
        active--;
        processed++;
      },
      { maxInFlight: 3, batchSize: 10 }
    );

    await waitFor(() => processed === total, 15000);
    await hc.stop();

    // Never more than maxInFlight handlers at once; and we genuinely ran
    // concurrently (peak > 1) so the cap is meaningful, not just serial.
    expect(peak).toBeLessThanOrEqual(3);
    expect(peak).toBeGreaterThan(1);
  });

  test('runs handlers concurrently when maxInFlight is unlimited (default)', async () => {
    const total = 8;
    for (let i = 0; i < total; i++) {
      await connection.publish('hc-topic', generateTestPayload(i));
    }

    let active = 0;
    let peak = 0;
    let processed = 0;

    // No maxInFlight -> unlimited; all messages in a batch should run at once.
    const hc = connection.consumeHandler(
      'hc-queue',
      async () => {
        active++;
        peak = Math.max(peak, active);
        await sleep(150);
        active--;
        processed++;
      },
      { batchSize: 10 }
    );

    await waitFor(() => processed === total, 15000);
    await hc.stop();

    // Unlimited concurrency: peak should exceed any small cap (well past 3).
    expect(peak).toBeGreaterThan(3);
  });

  test('deliveryAttempts increments across an auto-nack redelivery', async () => {
    await connection.publish('hc-topic', generateTestPayload(1));

    const attemptsSeen: number[] = [];
    let calls = 0;
    const hc = connection.consumeHandler('hc-queue', (msg: Message) => {
      calls++;
      attemptsSeen.push(msg.deliveryAttempts);
      if (calls === 1) {
        throw new Error('boom'); // auto-nack -> redelivery
      }
      // second delivery returns -> auto-ack
    });

    await waitFor(() => calls >= 2, 10000);
    await hc.stop();

    // First delivery is attempt 1; the redelivery after auto-nack must be > 1,
    // proving the message was requeued for retry, not re-fetched fresh.
    expect(attemptsSeen[0]).toBe(1);
    expect(attemptsSeen[1]).toBeGreaterThan(1);
  });

  test('stop() is idempotent under concurrent and repeated calls', async () => {
    await connection.publish('hc-topic', generateTestPayload(1));

    let processed = 0;
    const hc = connection.consumeHandler('hc-queue', () => {
      processed++;
    });

    await waitFor(() => processed === 1, 10000);

    // Concurrent stops share one shutdown; a later stop is a no-op. None throw.
    await Promise.all([hc.stop(), hc.stop()]);
    await hc.stop();

    const stats = await connection.getQueueStatistics('hc-queue');
    expect(stats.completedCount).toBe(1);
  });

  test('stop() waits for in-flight handlers to finish', async () => {
    await connection.publish('hc-topic', generateTestPayload(1));

    let handlerFinished = false;
    let started = false;

    const hc = connection.consumeHandler('hc-queue', async (msg) => {
      started = true;
      await sleep(300);
      handlerFinished = true;
      await msg.ack(); // explicit success remains valid during cancellation
    });

    await waitFor(() => started, 10000);
    // stop() must not resolve until the in-flight handler completes.
    await hc.stop();
    expect(handlerFinished).toBe(true);

    const stats = await connection.getQueueStatistics('hc-queue');
    expect(stats.completedCount).toBe(1);
  });

  test('connection.close() stops a handler consumer', async () => {
    await connection.publish('hc-topic', generateTestPayload(1));

    let processed = 0;
    connection.consumeHandler('hc-queue', () => {
      processed++;
    });

    await waitFor(() => processed === 1, 10000);

    // close() must drive the handler consumer's shutdown without hanging.
    await connection.close();
    expect(processed).toBe(1);
  });

  test('handler receives an abort signal that fires on stop', async () => {
    await connection.publish('hc-topic', generateTestPayload(1));

    let abortedDuringHandler = false;
    let started = false;

    const hc = connection.consumeHandler('hc-queue', async (msg: Message) => {
      started = true;
      // Wait until the signal fires (stop cancels in-flight messages).
      while (!msg.signal.aborted) {
        await sleep(20);
      }
      abortedDuringHandler = true;
      await msg.ack();
    });

    await waitFor(() => started, 10000);
    await hc.stop();
    expect(abortedDuringHandler).toBe(true);
  });
});
