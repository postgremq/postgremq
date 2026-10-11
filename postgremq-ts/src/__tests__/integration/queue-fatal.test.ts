/**
 * Queue-fatal teardown tests (TS port of the Go queuefatal tests).
 *
 * When a queue a consumer depends on is gone — deleted out-of-band (consume
 * returns PMQ02) or an exclusive queue whose keep-alive permanently failed —
 * the consumer is torn down and the reason is surfaced via Consumer.onClose and
 * the connection-level 'queueFatal' event / onQueueFatal handler.
 */

import { describe, test, expect, beforeAll, beforeEach, afterEach } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  sleep,
} from '../helpers';
import { Connection } from '../../connection';
import { QueueFatalError } from '../../errors';

describe('Queue-fatal teardown', () => {
  let testDb: TestDatabase;
  let connection: Connection;
  let isoPool: import('pg').Pool | null = null;
  let dropDb: (() => Promise<void>) | null = null;

  beforeAll(async () => {
    testDb = await getSharedTestDatabase();
  });

  beforeEach(async () => {
    const iso = await createIsolatedTestConnection(testDb);
    connection = iso.connection;
    isoPool = iso.pool;
    dropDb = iso.dropDatabase;
  });

  afterEach(async () => {
    if (connection) await connection.close();
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
  });

  // Promise that resolves with the next 'queueFatal' event's (queue, error).
  function nextQueueFatal(): Promise<{ queue: string; error: unknown }> {
    return new Promise((resolve) => {
      connection.on('queueFatal', (queue: string, error: unknown) => resolve({ queue, error }));
    });
  }

  function withTimeout<T>(p: Promise<T>, ms: number, msg: string): Promise<T> {
    return Promise.race([
      p,
      sleep(ms).then(() => {
        throw new Error(msg);
      }) as Promise<T>,
    ]);
  }

  test('deleted queue tears down its consumer (onClose + queueFatal event)', async () => {
    await connection.createTopic('topic');
    await connection.createQueue('q', 'topic', false);

    const fatalEvent = nextQueueFatal();
    const consumer = connection.consume('q', { visibilityTimeoutSec: 30, pollingIntervalMs: 200 });
    const closeReason = new Promise<Error | undefined>((resolve) => consumer.onClose(resolve));

    // Drive the iterator so the consumer is actively polling.
    const iterated = (async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _msg of consumer.messages()) {
        // no messages expected
      }
    })();

    await sleep(300); // let it start polling
    await connection.deleteQueue('q');

    const err = await withTimeout(closeReason, 5000, 'onClose did not fire after queue deletion');
    expect(err).toBeInstanceOf(QueueFatalError);
    expect((err as QueueFatalError).queue).toBe('q');

    const ev = await withTimeout(fatalEvent, 2000, 'queueFatal event did not fire');
    expect(ev.queue).toBe('q');
    expect(ev.error).toBeInstanceOf(QueueFatalError);

    // The message iterator ended (consumer torn down).
    await withTimeout(iterated, 2000, 'message iterator did not end');
  });

  test('normal stop closes with no error', async () => {
    await connection.createTopic('topic');
    await connection.createQueue('q', 'topic', false);

    const consumer = connection.consume('q', { visibilityTimeoutSec: 30 });
    const closeReason = new Promise<Error | undefined>((resolve) => consumer.onClose(resolve));

    // Start then stop normally.
    const iterated = (async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _msg of consumer.messages()) {
        // none
      }
    })();
    await sleep(100);
    await consumer.stop();

    const err = await withTimeout(closeReason, 2000, 'onClose did not fire on stop');
    expect(err).toBeUndefined();
    await withTimeout(iterated, 2000, 'iterator did not end after stop');
  });

  test('all consumers on a deleted queue are torn down; one queueFatal event', async () => {
    await connection.createTopic('topic');
    await connection.createQueue('q', 'topic', false);

    let fatalCount = 0;
    connection.on('queueFatal', () => {
      fatalCount++;
    });

    const c1 = connection.consume('q', { visibilityTimeoutSec: 30, pollingIntervalMs: 200 });
    const c2 = connection.consume('q', { visibilityTimeoutSec: 30, pollingIntervalMs: 200 });
    const close1 = new Promise<Error | undefined>((resolve) => c1.onClose(resolve));
    const close2 = new Promise<Error | undefined>((resolve) => c2.onClose(resolve));

    const it1 = (async () => { for await (const _m of c1.messages()) { /* */ } })();
    const it2 = (async () => { for await (const _m of c2.messages()) { /* */ } })();

    await sleep(300);
    await connection.deleteQueue('q');

    const [e1, e2] = await withTimeout(Promise.all([close1, close2]), 5000, 'not all consumers torn down');
    expect(e1).toBeInstanceOf(QueueFatalError);
    expect(e2).toBeInstanceOf(QueueFatalError);

    await withTimeout(Promise.all([it1, it2]), 2000, 'iterators did not end');

    // Idempotent per queue: exactly one connection-level event.
    await sleep(300);
    expect(fatalCount).toBe(1);
  });

  test('producer-only exclusive queue surfaces queueFatal when keep-alive permanently fails', async () => {
    // No consumer — only the keep-alive actor. Force a permanent keep-alive
    // failure by deleting the queue ROW directly (bypassing deleteQueue, which
    // would deregister keep-alive); the next keep-alive flush omits it.
    const fatalEvent = nextQueueFatal();
    await connection.createTopic('topic');
    await connection.createQueue('exq', 'topic', true, { keepAliveInterval: 1 }); // flush ~500ms

    await isoPool!.query(`DELETE FROM postgremq.queues WHERE name = 'exq'`);

    const ev = await withTimeout(fatalEvent, 4000, 'producer-only exclusive queue did not surface queueFatal');
    expect(ev.queue).toBe('exq');
    expect(ev.error).toBeInstanceOf(QueueFatalError);
  });

  test('handler consumer learns of a deleted queue via onClose', async () => {
    await connection.createTopic('topic');
    await connection.createQueue('q', 'topic', false);

    const hc = connection.consumeHandler('q', async () => { /* no-op */ }, {
      visibilityTimeoutSec: 30,
      pollingIntervalMs: 200,
    });
    const closeReason = new Promise<Error | undefined>((resolve) => hc.onClose(resolve));

    await sleep(300);
    await connection.deleteQueue('q');

    const err = await withTimeout(closeReason, 5000, 'handler consumer onClose did not fire');
    expect(err).toBeInstanceOf(QueueFatalError);

    await hc.stop();
  });
});
