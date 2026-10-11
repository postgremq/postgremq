/**
 * Tests for the per-topic / per-queue NOTIFY refactor.
 */

import { describe, test, expect, beforeAll, afterAll, jest } from '@jest/globals';
import { TestDatabase, createIsolatedTestConnection, getSharedTestDatabase, sleep, waitFor } from './helpers';
import { Connection } from '../connection';

// Test-only peek into the connection's per-channel subscription map.
// connection.ts intentionally does not expose this on the public surface;
// tests poke the private field via a typed cast.
type ChannelState = { refCount: number; listening: boolean };
function channelState(conn: Connection, channel: string): ChannelState | undefined {
  return ((conn as any).channelStates as Map<string, ChannelState>).get(channel);
}

describe('Per-topic NOTIFY', () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await getSharedTestDatabase();
  });

  afterAll(async () => {
    if (db) await db.stop();
  });

  test('cache hit: createQueue then consume needs no DB lookup', async () => {
    const { connection, dropDatabase, pool } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('TopicCacheHit');
      await connection.createQueue('QueueCacheHit', 'TopicCacheHit', false, { maxDeliveryAttempts: 0 });

      // Drop the queue at the SQL level so a DB lookup would fail. The
      // cache entry from createQueue lets consume() succeed regardless.
      await pool.query("DELETE FROM postgremq.queues WHERE name = 'QueueCacheHit'");

      const consumer = connection.consume('QueueCacheHit', { visibilityTimeoutSec: 30 });
      // Force the iterator to start so the topic resolution and subscribe
      // run; iterator should not throw.
      const it = consumer.messages();
      // Trigger the lazy start, then immediately stop.
      await Promise.race([it.next(), sleep(200)]);
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  test('out-of-band queue requires explicit topic option (no DB lookup)', async () => {
    const { connection, dropDatabase, pool } = await createIsolatedTestConnection(db);
    try {
      // Queue created out of band: not via this Connection's createQueue, so
      // the topic cache is empty for this queue.
      await pool.query("SELECT postgremq.create_topic('TopicOutOfBand')");
      await pool.query("SELECT postgremq.create_queue('QueueOutOfBand', 'TopicOutOfBand', 0, false, interval '30 seconds')");

      // Without explicit topic, starting iteration must throw — no DB fallback.
      const noTopicConsumer = connection.consume('QueueOutOfBand', { visibilityTimeoutSec: 30 });
      expect(() => noTopicConsumer.messages()).toThrow();

      // With explicit topic, it works.
      const consumer = connection.consume('QueueOutOfBand', {
        visibilityTimeoutSec: 30,
        topic: 'TopicOutOfBand',
      });
      const it = consumer.messages();
      await Promise.race([it.next(), sleep(200)]);
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  test('explicit topic option bypasses cache and DB lookup', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      const consumer = connection.consume('GhostQueue', {
        visibilityTimeoutSec: 30,
        topic: 'ExplicitTopic'
      });
      const it = consumer.messages();
      await Promise.race([it.next(), sleep(200)]);
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  test('refcount sharing: two consumers on same topic share one physical LISTEN', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('RefcountTopic');
      await connection.createQueue('RefA', 'RefcountTopic', false, { maxDeliveryAttempts: 0 });
      await connection.createQueue('RefB', 'RefcountTopic', false, { maxDeliveryAttempts: 0 });

      const c1 = connection.consume('RefA', { visibilityTimeoutSec: 30 });
      const c2 = connection.consume('RefB', { visibilityTimeoutSec: 30 });
      const it1 = c1.messages();
      const it2 = c2.messages();
      // Kick off start; iterators settle without messages.
      await Promise.race([it1.next(), sleep(50)]);
      await Promise.race([it2.next(), sleep(50)]);

      // Wait for both subscriptions to register.
      await waitFor(() => channelState(connection, 'pmq:t:RefcountTopic')?.refCount === 2, 2000, 25);

      const topicState = channelState(connection, 'pmq:t:RefcountTopic');
      expect(topicState).toBeDefined();
      expect(topicState!.refCount).toBe(2);

      expect(channelState(connection, 'pmq:q:RefA')!.refCount).toBe(1);
      expect(channelState(connection, 'pmq:q:RefB')!.refCount).toBe(1);

      await c1.stop();
      await waitFor(() => channelState(connection, 'pmq:t:RefcountTopic')?.refCount === 1, 2000, 25);

      await c2.stop();
      await waitFor(() => channelState(connection, 'pmq:t:RefcountTopic') === undefined, 2000, 25);
    } finally {
      await dropDatabase();
    }
  });

  test('reconnect re-issues LISTEN for all subscribed channels', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('ReconnectTopic');
      await connection.createQueue('ReconnectQueue', 'ReconnectTopic', false, { maxDeliveryAttempts: 0 });

      const consumer = connection.consume('ReconnectQueue', { visibilityTimeoutSec: 30, batchSize: 1 });
      const it = consumer.messages();
      // Settle subscription.
      await sleep(200);

      // Confirm the subscriptions are flagged listening.
      expect(channelState(connection, 'pmq:t:ReconnectTopic')!.listening).toBe(true);
      expect(channelState(connection, 'pmq:q:ReconnectQueue')!.listening).toBe(true);

      // Simulate notify client failure: emit an error event on the notify
      // client. This calls handleNotifyClientError which releases the client
      // and schedules a reconnect 1s later.
      const notifyClient = (connection as any).notifyClient;
      expect(notifyClient).not.toBeNull();
      notifyClient.emit('error', new Error('simulated disconnect'));

      // Wait for reconnect (handleNotifyClientError schedules reconnect after
      // 1000ms). Allow a generous margin.
      await waitFor(() => (connection as any).notifyClient !== notifyClient && channelState(connection, 'pmq:t:ReconnectTopic')?.listening === true && channelState(connection, 'pmq:q:ReconnectQueue')?.listening === true, 3000, 50);

      expect(channelState(connection, 'pmq:t:ReconnectTopic')!.listening).toBe(true);
      expect(channelState(connection, 'pmq:q:ReconnectQueue')!.listening).toBe(true);

      // End-to-end: publish should wake the consumer through the new client.
      await connection.publish('ReconnectTopic', { hello: 'reconnect' });
      const result = await Promise.race([
        it.next(),
        sleep(2000).then(() => ({ done: true, value: undefined as any })),
      ]);
      expect((result as IteratorResult<any>).done).toBe(false);
      const msg = (result as IteratorResult<any>).value as any;
      await msg.ack();
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  test('publish wakes consumer end-to-end via per-topic NOTIFY', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('WakeTopic');
      await connection.createQueue('WakeQueue', 'WakeTopic', false, { maxDeliveryAttempts: 0 });

      const consumer = connection.consume('WakeQueue', { visibilityTimeoutSec: 30, batchSize: 1 });
      const it = consumer.messages();

      // Settle subscription, then publish; iterator should yield the message.
      await sleep(100);
      await connection.publish('WakeTopic', { hello: 'world' });

      const result = await Promise.race([
        it.next(),
        sleep(2000).then(() => ({ done: true, value: undefined as any })),
      ]);
      expect((result as IteratorResult<any>).done).toBe(false);
      const msg = (result as IteratorResult<any>).value as any;
      expect(msg).toBeDefined();
      await msg.ack();
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  /**
   * Regression: a fast subscribe-then-stop sequence
   * could leak the notify-listener pg client. Mechanism pre-fix:
   *   1. consume() kicks off startNotificationListener (fire-and-forget).
   *   2. The IIFE awaits pool.connect() — slow, gives the test window.
   *   3. consumer.stop() runs, unregisterConsumer fires, consumers.size
   *      drops to 0, stopNotificationListener runs and finds notifyClient
   *      still null, returns.
   *   4. pool.connect() resolves; the IIFE assigns notifyClient, installs
   *      handlers, issues LISTENs against zero subscribers — and never
   *      releases the client.
   *
   * Post-fix: the IIFE re-checks consumers.size after the await and
   * releases the client if nobody owns it; stopNotificationListener
   * also awaits the in-flight notifyStartupPromise.
   */
  test('subscribe-then-stop does not leak the notify client', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('LeakTopic');
      await connection.createQueue('LeakQueue', 'LeakTopic', false);

      // Track every notify-listener client acquired and released. The
      // spy adds a delay only to the FIRST pool.connect — the notify
      // listener's — so the consumer's fetch (subsequent calls) doesn't
      // also get artificially slow and complicate the timing.
      const pool = (connection as any).pool;
      const realConnect = pool.connect.bind(pool);
      const acquired: any[] = [];
      const released: any[] = [];
      let firstCall = true;
      pool.connect = jest.fn(async () => {
        const isFirst = firstCall;
        firstCall = false;
        if (isFirst) await sleep(300); // gives stop() time to drop consumers.size to 0
        const client = await realConnect();
        if (isFirst) {
          const realRelease = client.release.bind(client);
          client.release = (...args: any[]) => {
            released.push(client);
            return realRelease(...args);
          };
          acquired.push(client);
        }
        return client;
      });

      try {
        const consumer = connection.consume('LeakQueue', { visibilityTimeoutSec: 30 });
        // Force consumer.start() to run by calling messages() — without
        // it, consumer.stop() would early-return on `if (!this.running)`
        // and never reach unregisterConsumer.
        const it = consumer.messages();
        await consumer.stop();
        // Discard the iterator.
        await it.return?.(undefined);

        // unregisterConsumer fires stopNotificationListener as
        // fire-and-forget. Wait for both the in-flight startup AND
        // the teardown to fully resolve before asserting.
        await waitFor(
          () =>
            (connection as any).notifyStartupPromise === null &&
            !(connection as any).stoppingNotify,
          3000,
          20,
        );

        // The notify-client slot must be empty.
        expect((connection as any).notifyClient).toBeNull();

        // Every acquired client must have been released. Pre-fix the
        // notify client we acquired during the race would be on the
        // acquired list with no matching release.
        expect(released.length).toBe(acquired.length);
      } finally {
        pool.connect = realConnect;
      }
    } finally {
      await dropDatabase();
    }
  });
});
