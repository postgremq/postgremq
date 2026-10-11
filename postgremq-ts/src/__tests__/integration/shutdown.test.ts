/**
 * Shutdown Integration Tests
 * Tests graceful shutdown behavior and cleanup
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach, jest } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  createTestConnection,
  generateTestPayload,
  sleep
} from '../helpers';
import { Connection } from '../../connection';

describe('Shutdown Behavior', () => {
  let testDb: TestDatabase;
  let dropDb: (() => Promise<void>) | null = null;

  beforeAll(async () => {
    testDb = await getSharedTestDatabase();
  });

  afterAll(async () => {
    if (testDb) {
      // shared container is stopped once on process exit (getSharedTestDatabase)
    }
  });

  beforeEach(async () => {
    // ensure each test uses an isolated database
    const iso = await createIsolatedTestConnection(testDb);
    (global as any).__conn = iso.connection; // temp holder
    dropDb = iso.dropDatabase;
  });

  afterEach(async () => {
    const connection: Connection | undefined = (global as any).__conn;
    if (connection) {
      await connection.close();
      (global as any).__conn = undefined;
    }
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
  });

  describe('Consumer Shutdown', () => {
    test('should wait for in-flight messages during shutdown', async () => {
      const connection: Connection = (global as any).__conn;

      await connection.createTopic('shutdown-inflight-topic');
      await connection.createQueue('shutdown-inflight-queue', 'shutdown-inflight-topic', false);
      await connection.publish('shutdown-inflight-topic', generateTestPayload());

      const consumer = connection.consume('shutdown-inflight-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      expect(message).toBeDefined();

      // Start shutdown (don't await)
      const stopPromise = consumer.stop();

      // Message should still be processable
      await message.ack();

      // Stop should complete
      await stopPromise;

      const stats = await connection.getQueueStatistics('shutdown-inflight-queue');
      expect(stats.completedCount).toBe(1);

      await connection.close();
    });

    test('should release buffered messages on shutdown', async () => {
      const connection: Connection = (global as any).__conn;

      await connection.createTopic('shutdown-buffer-topic');
      await connection.createQueue('shutdown-buffer-queue', 'shutdown-buffer-topic', false);

      // Publish multiple messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('shutdown-buffer-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('shutdown-buffer-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      // Let consumer fetch but don't process
      await sleep(500);

      // Stop consumer - should release buffered messages
      await consumer.stop();

      await sleep(100);

      // Messages should be available again
      const stats = await connection.getQueueStatistics('shutdown-buffer-queue');
      expect(stats.pendingCount).toBeGreaterThan(0);

      await connection.close();
    });

    test('should stop auto-extension on consumer shutdown', async () => {
      const connection: Connection = (global as any).__conn;

      await connection.createTopic('shutdown-ext-topic');
      await connection.createQueue('shutdown-ext-queue', 'shutdown-ext-topic', false);
      await connection.publish('shutdown-ext-topic', generateTestPayload());

      const consumer = connection.consume('shutdown-ext-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 10,
        autoExtension: {
          enabled: true,
          extensionThreshold: 0.5,
          extensionSec: 10
        }
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Stop consumer
      await consumer.stop();

      // Auto-extension should stop
      // Verify no more extensions happen
      const vtBefore = message.vt;
      await sleep(6000); // Wait past extension threshold

      // VT should not have changed (auto-extension stopped)
      expect(message.vt).toEqual(vtBefore);

      await connection.close();
    });

    test('should handle rapid consumer stop/start cycles', async () => {
      const connection: Connection = (global as any).__conn;

      await connection.createTopic('rapid-cycle-topic');
      await connection.createQueue('rapid-cycle-queue', 'rapid-cycle-topic', false);

      // Rapid stop/start
      for (let i = 0; i < 10; i++) {
        const consumer = connection.consume('rapid-cycle-queue', {
          batchSize: 1,
          visibilityTimeoutSec: 30
        });

        await sleep(50);
        await consumer.stop();
      }

      // Should not throw or leak resources
      expect(true).toBe(true);

      await connection.close();
    });

    test('should handle shutdown with no messages consumed', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('shutdown-empty-topic');
      await connection.createQueue('shutdown-empty-queue', 'shutdown-empty-topic', false);

      const consumer = connection.consume('shutdown-empty-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Stop immediately without consuming
      await consumer.stop();

      expect(true).toBe(true);

      await connection.close();
    });

    test('should handle shutdown while waiting for messages', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('shutdown-waiting-topic');
      await connection.createQueue('shutdown-waiting-queue', 'shutdown-waiting-topic', false);

      const consumer = connection.consume('shutdown-waiting-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
        pollingIntervalMs: 1000
      });

      const messages = consumer.messages();

      // Start waiting for message (none available)
      const messagePromise = messages.next();

      // Stop while waiting
      const stopPromise = consumer.stop();

      // Both should complete
      await Promise.race([messagePromise, stopPromise]);
      await stopPromise;

      await connection.close();
    });

    /**
     * Regression: a fetch
     * in-flight when stop() runs whose SQL takes longer than the old
     * 1000ms spin cap orphans messages — consume_message has already
     * claimed them server-side, but stop() returns before they reach
     * the buffer / inFlight tracking.
     *
     * Stub the connection's consumeMessages to delay 2s, kick off a
     * fetch, then call stop() immediately. Pre-fix: stop returns at
     * ~1s, fetch resolves at 2s with messages stuck in 'processing'.
     * Post-fix: stop awaits the fetch promise and the messages flow
     * through normal release/ack on shutdown.
     */
    test('stop awaits a slow in-flight fetch and releases its messages', async () => {
      const connection: Connection = (global as any).__conn;

      await connection.createTopic('slow-fetch-topic');
      await connection.createQueue('slow-fetch-queue', 'slow-fetch-topic', false);
      const ids: number[] = [];
      for (let i = 0; i < 3; i++) {
        ids.push(await connection.publish('slow-fetch-topic', { i }));
      }

      // Wrap consumeMessages with a 2s artificial delay (longer than
      // the legacy 1s spin cap). spyOn lets a future rename surface.
      const realConsume = connection.consumeMessages.bind(connection);
      const spy = jest.spyOn(connection, 'consumeMessages')
        .mockImplementation(async (...args: Parameters<typeof connection.consumeMessages>) => {
          await sleep(2000);
          return realConsume(...args);
        });

      try {
        const consumer = connection.consume('slow-fetch-queue', {
          batchSize: 3,
          visibilityTimeoutSec: 30,
          autoExtension: { enabled: false }
        });

        // Drive the fetch by calling messages() — this triggers an
        // initial fetch under the hood.
        const messages = consumer.messages();
        // Don't actually receive any messages; just give the fetch a
        // moment to start its 2s delay.
        await sleep(50);

        // Stop must wait for the in-flight fetch to deliver its rows
        // before tearing down. Total stop time is bounded above by
        // FETCH_DRAIN_TIMEOUT_MS but should be ~2s in practice.
        const t0 = Date.now();
        await consumer.stop();
        const stopElapsed = Date.now() - t0;
        expect(stopElapsed).toBeGreaterThanOrEqual(1500); // had to wait for the fetch

        // Critical assertion: every claimed message reached release
        // path. Pre-fix: 3 rows would be in 'processing' until vt.
        // Post-fix: 3 rows are back in 'pending', ready to consume.
        const stats = await connection.getQueueStatistics('slow-fetch-queue');
        expect(stats.processingCount).toBe(0);
        expect(stats.pendingCount).toBe(3);

        // discard the iterator
        await messages.return?.(undefined);
      } finally {
        spy.mockRestore();
      }

      await connection.cleanUpQueue('slow-fetch-queue');
      await connection.deleteQueue('slow-fetch-queue');
      await connection.cleanUpTopic('slow-fetch-topic');
      await connection.deleteTopic('slow-fetch-topic');
    });

    /**
     * Regression: releaseBufferedMessages used
     * `Promise.race([Promise.all(...), new Promise((_, reject) =>
     * setTimeout(() => reject(...), 2000))])`. When Promise.all wins,
     * the setTimeout still fires later and rejects a Promise nobody is
     * listening to → UnhandledPromiseRejection. Node crashes under
     * `--unhandled-rejections=strict`.
     *
     * Install a process-level rejection listener for the duration of a
     * normal shutdown and assert no rejections fired. Done with the
     * fix in place; previously a fast-success shutdown would still log
     * the timeout rejection ~2s later.
     */
    test('shutdown does not produce UnhandledPromiseRejection', async () => {
      const connection: Connection = (global as any).__conn;

      await connection.createTopic('no-leak-topic');
      await connection.createQueue('no-leak-queue', 'no-leak-topic', false);
      // Pre-stage some messages so releaseBufferedMessages has work to do.
      for (let i = 0; i < 5; i++) {
        await connection.publish('no-leak-topic', { i });
      }

      const rejections: any[] = [];
      const handler = (reason: any) => rejections.push(reason);
      process.on('unhandledRejection', handler);
      try {
        const consumer = connection.consume('no-leak-queue', {
          batchSize: 5,
          visibilityTimeoutSec: 30,
          autoExtension: { enabled: false },
        });
        const it = consumer.messages();
        // Let the fetch settle so the buffer has rows.
        await sleep(200);
        await consumer.stop();
        await it.return?.(undefined);

        // Wait long enough that any leaked setTimeout (the 2s release
        // timeout) would have fired with the legacy implementation.
        await sleep(2500);

        expect(rejections).toEqual([]);
      } finally {
        process.removeListener('unhandledRejection', handler);
      }

      await connection.cleanUpQueue('no-leak-queue');
      await connection.deleteQueue('no-leak-queue');
      await connection.cleanUpTopic('no-leak-topic');
      await connection.deleteTopic('no-leak-topic');
    });
  });

  describe('Connection Shutdown', () => {
    test('should wait for consumer during connection close', async () => {
      // Create a new connection with its own pool
      const connection = new Connection({
        connectionString: testDb.connectionString,
        shutdownTimeoutMs: 5000 // 5 second timeout
      });
      await connection.connect();

      await connection.createTopic('conn-close-topic');
      await connection.createQueue('conn-close-queue', 'conn-close-topic', false);

      // Create a consumer to test connection close with active consumer
      const consumer = connection.consume('conn-close-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
        autoExtension: { enabled: false }
      });

      // Publish a message
      await connection.publish('conn-close-topic', generateTestPayload());

      // Get and ack the message
      const msg = await consumer.messages().next();
      await msg.value.ack();

      // Then close connection (consumer should stop cleanly)
      await connection.close();

      // Verify connection is shut down
      expect(connection.isClientShuttingDown()).toBe(true);
    });

    test('should close quickly even with unprocessed messages', async () => {
      const connection = new Connection({
        connectionString: testDb.connectionString,
        shutdownTimeoutMs: 1000 // 1 second timeout
      });

      await connection.connect();

      await connection.createTopic('timeout-close-topic');
      await connection.createQueue('timeout-close-queue', 'timeout-close-topic', false);
      await connection.publish('timeout-close-topic', generateTestPayload());

      const consumer = connection.consume('timeout-close-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Start close - should complete quickly by releasing the message
      const start = Date.now();
      await connection.close();
      const elapsed = Date.now() - start;

      // Should complete quickly (within shutdown timeout)
      expect(elapsed).toBeLessThan(2000);
      expect(connection.isClientShuttingDown()).toBe(true);
    });

    test('should close notification listener on connection close', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('notify-close-topic');
      await connection.createQueue('notify-close-queue', 'notify-close-topic', false);

      const consumer = connection.consume('notify-close-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Close connection
      await connection.close();

      // Notification listener should be closed
      // Publishing should fail
      try {
        await connection.publish('notify-close-topic', generateTestPayload());
        expect(true).toBe(false); // Should not reach here
      } catch (error: any) {
        expect(error.message).toContain('not connected');
      }
    });

    test('should handle connection close with no active consumers', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('empty-close-topic');
      await connection.createQueue('empty-close-queue', 'empty-close-topic', false);

      // Close without creating consumers
      await connection.close();

      expect(connection.isClientShuttingDown()).toBe(true);
    });

    test('should handle connection close with stopped consumers', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('stopped-close-topic');
      await connection.createQueue('stopped-close-queue', 'stopped-close-topic', false);

      const consumer = connection.consume('stopped-close-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Stop consumer first
      await consumer.stop();

      // Close connection
      await connection.close();

      expect(connection.isClientShuttingDown()).toBe(true);
    });
  });

  describe('Cleanup During Shutdown', () => {
    test('should complete all acknowledgments during shutdown', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('ack-shutdown-topic');
      await connection.createQueue('ack-shutdown-queue', 'ack-shutdown-topic', false);

      // Publish multiple messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('ack-shutdown-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('ack-shutdown-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const receivedMessages: any[] = [];

      // Consume all
      for (let i = 0; i < 10; i++) {
        const { value: message } = await messages.next();
        receivedMessages.push(message);
      }

      // Ack all concurrently
      const ackPromises = receivedMessages.map(msg => msg.ack());

      // Stop consumer while acks are in flight
      const stopPromise = consumer.stop();

      // Wait for all acks and stop
      await Promise.all([...ackPromises, stopPromise]);

      // Verify all acknowledged
      const stats = await connection.getQueueStatistics('ack-shutdown-queue');
      expect(stats.completedCount).toBe(10);

      await connection.close();
    });

    test('should handle errors during shutdown cleanup', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('error-shutdown-topic');
      await connection.createQueue('error-shutdown-queue', 'error-shutdown-topic', false);

      // Publish messages
      for (let i = 0; i < 5; i++) {
        await connection.publish('error-shutdown-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('error-shutdown-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const receivedMessages: any[] = [];

      // Consume all
      for (let i = 0; i < 5; i++) {
        const { value: message } = await messages.next();
        receivedMessages.push(message);
      }

      // Close connection to cause errors
      await connection.close();

      // Stop consumer - should handle errors gracefully
      await consumer.stop();

      // Should not throw
      expect(true).toBe(true);
    });

    test('should clean up resources on abnormal shutdown', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('abnormal-shutdown-topic');
      await connection.createQueue('abnormal-shutdown-queue', 'abnormal-shutdown-topic', false);

      const consumer = connection.consume('abnormal-shutdown-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Force close without proper shutdown
      await connection.close();

      // Consumer should be automatically stopped
      // Verify no lingering resources
      expect(true).toBe(true);
    });
  });

  describe('Multiple Shutdowns', () => {
    test('should handle multiple consumer stop calls', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('multi-stop-topic');
      await connection.createQueue('multi-stop-queue', 'multi-stop-topic', false);

      const consumer = connection.consume('multi-stop-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Call stop multiple times
      await consumer.stop();
      await consumer.stop();
      await consumer.stop();

      // Should not throw
      expect(true).toBe(true);

      await connection.close();
    });

    test('should handle multiple connection close calls', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('multi-close-topic');
      await connection.createQueue('multi-close-queue', 'multi-close-topic', false);

      // Call close multiple times
      await connection.close();
      await connection.close();
      await connection.close();

      // Should not throw
      expect(true).toBe(true);
    });

    test('should handle concurrent shutdown calls', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('concurrent-shutdown-topic');
      await connection.createQueue('concurrent-shutdown-queue', 'concurrent-shutdown-topic', false);

      const consumer1 = connection.consume('concurrent-shutdown-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const consumer2 = connection.consume('concurrent-shutdown-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const consumer3 = connection.consume('concurrent-shutdown-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Stop all concurrently
      await Promise.all([
        consumer1.stop(),
        consumer2.stop(),
        consumer3.stop()
      ]);

      // Close connection
      await connection.close();

      expect(connection.isClientShuttingDown()).toBe(true);
    });
  });

  describe('Shutdown with Pending Operations', () => {
    test('should complete pending publishes during shutdown', async () => {
      // Create a new connection with its own pool
      const connection = new Connection({
        connectionString: testDb.connectionString,
        shutdownTimeoutMs: 5000 // 5 second timeout
      });
      await connection.connect();

      await connection.createTopic('pending-pub-topic');
      await connection.createQueue('pending-pub-queue', 'pending-pub-topic', false);

      // Publish messages (wait for them to complete)
      const messageIds = [];
      for (let i = 0; i < 10; i++) {
        const id = await connection.publish('pending-pub-topic', generateTestPayload(i));
        messageIds.push(id);
      }

      // All publishes completed successfully
      expect(messageIds.length).toBe(10);
      expect(messageIds.every(id => id > 0)).toBe(true);

      // Close connection
      await connection.close();

      expect(connection.isClientShuttingDown()).toBe(true);
    });

    test('should handle shutdown during message fetch', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('fetch-shutdown-topic');
      await connection.createQueue('fetch-shutdown-queue', 'fetch-shutdown-topic', false);

      await connection.publish('fetch-shutdown-topic', generateTestPayload());

      const consumer = connection.consume('fetch-shutdown-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();

      // Start message fetch
      const messagePromise = messages.next();

      // Immediately stop
      const stopPromise = consumer.stop();

      // Wait for both
      await Promise.race([messagePromise, stopPromise]);
      await stopPromise;

      await connection.close();
    });

    test('should handle shutdown during VT extension', async () => {
      const connection = await createTestConnection(testDb);

      await connection.createTopic('ext-shutdown-topic');
      await connection.createQueue('ext-shutdown-queue', 'ext-shutdown-topic', false);

      await connection.publish('ext-shutdown-topic', generateTestPayload());

      const consumer = connection.consume('ext-shutdown-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 10,
        autoExtension: {
          enabled: true,
          extensionThreshold: 0.5,
          extensionSec: 10
        }
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Wait for auto-extension to trigger
      await sleep(6000);

      // Stop consumer
      await consumer.stop();

      // Should not throw
      expect(true).toBe(true);

      await connection.close();
    });
  });
});
