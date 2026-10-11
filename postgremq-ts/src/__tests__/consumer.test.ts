/**
 * Consumer Unit Tests
 * Tests consumer behavior, batching, auto-extension, and shutdown
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach, jest } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  generateTestPayload,
  sleep,
  waitFor
} from './helpers';
import { Connection } from '../connection';
import { Pool } from 'pg';

describe('Consumer', () => {
  let testDb: TestDatabase;
  let connection: Connection;
  let isoPool: Pool | null = null;

  let dropDb: (() => Promise<void>) | null = null;

  beforeAll(async () => {
    testDb = await getSharedTestDatabase();
  });

  beforeEach(async () => {
    const iso = await createIsolatedTestConnection(testDb);
    connection = iso.connection;
    isoPool = iso.pool;
    dropDb = iso.dropDatabase;
    await connection.createTopic('consumer-test-topic');
    await connection.createQueue('consumer-test-queue', 'consumer-test-topic', false);
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

  describe('Message Consumption', () => {
    test('should consume messages with correct batch size', async () => {
      // Publish 10 messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('consumer-test-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const received: any[] = [];

      // Consume all messages
      for (let i = 0; i < 10; i++) {
        const { value: message } = await messages.next();
        received.push(message);
        await message.ack();
      }

      await consumer.stop();

      expect(received.length).toBe(10);
    });

    test('should respect visibility timeout', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer1 = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 2, // 2 seconds
        autoExtension: { enabled: false } // Disable auto-extension so VT can actually expire
      });

      const messages1 = consumer1.messages();
      const { value: message1 } = await messages1.next();

      expect(message1).toBeDefined();
      expect(message1.deliveryAttempts).toBe(1);

      // Don't ack - let VT expire naturally without stopping consumer
      // Wait for VT to expire
      await sleep(2500);

      // Stop consumer1 after VT has expired
      await consumer1.stop();

      // Start another consumer - should get the message with incremented attempts
      const consumer2 = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      expect(message2).toBeDefined();
      expect(typeof message1.id).toBe('number');
      expect(typeof message2.id).toBe('number');
      expect(message2.id).toBe(message1.id);
      expect(message2.deliveryAttempts).toBe(2);

      await message2.ack();
      await consumer2.stop();
    });

    test('should auto-extend visibility timeout', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 3, // 3 seconds
        autoExtension: {
          enabled: true,
          extensionThreshold: 0.5, // Extend at 50%
          extensionSec: 3
        }
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      const initialVT = message.vt;

      // Wait past 50% of VT (should trigger auto-extension)
      await sleep(2000);

      // VT should have been extended
      // We can't directly observe the extension, but message should still be "locked"
      // Try to consume with another consumer - shouldn't get it
      const consumer2 = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Should timeout waiting for message (it's still locked)
      const messages2 = consumer2.messages();

      let receivedInTime = false;
      const racePromise = Promise.race([
        messages2.next().then(() => { receivedInTime = true; }),
        sleep(1000)
      ]);

      await racePromise;

      expect(receivedInTime).toBe(false); // Should NOT have received message

      // Clean up
      await message.ack();
      await consumer.stop();
      await consumer2.stop();
    });

    test('should disable auto-extension when configured', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 2,
        autoExtension: {
          enabled: false,
          extensionThreshold: 0.5,
          extensionSec: 30
        }
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Wait for VT to expire (no auto-extension should happen)
      await sleep(2500);

      // Message should now be available to another consumer
      const consumer2 = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      expect(message2).toBeDefined();
      expect(message2.id).toBe(message.id);

      await message2.ack();
      await consumer.stop();
      await consumer2.stop();
    });
  });

  describe('Shutdown Behavior', () => {
    test('should stop gracefully and release buffered messages', async () => {
      // Publish multiple messages
      for (let i = 0; i < 5; i++) {
        await connection.publish('consumer-test-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30
      });

      // Let consumer fetch but don't process
      await sleep(500);

      // Stop consumer - should release buffered messages
      await consumer.stop();

      // Messages should be available again
      const stats = await connection.getQueueStatistics('consumer-test-queue');
      expect(stats.pendingCount).toBeGreaterThan(0);
    });

    test('should complete in-flight messages before stopping', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Start stop (don't await)
      const stopPromise = consumer.stop();

      // Message should still be processable
      await message.ack();

      // Stop should complete
      await stopPromise;

      const stats = await connection.getQueueStatistics('consumer-test-queue');
      expect(stats.completedCount).toBe(1);
    });

    test('should handle rapid stop/start', async () => {
      for (let i = 0; i < 3; i++) {
        const consumer = connection.consume('consumer-test-queue', {
          batchSize: 1,
          visibilityTimeoutSec: 30
        });

        await sleep(100);
        await consumer.stop();
      }

      // Should not throw or leave dangling resources
      expect(true).toBe(true);
    });
  });

  describe('Event Notifications', () => {
    test('should react to new message notifications', async () => {
      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
        pollingIntervalMs: 5000 // Long polling interval
      });

      const messages = consumer.messages();

      // Start consuming (no messages yet)
      const messagePromise = messages.next();

      // Publish a message after consumer is waiting
      await sleep(500);
      await connection.publish('consumer-test-topic', generateTestPayload());

      // Should receive message quickly (via notification, not polling)
      const start = Date.now();
      const { value: message } = await messagePromise;
      const elapsed = Date.now() - start;

      expect(message).toBeDefined();
      expect(elapsed).toBeLessThan(2000); // Should be much faster than polling interval

      await message.ack();
      await consumer.stop();
    });
  });

  describe('Concurrent Consumers', () => {
    test('should distribute messages across multiple consumers', async () => {
      const published = [];
      for (let i = 0; i < 10; i++) {
        published.push(await connection.publish('consumer-test-topic', generateTestPayload(i)));
      }

      const consumers = [0, 1].map(() => connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      }));
      const received: number[][] = [[], []];
      let releaseFirstDelivery!: () => void;
      const firstDeliveries = new Promise<void>(resolve => { releaseFirstDelivery = resolve; });
      const workers = consumers.map(async (consumer, index) => {
        for await (const message of consumer.messages()) {
          received[index].push(message.id);
          // Hold each worker's first delivery until both have claimed work.
          // The queue guarantees exclusive claims, not scheduling fairness.
          if (received.every(ids => ids.length > 0)) releaseFirstDelivery();
          await firstDeliveries;
          await message.ack();
        }
      });

      try {
        await waitFor(() => received.flat().length === published.length);
      } finally {
        releaseFirstDelivery();
        await Promise.all(consumers.map(consumer => consumer.stop()));
        await Promise.all(workers);
      }

      expect(received.every(ids => ids.length > 0)).toBe(true);
      expect(received.flat().sort((a, b) => a - b)).toEqual(published.sort((a, b) => a - b));
      expect((await connection.getQueueStatistics('consumer-test-queue')).completedCount).toBe(10);
    }, 15000);
  });

  describe('Error Handling', () => {
    test('should handle database connection errors gracefully', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Close connection while consumer is active
      await connection.close();

      // Mark connection as null so afterEach doesn't try to close it again
      connection = null as any;

      // Consumer should handle this gracefully
      // Message may have been released during connection.close() or connection may be closed
      try {
        await message.ack();
        // If it doesn't throw, that's unexpected
        expect(true).toBe(false);
      } catch (error: any) {
        const validErrors = [
          'not connected',
          'already been processed'  // Message was released during connection close
        ];
        const hasValidError = validErrors.some(msg => error.message.includes(msg));
        expect(hasValidError).toBe(true);
      }
    });
  });

  // The consumer's messages() returns an AsyncIterableIterator. When user
  // code does `for await (const msg of consumer) { ...; break; }`, the
  // runtime calls iterator.return() for early-exit cleanup. Without an
  // explicit return() implementation, the auto-extension timer, LISTEN
  // refcount, and any buffered/in-flight messages would leak until GC.
  describe('AsyncIterator cleanup on early exit', () => {
    test('break out of for-await triggers consumer.stop()', async () => {
      for (let i = 0; i < 3; i++) {
        await connection.publish('consumer-test-topic', generateTestPayload(i));
      }

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30,
      });
      const stopSpy = jest.spyOn(consumer, 'stop');

      let received = 0;
      for await (const msg of consumer.messages()) {
        received++;
        await msg.ack();
        break;
      }

      expect(received).toBe(1);
      expect(stopSpy).toHaveBeenCalled();
    });

    test('return out of for-await triggers consumer.stop()', async () => {
      for (let i = 0; i < 2; i++) {
        await connection.publish('consumer-test-topic', generateTestPayload(i));
      }

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30,
      });
      const stopSpy = jest.spyOn(consumer, 'stop');

      const wrap = async () => {
        for await (const msg of consumer.messages()) {
          await msg.ack();
          return 'early';
        }
        return 'completed';
      };
      expect(await wrap()).toBe('early');
      expect(stopSpy).toHaveBeenCalled();
    });

    test('throw inside for-await triggers consumer.stop()', async () => {
      for (let i = 0; i < 2; i++) {
        await connection.publish('consumer-test-topic', generateTestPayload(i));
      }

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30,
      });
      const stopSpy = jest.spyOn(consumer, 'stop');

      await expect(async () => {
        for await (const msg of consumer.messages()) {
          await msg.ack();
          throw new Error('intentional');
        }
      }).rejects.toThrow('intentional');

      expect(stopSpy).toHaveBeenCalled();
    });
  });

  // messages.id is BIGSERIAL — without the BIGINT type parser registered in
  // src/connection.ts, node-pg returns BIGINT values as strings; without the
  // schema widening, ids past 2^31 would either error or silently truncate.
  describe('Message ID widening (BIGINT round-trip)', () => {
    test('publish/consume round-trips a message id past 2^31', async () => {
      const seedTo = 2_200_000_000; // > 2^31 (2_147_483_647)
      await isoPool!.query("SELECT setval('postgremq.messages_id_seq', $1)", [seedTo]);

      const publishedId = await connection.publish('consumer-test-topic', { big: true });
      expect(typeof publishedId).toBe('number');
      expect(publishedId).toBeGreaterThan(seedTo);

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
      });

      let receivedId: number | undefined;
      for await (const msg of consumer.messages()) {
        receivedId = msg.id;
        await msg.ack();
        break;
      }
      expect(receivedId).toBe(publishedId);
    });
  });
});
