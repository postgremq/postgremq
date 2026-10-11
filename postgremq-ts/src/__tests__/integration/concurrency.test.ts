/**
 * Concurrency Integration Tests
 * Tests concurrent operations and race conditions
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  generateTestPayload,
  sleep,
  waitFor
} from '../helpers';
import { Connection } from '../../connection';
import { Consumer } from '../../consumer';
import { Message } from '../../message';

describe('Concurrency', () => {
  let testDb: TestDatabase;
  let connection: Connection;
  let rawPool: import('pg').Pool | null = null;
  let isolatedConnStr: string | null = null;

  let dropDb: (() => Promise<void>) | null = null;

  beforeAll(async () => {
    testDb = await getSharedTestDatabase();
  });

  beforeEach(async () => {
    const iso = await createIsolatedTestConnection(testDb);
    connection = iso.connection;
    dropDb = iso.dropDatabase;
    rawPool = iso.pool;
    isolatedConnStr = iso.connectionString;
  });

  afterEach(async () => {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.warn('Error closing connection in afterEach:', error);
      }
    }
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
    rawPool = null;
    isolatedConnStr = null;
  });

  afterAll(async () => {
    if (testDb) {
      // shared container is stopped once on process exit (getSharedTestDatabase)
    }
  });

  describe('Multiple Consumers', () => {
    test('should distribute messages across consumers', async () => {
      await connection.createTopic('distribute-topic');
      await connection.createQueue('distribute-queue', 'distribute-topic', false);

      // Publish 20 messages
      for (let i = 0; i < 20; i++) {
        await connection.publish('distribute-topic', generateTestPayload(i));
      }

      await sleep(200);

      // Start the consumers one at a time, each taking its first message
      // before the next starts. A consumer's first pull fetches a full batch
      // (5) and it fetches again only when its buffer runs low, so the four
      // first fetches split the 20 messages whatever the timing. (Waiting
      // for exactly 5 per consumer would hang whenever one fetched more.)
      const consumers: Consumer[] = [];
      const iterators: AsyncIterableIterator<Message>[] = [];
      const received: Message[][] = [];
      for (let i = 0; i < 4; i++) {
        const consumer = connection.consume('distribute-queue', {
          batchSize: 5,
          visibilityTimeoutSec: 30
        });
        const iterator = consumer.messages();
        const { value: message } = await iterator.next();
        await message.ack();
        consumers.push(consumer);
        iterators.push(iterator);
        received.push([message]);
      }

      // Drain the rest concurrently, however it ends up spread.
      let total = received.length;
      const drain = async (iterator: AsyncIterableIterator<Message>, into: Message[]) => {
        for (;;) {
          const { done, value: message } = await iterator.next();
          if (done) return;
          into.push(message);
          await message.ack();
          total++;
        }
      };
      const draining = iterators.map((iterator, i) => drain(iterator, received[i]));
      await waitFor(() => total === 20, 30000);
      await Promise.all(consumers.map(consumer => consumer.stop()));
      await Promise.all(draining);

      // All messages consumed, none twice, by every consumer
      const allIds = received.flat().map(m => m.id);
      expect(allIds).toHaveLength(20);
      expect(new Set(allIds).size).toBe(20);
      for (const share of received) {
        expect(share.length).toBeGreaterThan(0);
      }

      // Cleanup
      await connection.deleteQueue('distribute-queue');
      await connection.cleanUpTopic('distribute-topic');
      await connection.deleteTopic('distribute-topic');
    });

    test('should handle concurrent publish and consume', async () => {
      await connection.createTopic('concurrent-pubsub-topic');
      await connection.createQueue('concurrent-pubsub-queue', 'concurrent-pubsub-topic', false);

      const consumer = connection.consume('concurrent-pubsub-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30
      });

      const received: any[] = [];
      const messages = consumer.messages();

      // Start consuming
      const consumePromise = (async () => {
        for (let i = 0; i < 50; i++) {
          const { value: message } = await messages.next();
          received.push(message);
          await message.ack();
        }
      })();

      // Publish messages concurrently
      const publishPromises: Promise<number>[] = [];
      for (let i = 0; i < 50; i++) {
        publishPromises.push(
          connection.publish('concurrent-pubsub-topic', generateTestPayload(i))
        );
        await sleep(20); // Stagger publications
      }

      await Promise.all(publishPromises);
      await consumePromise;
      await consumer.stop();

      expect(received.length).toBe(50);

      // Cleanup
      await connection.deleteQueue('concurrent-pubsub-queue');
      await connection.cleanUpTopic('concurrent-pubsub-topic');
      await connection.deleteTopic('concurrent-pubsub-topic');
    });

    test('should prevent duplicate message consumption', async () => {
      await connection.createTopic('no-duplicate-topic');
      await connection.createQueue('no-duplicate-queue', 'no-duplicate-topic', false);

      // Publish 10 messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('no-duplicate-topic', generateTestPayload(i));
      }

      await sleep(100);

      // Create multiple consumers competing for same messages
      const consumer1 = connection.consume('no-duplicate-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      const consumer2 = connection.consume('no-duplicate-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      const consumer3 = connection.consume('no-duplicate-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      const allReceived: any[] = [];

      const consumeAll = async (consumer: any) => {
        const messages = consumer.messages();
        const localReceived: any[] = [];

        try {
          for (let i = 0; i < 10; i++) {
            const result = await Promise.race([
              messages.next(),
              sleep(2000).then(() => null)
            ]);

            if (result === null) break;

            const { value: message } = result as any;
            if (message) {
              localReceived.push(message);
              await message.ack();
            }
          }
        } catch (error) {
          // Expected when no more messages
        }

        return localReceived;
      };

      const results = await Promise.all([
        consumeAll(consumer1),
        consumeAll(consumer2),
        consumeAll(consumer3)
      ]);

      allReceived.push(...results[0], ...results[1], ...results[2]);

      await Promise.all([
        consumer1.stop(),
        consumer2.stop(),
        consumer3.stop()
      ]);

      // Should have consumed exactly 10 messages
      expect(allReceived.length).toBe(10);

      // No duplicates
      const allIds = allReceived.map(m => m.id);
      const uniqueIds = new Set(allIds);
      expect(uniqueIds.size).toBe(10);

      // Cleanup
      await connection.deleteQueue('no-duplicate-queue');
      await connection.cleanUpTopic('no-duplicate-topic');
      await connection.deleteTopic('no-duplicate-topic');
    });
  });

  describe('Concurrent Message Operations', () => {
    test('should handle concurrent VT extensions', async () => {
      await connection.createTopic('concurrent-vt-topic');
      await connection.createQueue('concurrent-vt-queue', 'concurrent-vt-topic', false);

      // Publish messages
      for (let i = 0; i < 5; i++) {
        await connection.publish('concurrent-vt-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('concurrent-vt-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 10,
        autoExtension: {
          enabled: false // Manual extension
        }
      });

      const messages = consumer.messages();
      const receivedMessages: any[] = [];

      // Consume all messages
      for (let i = 0; i < 5; i++) {
        const { value: message } = await messages.next();
        receivedMessages.push(message);
      }

      // Extend all VTs concurrently
      const extendPromises = receivedMessages.map(msg => msg.setVt(20));
      const newVts = await Promise.all(extendPromises);

      // All extensions should succeed
      expect(newVts.length).toBe(5);
      newVts.forEach(vt => {
        expect(vt.getTime()).toBeGreaterThan(Date.now());
      });

      // Ack all
      await Promise.all(receivedMessages.map(msg => msg.ack()));
      await consumer.stop();

      // Cleanup
      await connection.deleteQueue('concurrent-vt-queue');
      await connection.cleanUpTopic('concurrent-vt-topic');
      await connection.deleteTopic('concurrent-vt-topic');
    });

    test('should handle concurrent acks', async () => {
      await connection.createTopic('concurrent-ack-topic');
      await connection.createQueue('concurrent-ack-queue', 'concurrent-ack-topic', false);

      // Publish messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('concurrent-ack-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('concurrent-ack-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const receivedMessages: any[] = [];

      // Consume all messages
      for (let i = 0; i < 10; i++) {
        const { value: message } = await messages.next();
        receivedMessages.push(message);
      }

      // Ack all concurrently
      await Promise.all(receivedMessages.map(msg => msg.ack()));

      await consumer.stop();

      // Verify all acknowledged
      const stats = await connection.getQueueStatistics('concurrent-ack-queue');
      expect(stats.completedCount).toBe(10);
      expect(stats.pendingCount).toBe(0);

      // Cleanup
      await connection.deleteQueue('concurrent-ack-queue');
      await connection.cleanUpTopic('concurrent-ack-topic');
      await connection.deleteTopic('concurrent-ack-topic');
    });

    test('should handle concurrent nacks', async () => {
      await connection.createTopic('concurrent-nack-topic');
      await connection.createQueue('concurrent-nack-queue', 'concurrent-nack-topic', false);

      // Publish messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('concurrent-nack-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('concurrent-nack-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30,
        autoExtension: { enabled: false }
      });

      const messages = consumer.messages();
      const receivedMessages: any[] = [];

      // Consume all messages
      for (let i = 0; i < 10; i++) {
        const { value: message } = await messages.next();
        receivedMessages.push(message);
      }

      // Nack all concurrently
      await Promise.all(receivedMessages.map(msg => msg.nack()));

      await consumer.stop();

      // Give a small delay for database transactions to fully commit
      // This is especially important on Node.js 22 where async timing differs
      await sleep(100);

      // Wait for all messages to return to pending state
      await waitFor(async () => {
        const stats = await connection.getQueueStatistics('concurrent-nack-queue');
        return stats.pendingCount === 10;
      }, 5000, 100);

      const stats = await connection.getQueueStatistics('concurrent-nack-queue');
      expect(stats.pendingCount).toBe(10);

      // Clean up by consuming and acking all messages
      const consumer2 = connection.consume('concurrent-nack-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      for (let i = 0; i < 10; i++) {
        const { value: message } = await messages2.next();
        await message.ack();
      }

      await consumer2.stop();

      // Now safe to delete (cleanup topic first to remove all messages)
      await connection.deleteQueue('concurrent-nack-queue');
      await connection.cleanUpTopic('concurrent-nack-topic');
      await connection.deleteTopic('concurrent-nack-topic');
    });
  });

  describe('Multiple Connections', () => {
    test('should handle multiple connections consuming same queue', async () => {
      await connection.createTopic('multi-conn-topic');
      await connection.createQueue('multi-conn-queue', 'multi-conn-topic', false);

      // Publish messages
      for (let i = 0; i < 20; i++) {
        await connection.publish('multi-conn-topic', generateTestPayload(i));
      }

      await sleep(100);

      // Create separate connections with their own pools (no pool sharing)
      const conn1 = new Connection({ connectionString: isolatedConnStr! });
      const conn2 = new Connection({ connectionString: isolatedConnStr! });
      const conn3 = new Connection({ connectionString: isolatedConnStr! });
      await Promise.all([conn1.connect(), conn2.connect(), conn3.connect()]);

      // The queue was created on `connection`; conn1/2/3 don't have it in
      // their topic cache, so we must pass `topic` explicitly.
      const consumer1 = conn1.consume('multi-conn-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30,
        topic: 'multi-conn-topic'
      });

      const consumer2 = conn2.consume('multi-conn-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30,
        topic: 'multi-conn-topic'
      });

      const consumer3 = conn3.consume('multi-conn-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30,
        topic: 'multi-conn-topic'
      });

      const received1: any[] = [];
      const received2: any[] = [];
      const received3: any[] = [];

      const consume = async (consumer: any, receivedArray: any[], count: number) => {
        const messages = consumer.messages();
        for (let i = 0; i < count; i++) {
          try {
            const result = await Promise.race([
              messages.next(),
              sleep(3000).then(() => null)
            ]);

            if (result === null) break;

            const { value: message } = result as any;
            if (message) {
              receivedArray.push(message);
              await message.ack();
            }
          } catch (error) {
            break;
          }
        }
      };

      await Promise.all([
        consume(consumer1, received1, 10),
        consume(consumer2, received2, 10),
        consume(consumer3, received3, 10)
      ]);

      await Promise.all([
        consumer1.stop(),
        consumer2.stop(),
        consumer3.stop()
      ]);

      await Promise.all([
        conn1.close(),
        conn2.close(),
        conn3.close()
      ]);

      // Verify all messages consumed
      const totalReceived = received1.length + received2.length + received3.length;
      expect(totalReceived).toBe(20);

      // Verify no duplicates
      const allIds = [
        ...received1.map(m => m.id),
        ...received2.map(m => m.id),
        ...received3.map(m => m.id)
      ];
      const uniqueIds = new Set(allIds);
      expect(uniqueIds.size).toBe(20);

      // Cleanup
      await connection.deleteQueue('multi-conn-queue');
      await connection.cleanUpTopic('multi-conn-topic');
      await connection.deleteTopic('multi-conn-topic');
    });

    test('should handle concurrent topic/queue operations', async () => {
      // Create multiple topics concurrently
      const topicPromises = [];
      for (let i = 0; i < 5; i++) {
        topicPromises.push(connection.createTopic(`concurrent-topic-${i}`));
      }
      await Promise.all(topicPromises);

      // Create multiple queues concurrently
      const queuePromises = [];
      for (let i = 0; i < 5; i++) {
        queuePromises.push(
          connection.createQueue(`concurrent-queue-${i}`, `concurrent-topic-${i}`, false)
        );
      }
      await Promise.all(queuePromises);

      // Verify all created
      const topics = await connection.listTopics();
      const queues = await connection.listQueues();

      for (let i = 0; i < 5; i++) {
        expect(topics).toContain(`concurrent-topic-${i}`);
        expect(queues.some(q => q.queueName === `concurrent-queue-${i}`)).toBe(true);
      }

      // Cleanup
      const deleteQueuePromises = [];
      const deleteTopicPromises = [];

      for (let i = 0; i < 5; i++) {
        deleteQueuePromises.push(connection.deleteQueue(`concurrent-queue-${i}`));
      }
      await Promise.all(deleteQueuePromises);

      for (let i = 0; i < 5; i++) {
        deleteTopicPromises.push(connection.deleteTopic(`concurrent-topic-${i}`));
      }
      await Promise.all(deleteTopicPromises);
    });
  });

  describe('High Load Scenarios', () => {
    test('should handle burst of message publications', async () => {
      await connection.createTopic('burst-topic');
      await connection.createQueue('burst-queue', 'burst-topic', false);

      const startTime = Date.now();

      // Publish 50 messages as fast as possible
      const publishPromises = [];
      for (let i = 0; i < 50; i++) {
        publishPromises.push(
          connection.publish('burst-topic', generateTestPayload(i))
        );
      }

      const messageIds = await Promise.all(publishPromises);
      const publishTime = Date.now() - startTime;

      expect(messageIds.length).toBe(50);
      console.log(`Published 50 messages in ${publishTime}ms`);

      // Consume all
      const consumer = connection.consume('burst-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const received: any[] = [];

      const consumeStart = Date.now();
      for (let i = 0; i < 50; i++) {
        const { value: message } = await messages.next();
        received.push(message);
        await message.ack();
      }
      const consumeTime = Date.now() - consumeStart;

      await consumer.stop();

      expect(received.length).toBe(50);
      console.log(`Consumed 50 messages in ${consumeTime}ms`);

      // Cleanup
      await connection.deleteQueue('burst-queue');
      await connection.cleanUpTopic('burst-topic');
      await connection.deleteTopic('burst-topic');
    });

    test('should handle sustained message throughput', async () => {
      await connection.createTopic('throughput-topic');
      await connection.createQueue('throughput-queue', 'throughput-topic', false);

      const consumer = connection.consume('throughput-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const received: any[] = [];

      // Consume in background
      const consumePromise = (async () => {
        for (let i = 0; i < 50; i++) {
          const { value: message } = await messages.next();
          received.push(message);
          await message.ack();
        }
      })();

      // Publish messages over time
      for (let i = 0; i < 50; i++) {
        await connection.publish('throughput-topic', generateTestPayload(i));
        if (i % 10 === 0) {
          await sleep(50); // Brief pause every 10 messages
        }
      }

      await consumePromise;
      await consumer.stop();

      expect(received.length).toBe(50);

      // Cleanup
      await connection.deleteQueue('throughput-queue');
      await connection.cleanUpTopic('throughput-topic');
      await connection.deleteTopic('throughput-topic');
    });
  });

  describe('Race Conditions', () => {
    test('should handle race between VT expiration and ack', async () => {
      await connection.createTopic('race-vt-topic');
      await connection.createQueue('race-vt-queue', 'race-vt-topic', false);

      await connection.publish('race-vt-topic', generateTestPayload());

      const consumer = connection.consume('race-vt-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 1, // Very short VT
        autoExtension: { enabled: false } // Disable so VT can actually expire
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Stop consumer to prevent any auto-extensions
      await consumer.stop();

      // Wait for VT to expire
      await sleep(1500);

      // Another consumer should be able to get the message
      const consumer2 = connection.consume('race-vt-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();

      // Use timeout to avoid hanging if message isn't available
      const result = await Promise.race([
        messages2.next(),
        sleep(3000).then(() => null)
      ]);

      expect(result).not.toBeNull();
      const { value: message2 } = result as any;

      expect(message2.id).toBe(message.id);
      await expect(message.ack()).rejects.toThrow(); // the new token fences the old handler
      await message2.ack();

      await consumer2.stop();

      // Cleanup
      await connection.deleteQueue('race-vt-queue');
      await connection.cleanUpTopic('race-vt-topic');
      await connection.deleteTopic('race-vt-topic');
    });

    test('should handle race between VT extension and expiration', async () => {
      await connection.createTopic('race-extend-topic');
      await connection.createQueue('race-extend-queue', 'race-extend-topic', false);

      await connection.publish('race-extend-topic', generateTestPayload());

      const consumer = connection.consume('race-extend-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 2,
        autoExtension: {
          enabled: false
        }
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Wait until almost expired
      await sleep(1900);

      // Try to extend at the last moment
      try {
        await message.setVt(10);
        // Extension succeeded
        await message.ack();
      } catch (error) {
        // Extension failed - VT already expired
        // Another consumer should get it
      }

      await consumer.stop();

      // Cleanup
      await connection.cleanUpQueue('race-extend-queue');
      await connection.deleteQueue('race-extend-queue');
      await connection.cleanUpTopic('race-extend-topic');
      await connection.deleteTopic('race-extend-topic');
    });
  });
});
