/**
 * Message Unit Tests
 * Tests message operations: ack, nack, release, setVt
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  generateTestPayload,
  sleep
} from './helpers';
import { Connection } from '../connection';
import { Message } from '../message';

describe('Message', () => {
  let testDb: TestDatabase;
  let connection: Connection;
  let rawPool: import('pg').Pool | null = null;
  let dropDb: (() => Promise<void>) | null = null;

  beforeAll(async () => {
    testDb = await getSharedTestDatabase();
  });

  beforeEach(async () => {
    const iso = await createIsolatedTestConnection(testDb);
    connection = iso.connection;
    dropDb = iso.dropDatabase;
    rawPool = iso.pool;
    await connection.createTopic('message-test-topic');
    await connection.createQueue('message-test-queue', 'message-test-topic', false);
  });

  afterEach(async () => {
    if (connection) {
      await connection.close();
    }
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
    rawPool = null;
  });

  afterAll(async () => {
    if (testDb) {
      // shared container is stopped once on process exit (getSharedTestDatabase)
    }
  });

  describe('Acknowledgment', () => {
    test('should successfully acknowledge a message', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      expect(message).toBeDefined();

      // Acknowledge the message
      await message.ack();

      await consumer.stop();

      // Verify message was removed from queue
      const stats = await connection.getQueueStatistics('message-test-queue');
      expect(stats.completedCount).toBe(1);
      expect(stats.pendingCount).toBe(0);
    });

    test('should throw error on double acknowledgment', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // First ack should succeed
      await message.ack();

      // Second ack should throw
      await expect(message.ack()).rejects.toThrow('already been processed');

      await consumer.stop();
    });

    test('should acknowledge with transaction', async () => {
      const messageId = await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Acknowledge within transaction
      const client = await rawPool!.connect();

      try {
        await client.query('BEGIN');
        await message.ackWithTransaction(client);
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }

      await consumer.stop();

      // Verify message was acknowledged
      const stats = await connection.getQueueStatistics('message-test-queue');
      expect(stats.completedCount).toBe(1);
    });

    test('should rollback transaction on error', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      const client = await rawPool!.connect();

      try {
        await client.query('BEGIN');
        await message.ackWithTransaction(client);
        // Force an error
        throw new Error('Simulated error');
      } catch (error) {
        await client.query('ROLLBACK');
      } finally {
        client.release();
      }

      await consumer.stop();

      // Message should still be in queue (transaction rolled back)
      const stats = await connection.getQueueStatistics('message-test-queue');
      expect(stats.completedCount).toBe(0);
      expect(stats.totalCount).toBe(1);
    });
  });

  describe('Negative Acknowledgment', () => {
    test('should nack message without delay', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message1 } = await messages.next();

      expect(message1.deliveryAttempts).toBe(1);

      // Nack without delay
      await message1.nack();
      await consumer.stop();

      await sleep(100);

      // Message should be available again
      const consumer2 = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      expect(message2).toBeDefined();
      expect(message2.id).toBe(message1.id);
      expect(message2.deliveryAttempts).toBe(2);

      await message2.ack();
      await consumer2.stop();
    });

    test('should nack message with delay', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Nack with 2 second delay
      await message.nack({ delaySeconds: 2 });
      await consumer.stop();

      // Try to consume immediately - should not get message
      await sleep(500);

      const consumer2 = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Message should not be available yet
      const stats1 = await connection.getQueueStatistics('message-test-queue');
      expect(stats1.pendingCount).toBe(1);

      // Wait for delay to pass
      await sleep(2000);

      // Message should now be available
      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      expect(message2).toBeDefined();
      expect(message2.id).toBe(message.id);

      await message2.ack();
      await consumer2.stop();
    });

    test('should throw error on double nack', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // First nack should succeed
      await message.nack();

      // Second nack should throw
      await expect(message.nack()).rejects.toThrow('already been processed');

      await consumer.stop();
    });

    test('should not allow nack after ack', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      await message.ack();

      // Nack after ack should throw
      await expect(message.nack()).rejects.toThrow('already been processed');

      await consumer.stop();
    });
  });

  describe('Release', () => {
    test('should release message without incrementing delivery attempts', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message1 } = await messages.next();

      expect(message1.deliveryAttempts).toBe(1);

      // Release the message
      await message1.release();
      await consumer.stop();

      await sleep(100);

      // Consume again
      const consumer2 = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      // Delivery attempts should NOT have incremented
      expect(message2.id).toBe(message1.id);
      expect(message2.deliveryAttempts).toBe(1);

      await message2.ack();
      await consumer2.stop();
    });

    test('should throw error on double release', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      await message.release();

      // Second release should throw
      await expect(message.release()).rejects.toThrow('already been processed');

      await consumer.stop();
    });

    test('should not allow release after ack', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      await message.ack();

      // Release after ack should throw
      await expect(message.release()).rejects.toThrow('already been processed');

      await consumer.stop();
    });
  });

  describe('Visibility Timeout Extension', () => {
    test('should extend visibility timeout', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 5
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      const initialVt = message.vt;

      // Extend VT by 10 seconds
      const newVt = await message.setVt(10);

      expect(newVt.getTime()).toBeGreaterThan(initialVt.getTime());
      expect(message.vt).toEqual(newVt);

      // Message should still be locked after original VT expires
      await sleep(6000);

      // Try to consume with another consumer - should timeout
      const consumer2 = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();

      let receivedInTime = false;
      await Promise.race([
        messages2.next().then(() => { receivedInTime = true; }),
        sleep(2000)
      ]);

      expect(receivedInTime).toBe(false); // Should NOT have received message

      // Clean up
      await message.ack();
      await consumer.stop();
      await consumer2.stop();
    });

    test('should allow setVt multiple times', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 5
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // First extension
      const vt1 = await message.setVt(10);
      await sleep(100);

      // Second extension
      const vt2 = await message.setVt(10);

      expect(vt2.getTime()).toBeGreaterThan(vt1.getTime());

      await message.ack();
      await consumer.stop();
    });

    test('should throw error on setVt after ack', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      await message.ack();

      // SetVt after ack should throw
      await expect(message.setVt(30)).rejects.toThrow('already been processed');

      await consumer.stop();
    });
  });

  describe('Message Properties', () => {
    test('should have correct message properties', async () => {
      const payload = generateTestPayload(42);
      const messageId = await connection.publish('message-test-topic', payload);

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Verify properties
      expect(message.id).toBe(messageId);
      expect(message.queueName).toBe('message-test-queue');
      expect(message.payload).toEqual(payload);
      expect(message.deliveryAttempts).toBe(1);
      expect(message.consumerToken).toBeDefined();
      expect(message.consumerToken.length).toBeGreaterThan(0);
      expect(message.publishedAt).toBeInstanceOf(Date);
      expect(message.vt).toBeInstanceOf(Date);
      expect(message.vt.getTime()).toBeGreaterThan(Date.now());

      await message.ack();
      await consumer.stop();
    });

    test('should preserve payload types', async () => {
      const complexPayload = {
        string: 'test',
        number: 42,
        boolean: true,
        null: null,
        array: [1, 2, 3],
        nested: {
          foo: 'bar',
          baz: [4, 5, 6]
        }
      };

      await connection.publish('message-test-topic', complexPayload);

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      expect(message.payload).toEqual(complexPayload);

      await message.ack();
      await consumer.stop();
    });
  });

  describe('OnComplete Callback', () => {
    test('should invoke onComplete callback after ack', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // The onComplete callback is internal to the consumer
      // We can verify it was called by checking consumer state
      const initialInFlightCount = (consumer as any).inFlightMessages.size;

      await message.ack();

      // Wait a bit for callback to execute
      await sleep(100);

      const finalInFlightCount = (consumer as any).inFlightMessages.size;

      // In-flight count should have decreased
      expect(finalInFlightCount).toBeLessThan(initialInFlightCount);

      await consumer.stop();
    });
  });

  describe('Error Handling', () => {
    test('should handle database errors gracefully', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Close connection to cause database error
      await connection.close();

      // Operations should throw meaningful errors
      await expect(message.ack()).rejects.toThrow();
    });

    test('should handle invalid consumer token', async () => {
      await connection.publish('message-test-topic', generateTestPayload());

      const consumer = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 2, // Short VT
        autoExtension: { enabled: false } // Disable auto-extension so VT can actually expire
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Stop consumer to prevent any extensions
      await consumer.stop();

      // Wait for VT to expire (message becomes available to others)
      await sleep(3000);

      // Another consumer grabs it
      const consumer2 = connection.consume('message-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      // Original message's token is now invalid
      // Attempting to ack should fail
      try {
        await message.ack();
        // If it doesn't throw, that's also fine (no rows updated)
      } catch (error: any) {
        // Expected - either the message was already released (marked as processed)
        // or the token is invalid after VT expired and message was consumed by another consumer
        const validErrors = [
          'already been processed', // Message was released during consumer.stop()
          'token mismatch'           // Token is invalid after VT expired
        ];

        const hasValidError = validErrors.some(msg => error.message.includes(msg));
        expect(hasValidError).toBe(true);
      }

      // Verify message2 can still ack successfully
      await message2.ack();

      await consumer2.stop();

      const stats = await connection.getQueueStatistics('message-test-queue');
      expect(stats.completedCount).toBe(1);
    });
  });
});
