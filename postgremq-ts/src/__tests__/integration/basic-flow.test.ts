/**
 * Basic Integration Tests
 * Tests the complete message flow from publish to consume
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  generateTestPayload,
  sleep
} from '../helpers';
import { Connection } from '../../connection';

describe('Basic Message Flow', () => {
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
  });

  afterEach(async () => {
    if (connection) {
      await connection.close();
    }
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
  });

  afterAll(async () => {
    if (testDb) {
      // shared container is stopped once on process exit (getSharedTestDatabase)
    }
  });

  test('should publish and consume a single message', async () => {
    // Create topic and queue
    await connection.createTopic('test-topic');
    await connection.createQueue('test-queue', 'test-topic', false);

    // Publish a message
    const payload = generateTestPayload(1);
    const messageId = await connection.publish('test-topic', payload);

    expect(messageId).toBeGreaterThan(0);

    // Consume the message
    const consumer = connection.consume('test-queue', {
      batchSize: 1,
      visibilityTimeoutSec: 30
    });

    const messages = consumer.messages();
    const { value: message } = await messages.next();

    expect(message).toBeDefined();
    expect(message.id).toBe(messageId);
    expect(message.payload).toEqual(payload);
    expect(message.deliveryAttempts).toBe(1);

    // Acknowledge the message
    await message.ack();

    // Stop consumer
    await consumer.stop();

    // Verify message was acknowledged
    const stats = await connection.getQueueStatistics('test-queue');
    expect(stats.completedCount).toBe(1);
    expect(stats.pendingCount).toBe(0);
  });

  test('should handle multiple messages in order', async () => {
    await connection.createTopic('multi-topic');
    await connection.createQueue('multi-queue', 'multi-topic', false);

    // Publish multiple messages
    const payloads = [
      generateTestPayload(1),
      generateTestPayload(2),
      generateTestPayload(3)
    ];

    const messageIds: number[] = [];
    for (const payload of payloads) {
      const id = await connection.publish('multi-topic', payload);
      messageIds.push(id);
    }

    // Consume all messages
    const consumer = connection.consume('multi-queue', {
      batchSize: 10,
      visibilityTimeoutSec: 30
    });

    const receivedMessages: any[] = [];
    const messages = consumer.messages();

    for (let i = 0; i < 3; i++) {
      const { value: message } = await messages.next();
      expect(message).toBeDefined();
      receivedMessages.push(message);
      await message.ack();
    }

    await consumer.stop();

    // Verify all messages were received in order
    expect(receivedMessages.length).toBe(3);
    expect(receivedMessages[0].payload).toEqual(payloads[0]);
    expect(receivedMessages[1].payload).toEqual(payloads[1]);
    expect(receivedMessages[2].payload).toEqual(payloads[2]);
  });

  test('should support delayed message delivery', async () => {
    await connection.createTopic('delayed-topic');
    await connection.createQueue('delayed-queue', 'delayed-topic', false);

    const payload = generateTestPayload();
    const deliverAfter = new Date(Date.now() + 2000); // 2 seconds from now

    // Publish delayed message
    const messageId = await connection.publish('delayed-topic', payload, {
      deliverAfter
    });

    // Try to consume immediately - should not get the message yet
    const consumer = connection.consume('delayed-queue', {
      batchSize: 1,
      visibilityTimeoutSec: 30
    });

    // Wait a bit but not long enough
    await sleep(500);

    // Check that no message is available yet
    const stats1 = await connection.getQueueStatistics('delayed-queue');
    expect(stats1.pendingCount).toBe(1);

    // Now wait for the delay to pass
    await sleep(2000);

    // Message should now be available
    const messages = consumer.messages();
    const { value: message } = await messages.next();

    expect(message).toBeDefined();
    expect(message.id).toBe(messageId);
    expect(message.payload).toEqual(payload);

    await message.ack();
    await consumer.stop();
  });

  test('should handle nack and redelivery', async () => {
    await connection.createTopic('nack-topic');
    await connection.createQueue('nack-queue', 'nack-topic', false);

    const payload = generateTestPayload();
    await connection.publish('nack-topic', payload);

    // Use the same consumer for both attempts (like the Go test does)
    const consumer = connection.consume('nack-queue', {
      batchSize: 1,
      visibilityTimeoutSec: 30,
      autoExtension: { enabled: false }
    });

    const messages = consumer.messages();

    // First attempt - get and nack the message
    const { value: message1 } = await messages.next();
    expect(message1.deliveryAttempts).toBe(1);

    // Nack the message - should be immediately available
    await message1.nack();

    // Second attempt - should receive the same message with same consumer
    const { value: message2 } = await messages.next();

    expect(message2.id).toBe(message1.id);
    expect(message2.deliveryAttempts).toBe(2); // Incremented

    await message2.ack();
    await consumer.stop();
  }, 10000);

  test('should handle message release', async () => {
    await connection.createTopic('release-topic');
    await connection.createQueue('release-queue', 'release-topic', false);

    const payload = generateTestPayload();
    await connection.publish('release-topic', payload);

    // Use the same consumer for both attempts
    const consumer = connection.consume('release-queue', {
      batchSize: 1,
      visibilityTimeoutSec: 30,
      autoExtension: { enabled: false }
    });

    const messages = consumer.messages();

    // First attempt - get and release the message
    const { value: message1 } = await messages.next();
    expect(message1.deliveryAttempts).toBe(1);

    // Release the message - should be immediately available
    await message1.release();

    // Second attempt - should receive the same message with same consumer
    const { value: message2 } = await messages.next();

    expect(message2.id).toBe(message1.id);
    expect(message2.deliveryAttempts).toBe(1); // Should still be 1

    await message2.ack();
    await consumer.stop();
  }, 10000);

  test('should support exclusive queues', async () => {
    await connection.createTopic('exclusive-topic');
    await connection.createQueue('exclusive-queue', 'exclusive-topic', true, {
      keepAliveInterval: 30
    });

    // Publish and consume
    const payload = generateTestPayload();
    await connection.publish('exclusive-topic', payload);

    const consumer = connection.consume('exclusive-queue', {
      batchSize: 1,
      visibilityTimeoutSec: 30
    });

    const messages = consumer.messages();
    const { value: message } = await messages.next();

    expect(message).toBeDefined();
    await message.ack();
    await consumer.stop();

    // Verify queue info shows exclusive flag
    const queues = await connection.listQueues();
    const exclusiveQueue = queues.find(q => q.queueName === 'exclusive-queue');

    expect(exclusiveQueue).toBeDefined();
    expect(exclusiveQueue!.exclusive).toBe(true);
    expect(exclusiveQueue!.keepAliveUntil).toBeDefined();
  });
});
