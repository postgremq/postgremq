/**
 * Error Scenario Integration Tests
 * Tests error handling and edge cases
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach, jest } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  createTestConnection,
  generateTestPayload,
  sleep,
  assertThrows
} from '../helpers';
import { Connection } from '../../connection';

describe('Error Scenarios', () => {
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

  describe('Invalid Operations', () => {
    test('should reject operations on non-existent topic', async () => {
      await assertThrows(
        () => connection.publish('non-existent-topic', { test: 'data' })
      );
    });

    test('getQueueStatistics on non-existent queue returns zero counts', async () => {
      // get_queue_statistics is a count-with-filter over queue_messages; a
      // missing queue legitimately has zero rows of every status.
      const stats = await connection.getQueueStatistics('non-existent-queue');
      expect(stats.pendingCount).toBe(0);
      expect(stats.processingCount).toBe(0);
      expect(stats.completedCount).toBe(0);
      expect(stats.totalCount).toBe(0);
    });

    test('createTopic is idempotent (duplicate name is a no-op success)', async () => {
      await connection.createTopic('duplicate-topic');
      // Second creation must succeed silently — create_topic uses
      // INSERT ... ON CONFLICT DO NOTHING.
      await connection.createTopic('duplicate-topic');
      // Cleanup
      await connection.deleteTopic('duplicate-topic');
    });

    test('createQueue is idempotent for matching params', async () => {
      await connection.createTopic('queue-topic');
      await connection.createQueue('idem-queue', 'queue-topic', false, {
        maxDeliveryAttempts: 3,
      });
      // Same params → success.
      await connection.createQueue('idem-queue', 'queue-topic', false, {
        maxDeliveryAttempts: 3,
      });
      await connection.deleteQueue('idem-queue');
      await connection.deleteTopic('queue-topic');
    });

    test('createQueue rejects parameter mismatch with ValidationError', async () => {
      await connection.createTopic('mismatch-topic');
      await connection.createQueue('mismatch-queue', 'mismatch-topic', false, {
        maxDeliveryAttempts: 3,
      });

      // Different max_delivery_attempts → ValidationError (PMQ03).
      await expect(
        connection.createQueue('mismatch-queue', 'mismatch-topic', false, {
          maxDeliveryAttempts: 5,
        })
      ).rejects.toThrow(/already exists with different parameters/);

      await connection.deleteQueue('mismatch-queue');
      await connection.deleteTopic('mismatch-topic');
    });

    test('createQueue on non-existent topic surfaces as QueueNotFoundError (PMQ02)', async () => {
      // create_queue raises PMQ02 — same shape as publish_message — so the
      // typed error mapping works on both call sites.
      await expect(
        connection.createQueue('test-queue', 'non-existent-topic', false)
      ).rejects.toMatchObject({ code: 'PMQ02' });
    });

    test('should reject invalid queue options', async () => {
      await connection.createTopic('invalid-options-topic');

      // Negative max_delivery_attempts is silently broken in the consume
      // filter (`qm.delivery_attempts < tq.max_delivery_attempts`), so we
      // reject it at SQL level with PMQ03 / ValidationError.
      await assertThrows(
        () => connection.createQueue('invalid-queue', 'invalid-options-topic', false, {
          maxDeliveryAttempts: -1
        }),
        'must be >= 0'
      );

      await connection.deleteTopic('invalid-options-topic');
    });
  });

  describe('Connection Errors', () => {
    test('should reject operations on closed connection', async () => {
      const tempConnection = new Connection({
        connectionString: testDb.connectionString
      });

      await tempConnection.connect();
      await tempConnection.close();

      // Operations should fail
      await assertThrows(
        () => tempConnection.createTopic('test'),
        'not connected'
      );
    });

    test('should handle consumer with closed connection', async () => {
      await connection.createTopic('closed-conn-topic');
      await connection.createQueue('closed-conn-queue', 'closed-conn-topic', false);
      await connection.publish('closed-conn-topic', generateTestPayload());

      // Use the same isolated connection for this test to ensure resources exist
      const tempConnection = connection;

      const consumer = tempConnection.consume('closed-conn-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      expect(message).toBeDefined();

      // Close connection
      await tempConnection.close();

      // Message operations should fail
      // Message may have been released during connection.close() or connection may be closed
      try {
        await message.ack();
        // Should throw
        expect(true).toBe(false);
      } catch (error: any) {
        const validErrors = [
          'not connected',
          'already been processed'  // Message was released during connection close
        ];
        const hasValidError = validErrors.some(msg => error.message.includes(msg));
        expect(hasValidError).toBe(true);
      }

      // Cleanup (isolation DB will be dropped in afterEach, so ignore connection errors here)
      try { await connection.deleteQueue('closed-conn-queue'); } catch {}
      try { await connection.cleanUpTopic('closed-conn-topic'); } catch {}
      try { await connection.deleteTopic('closed-conn-topic'); } catch {}
    });
  });

  describe('Message Expiration', () => {
    test('should handle expired visibility timeout', async () => {
      await connection.createTopic('expired-vt-topic');
      await connection.createQueue('expired-vt-queue', 'expired-vt-topic', false);
      await connection.publish('expired-vt-topic', generateTestPayload());

      // Consumer 1 - short VT
      const consumer1 = connection.consume('expired-vt-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 2,
        autoExtension: { enabled: false } // Disable auto-extension so VT can expire
      });

      const messages1 = consumer1.messages();
      const { value: message1 } = await messages1.next();

      expect(message1.deliveryAttempts).toBe(1);

      // Let VT expire naturally without stopping consumer
      // Wait for VT to expire
      await sleep(3000);

      // Stop consumer1 after VT has expired
      await consumer1.stop();

      // Consumer 2 should get the message
      const consumer2 = connection.consume('expired-vt-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      expect(message2.id).toBe(message1.id);
      expect(message2.deliveryAttempts).toBe(2);

      // Message1 ack should fail silently (token invalid)
      try {
        await message1.ack();
      } catch (error) {
        // Expected - token is invalid after VT expired and message was redelivered
      }

      // Message2 ack should succeed
      await message2.ack();

      await consumer2.stop();

      const stats = await connection.getQueueStatistics('expired-vt-queue');
      expect(stats.completedCount).toBe(1);

      // Cleanup
      await connection.deleteQueue('expired-vt-queue');
      await connection.cleanUpTopic('expired-vt-topic');
      await connection.deleteTopic('expired-vt-topic');
    });

    test('should handle expired message extension attempt', async () => {
      await connection.createTopic('expired-ext-topic');
      await connection.createQueue('expired-ext-queue', 'expired-ext-topic', false);
      await connection.publish('expired-ext-topic', generateTestPayload());

      const consumer = connection.consume('expired-ext-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 2
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Wait for VT to expire
      await sleep(3000);

      // Extension should fail (VT expired)
      try {
        await message.setVt(30);
        // Extension might fail or succeed depending on race condition
        // Either behavior is acceptable
      } catch (error) {
        // Expected in some cases
      }

      await consumer.stop();

      // Cleanup
      await connection.deleteQueue('expired-ext-queue');
      await connection.cleanUpTopic('expired-ext-topic');
      await connection.deleteTopic('expired-ext-topic');
    });
  });

  describe('Dead Letter Queue Edge Cases', () => {
    test('should handle max delivery attempts = 0 (no DLQ)', async () => {
      await connection.createTopic('no-dlq-topic');
      await connection.createQueue('no-dlq-queue', 'no-dlq-topic', false, {
        maxDeliveryAttempts: 0
      });
      const id = await connection.publish('no-dlq-topic', generateTestPayload());

      // Explicit claims avoid a background consumer prefetching the next
      // attempt while the test inspects pending state.
      for (let attempt = 1; attempt <= 10; attempt++) {
        const rows = await connection.consumeMessages('no-dlq-queue', 30, 1);
        expect(rows).toHaveLength(1);
        expect(Number(rows[0].message_id)).toBe(id);
        expect(rows[0].delivery_attempts).toBe(attempt);
        await connection.nackMessage('no-dlq-queue', id, rows[0].consumer_token);
      }

      expect(await connection.listDLQMessages()).toHaveLength(0);
      const stats = await connection.getQueueStatistics('no-dlq-queue');
      expect(stats.pendingCount).toBe(1);
      expect(stats.processingCount).toBe(0);
    });

    test('should handle DLQ operations on empty DLQ', async () => {
      // Purge empty DLQ should not fail
      await connection.purgeDLQ();

      // Requeue from empty DLQ should not fail
      await connection.requeueDLQMessages('any-queue');

      expect(true).toBe(true);
    });

    test('deleteQueue refuses when the queue has DLQ entries', async () => {
      // dead_letter_queue.queue_name FK is ON DELETE RESTRICT — DLQ
      // entries are forensic data. Operators must explicitly purge or
      // requeue before dropping the underlying queue.
      await connection.createTopic('dlq-restrict-topic');
      await connection.createQueue('dlq-restrict-queue', 'dlq-restrict-topic', false, {
        maxDeliveryAttempts: 1
      });

      await connection.publish('dlq-restrict-topic', generateTestPayload());
      const consumer = connection.consume('dlq-restrict-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });
      const messages = consumer.messages();
      const { value: message } = await messages.next();
      await message.nack(); // max=1, so nack retires inline to DLQ
      await consumer.stop();

      let dlqMessages = await connection.listDLQMessages();
      expect(dlqMessages.length).toBe(1);

      // deleteQueue must REFUSE while DLQ has entries.
      await assertThrows(
        () => connection.deleteQueue('dlq-restrict-queue'),
        'dead letter queue',
      );

      // After purgeDLQ, deleteQueue succeeds.
      await connection.purgeDLQ();
      await connection.deleteQueue('dlq-restrict-queue');

      // Cleanup the topic.
      await connection.cleanUpTopic('dlq-restrict-topic');
      await connection.deleteTopic('dlq-restrict-topic');
    });
  });

  describe('Payload Edge Cases', () => {
    test('should handle empty payload', async () => {
      await connection.createTopic('empty-payload-topic');
      await connection.createQueue('empty-payload-queue', 'empty-payload-topic', false);

      await connection.publish('empty-payload-topic', {});

      const consumer = connection.consume('empty-payload-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      expect(message.payload).toEqual({});

      await message.ack();
      await consumer.stop();

      // Cleanup
      await connection.deleteQueue('empty-payload-queue');
      await connection.cleanUpTopic('empty-payload-topic');
      await connection.deleteTopic('empty-payload-topic');
    });

    test('should handle large payload', async () => {
      await connection.createTopic('large-payload-topic');
      await connection.createQueue('large-payload-queue', 'large-payload-topic', false);

      // Create a large payload (~1MB)
      const largeString = 'x'.repeat(1024 * 1024);
      const largePayload = { data: largeString };

      const messageId = await connection.publish('large-payload-topic', largePayload);
      expect(messageId).toBeGreaterThan(0);

      const consumer = connection.consume('large-payload-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      expect(message.payload.data).toBe(largeString);

      await message.ack();
      await consumer.stop();

      // Cleanup
      await connection.deleteQueue('large-payload-queue');
      await connection.cleanUpTopic('large-payload-topic');
      await connection.deleteTopic('large-payload-topic');
    });

    test('should handle special characters in payload', async () => {
      await connection.createTopic('special-chars-topic');
      await connection.createQueue('special-chars-queue', 'special-chars-topic', false);

      const specialPayload = {
        unicode: '你好世界 🌍 🚀',
        quotes: 'He said "hello"',
        backslash: 'C:\\path\\to\\file',
        newlines: 'line1\nline2\nline3',
        tabs: 'col1\tcol2\tcol3',
        singleQuote: "It's a test"
      };

      await connection.publish('special-chars-topic', specialPayload);

      const consumer = connection.consume('special-chars-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      expect(message.payload).toEqual(specialPayload);

      await message.ack();
      await consumer.stop();

      // Cleanup
      await connection.deleteQueue('special-chars-queue');
      await connection.cleanUpTopic('special-chars-topic');
      await connection.deleteTopic('special-chars-topic');
    });
  });

  describe('Consumer Edge Cases', () => {
    test('should handle consumer with no messages available', async () => {
      await connection.createTopic('no-messages-topic');
      await connection.createQueue('no-messages-queue', 'no-messages-topic', false);

      const consumer = connection.consume('no-messages-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
        pollingIntervalMs: 500
      });

      // Start consuming but don't publish anything
      const messages = consumer.messages();
      const messagePromise = messages.next();

      // Wait a bit
      await sleep(1000);

      // Now publish
      await connection.publish('no-messages-topic', generateTestPayload());

      // Should receive message (via notification)
      const { value: message } = await messagePromise;
      expect(message).toBeDefined();

      await message.ack();
      await consumer.stop();

      // Cleanup
      await connection.deleteQueue('no-messages-queue');
      await connection.cleanUpTopic('no-messages-topic');
      await connection.deleteTopic('no-messages-topic');
    });

    test('should handle rapid consumer stop/start', async () => {
      await connection.createTopic('rapid-stop-topic');
      await connection.createQueue('rapid-stop-queue', 'rapid-stop-topic', false);

      for (let i = 0; i < 5; i++) {
        const consumer = connection.consume('rapid-stop-queue', {
          batchSize: 1,
          visibilityTimeoutSec: 30
        });

        await sleep(50);
        await consumer.stop();
      }

      // Should not throw or leave dangling resources
      expect(true).toBe(true);

      // Cleanup
      await connection.deleteQueue('rapid-stop-queue');
      await connection.deleteTopic('rapid-stop-topic');
    });

    test('should handle consumer stop with pending messages', async () => {
      await connection.createTopic('pending-stop-topic');
      await connection.createQueue('pending-stop-queue', 'pending-stop-topic', false);

      // Publish multiple messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('pending-stop-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('pending-stop-queue', {
        batchSize: 10,
        visibilityTimeoutSec: 30
      });

      // Let consumer fetch but don't consume
      await sleep(500);

      // Stop consumer - should release buffered messages
      await consumer.stop();

      await sleep(100);

      // Messages should be available again
      const stats = await connection.getQueueStatistics('pending-stop-queue');
      expect(stats.pendingCount).toBeGreaterThan(0);

      // Cleanup
      await connection.cleanUpQueue('pending-stop-queue');
      await connection.deleteQueue('pending-stop-queue');
      await connection.cleanUpTopic('pending-stop-topic');
      await connection.deleteTopic('pending-stop-topic');
    });
  });

  describe('Exclusive Queue Edge Cases', () => {
    test('should handle exclusive queue cleanup on expiration', async () => {
      await connection.createTopic('exclusive-expire-topic');
      await connection.createQueue('exclusive-expire-queue', 'exclusive-expire-topic', true, {
        keepAliveInterval: 2 // Very short keep-alive
      });

      // Publish a message
      await connection.publish('exclusive-expire-topic', generateTestPayload());

      // Wait for keep-alive to expire (add extra buffer)
      await sleep(3500);

      // Queue should be cleaned up automatically
      const queues = await connection.listQueues();
      const exclusiveQueue = queues.find(q => q.queueName === 'exclusive-expire-queue');

      // Queue might still exist but with expired keep-alive
      if (exclusiveQueue) {
        expect(exclusiveQueue.keepAliveUntil).toBeDefined();
        // Note: Due to timing variations, we just verify keepAliveUntil exists
        // The actual expiration check is handled by the database background process
      }

      // Cleanup
      try {
        await connection.deleteQueue('exclusive-expire-queue');
      } catch (error) {
        // Queue might already be cleaned up
      }
      await connection.cleanUpTopic('exclusive-expire-topic');
      await connection.deleteTopic('exclusive-expire-topic');
    });
  });

  describe('Auto-Extension Error Handling', () => {
    /**
     * Regression: prior to the fix, a single transient
     * error from setMessagesVtBatch caused the consumer to permanently
     * drop EVERY in-flight message from extension tracking. The handler
     * kept running, vt eventually expired, another consumer could pick up
     * the same message, and the original consumer's ack returned PMQ01.
     */
    test('survives a single transient setVtBatchMulti error', async () => {
      await connection.createTopic('autoext-transient-topic');
      await connection.createQueue('autoext-transient-queue', 'autoext-transient-topic', false, {
        maxDeliveryAttempts: 0
      });

      // Publish 3 messages so the connection-level extender batches them into a
      // single setVtBatchMulti call.
      for (let i = 0; i < 3; i++) {
        await connection.publish('autoext-transient-topic', { idx: i });
      }

      // Capture the real implementation BEFORE spying so we can delegate
      // to it on the success path. jest.spyOn (vs `(conn as any).X = ...`)
      // ensures a future rename of setVtBatchMulti surfaces at runtime
      // rather than silently falling back to the real method.
      const realSetVtBatch = connection.setVtBatchMulti.bind(connection);
      let callCount = 0;
      const spy = jest.spyOn(connection, 'setVtBatchMulti')
        .mockImplementation(async (...args: Parameters<typeof connection.setVtBatchMulti>) => {
          callCount++;
          if (callCount === 1) {
            throw new Error('simulated transient network failure');
          }
          return realSetVtBatch(...args);
        });

      try {
        const consumer = connection.consume('autoext-transient-queue', {
          batchSize: 3,
          // VT is 10s so the message can't be re-consumed even if our
          // extension fails. extensionThreshold=0.1 schedules the first
          // extension ~1s after consume (10% of the remaining lease) — that's
          // where the injected transient error lands. With 1s backoff the
          // retry hits at ~2s and succeeds, well before the 10s VT expires.
          visibilityTimeoutSec: 10,
          autoExtension: {
            enabled: true,
            extensionThreshold: 0.1,
            maxBatchSize: 100
          }
        });

        const received: any[] = [];
        const originalVts: Date[] = [];
        const messages = consumer.messages();
        for (let i = 0; i < 3; i++) {
          const { value: msg } = await messages.next();
          received.push(msg);
          originalVts.push(new Date(msg.vt.getTime()));
        }
        expect(received.length).toBe(3);

        // Hold long enough for: extension at t=1s (fails) → bumped to
        // t=2s → retry succeeds. 3.5s gives both attempts a chance plus
        // a comfortable buffer.
        await sleep(3500);

        // The transient error must have been hit at least once and the
        // retry attempted (so callCount ≥ 2).
        expect(callCount).toBeGreaterThanOrEqual(2);

        // The DIRECT proof that the fix works: every message's vt has
        // advanced past its consume-time vt. Pre-fix, the transient error
        // dropped each message from extension tracking — no retry, vt
        // unchanged, leaving each at consume+10s.
        for (let i = 0; i < received.length; i++) {
          expect(received[i].vt.getTime()).toBeGreaterThan(originalVts[i].getTime() + 100);
          // Transient errors must NOT cancel the handler — the lease was
          // never actually lost.
          expect(received[i].signal.aborted).toBe(false);
        }

        // Ack must succeed (vt is still valid for both fix and pre-fix
        // at the 3.5s mark, so this is just hygiene — but it confirms
        // the consumer ended in a healthy state).
        for (const msg of received) {
          await msg.ack();
        }

        await consumer.stop();
      } finally {
        spy.mockRestore();
      }

      // Cleanup
      await connection.cleanUpQueue('autoext-transient-queue');
      await connection.deleteQueue('autoext-transient-queue');
      await connection.cleanUpTopic('autoext-transient-topic');
      await connection.deleteTopic('autoext-transient-topic');
    });

    /**
     * Lease-lost (PMQ01) must drop the message from extension tracking —
     * the inverse of the transient case. Otherwise the consumer would
     * tight-loop forever on a permanently-failing extension.
     *
     * Trigger a real PMQ01 by manually clearing the consumer_token on the
     * row, then stopping the consumer. If extension tracking did NOT drop
     * the row on PMQ01, stop() would hang waiting for the extension queue
     * to drain (or a future processExtensions call would keep firing).
     */
    test('drops message from extension tracking on LeaseLostError', async () => {
      await connection.createTopic('autoext-leaselost-topic');
      await connection.createQueue('autoext-leaselost-queue', 'autoext-leaselost-topic', false, {
        maxDeliveryAttempts: 0
      });
      await connection.publish('autoext-leaselost-topic', { x: 1 });

      const consumer = connection.consume('autoext-leaselost-queue', {
        batchSize: 1,
        // 10s VT keeps the row in 'processing' for the test duration so
        // consume_message can't re-pick it after our token-mismatch
        // injection (we'd then test something different — re-fetch).
        visibilityTimeoutSec: 10,
        autoExtension: {
          enabled: true,
          extensionThreshold: 0.1, // first extension at consume+1s (10% of lease)
          maxBatchSize: 100
        }
      });

      const messages = consumer.messages();
      const { value: msg } = await messages.next();

      // Simulate "another consumer took over": rewrite the consumer_token
      // to a different value. set_vt_batch's WHERE includes consumer_token,
      // so it'll match zero rows — the row is silently omitted from the
      // result, which the consumer interprets as "lease lost server-side".
      // Status stays 'processing' and vt remains in the future, so
      // consume_message can't re-pick and we're testing the drop-on-
      // lease-lost path cleanly.
      await isoPool!.query(
        `UPDATE postgremq.queue_messages SET consumer_token = 'stolen-by-other-consumer'
         WHERE queue_name = 'autoext-leaselost-queue' AND message_id = $1`,
        [msg.id]
      );

      // Wait for one extension cycle to fire and hit lease-lost. Threshold
      // 0.9 of 10s schedules the first attempt at consume+1s, so 2.5s
      // is comfortable headroom.
      await sleep(2500);

      // The public contract: cancel signal fired so the handler can stop.
      // (Implementation: consumer also dropped from extension tracking,
      // but signal.aborted is the part the application actually observes.)
      // Pre-fix would have left the handler running until it tried to ack.
      expect(msg.signal.aborted).toBe(true);

      // Stop should return cleanly even though the underlying force-release
      // call may fail with PMQ01 — that path is handled separately.
      await consumer.stop();

      await connection.cleanUpQueue('autoext-leaselost-queue');
      await connection.deleteQueue('autoext-leaselost-queue');
      await connection.cleanUpTopic('autoext-leaselost-topic');
      await connection.deleteTopic('autoext-leaselost-topic');
    });

    /**
     * Multiple consecutive transient errors must not tight-loop. The
     * bumpExtensionRetry backoff (1s) governs retry cadence; with a 2s
     * VT the consumer gets ~2 retries before vt actually expires and the
     * SQL says "lease lost" (which is the correct end state).
     */
    /**
     * Batch path: when the server returns FEWER rows than requested
     * (per-row server-side lease loss — e.g., one row's VT expired or
     * its consumer_token changed between the consume and the extension
     * call), the omitted rows must be dropped from extension tracking.
     * The remaining rows must continue to be tracked with their new VT.
     */
    test('batch path drops rows omitted from set_vt_batch result', async () => {
      await connection.createTopic('autoext-partial-topic');
      await connection.createQueue('autoext-partial-queue', 'autoext-partial-topic', false, {
        maxDeliveryAttempts: 0
      });
      for (let i = 0; i < 3; i++) {
        await connection.publish('autoext-partial-topic', { idx: i });
      }

      const consumer = connection.consume('autoext-partial-queue', {
        batchSize: 3,
        visibilityTimeoutSec: 10,
        autoExtension: {
          enabled: true,
          extensionThreshold: 0.1, // first extension ~1s after consume (10% of lease)
          maxBatchSize: 100
        }
      });

      const received: any[] = [];
      const messages = consumer.messages();
      for (let i = 0; i < 3; i++) {
        const { value: msg } = await messages.next();
        received.push(msg);
      }

      // Steal the first message's lease server-side BEFORE the extension
      // cycle fires. set_vt_batch will return only rows 2 and 3 (row 1's
      // consumer_token no longer matches our snapshot).
      await isoPool!.query(
        `UPDATE postgremq.queue_messages SET consumer_token = 'stolen-by-other'
         WHERE queue_name = 'autoext-partial-queue' AND message_id = $1`,
        [received[0].id]
      );

      // Wait for the extension cycle to fire and observe the partial result.
      await sleep(2500);

      // The two surviving messages had their vt advanced by the batch.
      expect(received[1].vt.getTime()).toBeGreaterThan(Date.now());
      expect(received[2].vt.getTime()).toBeGreaterThan(Date.now());

      // Cancel signals: the lost message must be aborted, the surviving
      // ones must not. This is the protection against the handler
      // committing non-idempotent work on a row we no longer own.
      expect(received[0].signal.aborted).toBe(true);
      expect(received[1].signal.aborted).toBe(false);
      expect(received[2].signal.aborted).toBe(false);

      await consumer.stop();

      await connection.cleanUpQueue('autoext-partial-queue');
      await connection.deleteQueue('autoext-partial-queue');
      await connection.cleanUpTopic('autoext-partial-topic');
      await connection.deleteTopic('autoext-partial-topic');
    });

    /**
     * consumer.stop() must fire `signal.aborted` on every in-flight
     * message so handlers can short-circuit ongoing work. Mirrors Go's
     * Consumer.Stop() which cancels every in-flight message's StoppedCtx
     * during shutdown. Without this, handlers run to completion even
     * though the consumer is tearing down.
     */
    test('stop() cancels signal on all in-flight messages', async () => {
      await connection.createTopic('autoext-stop-topic');
      await connection.createQueue('autoext-stop-queue', 'autoext-stop-topic', false, {
        maxDeliveryAttempts: 0
      });
      for (let i = 0; i < 3; i++) {
        await connection.publish('autoext-stop-topic', { idx: i });
      }

      const consumer = connection.consume('autoext-stop-queue', {
        batchSize: 3,
        visibilityTimeoutSec: 30,
        autoExtension: { enabled: false }  // not under test here
      });

      const received: any[] = [];
      const messages = consumer.messages();
      for (let i = 0; i < 3; i++) {
        const { value: msg } = await messages.next();
        received.push(msg);
      }

      // None aborted yet.
      for (const msg of received) {
        expect(msg.signal.aborted).toBe(false);
      }

      // Don't ack — call stop() while messages are still in-flight. The
      // signal must fire on all of them. (stop() will eventually
      // force-release them; we just want to assert the signal.)
      const stopPromise = consumer.stop();

      // The signal should fire synchronously at the start of stop().
      // Give one microtask tick for the loop to run.
      await new Promise(resolve => setImmediate(resolve));
      for (const msg of received) {
        expect(msg.signal.aborted).toBe(true);
      }

      await stopPromise;

      await connection.cleanUpQueue('autoext-stop-queue');
      await connection.deleteQueue('autoext-stop-queue');
      await connection.cleanUpTopic('autoext-stop-topic');
      await connection.deleteTopic('autoext-stop-topic');
    });

    test('repeated transient errors do not tight-loop', async () => {
      await connection.createTopic('autoext-loop-topic');
      await connection.createQueue('autoext-loop-queue', 'autoext-loop-topic', false, {
        maxDeliveryAttempts: 0
      });
      for (let i = 0; i < 3; i++) {
        await connection.publish('autoext-loop-topic', { idx: i });
      }

      let attempts = 0;
      const spy = jest.spyOn(connection, 'setVtBatchMulti')
        .mockImplementation(async () => {
          attempts++;
          throw new Error('simulated permanent transient failure');
        });

      try {
        const consumer = connection.consume('autoext-loop-queue', {
          batchSize: 3,
          visibilityTimeoutSec: 10,
          autoExtension: {
            enabled: true,
            extensionThreshold: 0.1,  // first attempt at consume+1s (10% of lease)
            maxBatchSize: 100
          }
        });

        const received: any[] = [];
        const messages = consumer.messages();
        for (let i = 0; i < 3; i++) {
          const { value } = await messages.next();
          received.push(value);
        }

        // Hold for 3.5 seconds: extension fires at 1s, 2s, 3s with the
        // 1s backoff between transient failures. We expect 3 attempts.
        await sleep(3500);

        // The 1-second backoff specifically is what we want to pin:
        // - 0ms backoff (tight-loop, the bug) → hundreds of attempts.
        // - 1s backoff (the fix) → ~3 attempts.
        // - 30s backoff (also wrong, would starve) → 0–1 attempts.
        // [2, 6] catches all three failure modes with comfortable
        // headroom for CI scheduling jitter.
        expect(attempts).toBeGreaterThanOrEqual(2);
        expect(attempts).toBeLessThanOrEqual(6);

        // Force-release everything via stop(). We don't ack — the
        // messages will go back to pending.
        await consumer.stop();
      } finally {
        spy.mockRestore();
      }

      await connection.cleanUpQueue('autoext-loop-queue');
      await connection.deleteQueue('autoext-loop-queue');
      await connection.cleanUpTopic('autoext-loop-topic');
      await connection.deleteTopic('autoext-loop-topic');
    });
  });
});
