/**
 * Typed-error tests
 *
 * Verifies that server-side SQLSTATE codes (PMQ01/02/03) are surfaced as
 * the matching typed error class on the client.
 */
import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import {
  TestDatabase,
  getSharedTestDatabase,
  createIsolatedTestConnection,
  generateTestPayload,
  sleep,
} from './helpers';
import { Connection } from '../connection';
import {
  LeaseLostError,
  QueueNotFoundError,
  ValidationError,
} from '../errors';

describe('Typed errors (SQLSTATE → class mapping)', () => {
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

  describe('LeaseLostError (PMQ01)', () => {
    test('ackMessage with stale token surfaces as LeaseLostError', async () => {
      // The Consumer abstraction couples message lifecycle with internal
      // polling/processed tracking, which complicates "lease lost" tests.
      // Drive the connection-level wrappers directly.
      await connection.createTopic('LLTopic');
      await connection.createQueue('LLQueue', 'LLTopic', false);
      const messageId = await connection.publish('LLTopic', generateTestPayload());

      // Consume the message (we don't care about the actual rows; we just
      // want to confirm a valid token exists, then attempt with a wrong one).
      const rows = await connection.consumeMessages('LLQueue', 30, 1);
      expect(rows).toHaveLength(1);

      // Ack with a deliberately wrong token → PMQ01 → LeaseLostError.
      await expect(
        connection.ackMessage('LLQueue', messageId, 'wrong-token')
      ).rejects.toBeInstanceOf(LeaseLostError);
    });

    test('nackMessage with stale token surfaces as LeaseLostError', async () => {
      await connection.createTopic('LLNackTopic');
      await connection.createQueue('LLNackQueue', 'LLNackTopic', false);
      const messageId = await connection.publish('LLNackTopic', generateTestPayload());
      await connection.consumeMessages('LLNackQueue', 30, 1);

      await expect(
        connection.nackMessage('LLNackQueue', messageId, 'wrong-token')
      ).rejects.toBeInstanceOf(LeaseLostError);
    });

    test('setMessageVt with stale token surfaces as LeaseLostError', async () => {
      await connection.createTopic('LLSetVtTopic');
      await connection.createQueue('LLSetVtQueue', 'LLSetVtTopic', false);
      const messageId = await connection.publish('LLSetVtTopic', generateTestPayload());
      await connection.consumeMessages('LLSetVtQueue', 30, 1);

      await expect(
        connection.setMessageVt('LLSetVtQueue', messageId, 'wrong-token', 60)
      ).rejects.toBeInstanceOf(LeaseLostError);
    });
  });

  describe('QueueNotFoundError (PMQ02)', () => {
    test('publish to a non-existent topic surfaces as QueueNotFoundError', async () => {
      await expect(
        connection.publish('GhostTopic', generateTestPayload())
      ).rejects.toBeInstanceOf(QueueNotFoundError);
    });
  });

  describe('ValidationError (PMQ03)', () => {
    test('setMessageVt with negative seconds surfaces as ValidationError', async () => {
      await connection.createTopic('VTopic');
      await connection.createQueue('VQueue', 'VTopic', false);
      const messageId = await connection.publish('VTopic', generateTestPayload());
      const rows = await connection.consumeMessages('VQueue', 30, 1);
      const token = rows[0].consumer_token;

      await expect(
        connection.setMessageVt('VQueue', messageId, token, -1)
      ).rejects.toBeInstanceOf(ValidationError);
    });

    test('consumeMessages with negative vt surfaces as ValidationError', async () => {
      await connection.createTopic('NVTopic');
      await connection.createQueue('NVQueue', 'NVTopic', false);
      await expect(
        connection.consumeMessages('NVQueue', -1, 1)
      ).rejects.toBeInstanceOf(ValidationError);
    });

    test('createTopic with invalid name surfaces as ValidationError', async () => {
      await expect(connection.createTopic('bad name with spaces')).rejects.toBeInstanceOf(
        ValidationError
      );
    });

    test('createQueue with invalid name surfaces as ValidationError', async () => {
      await connection.createTopic('VtTopic2');
      await expect(
        connection.createQueue('bad queue', 'VtTopic2', false)
      ).rejects.toBeInstanceOf(ValidationError);
    });

    test('createQueue with mismatched params on existing queue surfaces as ValidationError', async () => {
      // Re-creating with identical params is a no-op success (idempotent).
      // Re-creating with any parameter different raises PMQ03 with the
      // existing values surfaced.
      await connection.createTopic('VtTopic3');
      await connection.createQueue('Excl', 'VtTopic3', true, {
        maxDeliveryAttempts: 2,
      });
      await expect(
        connection.createQueue('Excl', 'VtTopic3', true, {
          maxDeliveryAttempts: 5,
        })
      ).rejects.toBeInstanceOf(ValidationError);
    });
  });

  describe('Error code field', () => {
    test('typed errors carry a `code` field with the SQLSTATE', async () => {
      try {
        await connection.publish('NoSuchTopic', generateTestPayload());
        throw new Error('should have rejected');
      } catch (err: any) {
        expect(err).toBeInstanceOf(QueueNotFoundError);
        expect(err.code).toBe('PMQ02');
      }
    });
  });
});
