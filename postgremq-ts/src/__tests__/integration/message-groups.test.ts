/**
 * Message groups (ordered delivery) integration tests.
 */

import { describe, test, expect, beforeAll, beforeEach, afterEach } from '@jest/globals';
import { TestDatabase, getSharedTestDatabase, createIsolatedTestConnection, sleep } from '../helpers';
import { Connection } from '../../connection';
import { ValidationError } from '../../errors';
import { Pool } from 'pg';

/** Rejects after `ms` unless `work` settles first; the timer never outlives the race. */
async function within<T>(work: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out: ${what}`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

describe('Message groups', () => {
  let testDb: TestDatabase;
  let connection: Connection;
  let pool: Pool;
  let connectionString: string;
  let dropDb: (() => Promise<void>) | null = null;
  const extraConnections: Connection[] = [];

  beforeAll(async () => {
    testDb = await getSharedTestDatabase();
  });

  beforeEach(async () => {
    const iso = await createIsolatedTestConnection(testDb);
    connection = iso.connection;
    pool = iso.pool;
    connectionString = iso.connectionString;
    dropDb = iso.dropDatabase;
    await connection.createTopic('groups-topic');
    await connection.createQueue('groups-queue', 'groups-topic', false);
  });

  afterEach(async () => {
    for (const c of extraConnections.splice(0)) {
      await c.close().catch(() => undefined);
    }
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
  }, 20000);

  test('order holds within each group with two consumers', async () => {
    const other = new Connection({ connectionString, shutdownTimeoutMs: 5000 });
    await other.connect();
    extraConnections.push(other);

    const groups = ['g0', 'g1', 'g2', 'g3'];
    const plan: (string | undefined)[] = [];
    for (let i = 0; i < 80; i++) plan.push(groups[i % groups.length]);
    for (let i = 0; i < 16; i++) plan.push(undefined);
    plan.sort(() => Math.random() - 0.5);

    const consumers = [connection, other].map((c) =>
      c.consume('groups-queue', {
        topic: 'groups-topic',
        batchSize: 3,
        visibilityTimeoutSec: 30,
        pollingIntervalMs: 200,
      })
    );

    const received = new Map<string | null, number[]>();
    const seen = new Map<number, number>();
    let acked = 0;
    let finish: () => void = () => undefined;
    const allAcked = new Promise<void>((resolve) => (finish = resolve));

    const run = async (consumer: (typeof consumers)[number]) => {
      const messages = consumer.messages();
      for (;;) {
        const { value: msg, done } = await messages.next();
        if (done || !msg) return;
        // Record before acking: the successor is not claimable until this
        // ack commits, so the record order is the delivery order.
        const seqs = received.get(msg.groupKey) ?? [];
        seqs.push(msg.groupSeq ?? 0);
        received.set(msg.groupKey, seqs);
        seen.set(msg.id, (seen.get(msg.id) ?? 0) + 1);
        await sleep(Math.floor(Math.random() * 3));
        await msg.ack();
        if (++acked === plan.length) finish();
      }
    };
    const runners = consumers.map(run);

    for (let i = 0; i < plan.length; i++) {
      await connection.publish('groups-topic', { i }, plan[i] ? { groupKey: plan[i] } : {});
    }

    await within(allAcked, 60000, `all ${plan.length} messages acked`);
    await Promise.all(consumers.map((c) => c.stop()));
    await Promise.all(runners);

    for (const [id, n] of seen) {
      expect({ id, n }).toEqual({ id, n: 1 });
    }
    for (const g of groups) {
      expect(received.get(g)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
    }
    expect(received.get(null)).toHaveLength(16);
    expect(received.get(null)!.every((s) => s === 0)).toBe(true);
  }, 90000);

  test('acking the head wakes its successor promptly', async () => {
    await connection.publish('groups-topic', { n: 1 }, { groupKey: 'session-1' });
    await connection.publish('groups-topic', { n: 2 }, { groupKey: 'session-1' });

    const consumer = connection.consume('groups-queue', {
      batchSize: 5,
      visibilityTimeoutSec: 60,
      pollingIntervalMs: 30000,
    });
    try {
      const messages = consumer.messages();
      const { value: head } = await messages.next();
      expect(head!.groupKey).toBe('session-1');
      expect(head!.groupSeq).toBe(1);

      const pending = messages.next();
      let early: unknown = 'blocked';
      await within(pending, 500, 'expected').then(
        (r) => (early = r),
        () => undefined
      );
      expect(early).toBe('blocked');

      const ackedAt = Date.now();
      await head!.ack();
      const succ = (await within(pending, 10000, 'successor after head ack')).value!;
      expect(succ.groupSeq).toBe(2);
      expect(Date.now() - ackedAt).toBeLessThan(5000);
      await succ.ack();
    } finally {
      await consumer.stop();
    }
  }, 30000);

  test('publish options, transactional publish and inspection carry the group', async () => {
    const first = await connection.publish('groups-topic', { n: 1 }, { groupKey: 'A' });

    const client = await pool.connect();
    let second: number;
    try {
      await client.query('BEGIN');
      second = await connection.publishWithTransaction(client, 'groups-topic', { n: 2 }, {
        groupKey: 'A',
        deliverAfter: new Date(Date.now() - 1000),
      });
      await client.query('COMMIT');
    } finally {
      client.release();
    }
    const ungrouped = await connection.publish('groups-topic', { n: 3 });

    await expect(connection.publish('groups-topic', {}, { groupKey: '' })).rejects.toBeInstanceOf(
      ValidationError
    );

    const pm = await connection.getMessage(second);
    expect(pm!.groupKey).toBe('A');
    expect(pm!.groupSeq).toBe(2);

    const list = await connection.listMessages('groups-queue');
    const byId = new Map(list.map((m) => [m.messageId, m]));
    expect(byId.get(first)).toMatchObject({ groupKey: 'A', groupSeq: 1 });
    expect(byId.get(second)).toMatchObject({ groupKey: 'A', groupSeq: 2 });
    expect(byId.get(ungrouped)).toMatchObject({ groupKey: null, groupSeq: null });
  });
});
