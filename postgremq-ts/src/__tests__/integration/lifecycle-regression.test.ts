import { test, expect, jest } from '@jest/globals';
import { Connection } from '../../connection';
import { getSharedTestDatabase, createIsolatedTestConnection, sleep } from '../helpers';
import { createDeferred } from '../../utils';

test('stopping a handler cannot release active work or auto-ack its cancelled return', async () => {
  const iso = await createIsolatedTestConnection(await getSharedTestDatabase());
  const c = iso.connection;
  const started = createDeferred<void>();
  const finish = createDeferred<void>();
  try {
    await c.createTopic('t');
    await c.createQueue('q', 't', false);
    const id = await c.publish('t', {});
    const hc = c.consumeHandler(
      'q',
      async () => {
        started[1]();
        await finish[0];
      },
      { batchSize: 1, maxInFlight: 1, visibilityTimeoutSec: 2 }
    );
    await started[0];
    const stop = hc.stop();
    await sleep(2300);
    const during = await iso.pool.query(
      'SELECT status,delivery_attempts,vt>clock_timestamp() AS live FROM postgremq.queue_messages WHERE message_id=$1',
      [id]
    );
    expect(during.rows[0]).toEqual({ status: 'processing', delivery_attempts: 1, live: true });
    finish[1]();
    await stop;
    const after = await iso.pool.query(
      'SELECT status,delivery_attempts FROM postgremq.queue_messages WHERE message_id=$1',
      [id]
    );
    expect(after.rows[0]).toEqual({ status: 'pending', delivery_attempts: 1 });
  } finally {
    finish[1]();
    await c.close();
    await iso.dropDatabase();
  }
});

test('an old delivery finishing leaves a redelivery on the same connection tracked', async () => {
  const iso = await createIsolatedTestConnection(await getSharedTestDatabase());
  const c = iso.connection;
  try {
    await c.createTopic('t');
    await c.createQueue('q', 't', false);
    await c.publish('t', {});
    const consumer = c.consume('q', {
      batchSize: 1,
      visibilityTimeoutSec: 10,
      pollingIntervalMs: 50,
    });
    const iterator = consumer.messages();
    const old = (await iterator.next()).value;
    await iso.pool.query("UPDATE postgremq.queue_messages SET vt=clock_timestamp()-interval '1 second'");
    const fresh = (await iterator.next()).value;
    expect(fresh.id).toBe(old.id);
    expect(fresh.consumerToken).not.toBe(old.consumerToken);
    await expect(old.ack()).rejects.toThrow();
    const entries = Array.from((c as any).extenderEntries.values()) as any[];
    expect(entries.some((e) => e.token === fresh.consumerToken)).toBe(true);
    await fresh.ack();
    await consumer.stop();
  } finally {
    await c.close();
    await iso.dropDatabase();
  }
});
