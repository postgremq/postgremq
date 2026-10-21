/**
 * Clock-skew tests: lease renewal and keep-alive timing must not depend on the
 * application and database clocks agreeing. The database clock is shifted by
 * shadowing clock_timestamp() (the only clock the schema reads) for the
 * connections of one pool; the schema is installed through that pool, so its
 * column defaults use the shifted clock too.
 */
import { test, expect } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';
import { Pool } from 'pg';
import { Connection } from '../../connection';
import { Message } from '../../message';
import { createEmptyTestDatabase, getSharedTestDatabase, sleep } from '../helpers';

async function skewedDatabase(skewMs: number) {
  const db = await createEmptyTestDatabase(await getSharedTestDatabase());
  await db.pool.query(`
    CREATE SCHEMA skew;
    CREATE FUNCTION skew.clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE
      AS $$ SELECT pg_catalog.clock_timestamp() + ${skewMs} * interval '1 ms' $$;`);
  const skewed = new Pool({
    connectionString: db.connectionString,
    max: 10,
    options: '-c search_path=skew,pg_catalog,public',
  });
  skewed.on('error', () => {});
  await skewed.query(fs.readFileSync(path.join(__dirname, '../../../../mq/sql/latest.sql'), 'utf-8'));
  const { rows } = await skewed.query(
    'SELECT extract(epoch FROM clock_timestamp() - pg_catalog.clock_timestamp()) * 1000 AS offset'
  );
  expect(Math.abs(Number(rows[0].offset) - skewMs)).toBeLessThan(500);
  return {
    plain: db.pool,
    skewed,
    drop: async () => {
      await skewed.end();
      await db.dropDatabase();
    },
  };
}

/** Polls check for up to timeoutMs (default 1.5 s); true once it holds. */
async function eventually(check: () => Promise<boolean>, timeoutMs = 1500): Promise<boolean> {
  for (let i = 0; i < timeoutMs / 50; i++) {
    if (await check()) return true;
    await sleep(50);
  }
  return false;
}

async function withSkewedConsumer(
  skewMs: number,
  vtSec: number,
  body: (plain: Pool, message: Message) => Promise<void>
) {
  const db = await skewedDatabase(skewMs);
  const c = new Connection({ pool: db.skewed });
  try {
    await c.connect();
    await c.createTopic('t');
    await c.createQueue('q', 't', false);
    await c.publish('t', {});
    const consumer = c.consume('q', { batchSize: 1, visibilityTimeoutSec: vtSec });
    const { value: message } = await consumer.messages().next();
    try {
      await body(db.plain, message as Message);
    } finally {
      // Unsettled after a failed assertion, it would hold close() for the
      // shutdown timeout; after an ack this throws and is ignored.
      await (message as Message).release().catch(() => {});
    }
    await consumer.stop();
  } finally {
    await c.close();
    await db.drop();
  }
}

// A database clock ahead of the application must not delay renewal past the
// real expiry: the lease ends vt seconds after the claim, whatever the
// database's timestamps read on the application's clock.
test('a database clock ahead renews the lease before it expires', async () => {
  await withSkewedConsumer(10000, 3, async (plain, message) => {
    await sleep(4000); // past the unrenewed 3 s lease
    const { rows } = await plain.query(
      "SELECT vt > clock_timestamp() + interval '10 s' AS live FROM postgremq.queue_messages WHERE message_id = $1",
      [message.id]
    );
    expect(rows[0].live).toBe(true);
    expect(message.signal.aborted).toBe(false);
    await message.ack();
  });
});

// message.vt is on the application's clock, so a handler comparing it with
// Date.now() sees the lease it really has, not one 10 s longer.
test('a database clock ahead does not lengthen the lease end a message shows', async () => {
  await withSkewedConsumer(10000, 30, async (_plain, message) => {
    expect(Math.abs(message.vt.getTime() - Date.now() - 30000)).toBeLessThan(2000);
    const vt = await message.setVt(20);
    expect(Math.abs(vt.getTime() - Date.now() - 20000)).toBeLessThan(2000);
    expect(message.vt).toEqual(vt);
    await message.ack();
  });
});

test('a database clock ahead does not lengthen the renewed lease end a message shows', async () => {
  await withSkewedConsumer(10000, 2, async (_plain, message) => {
    const first = message.vt.getTime();
    expect(await eventually(async () => message.vt.getTime() > first, 3000)).toBe(true);
    expect(Math.abs(message.vt.getTime() - Date.now() - 2000)).toBeLessThan(1000);
    await message.ack();
  });
});

// A database clock behind the application must not make a live lease look
// expired: a busy renewal is retried within the lease, not reported lost.
test('a database clock behind keeps a busy lease', async () => {
  await withSkewedConsumer(-10000, 8, async (plain, message) => {
    await sleep(3000);
    const locker = await plain.connect();
    try {
      await locker.query('BEGIN');
      await locker.query('SELECT 1 FROM postgremq.queue_messages WHERE message_id = $1 FOR UPDATE', [
        message.id,
      ]);
      await sleep(1500); // the renewal due at 4 s reports busy meanwhile
      await locker.query('ROLLBACK');
    } finally {
      locker.release();
    }
    expect(
      await eventually(async () => {
        const { rows } = await plain.query(
          "SELECT vt > clock_timestamp() - interval '10 s' + interval '4 s' AS renewed FROM postgremq.queue_messages WHERE message_id = $1",
          [message.id]
        );
        return rows[0].renewed;
      }, 3000)
    ).toBe(true);
    expect(message.signal.aborted).toBe(false);
    await message.ack();
  });
});

async function withSkewedExclusiveQueue(
  skewMs: number,
  intervalSec: number,
  body: (plain: Pool, fatal: string[]) => Promise<void>
) {
  const db = await skewedDatabase(skewMs);
  const fatal: string[] = [];
  const c = new Connection({ pool: db.skewed, onQueueFatal: (queue) => fatal.push(queue) });
  try {
    await c.connect();
    await c.createTopic('t');
    await c.createQueue('q', 't', true, { keepAliveInterval: intervalSec });
    await body(db.plain, fatal);
  } finally {
    await c.close();
    await db.drop();
  }
}

// A database clock ahead of the application must not space keep-alives past
// the queue's real expiry.
test('a database clock ahead keeps an exclusive queue alive', async () => {
  await withSkewedExclusiveQueue(9000, 3, async (plain, fatal) => {
    await sleep(6000); // unfixed, the queue expires at 4.5 s; the next keep-alive is due at 7.5 s
    expect(fatal).toEqual([]);
    const { rows } = await plain.query(
      "SELECT keep_alive_until > clock_timestamp() + interval '9 s' AS live FROM postgremq.queues WHERE name = 'q'"
    );
    expect(rows[0].live).toBe(true);
  });
});

// A database clock behind the application must not make a live queue look
// expired: a busy keep-alive is retried within the lease.
test('a database clock behind keeps a busy exclusive queue', async () => {
  await withSkewedExclusiveQueue(-10000, 4, async (plain, fatal) => {
    await sleep(3500); // keep-alives due at 2 s and 4 s
    const locker = await plain.connect();
    try {
      await locker.query('BEGIN');
      await locker.query("SELECT 1 FROM postgremq.queues WHERE name = 'q' FOR UPDATE");
      await sleep(1200); // the keep-alive due at 4 s reports busy meanwhile
      await locker.query('ROLLBACK');
    } finally {
      locker.release();
    }
    expect(
      await eventually(async () => {
        const { rows } = await plain.query(
          "SELECT keep_alive_until > clock_timestamp() - interval '10 s' + interval '2 s' AS renewed FROM postgremq.queues WHERE name = 'q'"
        );
        return rows[0].renewed;
      })
    ).toBe(true);
    expect(fatal).toEqual([]);
  });
});

async function withSkewedQueue(
  skewMs: number,
  body: (skewed: Pool, c: Connection) => Promise<void>
) {
  const db = await skewedDatabase(skewMs);
  const c = new Connection({ pool: db.skewed });
  try {
    await c.connect();
    await c.createTopic('t');
    await c.createQueue('q', 't', false);
    await body(db.skewed, c);
  } finally {
    await c.close();
    await db.drop();
  }
}

/** The iterator's next message, or null if none arrives within ms. */
async function nextWithin(
  messages: AsyncIterator<Message>,
  ms: number
): Promise<Message | null> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<null>((resolve) => (timer = setTimeout(() => resolve(null), ms)));
  try {
    return await Promise.race([messages.next().then((r) => (r.done ? null : r.value)), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

// A database clock ahead of the application must not make the consumer wait
// past the next visible time: it waits the time the database reports.
test('a database clock ahead wakes the consumer for a delayed message', async () => {
  await withSkewedQueue(10000, async (skewed, c) => {
    await skewed.query(
      "SELECT postgremq.publish_message('t', '{}', p_deliver_after => clock_timestamp() + interval '1 s')"
    );
    const consumer = c.consume('q', { batchSize: 1, pollingIntervalMs: 30000 });
    // Unfixed, the consumer sleeps until 11 s on the application's clock.
    const message = await nextWithin(consumer.messages(), 4000);
    expect(message).not.toBeNull();
    await message!.ack();
    await consumer.stop();
  });
});

// A nack delay is the time remaining on the application's clock, added to the
// database's: it does not grow or shrink with the skew.
test('a nack delay keeps its length whatever the database clock reads', async () => {
  await withSkewedQueue(-10000, async (_skewed, c) => {
    await c.publish('t', {});
    const consumer = c.consume('q', { batchSize: 1, pollingIntervalMs: 30000 });
    const messages = consumer.messages();
    const message = await nextWithin(messages, 5000);
    expect(message).not.toBeNull();
    const nacked = Date.now();
    await message!.nack({ delaySeconds: 1.5 });
    const again = await nextWithin(messages, 5000);
    expect(again?.id).toBe(message!.id);
    expect(Date.now() - nacked).toBeGreaterThanOrEqual(1400);
    await again!.ack();
    await consumer.stop();
  });
});
