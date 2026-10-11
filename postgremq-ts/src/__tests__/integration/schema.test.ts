import { test, expect } from '@jest/globals';
import { Connection } from '../../connection';
import { createIsolatedTestConnection, getSharedTestDatabase } from '../helpers';

test('application transactions publish and ack atomically with a separate queue schema', async () => {
  const iso = await createIsolatedTestConnection(await getSharedTestDatabase());
  const c = iso.connection;
  let consumer: ReturnType<Connection['consume']> | undefined;
  try {
    await iso.pool.query('CREATE SCHEMA app');
    for (const table of ['topics', 'queues', 'messages', 'queue_messages', 'dead_letter_queue']) {
      await iso.pool.query(`CREATE TABLE app.${table} (value text)`);
    }
    await iso.pool.query(`
      CREATE FUNCTION app.publish_message(varchar, jsonb) RETURNS bigint
      LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'application publish called'; END $$;
      CREATE FUNCTION app.ack_message(varchar, bigint, varchar) RETURNS void
      LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'application ack called'; END $$;
    `);
    await c.createTopic('in');
    await c.createTopic('out');
    await c.createQueue('q', 'in', false);
    const incoming = await c.publish('in', {});
    consumer = c.consume('q', {
      batchSize: 1,
      pollingIntervalMs: 20,
      visibilityTimeoutSec: 30,
      autoExtension: { enabled: false },
    });
    const messages = consumer.messages();
    for (const commit of [false, true]) {
      const msg = (await messages.next()).value;
      expect(msg.id).toBe(incoming);
      const tx = await iso.pool.connect();
      try {
        await tx.query('BEGIN');
        await tx.query('SET LOCAL search_path = app, pg_temp');
        await tx.query("INSERT INTO messages(value) VALUES ('application data')");
        const outgoing = await c.publishWithTransaction(tx, 'out', {});
        await msg.ackWithTransaction(tx);
        expect((await tx.query('SHOW search_path')).rows[0].search_path).toBe('app, pg_temp');
        const check = async (count: number, state: string) => {
          expect((await iso.pool.query('SELECT * FROM app.messages')).rows).toHaveLength(count);
          expect(
            (await iso.pool.query('SELECT * FROM postgremq.messages WHERE id=$1', [outgoing])).rows
          ).toHaveLength(count);
          expect(
            (
              await iso.pool.query(
                'SELECT status FROM postgremq.queue_messages WHERE message_id=$1',
                [incoming]
              )
            ).rows[0].status
          ).toBe(state);
        };
        await check(0, 'processing');
        await tx.query(commit ? 'COMMIT' : 'ROLLBACK');
        await check(commit ? 1 : 0, commit ? 'completed' : 'processing');
        if (!commit) {
          await iso.pool.query(
            "UPDATE postgremq.queue_messages SET vt=clock_timestamp()-interval '1 second' WHERE message_id=$1",
            [incoming]
          );
        }
      } finally {
        await tx.query('ROLLBACK');
        tx.release();
      }
    }
    await consumer.stop();
  } finally {
    await c.close();
    await iso.dropDatabase();
  }
});
