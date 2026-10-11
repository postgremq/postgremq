// Smoke test of the packed npm package, installed outside the repository
// (scripts/release/smoke-npm.sh): type-checked with --strict against the
// published declarations, then run: migrate a fresh database, connect
// (protocol check), publish, consume and ack one message.
import { Pool } from 'pg';
import { connect, migrate, getMigrationStatus, SUPPORTED_PROTOCOL_MAJORS, CompatibilityError } from 'postgremq';

async function main(): Promise<void> {
  const url = process.env.SMOKE_DATABASE_URL;
  if (!url) throw new Error('SMOKE_DATABASE_URL is not set');
  const pool = new Pool({ connectionString: url });
  try {
    await migrate(pool);
    const status = await getMigrationStatus(pool);
    if (status.needsMigration || status.dirty) throw new Error(`status ${JSON.stringify(status)}`);
    const connection = await connect({ pool });
    try {
      await connection.createTopic('smoke');
      await connection.createQueue('smoke-q', 'smoke', false);
      const id = await connection.publish('smoke', { ok: true });
      const consumer = connection.consume('smoke-q', { batchSize: 1, pollingIntervalMs: 50 });
      const next = await consumer.messages().next();
      if (next.done || next.value.id !== id) throw new Error('did not consume the published message');
      await next.value.ack();
      await consumer.stop();
    } finally {
      await connection.close();
    }
    const unused: typeof CompatibilityError = CompatibilityError;
    console.log(`npm smoke ok: protocol majors ${SUPPORTED_PROTOCOL_MAJORS.join(',')}, schema version ${status.currentVersion}`, unused.name);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
