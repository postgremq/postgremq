import { describe, test, expect, beforeAll, afterEach, jest } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';
import { Pool } from 'pg';
import { migrate, getMigrationStatus, migrationLockId, formatSchemaVersion } from '../../migrate';
import { MIGRATIONS } from '../../migrations.generated';
import { DirtySchemaError } from '../../errors';
import { Connection } from '../../connection';
import { createEmptyTestDatabase, createIsolatedTestConnection, getSharedTestDatabase, TestDatabase } from '../helpers';

let shared: TestDatabase;
const drops: Array<() => Promise<void>> = [];

beforeAll(async () => {
  shared = await getSharedTestDatabase();
});

afterEach(async () => {
  while (drops.length > 0) {
    await drops.pop()!();
  }
});

async function emptyDatabase(): Promise<{ pool: Pool; connectionString: string }> {
  const db = await createEmptyTestDatabase(shared);
  drops.push(db.dropDatabase);
  return db;
}

async function advisoryLocksHeld(pool: Pool): Promise<number> {
  const { rows } = await pool.query(
    "SELECT count(*)::int AS n FROM pg_locks WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
  );
  return rows[0].n;
}

const latest = Math.max(...MIGRATIONS.map((m) => m.version));

describe('embedded migrations', () => {
  test('are every migration in migrations/, in order, byte for byte', () => {
    const dir = path.join(__dirname, '../../../migrations');
    const files = fs.readdirSync(dir).filter((f) => /^\d+_.+\.up\.sql$/.test(f)).sort();
    expect(MIGRATIONS.map((m) => `${String(m.version).padStart(9, '0')}_${formatSchemaVersion(m.version)}.up.sql`))
      .toEqual(files);
    for (const [i, m] of MIGRATIONS.entries()) {
      expect(m.sql).toBe(fs.readFileSync(path.join(dir, files[i]), 'utf8'));
    }
  });

  test('formatSchemaVersion gives the mq version of a schema version', () => {
    expect(formatSchemaVersion(0)).toBe('0.0.0');
    expect(formatSchemaVersion(1)).toBe('0.0.1');
    expect(formatSchemaVersion(10_000)).toBe('0.1.0');
    expect(formatSchemaVersion(100_020_003)).toBe('1.2.3');
    expect(formatSchemaVersion(1_299_999_999)).toBe('12.9999.9999');
  });

  test('the advisory lock id matches golang-migrate', () => {
    // Values from golang-migrate's database.GenerateAdvisoryLockId(db, "postgremq", "postgremq_migrations").
    expect(migrationLockId('postgres')).toBe('2735559060');
    expect(migrationLockId('mydb')).toBe('1257508284');
    expect(migrationLockId('pmq_ts_1')).toBe('3263720984');
  });
});

describe('migrate', () => {
  test('status of an empty database', async () => {
    const { pool } = await emptyDatabase();
    await expect(getMigrationStatus(pool)).resolves.toEqual({
      currentVersion: 0,
      dirty: false,
      latestVersion: latest,
      needsMigration: true,
    });
    const { rows } = await pool.query("SELECT to_regnamespace('postgremq') IS NULL AS absent");
    expect(rows[0].absent).toBe(true);
  });

  test('installs a usable schema and records the version like golang-migrate', async () => {
    const { pool, connectionString } = await emptyDatabase();
    await migrate(pool);

    await expect(getMigrationStatus(pool)).resolves.toEqual({
      currentVersion: latest,
      dirty: false,
      latestVersion: latest,
      needsMigration: false,
    });
    const columns = await pool.query(
      `SELECT column_name, data_type, is_nullable FROM information_schema.columns
       WHERE table_schema = 'postgremq' AND table_name = 'postgremq_migrations' ORDER BY ordinal_position`,
    );
    expect(columns.rows).toEqual([
      { column_name: 'version', data_type: 'bigint', is_nullable: 'NO' },
      { column_name: 'dirty', data_type: 'boolean', is_nullable: 'NO' },
    ]);
    const versions = await pool.query('SELECT version::int AS version, dirty FROM postgremq.postgremq_migrations');
    expect(versions.rows).toEqual([{ version: latest, dirty: false }]);
    expect(await advisoryLocksHeld(pool)).toBe(0);

    const connection = new Connection({ connectionString, shutdownTimeoutMs: 5000 });
    await connection.connect();
    try {
      await connection.createTopic('t');
      await connection.createQueue('q', 't', false);
      const id = await connection.publish('t', { ok: true });
      const consumer = connection.consume('q', { batchSize: 1, pollingIntervalMs: 20 });
      const msg = (await consumer.messages().next()).value;
      expect(msg.id).toBe(id);
      await msg.ack();
      await consumer.stop();
    } finally {
      await connection.close();
    }
  });

  test('a latest.sql install is recognised as current', async () => {
    const iso = await createIsolatedTestConnection(shared);
    drops.push(iso.dropDatabase);
    const current = { currentVersion: latest, dirty: false, latestVersion: latest, needsMigration: false };
    await expect(getMigrationStatus(iso.pool)).resolves.toEqual(current);
    await migrate(iso.pool);
    await expect(getMigrationStatus(iso.pool)).resolves.toEqual(current);
  });

  test('is idempotent', async () => {
    const { pool } = await emptyDatabase();
    await migrate(pool);
    await migrate(pool);
    const status = await getMigrationStatus(pool);
    expect(status.currentVersion).toBe(latest);
    expect(status.needsMigration).toBe(false);
  });

  test('concurrent callers in separate pools are serialised', async () => {
    const { connectionString } = await emptyDatabase();
    const pools = [0, 1, 2].map(() => new Pool({ connectionString, max: 2 }));
    // Closing clients may be terminated by the database drop in afterEach.
    for (const p of pools) p.on('error', () => {});
    try {
      await Promise.all(pools.map((p) => migrate(p)));
      const status = await getMigrationStatus(pools[0]);
      expect(status).toMatchObject({ currentVersion: latest, dirty: false });
    } finally {
      await Promise.all(pools.map((p) => p.end()));
    }
  });

  test('leaves a database migrated by a newer client unchanged', async () => {
    const { pool } = await emptyDatabase();
    await migrate(pool);
    await pool.query('UPDATE postgremq.postgremq_migrations SET version = 900000000');

    await migrate(pool);

    await expect(getMigrationStatus(pool)).resolves.toEqual({
      currentVersion: 900_000_000,
      dirty: false,
      latestVersion: latest,
      needsMigration: false,
    });
  });

  test('refuses a dirty database and releases the lock', async () => {
    const { pool } = await emptyDatabase();
    await migrate(pool);
    await pool.query('UPDATE postgremq.postgremq_migrations SET dirty = true');

    const err = await migrate(pool).catch((e) => e);
    expect(err).toBeInstanceOf(DirtySchemaError);
    expect(err.version).toBe(latest);
    expect(await advisoryLocksHeld(pool)).toBe(0);
    await expect(getMigrationStatus(pool)).resolves.toMatchObject({ currentVersion: latest, dirty: true });
  });

  test('a negative version row (golang-migrate after a failed down) reads as dirty with no version', async () => {
    const { pool } = await emptyDatabase();
    await migrate(pool);
    await pool.query('UPDATE postgremq.postgremq_migrations SET version = -1, dirty = true');
    await expect(getMigrationStatus(pool)).resolves.toMatchObject({ currentVersion: 0, dirty: true });
    await expect(migrate(pool)).rejects.toMatchObject({ name: 'DirtySchemaError', version: 0 });
  });

  test('a failing migration leaves its version dirty, and later migrations are not run', async () => {
    const broken = [
      ...MIGRATIONS,
      { version: latest + 1, name: 'broken', sql: 'CREATE TABLE postgremq.half_done (id int); SELECT * FROM no_such_table;' },
      { version: latest + 2, name: 'after', sql: 'CREATE TABLE postgremq.after_broken (id int);' },
    ];
    let isolated!: typeof import('../../migrate');
    jest.isolateModules(() => {
      jest.doMock('../../migrations.generated', () => ({ MIGRATIONS: broken }));
      isolated = require('../../migrate');
    });
    const { pool } = await emptyDatabase();

    await expect(isolated.migrate(pool)).rejects.toThrow(/no_such_table/);

    await expect(isolated.getMigrationStatus(pool)).resolves.toEqual({
      currentVersion: latest + 1,
      dirty: true,
      latestVersion: latest + 2,
      needsMigration: true,
    });
    const { rows } = await pool.query(
      "SELECT to_regclass('postgremq.half_done') IS NULL AS rolled_back, to_regclass('postgremq.after_broken') IS NULL AS skipped",
    );
    expect(rows[0]).toEqual({ rolled_back: true, skipped: true });
    expect(await advisoryLocksHeld(pool)).toBe(0);
    // The isolated module registry has its own DirtySchemaError class.
    await expect(isolated.migrate(pool)).rejects.toMatchObject({ name: 'DirtySchemaError', version: latest + 1 });
  });
});
