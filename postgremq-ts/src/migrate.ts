/**
 * Schema migrations, compatible with the Go client and the CLI (golang-migrate):
 * the same version table (`postgremq.postgremq_migrations`, one row of
 * `version`, `dirty`), the same advisory lock, and the same dirty-flag
 * protocol, so any client can migrate a database another one installed.
 *
 * Migrations only go up, to the latest version embedded in this package.
 */

import { Pool, PoolClient } from 'pg';
import { DirtySchemaError } from './errors';
import { MIGRATIONS } from './migrations.generated';

/** The migration state of a database, from `getMigrationStatus()`. */
export interface MigrationStatus {
  /** The applied version; 0 when no migration has been applied. */
  currentVersion: number;
  /**
   * Whether the migration of `currentVersion` has not finished: it failed
   * partway, or another process is applying it right now (the status is read
   * without the migration lock).
   */
  dirty: boolean;
  /** The latest migration embedded in this package. */
  latestVersion: number;
  /**
   * Whether `currentVersion` is below `latestVersion`. A dirty database may
   * report `false`; `migrate()` refuses it either way.
   */
  needsMigration: boolean;
}

const LATEST_VERSION = Math.max(...MIGRATIONS.map((m) => m.version));

/**
 * Applies the embedded migrations the database has not applied yet, up to the
 * latest embedded version. A database already at a newer version (migrated by
 * a newer client) is left unchanged. Concurrent callers, in any client, are
 * serialised by an advisory lock.
 *
 * This is schema management, separate from {@link Connection}: run it once
 * before connecting, e.g. at application start-up.
 *
 * @throws DirtySchemaError when a previous migration failed partway.
 */
export async function migrate(pool: Pool): Promise<void> {
  const client = await pool.connect();
  let lockId: string | undefined;
  // A session-level advisory lock must not go back to the pool: if unlocking
  // fails, the connection is destroyed instead of released.
  let discard: Error | undefined;
  try {
    lockId = await advisoryLockId(client);
    await client.query('SELECT pg_advisory_lock($1::bigint)', [lockId]);
    await ensureVersionTable(client);
    const { version, dirty } = await readVersion(client);
    if (dirty) {
      throw new DirtySchemaError(version);
    }
    for (const migration of MIGRATIONS) {
      if (migration.version <= version) {
        continue;
      }
      await setVersion(client, migration.version, true);
      // No parameters: the simple query protocol runs the whole file in one
      // implicit transaction, as the Go client does.
      await client.query(migration.sql);
      await setVersion(client, migration.version, false);
    }
  } finally {
    if (lockId !== undefined) {
      try {
        await client.query('SELECT pg_advisory_unlock($1::bigint)', [lockId]);
      } catch (err) {
        discard = err instanceof Error ? err : new Error(String(err));
      }
    }
    client.release(discard);
  }
}

/**
 * Reads the migration state. Read-only: it works before the schema is
 * installed and needs no `CREATE` privilege.
 */
export async function getMigrationStatus(pool: Pool): Promise<MigrationStatus> {
  const { rows } = await pool.query<{ table_exists: boolean }>(
    "SELECT to_regclass('postgremq.postgremq_migrations') IS NOT NULL AS table_exists",
  );
  let current = { version: 0, dirty: false };
  if (rows[0].table_exists) {
    current = await readVersion(pool);
  }
  return {
    currentVersion: current.version,
    dirty: current.dirty,
    latestVersion: LATEST_VERSION,
    needsMigration: current.version < LATEST_VERSION,
  };
}

/**
 * golang-migrate's lock key for the version table: CRC-32 (IEEE) of
 * "postgremq\0postgremq_migrations\0<database>", times its salt, mod 2^32.
 */
async function advisoryLockId(client: PoolClient): Promise<string> {
  const { rows } = await client.query<{ name: string }>('SELECT current_database() AS name');
  return migrationLockId(rows[0].name);
}

/** @internal Exported for tests. */
export function migrationLockId(database: string): string {
  const key = Buffer.from(`postgremq\0postgremq_migrations\0${database}`, 'utf8');
  return String(Math.imul(crc32(key), 1486364155) >>> 0);
}

let crcTable: Uint32Array | undefined;

function crc32(data: Uint8Array): number {
  if (!crcTable) {
    crcTable = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) {
        c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      }
      crcTable[n] = c >>> 0;
    }
  }
  let crc = 0xffffffff;
  for (const byte of data) {
    crc = crcTable[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

/**
 * Creates the schema and version table if missing. Runs under the lock, and
 * checks first, so an installed database needs no `CREATE` privilege.
 */
async function ensureVersionTable(client: PoolClient): Promise<void> {
  const { rows } = await client.query<{ schema_exists: boolean; table_exists: boolean }>(
    `SELECT to_regnamespace('postgremq') IS NOT NULL AS schema_exists,
            to_regclass('postgremq.postgremq_migrations') IS NOT NULL AS table_exists`,
  );
  if (!rows[0].schema_exists) {
    await client.query('CREATE SCHEMA IF NOT EXISTS postgremq');
  }
  if (!rows[0].table_exists) {
    await client.query(
      'CREATE TABLE IF NOT EXISTS postgremq.postgremq_migrations (version bigint not null primary key, dirty boolean not null)',
    );
  }
}

async function readVersion(db: Pool | PoolClient): Promise<{ version: number; dirty: boolean }> {
  const { rows } = await db.query<{ version: string; dirty: boolean }>(
    'SELECT version, dirty FROM postgremq.postgremq_migrations LIMIT 1',
  );
  if (rows.length === 0) {
    return { version: 0, dirty: false };
  }
  // bigint arrives as a string. golang-migrate records -1 (no version) after a
  // failed first down migration.
  return { version: Math.max(0, Number(rows[0].version)), dirty: rows[0].dirty };
}

/** Replaces the version row atomically (one implicit transaction). */
async function setVersion(client: PoolClient, version: number, dirty: boolean): Promise<void> {
  await client.query(
    `TRUNCATE postgremq.postgremq_migrations; INSERT INTO postgremq.postgremq_migrations (version, dirty) VALUES (${version}, ${dirty})`,
  );
}
