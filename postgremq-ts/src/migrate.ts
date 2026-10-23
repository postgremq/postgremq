/**
 * Schema migrations, compatible with the Go and Rust clients and the CLI, which
 * all follow golang-migrate's protocol:
 * the same version table (`postgremq.postgremq_migrations`, one row of
 * `version`, `dirty`), the same advisory lock, and the same dirty-flag
 * protocol, so any client can migrate a database another one installed.
 *
 * Migrations only go up, to the latest version embedded in this package.
 *
 * A schema version is an mq version stored as one number,
 * major * 10^8 + minor * 10^4 + patch (see `formatSchemaVersion()`).
 */

import { Pool, PoolClient } from 'pg';
import { DirtySchemaError, MigrationError, UnknownSchemaVersionError } from './errors';
import { MIGRATIONS } from './migrations.generated';

/** The migration state of a database, from `getMigrationStatus()`. */
export interface MigrationStatus {
  /**
   * The applied schema version; 0 when no migration has been applied.
   * `formatSchemaVersion()` gives its mq version.
   */
  currentVersion: number;
  /**
   * Whether the migration of `currentVersion` has not finished: it failed
   * partway, or another process is applying it right now (the status is read
   * without the migration lock).
   */
  dirty: boolean;
  /** The schema version of the latest migration embedded in this package. */
  latestVersion: number;
  /**
   * Whether `currentVersion` is below `latestVersion`. A dirty database may
   * report `false`; `migrate()` refuses it either way. It is also `true` for
   * a version below the latest that none of the migrations knows, which
   * `migrate()` refuses.
   */
  needsMigration: boolean;
}

const LATEST_VERSION = Math.max(...MIGRATIONS.map((m) => m.version));

export { formatSchemaVersion } from './schema-version';

/**
 * Applies the embedded migrations the database has not applied yet, up to the
 * latest embedded version. A database already at a newer version (migrated by
 * a newer client) is left unchanged. Concurrent callers, in any client, are
 * serialised by an advisory lock.
 *
 * This is schema management, separate from {@link Connection}: run it once
 * before connecting, e.g. at application start-up.
 *
 * @throws MigrationError when a migration fails; its version is left dirty
 *   (and it is rolled back when the database reported the error).
 * @throws DirtySchemaError when a previous migration failed partway.
 * @throws UnknownSchemaVersionError when the recorded version is below the
 *   latest but none of the migrations, so no upgrade from it is known.
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
    // A version below the latest that is none of the migrations is not a
    // state they can upgrade from. A version above it was recorded by a
    // newer client: nothing to do.
    if (version !== 0 && version < LATEST_VERSION && !MIGRATIONS.some((m) => m.version === version)) {
      throw new UnknownSchemaVersionError(version);
    }
    const pending = MIGRATIONS.some((m) => m.version > version);
    const positions = pending ? await serverPositionUnit(client) : undefined;
    for (const migration of MIGRATIONS) {
      if (migration.version <= version) {
        continue;
      }
      await setVersion(client, migration.version, true);
      // No parameters: the simple query protocol runs the whole file in one
      // implicit transaction, as the Go client does.
      try {
        await client.query(migration.sql);
      } catch (err) {
        // Kept as it is (an error from another realm fails instanceof Error).
        const cause = typeof err === 'object' && err !== null ? (err as Error) : new Error(String(err));
        const position = Number((cause as { position?: unknown }).position);
        const at = sqlPosition(migration.sql, position, positions);
        throw new MigrationError({ version: migration.version, ...at, cause });
      }
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
 * What a PostgreSQL error position counts in the UTF-8 migration text:
 * characters (code points) or bytes. Undefined for characters of another
 * multibyte encoding, which `sqlPosition` does not decode.
 */
type PositionUnit = 'characters' | 'bytes' | undefined;

const SINGLE_BYTE_ENCODINGS = new Set([
  'LATIN1', 'LATIN2', 'LATIN3', 'LATIN4', 'LATIN5', 'LATIN6', 'LATIN7', 'LATIN8', 'LATIN9', 'LATIN10',
  'ISO_8859_5', 'ISO_8859_6', 'ISO_8859_7', 'ISO_8859_8', 'KOI8R', 'KOI8U', 'WIN866', 'WIN874',
  'WIN1250', 'WIN1251', 'WIN1252', 'WIN1253', 'WIN1254', 'WIN1255', 'WIN1256', 'WIN1257', 'WIN1258',
]);

/** What this session's server counts error positions in. */
async function serverPositionUnit(client: PoolClient): Promise<PositionUnit> {
  const { rows } = await client.query<{ server: string; client: string }>(
    "SELECT current_setting('server_encoding') AS server, current_setting('client_encoding') AS client",
  );
  return positionUnit(rows[0].server, rows[0].client);
}

/**
 * What the server counts an error position in. It counts characters of the
 * text in the database's encoding, converted from client_encoding unless that
 * is the database's or SQL_ASCII (taken as it is); conversion keeps the
 * characters, except into EUC_JIS_2004, which combines some pairs into one. A
 * SQL_ASCII database counts bytes.
 *
 * @internal Exported for tests.
 */
export function positionUnit(serverEncoding: string, clientEncoding: string): PositionUnit {
  if (serverEncoding === 'SQL_ASCII') {
    return 'bytes';
  }
  if (serverEncoding === 'EUC_JIS_2004') {
    return undefined;
  }
  if (clientEncoding === 'UNICODE') {
    clientEncoding = 'UTF8'; // its alias, reported as it was set
  }
  const encoding =
    clientEncoding === serverEncoding || clientEncoding === 'SQL_ASCII' ? serverEncoding : clientEncoding;
  if (encoding === 'UTF8') {
    return 'characters';
  }
  return SINGLE_BYTE_ENCODINGS.has(encoding) ? 'bytes' : undefined; // a single byte is one character
}

/**
 * The line and column, from 1 and the column in characters, of a PostgreSQL
 * error position in `sql`. The position counts from 1 in `unit`; one past the
 * end is the end of the input. Empty when there is no position or no unit.
 *
 * @internal Exported for tests.
 */
export function sqlPosition(sql: string, position: number, unit: PositionUnit): { line?: number; column?: number } {
  if (!Number.isInteger(position) || position <= 0 || unit === undefined) {
    return {};
  }
  let before: string;
  if (unit === 'bytes') {
    const bytes = Buffer.from(sql, 'utf8');
    if (position - 1 > bytes.length) {
      return {};
    }
    before = bytes.subarray(0, position - 1).toString('utf8');
  } else {
    const characters = Array.from(sql);
    if (position - 1 > characters.length) {
      return {};
    }
    before = characters.slice(0, position - 1).join('');
  }
  const lines = before.split('\n');
  return { line: lines.length, column: Array.from(lines[lines.length - 1]).length + 1 };
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
