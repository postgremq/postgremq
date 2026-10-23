import { describe, test, expect, beforeAll, afterEach, jest } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';
import * as vm from 'vm';
import { Pool } from 'pg';
import {
  migrate,
  getMigrationStatus,
  migrationLockId,
  formatSchemaVersion,
  positionUnit,
  sqlPosition,
} from '../../migrate';
import { MIGRATIONS } from '../../migrations.generated';
import { DirtySchemaError, UnknownSchemaVersionError } from '../../errors';
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

/** The migrate module with its embedded migrations replaced by `migrations`. */
function withMigrations(migrations: typeof MIGRATIONS): typeof import('../../migrate') {
  let isolated!: typeof import('../../migrate');
  jest.isolateModules(() => {
    jest.doMock('../../migrations.generated', () => ({ MIGRATIONS: migrations }));
    isolated = require('../../migrate');
  });
  return isolated;
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

  test('sqlPosition counts characters, or bytes for a single-byte client encoding', () => {
    const sql = 'ab\nc☕d\n';
    for (const [position, line, column] of [
      [1, 1, 1],
      [3, 1, 3],
      [4, 2, 1],
      [6, 2, 3],
      [7, 2, 4],
      [8, 3, 1], // the end of the input
    ]) {
      expect(sqlPosition(sql, position, 'characters')).toEqual({ line, column });
    }
    expect(sqlPosition('SELECT (', 9, 'characters')).toEqual({ line: 1, column: 9 });
    for (const [position, line, column] of [
      [4, 2, 1],
      [8, 2, 3],
      [10, 3, 1],
    ]) {
      expect(sqlPosition(sql, position, 'bytes')).toEqual({ line, column });
    }
    for (const position of [0, -1, Number.NaN, 9]) {
      expect(sqlPosition(sql, position, 'characters')).toEqual({});
    }
    expect(sqlPosition(sql, 11, 'bytes')).toEqual({});
    expect(sqlPosition(sql, 4, undefined)).toEqual({});
  });

  // EUC_JP and SJIS count characters of their own, neither the UTF-8 text's
  // characters nor its bytes; UTF8 into EUC_JIS_2004 combines か and ゚ into one.
  test('positionUnit follows the server\'s conversion (measured on PostgreSQL 15)', () => {
    for (const [server, client, unit] of [
      ['UTF8', 'UTF8', 'characters'],
      ['UTF8', 'SQL_ASCII', 'characters'],
      ['UTF8', 'LATIN1', 'bytes'],
      ['UTF8', 'EUC_JP', undefined],
      ['UTF8', 'SJIS', undefined],
      ['SQL_ASCII', 'UTF8', 'bytes'],
      ['SQL_ASCII', 'SQL_ASCII', 'bytes'],
      ['SQL_ASCII', 'LATIN1', 'bytes'],
      ['LATIN1', 'UTF8', 'characters'],
      ['LATIN1', 'SQL_ASCII', 'bytes'],
      ['LATIN1', 'LATIN1', 'bytes'],
      ['EUC_JP', 'UTF8', 'characters'],
      ['EUC_JP', 'EUC_JP', undefined],
      ['EUC_JP', 'SQL_ASCII', undefined],
      ['EUC_JP', 'SJIS', undefined],
      ['EUC_JIS_2004', 'UTF8', undefined],
      ['WIN1251', 'UNICODE', 'characters'],
    ] as const) {
      expect([server, client, positionUnit(server, client)]).toEqual([server, client, unit]);
    }
  });

  test('a database error from another realm is kept as the cause', async () => {
    // fails `instanceof Error` here, as an error from another realm does
    const foreign = vm.runInNewContext(
      `Object.assign(new Error('syntax error at or near "SELEC"'), { severity: 'ERROR', code: '42601', position: '3' })`,
    );
    expect(foreign instanceof Error).toBe(false);
    const client = {
      query: async (sql: string) => {
        if (sql === 'SELEC 1;') throw foreign;
        if (sql.includes('current_database')) return { rows: [{ name: 'db' }] };
        if (sql.includes('to_regnamespace')) return { rows: [{ schema_exists: true, table_exists: true }] };
        if (sql.startsWith('SELECT version, dirty')) return { rows: [] };
        if (sql.includes("current_setting('server_encoding')")) return { rows: [{ server: 'UTF8', client: 'UTF8' }] };
        return { rows: [] };
      },
      release: () => {},
    };
    const pool = { connect: async () => client } as unknown as Pool;
    const isolated = withMigrations([{ ...MIGRATIONS[0], version: 900_000_002, sql: 'SELEC 1;' }]);

    const err = await isolated.migrate(pool).catch((e) => e);

    expect(err).toMatchObject({ name: 'MigrationError', line: 1, column: 3 });
    expect(err.cause).toBe(foreign);
    expect(err.message).toContain('ERROR: syntax error at or near "SELEC" (SQLSTATE 42601)');
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
    const isolated = withMigrations(broken);
    const { pool } = await emptyDatabase();

    // The isolated module registry has its own error classes.
    const err = await isolated.migrate(pool).catch((e) => e);
    expect(err).toMatchObject({ name: 'MigrationError', version: latest + 1, line: 1, column: 58 });
    expect(err.message).toMatch(/no_such_table/);
    expect(err.cause).toMatchObject({ code: '42P01' });

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
    await expect(isolated.migrate(pool)).rejects.toMatchObject({ name: 'DirtySchemaError', version: latest + 1 });
  });

  test('refuses a version that is none of its migrations, and applies nothing', async () => {
    const { pool } = await emptyDatabase();
    await withMigrations(MIGRATIONS.slice(0, 1)).migrate(pool);
    const unknown = MIGRATIONS[1].version - 1; // between two released migrations
    await pool.query('UPDATE postgremq.postgremq_migrations SET version = $1', [unknown]);

    const err = await migrate(pool).catch((e) => e);
    expect(err).toBeInstanceOf(UnknownSchemaVersionError);
    expect(err.version).toBe(unknown);
    expect(err.message).toContain(
      `database schema version ${unknown} (mq ${formatSchemaVersion(unknown)}) is not one of this client's migrations`,
    );
    await expect(getMigrationStatus(pool)).resolves.toMatchObject({ currentVersion: unknown, dirty: false });
    expect(await advisoryLocksHeld(pool)).toBe(0);
  });

  // PostgreSQL counts an error position in characters of the text converted
  // to the database's encoding; the error is located in the file either way.
  test.each([
    { encoding: 'UTF8', clientEncoding: undefined },
    { encoding: 'UTF8', clientEncoding: 'SQL_ASCII' },
    { encoding: 'UTF8', clientEncoding: 'LATIN1' },
    { encoding: 'SQL_ASCII', clientEncoding: undefined },
    { encoding: 'SQL_ASCII', clientEncoding: 'UTF8' },
    { encoding: 'LATIN1', clientEncoding: undefined },
    { encoding: 'EUC_JP', clientEncoding: undefined },
  ])('a failing migration is located in a $encoding database with client encoding $clientEncoding', async (options) => {
    const isolated = withMigrations([
      // é is two bytes in UTF-8 and one character in every encoding here.
      { ...MIGRATIONS[0], version: 900_000_002, sql: '-- é é\n  SELEC 1;\n' },
    ]);
    const db = await createEmptyTestDatabase(shared, options);
    drops.push(db.dropDatabase);
    const { rows } = await db.pool.query<{ client_encoding: string }>('SHOW client_encoding');
    expect(rows[0].client_encoding).toBe(options.clientEncoding ?? 'UTF8');

    const err = await isolated.migrate(db.pool).catch((e) => e);
    expect(err).toMatchObject({ name: 'MigrationError', line: 2, column: 3 });
  });

  // The server's position counts neither the file's characters (9 here) nor
  // its bytes (13 with 㡡㡡) when it is in another multibyte encoding: with
  // EUC_JP on both ends it counts the UTF-8 bytes as EUC_JP characters, and
  // converting UTF8 into EUC_JIS_2004 combines か and ゚ into one. The error is
  // reported without a location rather than a wrong one.
  test.each([
    { encoding: 'EUC_JP', clientEncoding: 'EUC_JP', sql: '-- 㡡㡡\n  SELEC 1;\n', position: '10' },
    { encoding: 'EUC_JIS_2004', clientEncoding: undefined, sql: '-- \u304b\u309a\n  SELEC 1;\n', position: '8' },
  ])('a failing migration is not located in a $encoding database with client encoding $clientEncoding', async (tc) => {
    const isolated = withMigrations([{ ...MIGRATIONS[0], version: 900_000_002, sql: tc.sql }]);
    const db = await createEmptyTestDatabase(shared, { encoding: tc.encoding, clientEncoding: tc.clientEncoding });
    drops.push(db.dropDatabase);

    const { rows } = await db.pool.query<{ client_encoding: string }>('SHOW client_encoding');
    expect(rows[0].client_encoding).toBe(tc.clientEncoding ?? 'UTF8');

    const err = await isolated.migrate(db.pool).catch((e) => e);
    expect(err).toMatchObject({ name: 'MigrationError', line: undefined, column: undefined });
    expect(err.cause).toMatchObject({ code: '42601', position: tc.position });
    expect(err.message).not.toContain('at line');
  });

  // A failed migration's error says where in the file it failed and keeps the
  // database error as its cause.
  test.each([
    {
      name: 'a syntax error (positions count characters, not bytes)',
      sql: '-- café ☕\nCREATE TABLE postgremq.t (id int);\n  SELEC 1;\n',
      code: '42601',
      line: 3,
      column: 3,
      want: ['postgremq: migration 9.0.2 (schema version 900000002) failed at line 3, column 3: ', 'syntax error at or near "SELEC"'],
    },
    {
      name: 'a syntax error at the end of the file',
      sql: 'CREATE TABLE postgremq.t (id int);\nSELECT (\n',
      code: '42601',
      line: 3,
      column: 1,
      want: ['failed at line 3, column 1: ', 'syntax error at end of input'],
    },
    {
      name: "an error in a function's definition",
      sql: 'CREATE FUNCTION postgremq.f() RETURNS int LANGUAGE sql AS $$\n  SELECT 1;\n  SELECT x FROM no_such_table;\n$$;\n',
      code: '42P01',
      line: 3,
      column: 17,
      want: ['failed at line 3, column 17: ', 'relation "no_such_table" does not exist'],
    },
    {
      name: 'an error while a function runs',
      sql:
        'CREATE FUNCTION postgremq.h() RETURNS bigint LANGUAGE plpgsql AS $$\nBEGIN\n' +
        '  RETURN (SELECT count(*) FROM no_such_table);\nEND $$;\nSELECT postgremq.h();\n',
      code: '42P01',
      want: ['query: (SELECT count(*) FROM no_such_table)', 'context: PL/pgSQL function postgremq.h() line 3 at RETURN'],
    },
    {
      name: 'an error with a detail',
      sql: 'CREATE TABLE postgremq.t (id int PRIMARY KEY);\nINSERT INTO postgremq.t VALUES (1), (1);\n',
      code: '23505',
      want: ['duplicate key value', 'detail: Key (id)=(1) already exists.'],
    },
    {
      name: 'an error raised with a hint',
      sql: "DO $$\nBEGIN\n  RAISE EXCEPTION 'boom' USING HINT = 'look here';\nEND $$;\n",
      code: 'P0001',
      want: ['boom', 'hint: look here', 'context: PL/pgSQL function inline_code_block line 3 at RAISE'],
    },
  ])('$name', async ({ sql, code, line, column, want }) => {
    const isolated = withMigrations([MIGRATIONS[0], { ...MIGRATIONS[0], version: 900_000_002, sql }]);
    const { pool } = await emptyDatabase();

    const err = await isolated.migrate(pool).catch((e) => e);
    expect(err).toMatchObject({ name: 'MigrationError', version: 900_000_002, line, column });
    expect(err.cause).toMatchObject({ code });
    for (const text of want) {
      expect(err.message).toContain(text);
    }
  });
});
