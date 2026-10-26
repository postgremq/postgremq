import { describe, test, expect, beforeAll, afterEach } from '@jest/globals';
import { Connection } from '../../connection';
import { CompatibilityError } from '../../errors';
import { SUPPORTED_MQ_VERSIONS, mqLine } from '../../compatibility';
import { createEmptyTestDatabase, createIsolatedTestConnection, getSharedTestDatabase, TestDatabase } from '../helpers';

let shared: TestDatabase;
const cleanups: Array<() => Promise<void>> = [];

beforeAll(async () => {
  shared = await getSharedTestDatabase();
});

afterEach(async () => {
  while (cleanups.length > 0) {
    await cleanups.pop()!();
  }
});

async function installed() {
  const iso = await createIsolatedTestConnection(shared);
  cleanups.push(iso.dropDatabase);
  return iso;
}

async function connectTo(connectionString: string): Promise<unknown> {
  const connection = new Connection({ connectionString, shutdownTimeoutMs: 2000 });
  try {
    await connection.connect();
    return undefined;
  } catch (err) {
    return err;
  } finally {
    await connection.close();
  }
}

async function fakeInfo(pool: { query: (sql: string) => Promise<unknown> }, info: string): Promise<void> {
  await pool.query(`CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb
    LANGUAGE sql STABLE AS $$ SELECT '${info}'::jsonb $$`);
}

describe('schema compatibility', () => {
  test('declares mq line 0.3, read-only', () => {
    expect(SUPPORTED_MQ_VERSIONS).toEqual(['0.3']);
    expect(Object.isFrozen(SUPPORTED_MQ_VERSIONS)).toBe(true);
  });

  test('the installed schema is on a supported line', async () => {
    const iso = await installed();
    const { rows } = await iso.pool.query("SELECT (postgremq.info()->>'schema_version')::bigint AS version");
    expect(SUPPORTED_MQ_VERSIONS).toContain(mqLine(Number(rows[0].version)));
    expect(await connectTo(iso.connectionString)).toBeUndefined();
  });

  test('other versions of the same line are accepted; protocol_major is ignored', async () => {
    const iso = await installed();
    for (const info of [
      '{"schema_version": 30000, "protocol_major": 99}',
      '{"schema_version": 39999}',
      '{"schema_version": 30001}',
    ]) {
      await fakeInfo(iso.pool, info);
      expect(await connectTo(iso.connectionString)).toBeUndefined();
    }
  });

  test('an unsupported major is rejected with the versions involved', async () => {
    const iso = await installed();
    await fakeInfo(iso.pool, '{"schema_version": 100020003, "protocol_major": 1}');

    const err = await connectTo(iso.connectionString);

    expect(err).toBeInstanceOf(CompatibilityError);
    expect(err).toMatchObject({ schemaVersion: 100_020_003, supportedVersions: ['0.3'] });
    expect((err as Error).message).toContain('mq 1.2.3 (schema version 100020003)');
    expect((err as Error).message).toContain('supports mq [0.3]');
  });

  // Before 1.0 a breaking schema change bumps the minor, so another 0.y is
  // another contract.
  test('another minor before 1.0 is rejected', async () => {
    const iso = await installed();
    for (const [info, version] of [
      ['{"schema_version": 10002, "protocol_major": 1}', 10_002],
      ['{"schema_version": 20000}', 20_000],
      ['{"schema_version": 40000}', 40_000],
      ['{"schema_version": 1}', 1],
      ['{"schema_version": 99999999}', 99_999_999],
    ] as const) {
      await fakeInfo(iso.pool, info);
      const err = await connectTo(iso.connectionString);
      expect(err).toBeInstanceOf(CompatibilityError);
      expect(err).toMatchObject({ schemaVersion: version });
    }
  });

  test('a missing or malformed schema version is incompatible', async () => {
    const iso = await installed();
    for (const info of [
      '{"schema_version": null, "protocol_major": 1}',
      '{"schema_version": "one"}',
      '{"schema_version": -1}',
      '{"schema_version": 0}',
      '{"schema_version": 1.5}',
      '{"protocol_major": 1}',
    ]) {
      await fakeInfo(iso.pool, info);
      const err = await connectTo(iso.connectionString);
      expect(err).toBeInstanceOf(CompatibilityError);
      expect((err as CompatibilityError).schemaVersion).toBeUndefined();
      expect((err as Error).message).toContain('no schema version');
    }
  });

  test('missing discovery means the installation needs an upgrade', async () => {
    const iso = await installed();
    await iso.pool.query('DROP FUNCTION postgremq.info()');

    const err = await connectTo(iso.connectionString);

    expect(err).toBeInstanceOf(CompatibilityError);
    expect((err as CompatibilityError).cause).toMatchObject({ code: '42883' });
    expect((err as Error).message).toMatch(/upgrade/);
  });

  test('a database without PostgreMQ needs an installation', async () => {
    const empty = await createEmptyTestDatabase(shared);
    cleanups.push(empty.dropDatabase);

    const err = await connectTo(empty.connectionString);

    expect(err).toBeInstanceOf(CompatibilityError);
    expect((err as CompatibilityError).cause).toMatchObject({ code: '3F000' });
  });

  test('a permission error keeps its cause', async () => {
    const iso = await installed();
    const role = `pmq_noaccess_${Date.now()}`;
    await iso.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD 'x'`);
    cleanups.push(async () => {
      await shared.getPool().query(`DROP ROLE IF EXISTS ${role}`);
    });
    // In the query: node-postgres prefers it to the URL's credentials, and a
    // URL without a host (a socket in ?host=) cannot take credentials.
    const url = new URL(iso.connectionString);
    url.searchParams.set('user', role);
    url.searchParams.set('password', 'x');

    const err = await connectTo(url.toString());

    expect(err).not.toBeInstanceOf(CompatibilityError);
    expect(err).toMatchObject({ code: '42501' });
  });

  test('a function missing from the installation fails through normal error handling', async () => {
    const iso = await installed();
    await iso.pool.query('DROP FUNCTION postgremq.list_topics()');

    const err = await iso.connection.listTopics().catch((e) => e);

    expect(err).not.toBeInstanceOf(CompatibilityError);
    expect(err).toMatchObject({ code: '42883' });
  });
});
