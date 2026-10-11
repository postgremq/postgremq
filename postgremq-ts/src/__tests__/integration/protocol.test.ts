import { describe, test, expect, beforeAll, afterEach } from '@jest/globals';
import { Connection } from '../../connection';
import { CompatibilityError } from '../../errors';
import { SUPPORTED_PROTOCOL_MAJORS } from '../../protocol';
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

describe('protocol compatibility', () => {
  test('declares protocol major 1, read-only', () => {
    expect(SUPPORTED_PROTOCOL_MAJORS).toEqual([1]);
    expect(Object.isFrozen(SUPPORTED_PROTOCOL_MAJORS)).toBe(true);
  });

  test('the installed schema speaks a supported major', async () => {
    const iso = await installed();
    const { rows } = await iso.pool.query("SELECT (postgremq.info()->>'protocol_major')::int AS major");
    expect(SUPPORTED_PROTOCOL_MAJORS).toContain(rows[0].major);
    expect(await connectTo(iso.connectionString)).toBeUndefined();
  });

  test('an unsupported major is rejected with the versions involved', async () => {
    const iso = await installed();
    await iso.pool.query(`CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb
      LANGUAGE sql STABLE AS $$ SELECT jsonb_build_object('schema_version', 42, 'protocol_major', 99) $$`);

    const err = await connectTo(iso.connectionString);

    expect(err).toBeInstanceOf(CompatibilityError);
    expect(err).toMatchObject({ schemaVersion: 42, protocolMajor: 99, supportedMajors: [1] });
    expect((err as Error).message).toMatch(/version 42.*99/);
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
    const url = new URL(iso.connectionString);
    url.username = role;
    url.password = 'x';

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
