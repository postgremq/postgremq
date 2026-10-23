/**
 * Test Helpers and Utilities
 * Provides common functionality for tests including database setup
 */

import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { Pool } from 'pg';
import * as fs from 'fs';
import * as path from 'path';
import { Connection } from '../connection';

/**
 * PostgreSQL container for testing
 */
/**
 * The environment variable with the URL of a server to use instead of a
 * container, as in the Go and Rust suites: a PostgreSQL 15+ server whose user
 * can create databases and roles. The tests connect to the URL's database only
 * to create and drop their own.
 */
const SERVER_URL_ENV = 'POSTGREMQ_TEST_DATABASE_URL';

export class TestDatabase {
  private container: StartedTestContainer | null = null;
  private pool: Pool | null = null; // the shared database; also creates and drops the others
  public connectionString: string = '';
  private serverUrl: string = ''; // the server's admin database
  private sharedDatabase: string | null = null; // created on POSTGREMQ_TEST_DATABASE_URL's server
  private createdDatabases: Set<string> = new Set();

  /**
   * Start a PostgreSQL container with the queue schema, or, when
   * POSTGREMQ_TEST_DATABASE_URL is set, create a database with it on that
   * server.
   */
  async start(): Promise<void> {
    const serverUrl = process.env[SERVER_URL_ENV];

    if (serverUrl) {
      console.log('Using the PostgreSQL server of POSTGREMQ_TEST_DATABASE_URL...');
      this.serverUrl = serverUrl;
      this.databaseUrl('postgres'); // fails on a URL it cannot derive from, before anything is created
      const admin = new Pool({ connectionString: serverUrl, max: 1 });
      this.sharedDatabase = `pgmq_ts_shared_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
      try {
        await admin.query(`CREATE DATABASE ${this.sharedDatabase}`);
      } finally {
        await admin.end();
      }
      this.connectionString = this.databaseUrl(this.sharedDatabase);

      this.pool = new Pool({
        connectionString: this.connectionString,
        max: 10
      });
      // pool.end() resolves before the sockets close, so dropping the shared
      // database WITH (FORCE) on stop can terminate a closing client.
      this.pool.on('error', () => {});
      await this.waitForDatabase();
      await this.loadSchema();

      console.log('PostgreSQL database ready');
    } else {
      console.log('Starting PostgreSQL test container...');

      this.container = await new GenericContainer('postgres:15-alpine')
        .withEnvironment({
          POSTGRES_USER: 'postgres',
          POSTGRES_PASSWORD: 'postgres',
          POSTGRES_DB: 'postgremq_test'
        })
        .withExposedPorts(5432)
        .withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/))
        .withStartupTimeout(300000)
        .start();

      const host = this.container.getHost();
      const port = this.container.getMappedPort(5432);

      this.serverUrl = `postgresql://postgres:postgres@${host}:${port}/postgres`;
      this.connectionString = this.databaseUrl('postgremq_test');

      // Create connection pool
      this.pool = new Pool({
        connectionString: this.connectionString,
        max: 10
      });

      // Wait for database to be ready
      await this.waitForDatabase();

      // Load SQL schema
      await this.loadSchema();

      console.log('PostgreSQL test container started successfully');
    }
  }

  /**
   * Wait for database to be ready to accept connections
   */
  private async waitForDatabase(maxRetries: number = 30): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
      try {
        const client = await this.pool!.connect();
        await client.query('SELECT 1');
        client.release();
        return;
      } catch (error) {
        if (i === maxRetries - 1) {
          throw new Error(`Database not ready after ${maxRetries} attempts: ${error}`);
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }

  /** The URL of `database` on this server, with the server's credentials and options. */
  databaseUrl(database: string): string {
    // A Unix socket goes in ?host=, which node-postgres lets override the
    // URL's host; WHATWG URLs reject credentials without a host, so give it one.
    const serverUrl = this.serverUrl.replace(/^(postgres(?:ql)?:\/\/[^/?#]*@)(?=[/?#]|$)/, '$1localhost');
    let url: URL;
    try {
      url = new URL(serverUrl);
    } catch {
      // Not the URL's error: it would carry the password.
      throw new Error(`${SERVER_URL_ENV} is not a valid URL`);
    }
    url.pathname = `/${database}`;
    // node-postgres lets these override the path.
    url.searchParams.delete('dbname');
    url.searchParams.delete('database');
    return url.toString();
  }

  /**
   * Load PostgreMQ SQL schema
   */
  private async loadSchema(): Promise<void> {
    const schemaPath = path.join(__dirname, '../../../mq/sql/latest.sql');

    if (!fs.existsSync(schemaPath)) {
      throw new Error(`Schema file not found: ${schemaPath}`);
    }

    const schema = fs.readFileSync(schemaPath, 'utf-8');

    try {
      await this.pool!.query(schema);
      console.log('Schema loaded successfully');
    } catch (error) {
      throw new Error(`Failed to load schema: ${error}`);
    }
  }

  /**
   * Stop the container and clean up
   */
  async stop(): Promise<void> {
    if (this.pool) {
      // Best-effort drop any leftover test databases
      try {
        for (const dbName of this.createdDatabases) {
          try {
            await this.pool.query(`DROP DATABASE IF EXISTS ${dbName}`);
          } catch {}
        }
      } catch {}
      await this.pool.end();
      this.pool = null;
    }

    if (this.sharedDatabase) {
      const admin = new Pool({ connectionString: this.serverUrl, max: 1 });
      try {
        await admin.query(`DROP DATABASE IF EXISTS ${this.sharedDatabase} WITH (FORCE)`);
      } catch {
        // best effort, like the other test databases
      } finally {
        await admin.end();
      }
      this.sharedDatabase = null;
    }

    if (this.container) {
      await this.container.stop();
      this.container = null;
    }

    console.log('PostgreSQL test container stopped');
  }

  /**
   * Get a connection pool for direct database access
   */
  getPool(): Pool {
    if (!this.pool) {
      throw new Error('Database not started');
    }
    return this.pool;
  }

  /**
   * Clean up all data (useful between tests)
   */
  async cleanup(): Promise<void> {
    if (!this.pool) return;

    try {
      await this.pool.query('SELECT postgremq.purge_all_messages()');
      await this.pool.query('DELETE FROM postgremq.queues');
      await this.pool.query('DELETE FROM postgremq.topics');
    } catch (error) {
      console.warn('Error during cleanup:', error);
    }
  }
}

/**
 * Create a test connection using the test database
 */
export async function createTestConnection(db: TestDatabase): Promise<Connection> {
  // Backward-compatible helper: uses main database (not isolated)
  const connection = new Connection({
    connectionString: db.connectionString,
    shutdownTimeoutMs: 5000
  });
  await connection.connect();
  return connection;
}

/**
 * Create an isolated test database and return a connection bound to it.
 * Each call creates a fresh database, loads schema, and provides
 * a cleanup function to drop the database.
 */
export async function createIsolatedTestConnection(db: TestDatabase): Promise<{ connection: Connection; dropDatabase: () => Promise<void>; pool: Pool; connectionString: string }>{
  if (!db) throw new Error('TestDatabase not initialized');
  if (!db['pool']) throw new Error('Database pool not initialized');

  // Generate unique db name
  const suffix = `${Date.now()}_${Math.floor(Math.random()*1e6)}`;
  const dbName = `pgmq_ts_${suffix}`;

  // Create database using admin pool
  await db['pool']!.query(`CREATE DATABASE ${dbName}`);
  db['createdDatabases'].add(dbName);

  // Connect to the new database
  const isolatedConnStr = db.databaseUrl(dbName);
  const isolatedPool = new Pool({ connectionString: isolatedConnStr, max: 10 });

  // Wait for readiness
  const client = await isolatedPool.connect();
  await client.query('SELECT 1');
  client.release();

  // Load schema into isolated DB
  const schemaPath = path.join(__dirname, '../../../mq/sql/latest.sql');
  const schema = fs.readFileSync(schemaPath, 'utf-8');
  await isolatedPool.query(schema);

  // Create client connection to isolated DB
  const connection = new Connection({
    connectionString: isolatedConnStr,
    shutdownTimeoutMs: 5000
  });
  await connection.connect();

  // Provide cleanup to drop the database
  const dropDatabase = async () => {
    try { await connection.close(); } catch {}
    await isolatedPool.end();
    try {
      try {
        await db['pool']!.query(`DROP DATABASE IF EXISTS ${dbName}`);
      } catch (e: any) {
        const msg = e?.message ?? String(e);
        if (msg.includes('being accessed by other users')) {
          try {
            await db['pool']!.query(
              `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`,
              [dbName]
            );
          } catch {}
          await db['pool']!.query(`DROP DATABASE IF EXISTS ${dbName}`);
        } else {
          throw e;
        }
      }
    } finally {
      db['createdDatabases'].delete(dbName);
    }
  };

  return { connection, dropDatabase, pool: isolatedPool, connectionString: isolatedConnStr };
}

/**
 * Create a fresh database WITHOUT the PostgreMQ schema (for migration tests).
 * `connectionString` lets a test open further pools, as separate processes would.
 */
export async function createEmptyTestDatabase(
  db: TestDatabase,
  options: { encoding?: string; clientEncoding?: string } = {},
): Promise<{ pool: Pool; connectionString: string; dropDatabase: () => Promise<void> }> {
  if (!db['pool']) throw new Error('Database pool not initialized');
  const dbName = `pgmq_ts_empty_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
  await db['pool']!.query(
    options.encoding
      ? `CREATE DATABASE ${dbName} ENCODING '${options.encoding}' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0`
      : `CREATE DATABASE ${dbName}`,
  );
  db['createdDatabases'].add(dbName);
  const connectionString = db.databaseUrl(dbName);
  const pool = new Pool({ connectionString, max: 10 });
  if (options.clientEncoding) {
    // node-postgres ignores a client_encoding option.
    const clientEncoding = options.clientEncoding;
    pool.on('connect', (client) => {
      client.query(`SET client_encoding TO '${clientEncoding}'`).catch(() => {});
    });
  }
  // pool.end() resolves before the sockets close, so DROP DATABASE ... WITH
  // (FORCE) can terminate a closing client; that error is expected here.
  pool.on('error', () => {});
  const dropDatabase = async () => {
    await pool.end();
    try {
      await db['pool']!.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`);
    } finally {
      db['createdDatabases'].delete(dbName);
    }
  };
  return { pool, connectionString, dropDatabase };
}

/**
 * Sleep for the specified duration
 */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Wait for a condition to be true
 */
export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  timeoutMs: number = 5000,
  intervalMs: number = 100
): Promise<void> {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    if (await condition()) {
      return;
    }
    await sleep(intervalMs);
  }

  throw new Error(`Condition not met within ${timeoutMs}ms`);
}

/**
 * Generate random test data
 */
export function generateTestPayload(id?: number): any {
  return {
    id: id ?? Math.floor(Math.random() * 1000000),
    timestamp: new Date().toISOString(),
    data: `test-data-${Math.random().toString(36).substring(7)}`
  };
}

/**
 * Shared test database instance
 * Can be reused across test files to speed up test execution
 */
let sharedTestDb: TestDatabase | null = null;

export async function getSharedTestDatabase(): Promise<TestDatabase> {
  if (!sharedTestDb) {
    sharedTestDb = new TestDatabase();
    await sharedTestDb.start();


  }

  // Clean up data between tests
  await sharedTestDb.cleanup();

  return sharedTestDb;
}

/**
 * Assert that an async function throws an error.
 *
 * Captures the thrown value out-of-band rather than throwing the sentinel
 * "Expected function to throw" inside the same try/catch — that previous
 * shape silently swallowed its own sentinel, so any call site whose `fn`
 * stopped throwing kept passing for the wrong reason.
 */
export async function assertThrows(
  fn: () => Promise<any>,
  expectedMessage?: string
): Promise<void> {
  let thrown: any;
  try {
    await fn();
  } catch (e) {
    thrown = e;
  }
  if (thrown === undefined) {
    throw new Error('Expected function to throw, but it did not');
  }
  if (expectedMessage && !String(thrown.message ?? thrown).includes(expectedMessage)) {
    throw new Error(
      `Expected error message to include "${expectedMessage}", but got: "${thrown.message ?? thrown}"`
    );
  }
}

/** Await cleanup from Jest's afterAll; exit handlers cannot await async I/O. */
export async function stopSharedTestDatabase(): Promise<void> {
  const db = sharedTestDb; sharedTestDb = null;
  if (db) await db.stop();
}
