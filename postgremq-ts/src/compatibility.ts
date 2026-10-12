/**
 * Schema compatibility. Every PostgreMQ installation reports, through
 * `postgremq.info()`, its schema version (`schema_version`, the last migration
 * applied, an mq version stored as one number). `Connection.connect()` rejects
 * a schema whose mq major this client does not implement: a breaking schema
 * change bumps the mq major.
 */

import { PoolClient } from 'pg';
import { CompatibilityError } from './errors';

/** The mq (schema) majors this client implements and is tested against. */
export const SUPPORTED_MQ_MAJORS: readonly number[] = Object.freeze([0]);

/** SQLSTATEs meaning discovery is missing: the function or the whole schema. */
const UNDEFINED_FUNCTION = '42883';
const INVALID_SCHEMA_NAME = '3F000';

/**
 * Reads `postgremq.info()` and rejects a schema whose mq major is not
 * supported. Connection and permission errors are rethrown as they are.
 *
 * @internal
 */
export async function checkSchema(client: PoolClient): Promise<void> {
  let info: { schema_version?: unknown } | null;
  try {
    const { rows } = await client.query<{ info: typeof info }>('SELECT postgremq.info() AS info');
    info = rows[0].info;
  } catch (err: any) {
    if (err?.code === UNDEFINED_FUNCTION || err?.code === INVALID_SCHEMA_NAME) {
      throw new CompatibilityError({ supportedMajors: SUPPORTED_MQ_MAJORS, cause: err });
    }
    throw err;
  }
  const version = info?.schema_version;
  if (typeof version !== 'number' || !Number.isSafeInteger(version) || version <= 0) {
    throw new CompatibilityError({ supportedMajors: SUPPORTED_MQ_MAJORS });
  }
  if (!SUPPORTED_MQ_MAJORS.includes(Math.floor(version / 100_000_000))) {
    throw new CompatibilityError({ schemaVersion: version, supportedMajors: SUPPORTED_MQ_MAJORS });
  }
}
