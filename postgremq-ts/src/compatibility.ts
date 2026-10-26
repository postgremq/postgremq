/**
 * Schema compatibility. Every PostgreMQ installation reports, through
 * `postgremq.info()`, its schema version (`schema_version`, the last migration
 * applied, an mq version stored as one number). `Connection.connect()` rejects
 * a schema on an mq compatibility line this client does not implement: a
 * breaking schema change starts a new line, bumping the mq major, or before
 * 1.0 the minor.
 */

import { PoolClient } from 'pg';
import { CompatibilityError } from './errors';

/**
 * The mq (schema) compatibility lines this client implements and is tested
 * against: an mq major (`'1'`), or before 1.0 a major and minor (`'0.1'`).
 */
export const SUPPORTED_MQ_VERSIONS: readonly string[] = Object.freeze(['0.3']);

/** SQLSTATEs meaning discovery is missing: the function or the whole schema. */
const UNDEFINED_FUNCTION = '42883';
const INVALID_SCHEMA_NAME = '3F000';

/**
 * The compatibility line of a stored schema version.
 *
 * @internal
 */
export function mqLine(version: number): string {
  const major = Math.floor(version / 100_000_000);
  return major > 0 ? String(major) : `0.${Math.floor(version / 10_000) % 10_000}`;
}

/**
 * Reads `postgremq.info()` and rejects a schema whose mq line is not
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
      throw new CompatibilityError({ supportedVersions: SUPPORTED_MQ_VERSIONS, cause: err });
    }
    throw err;
  }
  const version = info?.schema_version;
  if (typeof version !== 'number' || !Number.isSafeInteger(version) || version <= 0) {
    throw new CompatibilityError({ supportedVersions: SUPPORTED_MQ_VERSIONS });
  }
  if (!SUPPORTED_MQ_VERSIONS.includes(mqLine(version))) {
    throw new CompatibilityError({ schemaVersion: version, supportedVersions: SUPPORTED_MQ_VERSIONS });
  }
}
