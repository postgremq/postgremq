/**
 * Protocol compatibility. Every PostgreMQ installation reports, through
 * `postgremq.info()`, its schema version (`schema_version`, the last migration
 * applied) and the protocol major clients speak. `Connection.connect()` rejects a major this
 * client does not implement.
 */

import { PoolClient } from 'pg';
import { CompatibilityError } from './errors';

/** The PostgreMQ protocol majors this client implements and is tested against. */
export const SUPPORTED_PROTOCOL_MAJORS: readonly number[] = Object.freeze([1]);

/** SQLSTATEs meaning discovery is missing: the function or the whole schema. */
const UNDEFINED_FUNCTION = '42883';
const INVALID_SCHEMA_NAME = '3F000';

/**
 * Reads `postgremq.info()` and rejects an unsupported protocol major.
 * Connection and permission errors are rethrown as they are.
 *
 * @internal
 */
export async function checkProtocol(client: PoolClient): Promise<void> {
  let info: { schema_version?: unknown; protocol_major?: unknown };
  try {
    const { rows } = await client.query<{ info: typeof info }>('SELECT postgremq.info() AS info');
    info = rows[0].info;
  } catch (err: any) {
    if (err?.code === UNDEFINED_FUNCTION || err?.code === INVALID_SCHEMA_NAME) {
      throw new CompatibilityError({ supportedMajors: SUPPORTED_PROTOCOL_MAJORS, cause: err });
    }
    throw err;
  }
  const major = typeof info.protocol_major === 'number' ? info.protocol_major : NaN;
  if (!SUPPORTED_PROTOCOL_MAJORS.includes(major)) {
    throw new CompatibilityError({
      schemaVersion: typeof info.schema_version === 'number' ? info.schema_version : undefined,
      protocolMajor: major,
      supportedMajors: SUPPORTED_PROTOCOL_MAJORS,
    });
  }
}
