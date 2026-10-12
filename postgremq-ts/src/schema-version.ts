/**
 * The mq version (`major.minor.patch`) of a schema version, the number the
 * version table stores (`major * 10^8 + minor * 10^4 + patch`):
 * `formatSchemaVersion(10000)` is `'0.1.0'`.
 */
export function formatSchemaVersion(version: number): string {
  return `${Math.floor(version / 100_000_000)}.${Math.floor(version / 10_000) % 10_000}.${version % 10_000}`;
}
