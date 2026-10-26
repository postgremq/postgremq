//! Schema compatibility: every PostgreMQ installation reports, through
//! `postgremq.info()`, its schema version (`schema_version`, the last
//! migration applied, an mq version stored as one number). Connecting
//! rejects a schema on an mq compatibility line this client does not
//! implement: a breaking schema change starts a new line, bumping the mq
//! major, or before 1.0 the minor.

use sqlx::PgPool;

use crate::error::{Error, Result, sqlstate_of};

/// The mq (schema) compatibility lines this client implements and is tested
/// against: an mq major (`"1"`), or before 1.0 a major and minor (`"0.1"`).
/// [`Connection::connect`](crate::Connection::connect) and
/// [`Connection::from_pool`](crate::Connection::from_pool) reject a schema
/// version on any other line.
pub const SUPPORTED_MQ_VERSIONS: &[&str] = &["0.3"];

/// The compatibility line of a stored schema version.
pub(crate) fn mq_line(version: u64) -> String {
    match version / 100_000_000 {
        0 => format!("0.{}", version / 10_000 % 10_000),
        major => major.to_string(),
    }
}

/// `undefined_function`: the discovery function is missing.
const UNDEFINED_FUNCTION: &str = "42883";
/// `invalid_schema_name`: the whole `postgremq` schema is missing.
const INVALID_SCHEMA_NAME: &str = "3F000";

/// Reads `postgremq.info()` and rejects a schema whose mq line is not
/// supported. Connection and permission errors are returned as they are;
/// discovery data that is missing or malformed is [`Error::Incompatible`].
pub(crate) async fn check(pool: &PgPool) -> Result<()> {
    let info: Option<Option<serde_json::Value>> =
        match sqlx::query_scalar("SELECT postgremq.info()")
            .fetch_optional(pool)
            .await
        {
            Ok(info) => info,
            Err(err) => {
                let missing = matches!(
                    sqlstate_of(&err).as_deref(),
                    Some(UNDEFINED_FUNCTION | INVALID_SCHEMA_NAME)
                );
                return Err(if missing {
                    Error::Incompatible {
                        schema_version: None,
                        source: Some(err),
                    }
                } else {
                    Error::Sqlx(err)
                });
            }
        };
    let schema_version = info
        .flatten()
        .unwrap_or_default()
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .filter(|version| *version > 0);
    if schema_version
        .is_some_and(|version| SUPPORTED_MQ_VERSIONS.contains(&mq_line(version).as_str()))
    {
        return Ok(());
    }
    Err(Error::Incompatible {
        schema_version,
        source: None,
    })
}

#[cfg(test)]
mod tests {
    use super::mq_line;

    #[test]
    fn line_is_the_major_or_before_1_the_minor() {
        for (version, line) in [
            (1, "0.0"),
            (10_000, "0.1"),
            (19_999, "0.1"),
            (20_000, "0.2"),
            (99_999_999, "0.9999"),
            (100_000_000, "1"),
            (100_020_003, "1"),
            (299_990_000, "2"),
        ] {
            assert_eq!(mq_line(version), line, "{version}");
        }
    }
}
