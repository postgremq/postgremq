//! Schema compatibility: every PostgreMQ installation reports, through
//! `postgremq.info()`, its schema version (`schema_version`, the last
//! migration applied, an mq version stored as one number). Connecting
//! rejects a schema whose mq major this client does not implement: a
//! breaking schema change bumps the mq major.

use sqlx::PgPool;

use crate::error::{Error, Result, sqlstate_of};

/// The mq (schema) majors this client implements and is tested against.
/// [`Connection::connect`](crate::Connection::connect) and
/// [`Connection::from_pool`](crate::Connection::from_pool) reject a schema
/// version with any other major.
pub const SUPPORTED_MQ_MAJORS: &[u64] = &[0];

/// `undefined_function`: the discovery function is missing.
const UNDEFINED_FUNCTION: &str = "42883";
/// `invalid_schema_name`: the whole `postgremq` schema is missing.
const INVALID_SCHEMA_NAME: &str = "3F000";

/// Reads `postgremq.info()` and rejects a schema whose mq major is not
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
    if schema_version.is_some_and(|version| SUPPORTED_MQ_MAJORS.contains(&(version / 100_000_000)))
    {
        return Ok(());
    }
    Err(Error::Incompatible {
        schema_version,
        source: None,
    })
}
