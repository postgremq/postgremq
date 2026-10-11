//! Protocol compatibility: every PostgreMQ installation reports, through
//! `postgremq.info()`, its schema version (`schema_version`, the last
//! migration applied) and the protocol major clients speak. Connecting rejects a major this client does
//! not implement.

use sqlx::PgPool;

use crate::error::{Error, Result, sqlstate_of};

/// The PostgreMQ protocol majors this client implements and is tested
/// against. [`Connection::connect`](crate::Connection::connect) and
/// [`Connection::from_pool`](crate::Connection::from_pool) reject any other.
pub const SUPPORTED_PROTOCOL_MAJORS: &[u32] = &[1];

/// `undefined_function`: the discovery function is missing.
const UNDEFINED_FUNCTION: &str = "42883";
/// `invalid_schema_name`: the whole `postgremq` schema is missing.
const INVALID_SCHEMA_NAME: &str = "3F000";

/// Reads `postgremq.info()` and rejects an unsupported protocol major.
/// Connection and permission errors are returned as they are; discovery data
/// that is missing or malformed is [`Error::Incompatible`].
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
                        protocol_major: None,
                        source: Some(err),
                    }
                } else {
                    Error::Sqlx(err)
                });
            }
        };
    let info = info.flatten().unwrap_or_default();
    let schema_version = info
        .get("schema_version")
        .and_then(serde_json::Value::as_u64);
    let protocol_major = info
        .get("protocol_major")
        .and_then(serde_json::Value::as_u64)
        .and_then(|major| u32::try_from(major).ok());
    if protocol_major.is_some_and(|major| SUPPORTED_PROTOCOL_MAJORS.contains(&major)) {
        return Ok(());
    }
    Err(Error::Incompatible {
        schema_version,
        protocol_major,
        source: None,
    })
}
