//! The crate's error type and the SQLSTATE mapping.

use std::borrow::Cow;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use sqlx::postgres::{PgDatabaseError, PgErrorPosition, PgSeverity};

/// Result alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SQLSTATE codes raised by the PostgreMQ SQL functions.
pub(crate) mod sqlstate {
    /// The delivery's lease is gone: wrong token or not processing (and, for
    /// extension only, expired).
    pub(crate) const LEASE_LOST: &str = "PMQ01";
    /// A queue or topic does not exist (or an exclusive queue expired).
    pub(crate) const QUEUE_NOT_FOUND: &str = "PMQ02";
    /// The request was rejected before any state changed.
    pub(crate) const VALIDATION: &str = "PMQ03";
    /// `lock_not_available`: a row lock was contended (`NOWAIT`).
    pub(crate) const LOCK_NOT_AVAILABLE: &str = "55P03";
    /// `serialization_failure`.
    pub(crate) const SERIALIZATION_FAILURE: &str = "40001";
    /// `deadlock_detected`.
    pub(crate) const DEADLOCK_DETECTED: &str = "40P01";
    /// `admin_shutdown`.
    pub(crate) const ADMIN_SHUTDOWN: &str = "57P01";
    /// `crash_shutdown`.
    pub(crate) const CRASH_SHUTDOWN: &str = "57P02";
    /// `cannot_connect_now`.
    pub(crate) const CANNOT_CONNECT_NOW: &str = "57P03";
    /// Class `08`: connection exceptions.
    pub(crate) const CONNECTION_EXCEPTION_CLASS: &str = "08";
}

/// Errors returned by this crate.
///
/// PostgreMQ's SQL functions signal domain failures with custom SQLSTATEs.
/// Those map to dedicated variants that keep the database error as their
/// [`source`](std::error::Error::source); every other database or driver
/// error is [`Error::Sqlx`]. Use [`Error::kind`] to match without
/// destructuring.
///
/// The `Display` of [`QueueNotFound`](Error::QueueNotFound),
/// [`Validation`](Error::Validation) and [`Migration`](Error::Migration)
/// includes the server's message, so plain
/// `{}` logging keeps the reason; reporters that also print the source chain
/// show that message twice.
///
/// Variants may gain fields, so patterns name the fields they use and end
/// with `..`:
///
/// ```
/// fn gone_queue(err: &postgremq::Error) -> Option<&str> {
///     match err {
///         postgremq::Error::QueueGone { queue, .. } => Some(queue),
///         _ => None,
///     }
/// }
/// ```
///
/// ```compile_fail,E0638
/// fn gone_queue(err: &postgremq::Error) -> Option<&str> {
///     match err {
///         postgremq::Error::QueueGone { queue } => Some(queue),
///         _ => None,
///     }
/// }
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The delivery's lease is no longer held (`PMQ01`): its token was
    /// superseded or it is no longer processing. Settlement is fenced by the
    /// delivery token only, so an expired visibility timeout alone does not
    /// fail it; [`extend`](crate::Delivery::extend) also fails once the
    /// visibility timeout has expired. Also returned, without a source, when
    /// an already settled delivery is settled again.
    #[error("lease lost")]
    #[non_exhaustive]
    LeaseLost {
        /// The database error, when the server reported the loss.
        #[source]
        source: Option<sqlx::Error>,
    },

    /// A queue or topic does not exist, or an exclusive queue expired
    /// (`PMQ02`).
    #[error("queue or topic not found: {message}")]
    #[non_exhaustive]
    QueueNotFound {
        /// What was not found.
        message: Cow<'static, str>,
        /// The database error, when the server reported it.
        #[source]
        source: Option<sqlx::Error>,
    },

    /// The request was rejected before any state changed (`PMQ03`), or the
    /// client rejected invalid options.
    #[error("validation failed: {message}")]
    #[non_exhaustive]
    Validation {
        /// Why the request was rejected.
        message: Cow<'static, str>,
        /// The database error, when the server rejected the request.
        #[source]
        source: Option<sqlx::Error>,
    },

    /// A row lock was contended (`55P03`). Lease ownership is unaffected:
    /// retry within the last confirmed lease.
    #[error("row is busy")]
    #[non_exhaustive]
    Busy {
        /// The database error.
        #[source]
        source: sqlx::Error,
    },

    /// The queue a consumer depends on is gone: it was deleted out of band,
    /// or (for an exclusive queue) its keep-alive failed permanently. Fatal
    /// for the consumer, whose stream ends with this error.
    #[error("queue '{queue}' is gone")]
    #[non_exhaustive]
    QueueGone {
        /// The queue that is gone.
        queue: Arc<str>,
    },

    /// A payload could not be serialized to JSON.
    #[error("payload serialization failed")]
    Payload(#[source] serde_json::Error),

    /// The database's PostgreMQ installation is not compatible with this
    /// client: its schema version's mq line is not in
    /// [`SUPPORTED_MQ_VERSIONS`](crate::SUPPORTED_MQ_VERSIONS) (the message lists
    /// them), it reports no schema version, or it has no discovery function
    /// (`postgremq.info()`) and needs an installation or upgrade (then
    /// `source` is the database error).
    #[error("{}", incompatible_message(*schema_version, source.is_some()))]
    #[non_exhaustive]
    Incompatible {
        /// The installed schema version (the last migration applied;
        /// [`format_schema_version`](crate::format_schema_version) gives its
        /// mq version); `None` when discovery is missing or reports none.
        schema_version: Option<u64>,
        /// The database error when discovery is missing.
        #[source]
        source: Option<sqlx::Error>,
    },

    /// [`migrate`](crate::migrate) found the schema version marked dirty: a
    /// migration failed partway, so the schema needs manual repair before
    /// the dirty flag in `postgremq.postgremq_migrations` is cleared.
    #[error(
        "database schema is dirty at version {version}; a migration failed partway and needs manual repair"
    )]
    #[non_exhaustive]
    DirtySchema {
        /// The version whose migration did not finish.
        version: u64,
    },

    /// [`migrate`](crate::migrate) found a recorded schema version below its
    /// latest migration that is none of its migrations, so no upgrade from it
    /// is known. Nothing is applied.
    #[error(
        "database schema version {version} (mq {}) is not one of this client's migrations, so no upgrade from it is known",
        crate::format_schema_version(*version)
    )]
    #[non_exhaustive]
    UnknownSchemaVersion {
        /// The recorded schema version.
        version: u64,
    },

    /// A migration failed in [`migrate`](crate::migrate). Its version is left
    /// dirty (`migrate` then returns [`Error::DirtySchema`] until it is
    /// repaired). The migration runs in one transaction: when the database
    /// reported the error (`source` is [`sqlx::Error::Database`]), it was
    /// rolled back; after a connection failure its outcome is unknown, so
    /// inspect the schema before repairing.
    /// The message adds the server's detail, hint, the failing query inside a
    /// function and its context.
    #[error("{}", MigrationMessage { version: *version, position: *position, source })]
    #[non_exhaustive]
    Migration {
        /// The schema version of the migration that failed.
        version: u64,
        /// Where in the migration's file the error is, when PostgreSQL
        /// reports a position (it does for errors in the file's statements,
        /// including a function's definition). `None` too in an EUC_JIS_2004
        /// database, whose positions do not map to the file.
        position: Option<SqlPosition>,
        /// The database error.
        #[source]
        source: sqlx::Error,
    },

    /// Any other database or driver error.
    #[error(transparent)]
    Sqlx(sqlx::Error),

    /// The [`Connection`](crate::Connection) is closing or closed. Also
    /// returned when an operation in flight is abandoned at the shutdown
    /// deadline; its outcome (e.g. whether an ack committed) is then unknown.
    #[error("connection is closed")]
    Closed,
}

/// A position in a migration's file, from [`Error::Migration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct SqlPosition {
    /// The line, from 1.
    pub line: NonZeroU32,
    /// The column, in characters from 1.
    pub column: NonZeroU32,
}

impl SqlPosition {
    /// A position from a line and column counted from 1; `None` when either
    /// is 0 or above `u32::MAX`.
    pub(crate) fn new(line: usize, column: usize) -> Option<Self> {
        Some(Self {
            line: NonZeroU32::new(u32::try_from(line).ok()?)?,
            column: NonZeroU32::new(u32::try_from(column).ok()?)?,
        })
    }
}

/// The category of an [`Error`], for matching without destructuring.
///
/// New kinds may be added, so a `match` needs a wildcard arm:
///
/// ```compile_fail,E0004
/// fn describe(kind: postgremq::ErrorKind) -> &'static str {
///     match kind {
///         postgremq::ErrorKind::LeaseLost => "lease lost",
///         postgremq::ErrorKind::QueueNotFound => "not found",
///         postgremq::ErrorKind::Validation => "invalid",
///         postgremq::ErrorKind::Busy => "busy",
///         postgremq::ErrorKind::QueueGone => "gone",
///         postgremq::ErrorKind::Payload => "payload",
///         postgremq::ErrorKind::DirtySchema => "dirty schema",
///         postgremq::ErrorKind::UnknownSchemaVersion => "unknown schema version",
///         postgremq::ErrorKind::Migration => "migration failed",
///         postgremq::ErrorKind::Incompatible => "incompatible",
///         postgremq::ErrorKind::Sqlx => "database",
///         postgremq::ErrorKind::Closed => "closed",
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// See [`Error::LeaseLost`].
    LeaseLost,
    /// See [`Error::QueueNotFound`].
    QueueNotFound,
    /// See [`Error::Validation`].
    Validation,
    /// See [`Error::Busy`].
    Busy,
    /// See [`Error::QueueGone`].
    QueueGone,
    /// See [`Error::Payload`].
    Payload,
    /// See [`Error::DirtySchema`].
    DirtySchema,
    /// See [`Error::UnknownSchemaVersion`].
    UnknownSchemaVersion,
    /// See [`Error::Migration`].
    Migration,
    /// See [`Error::Incompatible`].
    Incompatible,
    /// See [`Error::Sqlx`].
    Sqlx,
    /// See [`Error::Closed`].
    Closed,
}

/// The PostgreMQ domain an SQLSTATE maps to.
#[derive(Clone, Copy)]
enum Domain {
    LeaseLost,
    QueueNotFound,
    Validation,
    Busy,
}

impl Domain {
    fn of(err: &sqlx::Error) -> Option<Self> {
        match sqlstate_of(err)?.as_ref() {
            sqlstate::LEASE_LOST => Some(Self::LeaseLost),
            sqlstate::QUEUE_NOT_FOUND => Some(Self::QueueNotFound),
            sqlstate::VALIDATION => Some(Self::Validation),
            sqlstate::LOCK_NOT_AVAILABLE => Some(Self::Busy),
            _ => None,
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        match Domain::of(&err) {
            Some(Domain::LeaseLost) => Self::LeaseLost { source: Some(err) },
            Some(Domain::QueueNotFound) => Self::QueueNotFound {
                message: Cow::Owned(database_message(&err)),
                source: Some(err),
            },
            Some(Domain::Validation) => Self::Validation {
                message: Cow::Owned(database_message(&err)),
                source: Some(err),
            },
            Some(Domain::Busy) => Self::Busy { source: err },
            None => Self::Sqlx(err),
        }
    }
}

impl Error {
    /// The category of this error.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::LeaseLost { .. } => ErrorKind::LeaseLost,
            Self::QueueNotFound { .. } => ErrorKind::QueueNotFound,
            Self::Validation { .. } => ErrorKind::Validation,
            Self::Busy { .. } => ErrorKind::Busy,
            Self::QueueGone { .. } => ErrorKind::QueueGone,
            Self::Payload(_) => ErrorKind::Payload,
            Self::DirtySchema { .. } => ErrorKind::DirtySchema,
            Self::UnknownSchemaVersion { .. } => ErrorKind::UnknownSchemaVersion,
            Self::Migration { .. } => ErrorKind::Migration,
            Self::Incompatible { .. } => ErrorKind::Incompatible,
            Self::Sqlx(_) => ErrorKind::Sqlx,
            Self::Closed => ErrorKind::Closed,
        }
    }

    /// The SQLSTATE the database reported, if this error came from it.
    #[must_use]
    pub fn sqlstate(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::Sqlx(err) | Self::Busy { source: err } | Self::Migration { source: err, .. } => {
                sqlstate_of(err)
            }
            Self::LeaseLost { source }
            | Self::QueueNotFound { source, .. }
            | Self::Validation { source, .. }
            | Self::Incompatible { source, .. } => source.as_ref().and_then(sqlstate_of),
            Self::QueueGone { .. }
            | Self::Payload(_)
            | Self::DirtySchema { .. }
            | Self::UnknownSchemaVersion { .. }
            | Self::Closed => None,
        }
    }

    /// A client-side validation error.
    pub(crate) fn invalid(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Validation {
            message: message.into(),
            source: None,
        }
    }

    /// A client-side "already settled" lease loss.
    pub(crate) fn already_settled() -> Self {
        Self::LeaseLost { source: None }
    }
}

fn incompatible_message(schema_version: Option<u64>, discovery_missing: bool) -> String {
    let supported = format!("[{}]", crate::SUPPORTED_MQ_VERSIONS.join(", "));
    if discovery_missing {
        return format!(
            "postgremq.info() is unavailable; the database needs a PostgreMQ installation or upgrade (client supports mq {supported})"
        );
    }
    match schema_version {
        None => format!(
            "the database's PostgreMQ installation reports no schema version; it needs an installation or upgrade (client supports mq {supported})"
        ),
        Some(version) => format!(
            "the database's PostgreMQ schema is mq {} (schema version {version}); this client supports mq {supported}",
            crate::format_schema_version(version)
        ),
    }
}

/// The SQLSTATE of a database error.
pub(crate) fn sqlstate_of(err: &sqlx::Error) -> Option<Cow<'_, str>> {
    match err {
        sqlx::Error::Database(db) => db.code(),
        _ => None,
    }
}

/// The PostgreSQL error inside `err`, if it is one.
pub(crate) fn pg_error(err: &sqlx::Error) -> Option<&PgDatabaseError> {
    err.as_database_error()?
        .try_downcast_ref::<PgDatabaseError>()
}

/// The message of [`Error::Migration`]: the migration, where in its file the
/// error is, and the database error with its detail, hint, failing query
/// inside a function and context.
struct MigrationMessage<'a> {
    version: u64,
    position: Option<SqlPosition>,
    source: &'a sqlx::Error,
}

impl fmt::Display for MigrationMessage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "migration {} (schema version {}) failed",
            crate::format_schema_version(self.version),
            self.version
        )?;
        if let Some(SqlPosition { line, column }) = self.position {
            write!(f, " at line {line}, column {column}")?;
        }
        let Some(db) = pg_error(self.source) else {
            return write!(f, ": {}", self.source);
        };
        let severity = match db.severity() {
            PgSeverity::Panic => "PANIC",
            PgSeverity::Fatal => "FATAL",
            PgSeverity::Error => "ERROR",
            PgSeverity::Warning => "WARNING",
            PgSeverity::Notice => "NOTICE",
            PgSeverity::Debug => "DEBUG",
            PgSeverity::Info => "INFO",
            PgSeverity::Log => "LOG",
        };
        write!(f, ": {severity}: {} (SQLSTATE {})", db.message(), db.code())?;
        let query = match db.position() {
            Some(PgErrorPosition::Internal { query, .. }) => Some(query),
            _ => None,
        };
        for (name, value) in [
            ("detail", db.detail()),
            ("hint", db.hint()),
            ("query", query),
            ("context", db.r#where()),
        ] {
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                write!(f, "; {name}: {value}")?;
            }
        }
        Ok(())
    }
}

fn database_message(err: &sqlx::Error) -> String {
    match err {
        sqlx::Error::Database(db) => db.message().to_owned(),
        other => other.to_string(),
    }
}
