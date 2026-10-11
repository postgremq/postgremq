#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

// Lets unit tests share the integration-test harness, which names the crate.
#[cfg(test)]
extern crate self as postgremq;

mod admin;
mod checkout;
mod connection;
mod consumer;
mod delivery;
mod error;
mod handler;
mod keepalive;
mod listener;
mod metrics;
mod migrate;
mod options;
mod protocol;
mod renewal;
mod retry;
mod scheduler;
mod sync;
mod types;

pub use admin::{DlqMessage, MaintenanceCounters, QueueInfo, QueueStatistics};
pub use connection::Connection;
pub use consumer::Consumer;
pub use delivery::Delivery;
pub use error::{Error, ErrorKind, Result};
pub use handler::{HandlerConsumer, HandlerError};
pub use migrate::{MigrationStatus, migrate, migration_status};
pub use options::{
    ConnectionOptions, ConsumeOptions, PublishOptions, QueueFatalHook, QueueOptions, RetryConfig,
};
pub use protocol::SUPPORTED_PROTOCOL_MAJORS;
pub use types::{Generation, MessageId};

/// Re-exported so applications use the same version as
/// [`ConnectionOptions::meter_provider`].
#[cfg(feature = "otel")]
#[cfg_attr(docsrs, doc(cfg(feature = "otel")))]
pub use opentelemetry;
/// Re-exported so applications use the same versions as this crate's API.
pub use serde_json;
/// Re-exported so applications use the same versions as this crate's API.
pub use sqlx;
/// Re-exported so applications use the same versions as this crate's API.
pub use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_types_are_send_and_sync() {
        assert_send_sync::<Connection>();
        assert_send_sync::<Consumer>();
        assert_send_sync::<Delivery>();
        assert_send_sync::<HandlerConsumer>();
        assert_send_sync::<Error>();
    }
}
