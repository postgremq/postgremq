//! A pool connection that is never handed back to the pool if its use was
//! abandoned.
//!
//! When a future holding a [`PoolConnection`] is dropped mid-statement (a
//! timeout, the shutdown deadline), sqlx returns the connection to the pool by
//! pinging it first — without a time limit. On a black-holed socket that ping
//! never answers and the connection keeps its pool slot. Every statement on
//! the crate's pool (internal work abandoned at a deadline, and settlements or
//! admin calls whose futures the application drops) therefore checks out
//! through [`Checkout`] / [`checked_out`]: once the statement has completed
//! (successfully or not) the connection is released normally; if the checkout
//! is dropped before that, the connection is detached from the pool and its
//! socket closed. (The `*_tx` calls run on the caller's connection.)

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

/// A checked-out connection; see the module docs.
pub(crate) struct Checkout(Option<PoolConnection<Postgres>>);

impl Checkout {
    pub(crate) async fn acquire(pool: &PgPool) -> Result<Self, sqlx::Error> {
        // Boxed: the pool's acquire future is large, and every settlement
        // would otherwise carry it inline.
        Ok(Self(Some(Box::pin(pool.acquire()).await?)))
    }

    /// The connection, for running statements. (Always present: only
    /// `release`, which consumes the checkout, and `Drop` take it.)
    pub(crate) fn conn(&mut self) -> Result<&mut PgConnection, sqlx::Error> {
        self.0.as_deref_mut().ok_or(sqlx::Error::PoolClosed)
    }

    /// The statement completed: return the connection to the pool normally.
    pub(crate) fn release(mut self) {
        drop(self.0.take());
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        if let Some(conn) = self.0.take() {
            // Abandoned mid-use: close the socket instead of pinging it back
            // into the pool. The pool opens a replacement when needed.
            drop(conn.detach());
        }
    }
}

/// Runs `op` on a checked-out connection: released normally once `op`
/// completes (successfully or not), closed if the returned future is dropped
/// before that.
pub(crate) async fn checked_out<T>(
    pool: &PgPool,
    op: impl AsyncFnOnce(&mut PgConnection) -> Result<T, sqlx::Error>,
) -> Result<T, sqlx::Error> {
    let mut checkout = Checkout::acquire(pool).await?;
    let result = op(checkout.conn()?).await;
    checkout.release();
    result
}
