//! Locking helpers.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// Locks `mutex`, tolerating poisoning. Every mutex in this crate guards
/// state that a panic cannot leave half-updated in a way later users would
/// misread (each critical section is a few map or heap operations).
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
