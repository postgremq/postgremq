//! Identifier newtypes and time conversions.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use uuid::Uuid;

/// A message ID (`postgremq.messages.id`, a `BIGINT`).
///
/// The same message ID is distributed to every queue subscribed to its topic,
/// so a delivery is identified by its queue *and* message ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId(i64);

impl MessageId {
    /// Wraps a raw message ID.
    #[must_use]
    pub const fn new(id: i64) -> Self {
        Self(id)
    }

    /// The raw message ID.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<MessageId> for i64 {
    fn from(id: MessageId) -> Self {
        id.0
    }
}

/// A queue generation: the UUID identifying one incarnation of a queue.
///
/// A queue that is deleted and recreated under the same name gets a new
/// generation. Consumers bound to the old generation cannot touch the new
/// queue (they observe [`Error::QueueGone`](crate::Error::QueueGone)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(Uuid);

impl Generation {
    /// Wraps a raw generation UUID.
    #[must_use]
    pub const fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// The raw generation UUID.
    #[must_use]
    pub const fn get(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<Generation> for Uuid {
    fn from(generation: Generation) -> Self {
        generation.0
    }
}

/// Converts Unix microseconds (as selected from SQL) to a [`SystemTime`].
pub(crate) fn from_unix_micros(micros: i64) -> SystemTime {
    let magnitude = Duration::from_micros(micros.unsigned_abs());
    if micros >= 0 {
        UNIX_EPOCH.checked_add(magnitude).unwrap_or(UNIX_EPOCH)
    } else {
        UNIX_EPOCH.checked_sub(magnitude).unwrap_or(UNIX_EPOCH)
    }
}

/// Converts a [`SystemTime`] to Unix microseconds for a SQL parameter,
/// saturating at the `i64` range.
pub(crate) fn to_unix_micros(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_micros()).unwrap_or(i64::MAX),
        Err(before) => i64::try_from(before.duration().as_micros())
            .map(i64::wrapping_neg)
            .unwrap_or(i64::MIN),
    }
}

/// Converts a millisecond count reported by SQL (possibly negative, meaning
/// "already due") to a non-negative [`Duration`].
pub(crate) fn millis_until(millis: i64) -> Duration {
    Duration::from_millis(u64::try_from(millis).unwrap_or(0))
}

/// The furthest ahead a delay or point in time may be (about 100 years):
/// anything beyond is rejected as invalid rather than sent to the server.
pub(crate) const MAX_HORIZON: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

/// Converts a [`Duration`] to whole milliseconds for a SQL `BIGINT`
/// parameter, saturating.
pub(crate) fn duration_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_micros_round_trip_on_both_sides_of_the_epoch() {
        for micros in [0, 1, -1, 1_700_000_000_123_456, -86_400_000_000] {
            assert_eq!(to_unix_micros(from_unix_micros(micros)), micros);
        }
    }

    #[test]
    fn negative_millis_mean_already_due() {
        assert_eq!(millis_until(-5), Duration::ZERO);
        assert_eq!(millis_until(1500), Duration::from_millis(1500));
    }
}
