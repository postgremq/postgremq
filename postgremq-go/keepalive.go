package postgremq

import (
	"context"
	"time"
)

var _ scheduler[*kaEntry, string, kaResult] = (*keepAliveScheduler)(nil)

// Connection-level keep-alive scheduler — the keep-alive instance of the shared
// background actor (actor.go). It replaces the old N-goroutines-one-per-queue
// model with a single goroutine that batches every exclusive queue's keep-alive
// into one extend_queue_keep_alive_multi round-trip per tick.
//
// Lifetime: the actor runs on its own context (NOT conn.ctx), so it
// outlives the consumer drain and keeps firing for exclusive queues with zero
// consumers (producer-only or created-but-unconsumed). It is stopped last in
// Close().

// kaEntry is one exclusive queue the keep-alive actor is responsible for. It is
// the register payload AND the stored entry — the registrant builds it with the
// initial nextAt; the actor owns it thereafter.
type kaEntry struct {
	queue      string
	generation string
	intervalMs int64
	nextAt     time.Time // when to extend next (= last + interval/2)
	expiresAt  time.Time
}

// kaResult carries one flush's outcome back to the actor loop. dueNames lists
// the queues that were sent so apply can correlate kept/omitted.
type kaResult struct {
	leases map[string]kaLease
	due    []*kaEntry
	err    error
}

// keepAliveRegister upserts an exclusive queue into the keep-alive schedule.
// Buffered + ctx-guarded so it never blocks the caller (CreateQueue). The fresh
// entry carries a new deadline and nextAt.
func (c *Connection) keepAliveRegister(queue string, intervalMs int64, generation string) {
	c.keepAlive.register(&kaEntry{
		queue:      queue,
		generation: generation,
		intervalMs: intervalMs,
		nextAt:     time.Now().Add(time.Duration(intervalMs/2) * time.Millisecond),
		expiresAt:  time.Now().Add(time.Duration(intervalMs) * time.Millisecond),
	})
}

// keepAliveDeregister removes a queue from the keep-alive schedule. No-op if the
// queue is absent. Called by DeleteQueue so an intentional delete doesn't
// trigger a spurious onKeepAliveFailure on the next tick (the queue would
// otherwise be "omitted = permanent").
func (c *Connection) keepAliveDeregister(queue string) {
	c.keepAlive.deregister(queue)
}

// keepAliveScheduler is the actor's schedule: a map of exclusive queues keyed by
// name. Entries stay in the map while a flush is in flight (the flushing guard
// prevents re-collection), so a concurrent deregister is observed by apply.
type keepAliveScheduler struct {
	conn    *Connection
	entries map[string]*kaEntry
}

func newKeepAliveScheduler(conn *Connection) *keepAliveScheduler {
	return &keepAliveScheduler{conn: conn, entries: make(map[string]*kaEntry)}
}

// add upserts by queue (dedupe — CreateQueue may be called twice). Replacing the
// entry wholesale also fences any old asynchronous result.
func (s *keepAliveScheduler) add(entry *kaEntry) { s.entries[entry.queue] = entry }

func (s *keepAliveScheduler) remove(queue string) { delete(s.entries, queue) }

func (s *keepAliveScheduler) earliest() (time.Time, bool) {
	var earliest time.Time
	for _, entry := range s.entries {
		earliest = minTime(earliest, entry.nextAt)
	}
	return earliest, !earliest.IsZero()
}

// collectDue returns the due entries. They're safe to read in flush (off-loop)
// because add only ever REPLACES a map slot (it never mutates an existing entry)
// and the only in-place mutator, apply, runs after flush completes.
func (s *keepAliveScheduler) collectDue(now time.Time) []*kaEntry {
	var due []*kaEntry
	for _, entry := range s.entries {
		if !entry.nextAt.After(now) {
			due = append(due, entry)
		}
	}
	return due
}

// flush extends every due queue in one extend_queue_keep_alive_multi call.
func (s *keepAliveScheduler) flush(ctx context.Context, due []*kaEntry) kaResult {
	names := make([]string, len(due))
	intervals := make([]int64, len(due))
	generations := make([]string, len(due))
	for i, entry := range due {
		names[i] = entry.queue
		intervals[i] = entry.intervalMs
		generations[i] = entry.generation
	}
	deadline := time.Now().Add(time.Second)
	for _, e := range due {
		deadline = minTime(deadline, e.expiresAt)
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	leases, err := s.conn.extendKeepAliveLeases(ctx, names, intervals, generations)
	return kaResult{leases: leases, err: err, due: due}
}

// apply accepts only results for the still-registered queue incarnation.
// Retry until the last confirmed deadline, then report loss to the connection.
func (s *keepAliveScheduler) apply(res kaResult) time.Time {
	now := time.Now()
	for _, entry := range res.due {
		if s.entries[entry.queue] != entry {
			continue
		}
		lease, ok := res.leases[entry.queue]
		if res.err == nil && ok && !lease.busy {
			entry.expiresAt = lease.until
			entry.nextAt = now.Add(time.Until(lease.until) / 2)
		} else if (res.err != nil || ok && lease.busy) && entry.expiresAt.After(now) {
			entry.nextAt = minTime(now.Add(100*time.Millisecond), entry.expiresAt)
		} else {
			delete(s.entries, entry.queue)
			go s.conn.queueFatal(entry.queue, ErrQueueGone, entry.generation)
		}
	}
	return time.Time{}
}

type kaLease struct {
	until time.Time
	busy  bool
}

func (c *Connection) extendKeepAliveLeases(ctx context.Context, names []string, intervalsMs []int64, generations []string) (result map[string]kaLease, resultErr error) {
	finishMetric := c.metrics.startOperation(ctx, "keep_alive", "", false)
	defer func() { finishMetric(resultErr) }()
	leases := make(map[string]kaLease)
	err := c.withRetry(ctx, func(ctx context.Context) error {
		clear(leases)
		rows, err := c.pool.Query(ctx, "SELECT queue_name, keep_alive_until, outcome FROM postgremq.extend_queue_keep_alive_multi($1, $2, $3)", names, intervalsMs, generations)
		if err != nil {
			return mapPgError(err)
		}
		defer rows.Close()
		for rows.Next() {
			var name, outcome string
			var until *time.Time
			if err := rows.Scan(&name, &until, &outcome); err != nil {
				return err
			}
			lease := kaLease{busy: outcome == "busy"}
			if until != nil {
				lease.until = *until
			}
			leases[name] = lease
		}
		return mapPgError(rows.Err())
	})
	return leases, err
}
