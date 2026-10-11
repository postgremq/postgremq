package postgremq

import (
	"container/heap"
	"context"
	"time"
)

var _ scheduler[*extEntry, extKey, extResult] = (*extScheduler)(nil)

// One connection-level scheduler extends deliveries across all consumers.
// (queue, message ID, token) is the delivery identity. The live registry owns
// lifetime; the heap contains only entries waiting for their next heartbeat.
// A flush result cannot reinsert a delivery removed while I/O was in flight.
// Leases remain registered throughout graceful drain, until settlement or loss.

const (
	// ExtendWindowPercent defines what percentage of the VT period to use as the
	// extension window. Messages due within this window are batched together for
	// efficiency.
	ExtendWindowPercent = 20 // 20% of VT period

	// defaultExtenderBatchSize bounds how many messages one tick extends in a
	// single set_vt_batch_multi call (statement-size guard, not correctness —
	// the global SQL ordering makes any batch deadlock-safe). The extender is
	// connection-level, so this is a connection option (WithExtenderBatchSize),
	// not a per-consumer setting.
	defaultExtenderBatchSize = 100
)

// calculateExtendAt computes when a message's visibility timeout should be
// extended: once `threshold` of the remaining VT has elapsed (default 0.5 — the
// historical halfway point), leaving the rest as headroom for latency and the
// extension round-trip itself. Returns now if the VT has already passed.
func calculateExtendAt(vtUntil time.Time, threshold float64) time.Time {
	remaining := time.Until(vtUntil)
	if remaining < 0 {
		return time.Now()
	}
	return time.Now().Add(time.Duration(float64(remaining) * threshold))
}

// extKey is the composite (queue, message_id, token) index key.
type extKey struct {
	queue string
	id    int64
	token string
}

// extEntry is one in-flight message the extender is responsible for. It is the
// register payload AND the stored entry — the registrant builds it with the
// initial extendAt (via calculateExtendAt); the actor owns it thereafter.
type extEntry struct {
	queue, token string
	id           int64
	vtSec        int
	threshold    float64
	extendAt     time.Time
	expiresAt    time.Time
	onExtended   func(time.Time)
	// cancel == msg.cancel; called directly on lease-loss.
	cancel context.CancelFunc
}

func (e *extEntry) key() extKey { return extKey{queue: e.queue, id: e.id, token: e.token} }

// extResult carries one flush's outcome back to the actor loop. popped is the
// batch that was sent, so apply can correlate extended/omitted.
type extResult struct {
	locks  []MultiLock
	err    error
	popped []*extEntry
}

// extenderRegister enqueues a message for auto-extension. The caller builds the
// entry (computing extendAt). Buffered + ctx-guarded so it never blocks the
// consumer fetch path.
func (c *Connection) extenderRegister(e *extEntry) {
	c.extender.register(e)
}

// extenderDeregister removes a message (by composite key) from auto-extension.
// No-op if absent. Called when a message settles.
func (c *Connection) extenderDeregister(queue string, id int64, token string) {
	c.extender.deregister(extKey{queue: queue, id: id, token: token})
}

// extScheduler is the actor's schedule: a min-heap of in-flight messages ordered by
// extendAt, indexed by the composite (queue, id, token) key. cap bounds the per-tick
// batch (WithExtenderBatchSize).
type extScheduler struct {
	conn *Connection
	h    *extHeap
	cap  int
	live map[extKey]*extEntry
}

func newExtScheduler(conn *Connection, cap int) *extScheduler {
	return &extScheduler{conn: conn, h: newExtHeap(), cap: cap, live: make(map[extKey]*extEntry)}
}

// add inserts or overrides by composite key (override-on-push).
func (s *extScheduler) add(entry *extEntry) { s.live[entry.key()] = entry; s.h.push(entry) }

func (s *extScheduler) remove(k extKey) { delete(s.live, k); s.h.remove(k) }

func (s *extScheduler) earliest() (time.Time, bool) {
	if head := s.h.peek(); head != nil {
		return head.extendAt, true
	}
	return time.Time{}, false
}

// collectDue pops the due entries (up to cap) OUT of the heap, so the off-loop
// flush reads them with no concurrent mutation; apply re-pushes the survivors.
func (s *extScheduler) collectDue(now time.Time) []*extEntry {
	var due []*extEntry
	for {
		head := s.h.peek()
		if head == nil || head.extendAt.After(now) || len(due) >= s.cap {
			break
		}
		due = append(due, s.h.pop())
	}
	return due
}

// flush extends every due message in one set_vt_batch_multi call.
func (s *extScheduler) flush(ctx context.Context, due []*extEntry) extResult {
	deadline := time.Now().Add(time.Second)
	for _, e := range due {
		if !e.expiresAt.IsZero() {
			deadline = minTime(deadline, e.expiresAt)
		}
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	exts := make([]MultiExtension, len(due))
	for i, entry := range due {
		exts[i] = MultiExtension{Queue: entry.queue, ID: entry.id, Token: entry.token, VTSec: entry.vtSec}
	}
	locks, err := s.conn.SetVTBatchMulti(ctx, exts)
	return extResult{locks: locks, err: err, popped: due}
}

// apply reschedules extended messages, cancels lease-lost ones, and returns the
// backoff floor (non-zero only on a persistent error).
func (s *extScheduler) apply(res extResult) time.Time {
	outcomes := make(map[extKey]MultiLock, len(res.locks))
	for _, l := range res.locks {
		outcomes[extKey{queue: l.Queue, id: l.ID, token: l.Token}] = l
	}
	now := time.Now()
	for _, e := range res.popped {
		// The live registry owns lifetime; the heap only schedules work.
		if s.live[e.key()] != e {
			continue
		}
		l, found := outcomes[e.key()]
		if res.err == nil && found && !l.Busy {
			e.expiresAt = l.VT
			e.extendAt = calculateExtendAt(l.VT, e.threshold)
			if e.onExtended != nil {
				e.onExtended(l.VT)
			}
			s.h.push(e)
		} else if (res.err != nil || found && l.Busy) && (e.expiresAt.IsZero() || e.expiresAt.After(now)) {
			delay := 100 * time.Millisecond
			if res.err != nil {
				delay = time.Second
			}
			e.extendAt = now.Add(delay)
			if !e.expiresAt.IsZero() {
				e.extendAt = minTime(e.extendAt, e.expiresAt)
			}
			s.h.push(e)
		} else {
			delete(s.live, e.key())
			s.conn.metrics.recordRenewalLost(e.queue)
			if e.cancel != nil {
				e.cancel()
			}
		}
	}
	return time.Time{}
}

// extHeap is a min-heap of *extEntry ordered by extendAt, indexed by the
// composite (queue, message_id, token) key for O(1) lookup / override / removal.
type extHeap struct {
	items     []*extEntry
	itemIndex map[extKey]int
}

func newExtHeap() *extHeap {
	return &extHeap{
		items:     make([]*extEntry, 0),
		itemIndex: make(map[extKey]int),
	}
}

func (h *extHeap) Len() int           { return len(h.items) }
func (h *extHeap) Less(i, j int) bool { return h.items[i].extendAt.Before(h.items[j].extendAt) }
func (h *extHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.itemIndex[h.items[i].key()] = i
	h.itemIndex[h.items[j].key()] = j
}

func (h *extHeap) Push(x interface{}) {
	item := x.(*extEntry)
	h.itemIndex[item.key()] = len(h.items)
	h.items = append(h.items, item)
}

func (h *extHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	delete(h.itemIndex, item.key())
	return item
}

// push replaces schedule state only for the exact same delivery identity.
func (h *extHeap) push(item *extEntry) {
	if idx, ok := h.itemIndex[item.key()]; ok {
		h.items[idx] = item
		heap.Fix(h, idx)
		return
	}
	heap.Push(h, item)
}

func (h *extHeap) pop() *extEntry {
	return heap.Pop(h).(*extEntry)
}

func (h *extHeap) peek() *extEntry {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

func (h *extHeap) remove(k extKey) *extEntry {
	if idx, ok := h.itemIndex[k]; ok {
		return heap.Remove(h, idx).(*extEntry)
	}
	return nil
}
