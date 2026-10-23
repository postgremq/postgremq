package postgremq

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventListener subscribes to PostgreSQL LISTEN/NOTIFY events produced by the
// SQL functions in this schema and fan-outs the signal to in-process
// subscribers (typically Consumers) to wake them up immediately.
//
// Subscriptions are reference counted per channel. The first subscriber on a
// channel triggers a physical LISTEN; the last unsubscribe triggers UNLISTEN.
//
// This type is internal to the package; it's exported only for tests.
type EventListener struct {
	pool      Pool
	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	logger    LevelLogger

	mu      sync.Mutex
	desired map[string]int               // channel -> ref count
	handles map[string][]*ListenerHandle // channel -> active subscriber handles

	// changed records that `desired` changed since the session's last
	// reconcile; waiting is the session's connection while it waits for a
	// notification, so a change can interrupt that wait to reconcile at
	// once. All in-process: applying a subscription change needs no other
	// connection, so it works even when the pool has none free. Guarded by
	// mu.
	changed bool
	waiting net.Conn

	stopped chan struct{}
}

// ListenerHandle is returned by AddTopicListener / AddQueueListener (and, for
// a consumer, addConsumerListener) and gives the caller a wake channel plus a
// Close function to drop the subscription.
//
// Synchronization. The wake channel is closed exactly once, by whichever of
// `handle.Close()` or `EventListener.Close()` reaches the handle first. The
// EventListener's main mutex (`el.mu`) is the single point of synchronization:
//   - `dispatch` holds `el.mu` while doing its non-blocking sends, so a
//     concurrent close cannot land between the snapshot and the send.
//   - `Close` removes the handle from the subscription map and closes the
//     wake channel under the same lock; once it returns, no further dispatch
//     will see this handle.
type ListenerHandle struct {
	el       *EventListener
	channels []string
	ch       chan struct{}
}

// Wake returns a channel that fires whenever a NOTIFY for one of this
// handle's channels arrives, and once, without a NOTIFY, when the listener
// starts listening to them (on a new session or a new subscription), since a
// message published before the LISTEN is never notified. It carries no
// payload — wakes are signals; consumers re-fetch via consume_message on
// receipt. The channel is closed when the handle is closed (either
// explicitly via Close or implicitly by EventListener.Close).
func (h *ListenerHandle) Wake() <-chan struct{} {
	return h.ch
}

// Close drops the subscription and closes the wake channel. Idempotent.
// If the handle was already removed (e.g., by EventListener.Close), Close
// is a no-op.
func (h *ListenerHandle) Close() {
	if h == nil {
		return
	}
	h.el.removeAndClose(h)
}

func newEventListener(ctx context.Context, pool Pool, logger LevelLogger) *EventListener {
	ctx, cancel := context.WithCancel(ctx)
	el := &EventListener{
		pool:    pool,
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		desired: make(map[string]int),
		handles: make(map[string][]*ListenerHandle),
		stopped: make(chan struct{}),
	}
	close(el.stopped)
	return el
}

// AddTopicListener subscribes to the per-topic channel `pmq:t:<topic>`.
func (el *EventListener) AddTopicListener(topic string) *ListenerHandle {
	return el.addListener("pmq:t:" + topic)
}

// AddQueueListener subscribes to the per-queue channel `pmq:q:<queue>`.
func (el *EventListener) AddQueueListener(queue string) *ListenerHandle {
	return el.addListener("pmq:q:" + queue)
}

// addConsumerListener subscribes one handle to both of a consumer's
// channels, so the wake when the listener starts listening to them is one
// wake, not one per channel: a consumer woken by the first could start
// fetching before the second and fetch again for it.
func (el *EventListener) addConsumerListener(topic, queue string) *ListenerHandle {
	return el.addListener("pmq:t:"+topic, "pmq:q:"+queue)
}

// addListener subscribes one handle to channels, which are distinct and at
// least one.
func (el *EventListener) addListener(channels ...string) *ListenerHandle {
	h := &ListenerHandle{
		el:       el,
		channels: channels,
		ch:       make(chan struct{}, 8),
	}
	el.mu.Lock()
	defer el.mu.Unlock()
	for _, channel := range channels {
		el.desired[channel]++
		el.handles[channel] = append(el.handles[channel], h)
		if el.desired[channel] == 1 {
			el.markChanged()
		}
	}
	return h
}

// removeAndClose removes the handle from the subscription map and closes
// its wake channel under `el.mu`. It is idempotent: if the handle has
// already been removed (e.g. by EventListener.Close, or by a previous
// handle.Close), it returns without touching anything.
//
// The whole operation runs under `el.mu`. Because `dispatch` also holds
// `el.mu` while sending, a concurrent dispatch on this channel is either
// (a) happening with the handle still in the map (the send is safe before
// our close), or (b) blocked on `el.mu` waiting for us, and on resume will
// see the handle gone from the map and skip it entirely. Either way, no
// goroutine sends on a closed channel.
func (el *EventListener) removeAndClose(h *ListenerHandle) {
	el.mu.Lock()
	defer el.mu.Unlock()

	// A handle is on all its channels or (removed) on none.
	if !slices.Contains(el.handles[h.channels[0]], h) {
		// Already removed — wake channel was closed by whoever removed it.
		return
	}

	for _, channel := range h.channels {
		// Remove from the subscriber list (order doesn't matter; dispatch
		// iterates all entries).
		subscribers := el.handles[channel]
		idx := slices.Index(subscribers, h)
		subscribers[idx] = subscribers[len(subscribers)-1]
		subscribers = subscribers[:len(subscribers)-1]
		if len(subscribers) == 0 {
			delete(el.handles, channel)
		} else {
			el.handles[channel] = subscribers
		}

		el.desired[channel]--
		if el.desired[channel] <= 0 {
			delete(el.desired, channel)
			// Reconcile will pick up the now-removed channel and issue UNLISTEN.
			el.markChanged()
		}
	}

	// Close the wake channel under the lock so dispatch (which holds the
	// same lock during its sends) cannot race us.
	close(h.ch)
}

// markChanged records a subscription change and interrupts the session's
// wait for a notification so it reconciles. A burst of changes coalesces
// into one reconcile. The caller holds el.mu.
func (el *EventListener) markChanged() {
	el.changed = true
	if el.waiting != nil {
		_ = el.waiting.SetReadDeadline(time.Now())
	}
}

// Start launches the listener goroutine if not already started.
//
// If Close has already run (ctx is cancelled), Start is a no-op even on the
// cold path. Without this guard, a Connection.Consume() call that races a
// Connection.Close() — Consume passes checkClosed before Close grabs the
// closedFlag — could land here after Close has already drained el.stopped
// and returned. Letting startOnce.Do replace el.stopped with a fresh open
// channel and launch goroutines would leak them past Close: they exit
// promptly because ctx is cancelled, but Close never waited for the new
// stopped channel to close, so the goroutine lingers briefly without
// synchronization.
func (el *EventListener) Start() {
	el.startOnce.Do(func() {
		if el.ctx.Err() != nil {
			return
		}
		el.stopped = make(chan struct{})
		go el.run()
	})
}

func (el *EventListener) run() {
	defer close(el.stopped)
	minBackoff, maxBackoff := 1*time.Second, 30*time.Second
	backoff := minBackoff
	// A session that had been listening this long starts the backoff again.
	stableAfter := 5 * time.Second
	for {
		select {
		case <-el.ctx.Done():
			return
		default:
		}
		listening, err := el.session()
		if el.ctx.Err() != nil {
			return
		}
		var listened time.Duration
		if !listening.IsZero() {
			listened = time.Since(listening)
		}
		if listened >= stableAfter {
			backoff = minBackoff
		}
		// Jittered within [backoff/2, backoff] so instances that lost their
		// sessions together do not reconnect in lockstep.
		delay := backoff - rand.N(backoff/2+1)
		if listening.IsZero() {
			el.logger.Warnf("Listener disconnected before listening, reconnecting in %v: %v", delay, err)
		} else {
			el.logger.Warnf("Listener disconnected after listening for %v, reconnecting in %v: %v", listened, delay, err)
		}
		select {
		case <-el.ctx.Done():
			return
		case <-time.After(delay):
		}
		backoff = min(time.Duration(float64(backoff)*1.5), maxBackoff)
	}
}

// session holds a dedicated pool connection, then loops reconciling
// subscriptions and dispatching notifications. Returns when the underlying
// connection fails or the context is cancelled, with the time the session's
// LISTENs were all in place (zero if they never were).
func (el *EventListener) session() (listening time.Time, err error) {
	conn, err := el.pool.Acquire(el.ctx)
	if err != nil {
		return listening, fmt.Errorf("acquire listen connection: %w", err)
	}
	defer func() {
		// A LISTEN session must never return subscriptions to the shared pool.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = conn.Conn().Close(ctx)
		conn.Release()
	}()

	// Tracks channels we have actually issued LISTEN on with this connection.
	actual := make(map[string]struct{})

	// Every reconcile wakes the subscribers of the channels it started
	// listening to: a message published before a channel's LISTEN is never
	// notified (on a new session that is every channel, since notifications
	// sent while no session was listening are lost). The wake comes only
	// once the LISTENs are in place: a consumer woken earlier could fetch
	// before them and miss a message published in between.
	added, err := el.reconcile(el.ctx, conn, actual)
	if err != nil {
		return listening, fmt.Errorf("initial reconcile: %w", err)
	}
	listening = time.Now()
	el.dispatch(added...)

	for {
		notification, err := el.wait(conn)
		if err := el.ctx.Err(); err != nil {
			return listening, err
		}
		if err != nil {
			return listening, fmt.Errorf("wait for notification: %w", err)
		}

		if notification == nil {
			added, err := el.reconcile(el.ctx, conn, actual)
			if err != nil {
				return listening, fmt.Errorf("reconcile: %w", err)
			}
			el.dispatch(added...)
			continue
		}

		// NOTIFYs from PostgreMQ carry no payload — the channel name is the
		// wake-up signal, and the consumer re-fetches via consume_message.
		el.dispatch(notification.Channel)
	}
}

// wait returns the session's next notification, or nil when the
// subscriptions changed (before or during the wait) and must be reconciled.
//
// A change interrupts the wait by setting a read deadline on the session's
// connection, as pgx's default context handling does, but independently of
// the handler the pool configures (one that sends the server a cancel
// request would delay the change and open another connection). pgx keeps
// the connection on a timeout, resumes a message cut short by it, and
// buffers notifications that arrive during the reconcile's statements,
// which WaitForNotification returns before it reads.
func (el *EventListener) wait(conn *pgxpool.Conn) (*pgconn.Notification, error) {
	el.mu.Lock()
	if el.changed {
		el.mu.Unlock()
		return nil, nil
	}
	netConn := conn.Conn().PgConn().Conn()
	el.waiting = netConn
	el.mu.Unlock()

	notification, err := conn.Conn().WaitForNotification(el.ctx)

	el.mu.Lock()
	el.waiting = nil
	el.mu.Unlock()
	// A timeout is a change's deadline or a done el.ctx (one with a deadline
	// times out too), which the session checks before the error.
	if err != nil && !pgconn.Timeout(err) {
		return nil, err
	}
	// A change may also have set its deadline after the wait returned.
	if err := netConn.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear read deadline: %w", err)
	}
	return notification, nil
}

// reconcile issues LISTEN/UNLISTEN to bring `actual` in line with `desired`
// and returns the channels newly listened to. Changes after its snapshot set
// `changed` again, so the next wait returns at once for another reconcile.
func (el *EventListener) reconcile(ctx context.Context, conn *pgxpool.Conn, actual map[string]struct{}) ([]string, error) {
	el.mu.Lock()
	el.changed = false
	desiredSnapshot := make(map[string]struct{}, len(el.desired))
	for ch := range el.desired {
		desiredSnapshot[ch] = struct{}{}
	}
	el.mu.Unlock()

	var added []string
	for ch := range desiredSnapshot {
		if _, ok := actual[ch]; ok {
			continue
		}
		if _, err := conn.Exec(ctx, "LISTEN "+quoteIdent(ch)); err != nil {
			return nil, fmt.Errorf("LISTEN %q: %w", ch, err)
		}
		actual[ch] = struct{}{}
		added = append(added, ch)
	}
	for ch := range actual {
		if _, ok := desiredSnapshot[ch]; ok {
			continue
		}
		if _, err := conn.Exec(ctx, "UNLISTEN "+quoteIdent(ch)); err != nil {
			return nil, fmt.Errorf("UNLISTEN %q: %w", ch, err)
		}
		delete(actual, ch)
	}
	return added, nil
}

// dispatch delivers a wake event to every subscriber currently registered on
// the given channels, once per subscriber even if it is on several of them.
// Sends are non-blocking; full subscriber buffers drop the event, since the
// wake is informational.
//
// `el.mu` is held for the entire send loop. The sends are non-blocking so
// the lock is held only as long as it takes to iterate subscribers and
// attempt one send each — micro-scale even for many subscribers. Holding
// the lock makes the close-vs-send race trivial: a concurrent `Close` on
// any handle on these channels is blocked until we finish; once it acquires
// the lock, the handle has already been removed (we never see it) or it
// hasn't been removed yet (we sent on an open channel). Either way, the
// close happens after our send returns.
func (el *EventListener) dispatch(channels ...string) {
	el.mu.Lock()
	defer el.mu.Unlock()
	woken := make(map[*ListenerHandle]bool)
	for _, channel := range channels {
		for _, h := range el.handles[channel] {
			if woken[h] {
				continue
			}
			woken[h] = true
			select {
			case h.ch <- struct{}{}:
			default:
			}
		}
	}
}

// Close stops the listener loop and closes every handle's wake channel.
//
// By the time we acquire `el.mu`, the dispatch goroutine has exited (we
// waited on `el.stopped`), so dispatch cannot race the close. Concurrent
// `handle.Close()` calls block on `el.mu`; when they resume they find
// their handle already gone from the map and short-circuit (the wake
// channel was closed by us).
func (el *EventListener) Close() {
	el.cancel()
	<-el.stopped

	el.mu.Lock()
	defer el.mu.Unlock()
	closed := make(map[*ListenerHandle]bool)
	for _, subs := range el.handles {
		for _, h := range subs {
			if !closed[h] {
				closed[h] = true
				close(h.ch)
			}
		}
	}
	el.handles = make(map[string][]*ListenerHandle)
	el.desired = make(map[string]int)
}

// quoteIdent quotes an identifier for use in LISTEN/UNLISTEN commands. NOTIFY
// channel names follow the same syntactic rules as identifiers; doubled
// double-quotes escape embedded double-quotes.
func quoteIdent(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, c)
		}
	}
	out = append(out, '"')
	return string(out)
}
