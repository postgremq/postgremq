package postgremq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// controlChannelPrefix is the prefix for the per-instance control channel
// used to wake the listener loop when subscriptions change. The full name
// includes a random suffix unique to each EventListener so multiple service
// instances don't wake each other on every subscription change.
const controlChannelPrefix = "pmq:control:"

// newControlChannel returns a new control-channel name with a random suffix
// unique to this EventListener instance.
func newControlChannel() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return controlChannelPrefix + hex.EncodeToString(b[:])
}

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

	// controlChannel is unique per EventListener so a wake signal from one
	// process doesn't fan out to every other service instance also using
	// PostgreMQ.
	controlChannel string

	mu      sync.Mutex
	desired map[string]int               // channel -> ref count
	handles map[string][]*ListenerHandle // channel -> active subscriber handles

	// notifyReq is read by a single drainer goroutine that sends NOTIFY
	// on the control channel. Buffered to size 1 so multiple rapid
	// subscribe/unsubscribe events coalesce into a single NOTIFY round-trip
	// — avoids spawning one goroutine per change under churn.
	notifyReq chan struct{}
	stopped   chan struct{}
	wg        sync.WaitGroup
}

// ListenerHandle is returned by AddTopicListener / AddQueueListener and gives
// the caller a wake channel plus a Close function to drop the subscription.
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
	el      *EventListener
	channel string
	ch      chan struct{}
}

// Wake returns a channel that fires whenever a NOTIFY for this handle's
// channel arrives. It carries no payload — NOTIFYs are wake-up signals;
// consumers re-fetch via consume_message on receipt. The channel is
// closed when the handle is closed (either explicitly via Close or
// implicitly by EventListener.Close).
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
		pool:           pool,
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		controlChannel: newControlChannel(),
		desired:        make(map[string]int),
		handles:        make(map[string][]*ListenerHandle),
		notifyReq:      make(chan struct{}, 1),
		stopped:        make(chan struct{}),
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

func (el *EventListener) addListener(channel string) *ListenerHandle {
	h := &ListenerHandle{
		el:      el,
		channel: channel,
		ch:      make(chan struct{}, 8),
	}
	el.mu.Lock()
	el.desired[channel]++
	el.handles[channel] = append(el.handles[channel], h)
	needWake := el.desired[channel] == 1
	el.mu.Unlock()
	if needWake {
		el.signalWake()
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

	subscribers := el.handles[h.channel]
	idx := -1
	for i, s := range subscribers {
		if s == h {
			idx = i
			break
		}
	}
	if idx < 0 {
		// Already removed — wake channel was closed by whoever removed it.
		return
	}

	// Remove from the subscriber list (order doesn't matter; dispatch
	// iterates all entries).
	subscribers[idx] = subscribers[len(subscribers)-1]
	subscribers = subscribers[:len(subscribers)-1]
	if len(subscribers) == 0 {
		delete(el.handles, h.channel)
	} else {
		el.handles[h.channel] = subscribers
	}

	// Close the wake channel under the lock so dispatch (which holds the
	// same lock during its sends) cannot race us.
	close(h.ch)

	el.desired[h.channel]--
	if el.desired[h.channel] <= 0 {
		delete(el.desired, h.channel)
		// signalWake is non-blocking; safe under the lock. Reconcile
		// will pick up the now-removed channel and issue UNLISTEN.
		el.signalWake()
	}
}

// signalWake asks the drainer goroutine to send a control NOTIFY so the
// listener's WaitForNotification returns and reconciles. The drainer is a
// single long-running goroutine — see notifyDrainer — so churn cannot spawn
// unbounded goroutines or saturate the pool.
func (el *EventListener) signalWake() {
	select {
	case el.notifyReq <- struct{}{}:
	default:
	}
}

// notifyDrainer is the single goroutine responsible for issuing control
// NOTIFYs. It reads from notifyReq (size 1, coalescing) and runs each Exec
// sequentially, so a burst of subscribe/unsubscribe calls produces at most
// one in-flight NOTIFY at a time.
func (el *EventListener) notifyDrainer() {
	defer el.wg.Done()
	for {
		select {
		case <-el.ctx.Done():
			return
		case <-el.notifyReq:
			ctx, cancel := context.WithTimeout(el.ctx, 5*time.Second)
			_, err := el.pool.Exec(ctx, "SELECT pg_notify($1, '')", el.controlChannel)
			cancel()
			if err != nil && el.ctx.Err() == nil {
				el.logger.Warnf("control notify failed: %v", err)
			}
		}
	}
}

// Start launches the listener and drainer goroutines if not already started.
//
// If Close has already run (ctx is cancelled), Start is a no-op even on the
// cold path. Without this guard, a Connection.Consume() call that races a
// Connection.Close() — Consume passes checkClosed before Close grabs the
// closedFlag — could land here after Close has already drained el.stopped
// and returned. Letting startOnce.Do replace el.stopped with a fresh open
// channel and launch goroutines would leak them past Close: they exit
// promptly because ctx is cancelled, but Close never waited for the new
// stopped channel to close, so the wg goroutines linger briefly without
// synchronization.
func (el *EventListener) Start() {
	el.startOnce.Do(func() {
		if el.ctx.Err() != nil {
			return
		}
		el.stopped = make(chan struct{})
		el.wg.Add(1)
		go el.notifyDrainer()
		go el.run()
	})
}

func (el *EventListener) run() {
	defer close(el.stopped)
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	for {
		select {
		case <-el.ctx.Done():
			return
		default:
		}
		err := el.session()
		if el.ctx.Err() != nil {
			return
		}
		el.logger.Warnf("Listener disconnected, reconnecting in %v: %v", backoff, err)
		select {
		case <-el.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// session holds a dedicated pool connection, issues the control LISTEN, then
// loops reconciling subscriptions and dispatching notifications. Returns when
// the underlying connection fails or the context is cancelled.
func (el *EventListener) session() error {
	conn, err := el.pool.Acquire(el.ctx)
	if err != nil {
		return fmt.Errorf("acquire listen connection: %w", err)
	}
	defer func() {
		// A LISTEN session must never return subscriptions to the shared pool.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = conn.Conn().Close(ctx)
		conn.Release()
	}()

	if _, err := conn.Exec(el.ctx, "LISTEN "+quoteIdent(el.controlChannel)); err != nil {
		return fmt.Errorf("listen control channel: %w", err)
	}

	// Tracks channels we have actually issued LISTEN on with this connection.
	actual := make(map[string]struct{})

	if err := el.reconcile(el.ctx, conn, actual); err != nil {
		return fmt.Errorf("initial reconcile: %w", err)
	}

	for {
		if err := el.ctx.Err(); err != nil {
			return err
		}

		notification, err := conn.Conn().WaitForNotification(el.ctx)
		if err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}

		if notification.Channel == el.controlChannel {
			if err := el.reconcile(el.ctx, conn, actual); err != nil {
				return fmt.Errorf("reconcile: %w", err)
			}
			continue
		}

		// NOTIFYs from PostgreMQ carry no payload — the channel name is the
		// wake-up signal, and the consumer re-fetches via consume_message.
		el.dispatch(notification.Channel)
	}
}

// reconcile issues LISTEN/UNLISTEN to bring `actual` in line with `desired`.
func (el *EventListener) reconcile(ctx context.Context, conn *pgxpool.Conn, actual map[string]struct{}) error {
	el.mu.Lock()
	desiredSnapshot := make(map[string]struct{}, len(el.desired))
	for ch := range el.desired {
		desiredSnapshot[ch] = struct{}{}
	}
	el.mu.Unlock()

	for ch := range desiredSnapshot {
		if _, ok := actual[ch]; ok {
			continue
		}
		if _, err := conn.Exec(ctx, "LISTEN "+quoteIdent(ch)); err != nil {
			return fmt.Errorf("LISTEN %q: %w", ch, err)
		}
		actual[ch] = struct{}{}
	}
	for ch := range actual {
		if _, ok := desiredSnapshot[ch]; ok {
			continue
		}
		if _, err := conn.Exec(ctx, "UNLISTEN "+quoteIdent(ch)); err != nil {
			return fmt.Errorf("UNLISTEN %q: %w", ch, err)
		}
		delete(actual, ch)
	}
	return nil
}

// dispatch delivers a wake event to every subscriber currently registered on
// the given channel. Sends are non-blocking; full subscriber buffers drop the
// event, since the wake is informational.
//
// `el.mu` is held for the entire send loop. The sends are non-blocking so
// the lock is held only as long as it takes to iterate subscribers and
// attempt one send each — micro-scale even for many subscribers. Holding
// the lock makes the close-vs-send race trivial: a concurrent `Close` on
// any handle on this channel is blocked until we finish; once it acquires
// the lock, the handle has already been removed (we never see it) or it
// hasn't been removed yet (we sent on an open channel). Either way, the
// close happens after our send returns.
func (el *EventListener) dispatch(channel string) {
	el.mu.Lock()
	defer el.mu.Unlock()
	for _, h := range el.handles[channel] {
		select {
		case h.ch <- struct{}{}:
		default:
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
	el.wg.Wait() // drainer also exits on ctx.Done

	el.mu.Lock()
	defer el.mu.Unlock()
	for _, subs := range el.handles {
		for _, h := range subs {
			close(h.ch)
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
