package postgremq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Consumer receives messages from a single queue.
//
// A Consumer runs ONE goroutine (Consumer.run) that owns all of its state: it
// fetches messages in batches, delivers them to the Messages() channel, tracks
// in-flight messages until they are Ack/Nack/Release-d, and drains cleanly on
// shutdown. Visibility-timeout auto-extension is NOT a per-consumer goroutine —
// in-flight messages are registered with the connection-level extender actor
// (extender.go) and deregistered when they settle. Create via Connection.Consume.
//
// The loop owns delivery and drain accounting. Fetch I/O is dispatched with a
// deadline. During drain, releases of known-undelivered messages use bounded
// contexts; the connection's overall deadline cancels remaining cleanup.
type Consumer struct {
	conn               *Connection
	queue              string
	generation         string
	topic              string
	messages           chan *Message
	batchSize          int // number of messages to fetch in one batch
	vtSec              int // visibility timeout in seconds
	noAutoExtension    bool
	checkTimeout       time.Duration // how often to check for new messages, if no events comming
	extensionThreshold float64       // fraction of vt that must elapse before extension fires
	ctx                context.Context
	cancel             context.CancelFunc
	dbCtx              context.Context  // some operations must not be cancelled by consumer stop
	topicHandle        *ListenerHandle  // wakes on per-topic publish notifications
	queueHandle        *ListenerHandle  // wakes on nack/release for this queue
	done               chan struct{}    // closed when run() exits
	fetched            chan fetchResult // a fetch goroutine reports its batch here
	completed          chan string      // a settled message reports its trackingID here
	logger             LevelLogger

	// Queue-fatal teardown signalling. fatalErr is the reason the consumer
	// closed (nil = a normal Stop); it's delivered on the NotifyClose channels
	// once run() exits. Guarded by fatalMu; set at most once via fatalOnce.
	fatalMu     sync.Mutex
	fatalOnce   sync.Once
	fatalErr    error
	notifyChans []chan error
	notified    bool // close already delivered to notifyChans
}

// fetchResult is one fetch goroutine's output: the messages it claimed plus the
// hint for when the next fetch should run. When fatal is set the queue is gone
// (consumeMessages returned ErrQueueNotFound) and cause carries why.
type fetchResult struct {
	msgs   []*Message
	nextAt time.Time
	fatal  bool
	cause  error
}

func newConsumerFromOptions(parentCtx context.Context, conn *Connection, logger LevelLogger, queue string, topicHandle, queueHandle *ListenerHandle, options consumeOptions) (*Consumer, error) {
	// Validate options
	if err := validateConsumeOptions(&options); err != nil {
		return nil, err
	}
	if options.vt == 0 {
		options.vt = 30 // if client didn't specify visibility timeout, we default to 30 seconds
	}
	ctx, cancel := context.WithCancel(parentCtx)
	dbCtx := conn.ioCtx // some operations should not be cancelled by consumer stop
	return &Consumer{
		conn:               conn,
		queue:              queue,
		generation:         options.generation,
		topic:              options.topic,
		messages:           make(chan *Message, options.batchSize),
		batchSize:          options.batchSize,
		vtSec:              options.vt,
		noAutoExtension:    options.noAutoExtension,
		checkTimeout:       options.checkTimeout,
		extensionThreshold: options.extensionThreshold,
		ctx:                ctx,
		cancel:             cancel,
		dbCtx:              dbCtx,
		topicHandle:        topicHandle,
		queueHandle:        queueHandle,
		done:               make(chan struct{}),
		fetched:            make(chan fetchResult, 1),
		completed:          make(chan string, max(options.batchSize, 64)),
		logger:             logger,
	}, nil
}

func (c *Consumer) start() {
	go c.run()
}

// run is the consumer's single owner goroutine. It holds two sets of messages:
//   - outbox:   fetched + registered, not yet delivered to the Messages() channel
//   - inflight: booked for tracking, not yet settled (covers both outbox entries
//     and already-delivered messages)
//
// Backpressure: a new fetch is dispatched only when the outbox is empty, so at
// most one batch sits in the outbox and one batch sits in the channel buffer —
// when the consumer stops reading, the channel fills, the outbox stops draining,
// and fetching pauses (exactly the old buffered-channel backpressure).
func (c *Consumer) run() {
	defer close(c.done)
	defer c.signalClose()
	defer c.conn.unregisterConsumer(c)
	defer c.topicHandle.Close()
	defer c.queueHandle.Close()

	outbox := make([]*Message, 0, c.batchSize)
	inflight := make(map[string]*Message)
	fetching := false
	// wokeDuringFetch latches a NOTIFY that arrives while a fetch is in flight.
	// That fetch's snapshot may predate the event (e.g. the ack of a group's
	// head, which is what makes its successor claimable), so its result must
	// not overwrite the wake with a later fetchAfter.
	wokeDuringFetch := false
	shutting := false
	cancelledInflight := false
	closedMessages := false
	ctxDone := c.ctx.Done()
	ioDone := c.dbCtx.Done()
	fetchAfter := time.Now() // fetch immediately on start

	topicCh := c.topicHandle.Wake()
	queueCh := c.queueHandle.Wake()

	// armFetch (re)arms the fetch timer only while a fetch is eligible — not
	// fetching, not shutting, and the outbox is empty — otherwise it disarms by
	// nil-ing the channel (a nil channel disables that select case). A fresh
	// time.After per arm keeps this simple and matches the connection actors;
	// Go 1.23+ GCs an abandoned timer promptly even before it fires, so the
	// frequent re-arming here (per delivered message) doesn't pile up timers
	// (pre-1.23 it would — hence go.mod's `go 1.23`).
	var fetchTimerC <-chan time.Time
	armFetch := func() {
		if fetching || shutting || len(outbox) > 0 {
			fetchTimerC = nil
			return
		}
		due := minTime(fetchAfter, time.Now().Add(c.checkTimeout))
		wait := time.Until(due)
		if wait < 0 {
			wait = 0
		}
		fetchTimerC = time.After(wait)
	}
	armFetch()

	releaseUndelivered := func(m *Message) {
		// A fetched-but-undelivered message: release it directly (no
		// delivery-attempt bump) and unbook it WITHOUT routing through
		// complete()/completed — the loop owns these and must not send to its
		// own completed channel (self-deadlock). Deregister from the extender
		// since it was registered at fetch.
		delete(inflight, m.trackingID)
		if !c.noAutoExtension {
			c.conn.extenderDeregister(c.queue, m.ID, m.consumerToken)
		}
		releaseCtx, cancelRelease := context.WithTimeout(c.dbCtx, time.Second)
		defer cancelRelease()
		if err := c.conn.releaseMessage(releaseCtx, c.queue, m.ID, m.consumerToken); err != nil {
			c.logger.Errorf("Failed to release message %d: %v", m.ID, err)
		}
	}

	for {
		var sendCh chan *Message
		var next *Message
		if !shutting && len(outbox) > 0 {
			sendCh, next = c.messages, outbox[0]
		}

		select {
		case <-ctxDone:
			ctxDone = nil
			shutting = true
		case <-ioDone:
			ioDone = nil
			shutting = true
			for _, m := range inflight {
				m.cancel()
				c.conn.extenderDeregister(c.queue, m.ID, m.consumerToken)
			}
			clear(inflight)

		case _, ok := <-topicCh:
			if !ok {
				topicCh = nil // listener closed; rely on queue/timer wake
				break
			}
			fetchAfter = time.Now()
			wokeDuringFetch = fetching
			armFetch()

		case _, ok := <-queueCh:
			if !ok {
				queueCh = nil
				break
			}
			fetchAfter = time.Now()
			wokeDuringFetch = fetching
			armFetch()

		case fr := <-c.fetched:
			fetching = false
			woke := wokeDuringFetch
			wokeDuringFetch = false
			if fr.fatal && !shutting {
				// The queue is gone (consume returned PMQ02). Record the reason
				// (so NotifyClose delivers it), begin our own drain, and notify
				// the connection so it tears down any sibling consumers on this
				// queue, stops keep-alive, and fires the queue-fatal handler.
				c.recordFatal(&QueueFatalError{Queue: c.queue, Err: fr.cause})
				go c.conn.queueFatal(c.queue, fr.cause, c.generation)
				shutting = true
			}
			if shutting {
				// Fetched after shutdown began: never deliver — release them.
				// They were not yet booked into inflight or the extender.
				for _, m := range fr.msgs {
					if err := c.conn.releaseMessage(c.dbCtx, c.queue, m.ID, m.consumerToken); err != nil {
						c.logger.Errorf("Failed to release message %d: %v", m.ID, err)
					}
				}
			} else {
				fetchAfter = fr.nextAt
				if woke {
					fetchAfter = time.Now()
				}
				for _, m := range fr.msgs {
					inflight[m.trackingID] = m // book BEFORE it becomes deliverable
					if !c.noAutoExtension {
						c.conn.extenderRegister(&extEntry{
							queue:      c.queue,
							token:      m.consumerToken,
							id:         m.ID,
							vtSec:      c.vtSec,
							threshold:  c.extensionThreshold,
							extendAt:   calculateExtendAt(m.GetVT(), c.extensionThreshold),
							expiresAt:  m.GetVT(),
							onExtended: m.setVT,
							cancel:     m.cancel,
						})
					}
					outbox = append(outbox, m)
				}
			}
			armFetch()

		case sendCh <- next:
			outbox[0] = nil // release the reference for GC
			outbox = outbox[1:]
			armFetch() // outbox may now be empty → eligible to fetch again

		case id := <-c.completed:
			if m, ok := inflight[id]; ok {
				delete(inflight, id)
				if !c.noAutoExtension {
					c.conn.extenderDeregister(c.queue, m.ID, m.consumerToken)
				}
			}

		case <-fetchTimerC:
			// Re-check eligibility: the timer may have been armed and then
			// ctx.Done set `shutting` without re-arming (that path doesn't
			// disarm), so a late fire must not start a fetch during shutdown.
			if !fetching && !shutting && len(outbox) == 0 {
				fetching = true
				go c.fetchInto()
			}
		}

		if shutting {
			// Close the Messages() channel once (no further sends happen — sendCh
			// is nil while shutting), then drain anything buffered-but-unread and
			// release it. A value in the buffer goes to exactly one receiver
			// (this drain OR a still-active caller), so each is handled once.
			if !closedMessages {
				close(c.messages)
				closedMessages = true
				for m := range c.messages {
					releaseUndelivered(m)
				}
			}
			// Release fetched-but-undelivered messages still in the outbox.
			for _, m := range outbox {
				releaseUndelivered(m)
			}
			outbox = outbox[:0]
			// Cancel in-flight handlers ONCE. They stay registered with the
			// extender and keep getting extended until they settle; their
			// settle removes them from inflight via the completed channel.
			if !cancelledInflight {
				for _, m := range inflight {
					m.cancel()
				}
				cancelledInflight = true
			}
			// Return only once the in-flight set is empty AND no fetch goroutine
			// is still outstanding (it must report back so its batch is released
			// and `fetching` clears, so no fetched batch is stranded).
			if !fetching && len(inflight) == 0 {
				return
			}
		}
	}
}

// fetchInto runs OFF the run loop: it claims a batch and reports it on
// c.fetched. It sets up each message's per-message context, completion hook and
// tracking id, but does NOT book them — the run loop does that so all state
// stays owned by one goroutine.
func (c *Consumer) fetchInto() {
	if c.ctx.Err() != nil {
		// Shutting down; still report an empty batch so the loop clears
		// `fetching` and can finish draining.
		c.fetched <- fetchResult{nextAt: time.Time{}}
		return
	}
	fetchCtx, cancelFetch := context.WithTimeout(c.dbCtx, time.Second)
	defer cancelFetch()
	msgs, err := c.conn.consumeMessages(fetchCtx, c.queue, c.batchSize, c.vtSec, c.generation)
	if err != nil {
		c.logger.Errorf("Failed to consume messages: %v", err)
	}
	c.logger.Debugf("Consumer - fetched %d messages (err=%v)", len(msgs), err)

	// The queue was deleted out-of-band: this is fatal for the consumer (it can
	// never get messages again). Flag it so the run loop tears down and signals
	// instead of retrying the fetch forever. (Any rows scanned before the error
	// are still delivered/released by the run loop's shutdown path.)
	if errors.Is(err, ErrQueueNotFound) {
		c.fetched <- fetchResult{msgs: msgs, fatal: true, cause: err}
		return
	}

	// Process whatever messages we got — even on a partial-batch error.
	// Successfully-scanned rows are already claimed server-side; dropping them
	// would just make us wait for vt expiry to see them again.
	for _, msg := range msgs {
		// Background parent so clients can't read values from the parent ctx.
		msg.StoppedCtx, msg.cancel = context.WithCancel(context.Background())
		msg.onComplete = c.onMessageSettled
		msg.trackingID = fmt.Sprintf("%d--%s", msg.ID, msg.consumerToken)
	}

	c.fetched <- fetchResult{msgs: msgs, nextAt: c.computeNextAt(len(msgs), err)}
}

// dueHintBackoff bounds how soon an empty fetch is retried when
// get_next_visible_time reports a row that is already due.
const dueHintBackoff = 100 * time.Millisecond

// computeNextAt mirrors the old fetch-loop scheduling: retry soon on error,
// refetch immediately after a full batch (drain a backlog), wait for the next
// visible message when the queue is empty, otherwise leave it to checkTimeout.
func (c *Consumer) computeNextAt(n int, err error) time.Time {
	if err != nil {
		return time.Now().Add(1 * time.Second)
	}
	if n == c.batchSize {
		return time.Now() // consumed max available; try for more immediately
	}
	if n == 0 {
		nextVisible, err := c.conn.getNextVisibleTime(c.ctx, c.queue)
		if err != nil {
			c.logger.Errorf("Failed to get next visible time: %v", err)
			return time.Now().Add(1 * time.Second)
		}
		c.logger.Debugf("Consumer - next available message in %d ms", time.Until(nextVisible).Milliseconds())
		if !nextVisible.IsZero() && !nextVisible.After(time.Now()) {
			// A visible-but-unclaimable row: locked by another transaction
			// (a claim in progress, or an AckWithTx whose transaction is
			// still open past the lease). Refetching at once would spin;
			// back off briefly. NOTIFYs still wake the loop immediately.
			return time.Now().Add(dueHintBackoff)
		}
		return nextVisible
	}
	return time.Time{} // partial batch → no specific time; checkTimeout governs
}

// onMessageSettled is the message completion hook. It runs once per message
// after the owning Ack/Nack/Release/AckWithTx operation finishes, and
// hands the trackingID to the run loop, which untracks it and deregisters it
// from the extender. Buffered + done-guarded so a settle never blocks the
// caller. (Untracking is unconditional: this fires even on a failed settle.)
func (c *Consumer) onMessageSettled(m *Message) {
	select {
	case c.completed <- m.trackingID:
	case <-c.done:
	}
}

// Messages returns a receive-only channel that yields messages as they are fetched.
//
// The channel is created with a buffer size equal to the consumer's batch size
// (configured via WithBatchSize). Messages are fetched in batches and pushed
// to this channel for consumption.
//
// Returns a receive-only channel of *Message pointers.
//
// Typical usage pattern:
//
//	for msg := range consumer.Messages() {
//	    // Process the message
//	    err := processMessage(msg.Payload)
//	    if err != nil {
//	        msg.Nack(ctx)
//	    } else {
//	        msg.Ack(ctx)
//	    }
//	}
//
// Behavior:
//   - The channel is closed when Consumer.Stop() is called or the Connection closes.
//   - Messages buffered in the channel but not yet delivered to the application
//     are automatically released back to the queue during shutdown WITHOUT
//     incrementing their delivery_attempts counter.
//   - If auto-extension is enabled (default), messages are automatically extended
//     to prevent visibility timeout expiration while being processed.
//
// The channel will block when the buffer is full, providing natural backpressure.
func (c *Consumer) Messages() <-chan *Message {
	return c.messages
}

// Stop gracefully stops the consumer and waits for all operations to complete.
//
// This method performs the following shutdown sequence:
//
//  1. Cancels the internal context to signal the run loop to shut down.
//  2. Stops fetching new messages from the queue.
//  3. Closes the Messages() channel.
//  4. Releases all buffered messages (not yet delivered to application) back
//     to the queue WITHOUT incrementing their delivery_attempts.
//  5. Cancels the StoppedCtx context on all in-flight messages to signal the
//     application that shutdown is in progress.
//  6. Waits for all in-flight messages to be completed (Ack/Nack/Release).
//  7. Closes the consumer's event-listener handles.
//
// This method is safe to call multiple times - subsequent calls are no-ops.
//
// The method blocks until the run loop has terminated and all in-flight messages
// have been processed. Applications should ensure they complete message
// processing promptly or check msg.StoppedCtx.Done() to detect shutdown and
// release messages early.
//
// Example:
//
//	consumer, _ := conn.Consume("queue-name")
//	defer consumer.Stop()
//
//	for msg := range consumer.Messages() {
//	    select {
//	    case <-msg.StoppedCtx.Done():
//	        // Consumer is stopping, release message quickly
//	        msg.Release(context.Background())
//	        return
//	    default:
//	    }
//	    // Normal processing...
//	}
func (c *Consumer) Stop() {
	c.cancel()
	<-c.done
	if c.topicHandle != nil {
		c.topicHandle.Close()
	}
	if c.queueHandle != nil {
		c.queueHandle.Close()
	}
}

// queueName reports the queue this consumer is bound to (implements
// fatalConsumer for the connection's queueFatal routing).
func (c *Consumer) queueName() string { return c.queue }

// fatal tears the consumer down because its queue is gone: it records the reason
// (delivered on the NotifyClose channels) and cancels, which runs the normal
// drain (handlers cancelled, in-flight deregistered from the extender). Safe to
// call repeatedly and concurrently with Stop/self-teardown — the reason is
// recorded once.
func (c *Consumer) fatal(err error) {
	c.recordFatal(err)
	c.cancel()
}

// recordFatal stores the close reason at most once.
func (c *Consumer) recordFatal(err error) {
	c.fatalOnce.Do(func() {
		c.fatalMu.Lock()
		c.fatalErr = err
		c.fatalMu.Unlock()
	})
}

// NotifyClose registers ch to receive the reason this consumer closed. On a
// fatal teardown (the queue is gone) the reason — a *QueueFatalError matching
// errors.Is(err, ErrQueueGone) — is sent once, then ch is closed; on a normal
// Stop, ch is simply closed with no value. Pass a BUFFERED channel (cap >= 1) so
// the reason is delivered even if you aren't selecting on it yet. If the consumer
// has already closed, ch receives the outcome immediately. Mirrors the RabbitMQ
// Go client's Channel.NotifyClose.
func (c *Consumer) NotifyClose(ch chan error) chan error {
	c.fatalMu.Lock()
	defer c.fatalMu.Unlock()
	if c.notified {
		c.deliverClose(ch)
	} else {
		c.notifyChans = append(c.notifyChans, ch)
	}
	return ch
}

// deliverClose sends the stored reason (if any) and closes ch. Caller holds
// fatalMu. The send is non-blocking so an unbuffered/full channel can't wedge
// the run loop — the close still fires.
func (c *Consumer) deliverClose(ch chan error) {
	if c.fatalErr != nil {
		select {
		case ch <- c.fatalErr:
		default:
		}
	}
	close(ch)
}

// signalClose delivers the close outcome to all registered NotifyClose channels
// and marks the consumer notified (so later NotifyClose calls deliver
// immediately). Called once from run()'s defer.
func (c *Consumer) signalClose() {
	c.fatalMu.Lock()
	defer c.fatalMu.Unlock()
	c.notified = true
	for _, ch := range c.notifyChans {
		c.deliverClose(ch)
	}
	c.notifyChans = nil
}

func (c *Consumer) queueGeneration() string { return c.generation }
