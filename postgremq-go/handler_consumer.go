package postgremq

import (
	"context"
	"sync"
)

// MessageHandler processes a message. The handler should call msg.Ack() or msg.Nack()
// to acknowledge or reject the message. If the handler returns without calling
// either, it is auto-acked only if its context is still live. A cancelled
// return or panic is auto-nacked; explicit settlement takes precedence.
//
// The context is cancelled when the consumer is stopping - handlers should
// check ctx.Done() and return promptly.
type MessageHandler func(ctx context.Context, msg *Message)

// HandlerConsumer wraps a Consumer and dispatches messages to a handler function
// with concurrency limiting. Messages are automatically acked if the handler
// returns without calling Ack/Nack, and automatically nacked on panic.
type HandlerConsumer struct {
	conn        *Connection
	consumer    *Consumer
	handler     MessageHandler
	maxInFlight int
	logger      LevelLogger

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup // dispatch loop
	handlerWg sync.WaitGroup // active handlers
	sem       chan struct{}  // semaphore for maxInFlight
}

func newHandlerConsumer(
	conn *Connection,
	consumer *Consumer,
	handler MessageHandler,
	logger LevelLogger,
	options handlerConsumeOptions,
) *HandlerConsumer {
	// hc.ctx is parented on conn.ctx, NOT on the user-supplied ctx that
	// ConsumeHandler accepts. The dispatch-loop lifetime is owned by
	// HandlerConsumer.Stop() and (transitively) Connection.Close(); the
	// user ctx parameter is bootstrap-only and does not control the
	// returned resource. Cancelling the user ctx therefore does not exit
	// dispatchLoop — call Stop() for that. This matches the canonical
	// Go pattern (pgxpool, amqp, sql) and prevents the half-shutdown
	// where dispatchLoop exits but the underlying Consumer keeps
	// fetching with no one to drain its Messages channel.
	ctx, cancel := context.WithCancel(conn.ctx)
	hc := &HandlerConsumer{
		conn:        conn,
		consumer:    consumer,
		handler:     handler,
		maxInFlight: options.maxInFlight,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}
	if options.maxInFlight > 0 {
		hc.sem = make(chan struct{}, options.maxInFlight)
	}
	return hc
}

// start begins the underlying consumer and dispatch loop. Called internally after creation.
func (hc *HandlerConsumer) start() {
	hc.consumer.start()
	hc.wg.Add(1)
	go hc.dispatchLoop()
}

// dispatchLoop reads from consumer and dispatches to handlers
func (hc *HandlerConsumer) dispatchLoop() {
	defer hc.wg.Done()
	defer func() { hc.handlerWg.Wait(); <-hc.consumer.done; hc.conn.unregisterConsumer(hc) }()

	for {
		// Acquire slot first (if maxInFlight is set)
		if hc.sem != nil {
			select {
			case hc.sem <- struct{}{}:
				// Got a slot
			case <-hc.ctx.Done():
				return
			}
		}

		// Now read a message (we have a slot reserved)
		var msg *Message
		var ok bool
		select {
		case msg, ok = <-hc.consumer.Messages():
			if !ok {
				// Channel closed, release slot and exit
				if hc.sem != nil {
					<-hc.sem
				}
				return
			}
		case <-hc.ctx.Done():
			// Context cancelled, release slot and exit
			if hc.sem != nil {
				<-hc.sem
			}
			return
		}

		hc.handlerWg.Add(1)
		go hc.runHandler(msg)
	}
}

// runHandler executes the handler for a single message
func (hc *HandlerConsumer) runHandler(msg *Message) {
	defer func() {
		if hc.sem != nil {
			<-hc.sem
		}
		hc.handlerWg.Done()
	}()

	finishMetric := hc.conn.metrics.startHandler(msg.StoppedCtx, msg.queue)
	panicked := hc.callHandler(msg)
	code := ""
	if panicked {
		code = "handler_error"
	} else if msg.StoppedCtx.Err() != nil {
		code = "cancelled"
	}
	finishMetric(code)
	if msg.settlementStarted.Load() {
		return
	}
	// Cancellation does not establish successful processing. Explicit Ack is
	// allowed, but an unfinished handler returning on cancellation is retried.
	var err error
	if panicked || msg.StoppedCtx.Err() != nil {
		err = msg.Nack(hc.conn.ioCtx)
	} else {
		err = msg.Ack(hc.conn.ioCtx)
	}
	if err != nil {
		hc.logger.Errorf("failed to settle message %d: %v", msg.ID, err)
	}

}

// callHandler calls the handler with panic recovery. Returns true if panicked.
func (hc *HandlerConsumer) callHandler(msg *Message) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			hc.logger.Errorf("handler panic for message %d: %v", msg.ID, r)
			panicked = true
		}
	}()
	hc.handler(msg.StoppedCtx, msg)
	return false
}

// Stop signals the handler consumer to stop and waits for all handlers to complete.
// This method blocks until all in-flight handlers have finished.
func (hc *HandlerConsumer) Stop() {
	// Stop underlying consumer first (non-blocking) to stop new messages arriving
	// and cancel StoppedCtx for in-flight messages (signals handlers to stop)
	consumerStopped := make(chan struct{})
	go func() {
		hc.consumer.Stop()
		close(consumerStopped)
	}()

	// Signal dispatch loop to exit
	hc.cancel()

	// Wait for dispatch loop to finish (it will exit when consumer.Messages() closes)
	hc.wg.Wait()

	// Wait for all in-flight handlers to complete
	hc.handlerWg.Wait()

	// Wait for consumer stop to fully complete
	<-consumerStopped
}

// queueName reports the queue this handler consumer is bound to (implements
// fatalConsumer for the connection's queueFatal routing).
func (hc *HandlerConsumer) queueName() string { return hc.consumer.queue }

// fatal tears the handler consumer down because its queue is gone. It delegates
// to the underlying consumer: the dispatch loop exits when Messages() closes and
// in-flight handlers are cancelled via their StoppedCtx.
func (hc *HandlerConsumer) fatal(err error) { hc.consumer.fatal(err) }

// NotifyClose registers ch to receive the reason this consumer closed — for a
// handler consumer this is the primary way to learn the queue is gone, since
// there is no Messages() loop to end. See Consumer.NotifyClose.
func (hc *HandlerConsumer) NotifyClose(ch chan error) chan error {
	return hc.consumer.NotifyClose(ch)
}

func (hc *HandlerConsumer) queueGeneration() string { return hc.consumer.generation }
