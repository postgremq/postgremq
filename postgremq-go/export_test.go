package postgremq

import (
	"context"
	"time"
)

// This file is compiled only during `go test`. It exposes a few unexported
// pieces of state to the external `postgremq_test` package so internal
// invariants (refcounted LISTEN sharing, etc.) can be asserted without
// leaking those hooks into the public API.

// EventListener returns the connection's event listener. Tests only.
func (c *Connection) EventListener() *EventListener {
	return c.eventListener
}

// SubscriberCount returns the number of subscribers currently registered for
// a given channel (e.g. "pmq:t:topic" or "pmq:q:queue"). Tests only.
func (el *EventListener) SubscriberCount(channel string) int {
	el.mu.Lock()
	defer el.mu.Unlock()
	return el.desired[channel]
}

// StoppedChan returns the current el.stopped channel for identity comparison
// in tests. newEventListener seeds it with an already-closed channel; Start
// replaces it with a fresh open one when it actually launches goroutines.
// Tests rely on this to assert "Start was a no-op" by checking the channel
// reference is unchanged. Tests only.
func (el *EventListener) StoppedChan() <-chan struct{} {
	return el.stopped
}

// ConsumeMessages exposes the unexported consumeMessages so tests can
// drive it directly with a MockPool — useful for asserting retry
// behavior without spinning up a Consumer goroutine. Tests only.
func (c *Connection) ConsumeMessages(ctx context.Context, queue string, limit, vt int) ([]*Message, error) {
	return c.consumeMessages(ctx, queue, limit, vt)
}

// CalculateExtendAt exposes the (now package-level) extension-timing math so
// tests can verify it directly without spinning up a real Consumer goroutine +
// database. Tests only.
func (c *Consumer) CalculateExtendAt(vtUntil time.Time) time.Time {
	return calculateExtendAt(vtUntil, c.extensionThreshold)
}

// NewConsumerForTest constructs a Consumer with just the fields needed for
// pure-math tests of CalculateExtendAt — does not start any goroutines or
// touch the database. Tests only.
func NewConsumerForTest(extensionThreshold float64) *Consumer {
	return &Consumer{extensionThreshold: extensionThreshold}
}

// ExtenderRegister drives the connection-level vt-extender actor directly so
// its batching / composite-key / lease-loss behavior can be tested without a
// full consumer + database. Tests only.
func (c *Connection) ExtenderRegister(queue, token string, id int64, vtSec int, threshold float64, vt time.Time, cancel context.CancelFunc) {
	c.extenderRegister(&extEntry{
		queue:     queue,
		token:     token,
		id:        id,
		vtSec:     vtSec,
		threshold: threshold,
		extendAt:  calculateExtendAt(vt, threshold),
		cancel:    cancel,
	})
}

// ExtenderDeregister removes a message from the extender. Tests only.
func (c *Connection) ExtenderDeregister(queue string, id int64, token string) {
	c.extenderDeregister(queue, id, token)
}

// ValidateConsumeOptionsForTest exposes the unexported validation function
// to external tests so option misuse can be unit-tested without spinning
// up a real Consumer. Tests only.
func ValidateConsumeOptionsForTest(opts ...ConsumeOption) error {
	options := defaultConsumeOptions()
	for _, o := range opts {
		o(&options)
	}
	return validateConsumeOptions(&options)
}
