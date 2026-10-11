// vt-extender actor tests: cover the connection-level extender that coalesces
// every consumer's due visibility-timeout extensions into one set_vt_batch_multi
// call per tick. The headline guarantee is the
// composite (queue, message_id) key: the same message_id distributed to
// two queues is two independent entries, both extended.
package postgremq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// qid is a (queue, id) pair recorded from a set_vt_batch_multi call.
type qid struct {
	queue string
	id    int64
}

// extenderSpyPool records every set_vt_batch_multi call and lets a test choose
// which (queue,id) pairs to report extended (and with what new vt).
type extenderSpyPool struct {
	mu sync.Mutex
	// calls is the list of (queue,id) pairs from each set_vt_batch_multi call.
	calls [][]qid
	// keepFn decides, per call, which pairs to report as extended. nil => all.
	keepFn func([]qid) []qid
	// vtFromNow is how far in the future the reported new vt is, per call. The
	// extender reschedules kept entries off this, so a short value makes a kept
	// message re-extend soon; zero defaults to 1h (one call is enough).
	vtFromNow time.Duration
}

func (p *extenderSpyPool) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (p *extenderSpyPool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	queues, ok := args[0].([]string)
	if !ok {
		return &MockRows{}, nil
	}
	ids := args[1].([]int64)

	pairs := make([]qid, len(queues))
	for i := range queues {
		pairs[i] = qid{queue: queues[i], id: ids[i]}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	rec := make([]qid, len(pairs))
	copy(rec, pairs)
	p.calls = append(p.calls, rec)

	kept := pairs
	if p.keepFn != nil {
		kept = p.keepFn(pairs)
	}
	horizon := p.vtFromNow
	if horizon == 0 {
		horizon = time.Hour
	}
	vt := time.Now().Add(horizon)
	scans := make([]func(dest ...any) error, len(kept))
	for i := range kept {
		k := kept[i]
		scans[i] = func(dest ...any) error {
			*(dest[0].(*string)) = k.queue
			*(dest[1].(*int64)) = k.id
			*(dest[2].(**time.Time)) = &vt
			for j, pair := range pairs {
				if pair == k {
					*(dest[3].(*string)) = args[2].([]string)[j]
					break
				}
			}
			*(dest[4].(*string)) = "extended"
			return nil
		}
	}
	return &MockRows{ScanFuncs: scans}, nil
}

func (p *extenderSpyPool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	if sql == discoverySQL {
		return discoveryRow{}
	}
	return nil
}
func (p *extenderSpyPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) { return nil, nil }
func (p *extenderSpyPool) Close()                                             {}

func (p *extenderSpyPool) callsSnapshot() [][]qid {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]qid, len(p.calls))
	copy(out, p.calls)
	return out
}

// maxCallSize returns the size of the largest single recorded call.
func (p *extenderSpyPool) maxCallSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	max := 0
	for _, c := range p.calls {
		if len(c) > max {
			max = len(c)
		}
	}
	return max
}

// distinctExtended returns the set of (queue,id) pairs ever extended.
func (p *extenderSpyPool) distinctExtended() map[qid]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[qid]bool{}
	for _, c := range p.calls {
		for _, k := range c {
			out[k] = true
		}
	}
	return out
}

// callContaining returns the first recorded call that contains the given pair.
func (p *extenderSpyPool) callContaining(want qid) []qid {
	for _, call := range p.callsSnapshot() {
		for _, k := range call {
			if k == want {
				return call
			}
		}
	}
	return nil
}

// TestExtenderCompositeKeyNoBleed verifies that the same message_id on two
// queues is tracked as two independent entries and both are extended in ONE
// tick. A heap keyed on message_id alone would collapse them.
func TestExtenderCompositeKeyNoBleed(t *testing.T) {
	t.Parallel()
	spy := &extenderSpyPool{}
	conn, err := postgremq.DialFromPool(context.Background(), spy)
	require.NoError(t, err)
	defer conn.Close()

	vt := time.Now().Add(100 * time.Millisecond)
	conn.ExtenderRegister("q1", "tok1", 5, 30, 0.5, vt, func() {})
	conn.ExtenderRegister("q2", "tok2", 5, 30, 0.5, vt, func() {})

	require.Eventually(t, func() bool {
		return spy.callContaining(qid{"q1", 5}) != nil
	}, 3*time.Second, 10*time.Millisecond, "expected an extension call")

	// Both copies of message 5 must ride a single set_vt_batch_multi call.
	call := spy.callContaining(qid{"q1", 5})
	assert.Contains(t, call, qid{"q1", 5})
	assert.Contains(t, call, qid{"q2", 5}, "same message_id on a second queue must also be extended")
}

// TestExtenderLeaseLostCancels verifies that a (queue,id) omitted from the
// result has its handler cancelled and is dropped (no further calls).
func TestExtenderLeaseLostCancels(t *testing.T) {
	t.Parallel()
	spy := &extenderSpyPool{
		vtFromNow: 100 * time.Millisecond, // kept entry re-extends ~every 50ms
		keepFn: func(pairs []qid) []qid { // q2's copy is "lost"
			var kept []qid
			for _, k := range pairs {
				if k.queue != "q2" || k.id != 5 {
					kept = append(kept, k)
				}
			}
			return kept
		},
	}
	conn, err := postgremq.DialFromPool(context.Background(), spy)
	require.NoError(t, err)
	defer conn.Close()

	lostCtx, lostCancel := context.WithCancel(context.Background())
	keptCtx, keptCancel := context.WithCancel(context.Background())
	defer keptCancel()

	vt := time.Now().Add(100 * time.Millisecond)
	conn.ExtenderRegister("q1", "tok1", 5, 30, 0.5, vt, keptCancel)
	conn.ExtenderRegister("q2", "tok2", 5, 30, 0.5, vt, lostCancel)

	// The lost message's StoppedCtx-equivalent is cancelled.
	select {
	case <-lostCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("lease-lost message was not cancelled")
	}
	// The kept message keeps being extended; its cancel is never called.
	select {
	case <-keptCtx.Done():
		t.Fatal("kept message was wrongly cancelled")
	case <-time.After(300 * time.Millisecond):
	}

	// The lost (queue,id) must not appear in any later call.
	callsAfter := len(spy.callsSnapshot())
	require.Eventually(t, func() bool { return len(spy.callsSnapshot()) > callsAfter },
		3*time.Second, 10*time.Millisecond, "kept message should keep being extended")
	for _, call := range spy.callsSnapshot()[callsAfter:] {
		for _, k := range call {
			assert.NotEqual(t, qid{"q2", 5}, k, "dropped lease must not be re-extended")
		}
	}
}

// TestExtenderBatchSizeCapsPerTick verifies WithExtenderBatchSize (a connection
// option) bounds how many messages a single set_vt_batch_multi call carries,
// while all registered messages still get extended across ticks.
func TestExtenderBatchSizeCapsPerTick(t *testing.T) {
	t.Parallel()
	spy := &extenderSpyPool{vtFromNow: 150 * time.Millisecond}
	conn, err := postgremq.DialFromPool(context.Background(), spy, postgremq.WithExtenderBatchSize(2))
	require.NoError(t, err)
	defer conn.Close()

	vt := time.Now().Add(100 * time.Millisecond)
	for i := int64(1); i <= 5; i++ {
		conn.ExtenderRegister("q", "tok", i, 30, 0.5, vt, func() {})
	}

	// All five eventually extended...
	require.Eventually(t, func() bool {
		return len(spy.distinctExtended()) == 5
	}, 3*time.Second, 10*time.Millisecond, "all messages should be extended over time")
	// ...but no single call ever exceeded the cap of 2.
	assert.LessOrEqual(t, spy.maxCallSize(), 2, "per-tick batch must respect WithExtenderBatchSize")
}

// TestExtenderDeregisterStopsExtension verifies a deregistered message is not
// extended.
func TestExtenderDeregisterStopsExtension(t *testing.T) {
	t.Parallel()
	spy := &extenderSpyPool{}
	conn, err := postgremq.DialFromPool(context.Background(), spy)
	require.NoError(t, err)
	defer conn.Close()

	// Long VT so extendAt is far out; deregister before it ever fires.
	vt := time.Now().Add(1 * time.Hour)
	conn.ExtenderRegister("q1", "tok1", 7, 3600, 0.5, vt, func() {})
	conn.ExtenderDeregister("q1", 7, "tok")

	time.Sleep(200 * time.Millisecond)
	assert.Nil(t, spy.callContaining(qid{"q1", 7}), "deregistered message must not be extended")
}

// TestSetVTBatchMultiCompositeKey is the real-DB end-to-end check that the SQL
// function correlates by (queue, message_id) and does not bleed across queues:
// one published message fans out to two queues (same message_id); extending
// only q1 must leave q2's vt untouched.
func TestSetVTBatchMultiCompositeKey(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "q1", "topic", false))
	require.NoError(t, conn.CreateQueue(ctx, "q2", "topic", false))

	msgID, err := conn.Publish(ctx, "topic", []byte(`{}`))
	require.NoError(t, err)

	// Consume the same message from both queues (no auto-extension so the
	// connection extender doesn't move the vt under us).
	c1, err := conn.Consume("q1", postgremq.WithVT(60), postgremq.WithNoAutoExtension())
	require.NoError(t, err)
	defer c1.Stop()
	c2, err := conn.Consume("q2", postgremq.WithVT(60), postgremq.WithNoAutoExtension())
	require.NoError(t, err)
	defer c2.Stop()

	m1 := <-c1.Messages()
	m2 := <-c2.Messages()
	require.Equal(t, msgID, m1.ID)
	require.Equal(t, msgID, m2.ID)

	// Extend BOTH copies in one call → both returned, correlated by (queue,id).
	locks, err := conn.SetVTBatchMulti(ctx, []postgremq.MultiExtension{
		{Queue: "q1", ID: m1.ID, Token: m1.ConsumerToken(), VTSec: 120},
		{Queue: "q2", ID: m2.ID, Token: m2.ConsumerToken(), VTSec: 120},
	})
	require.NoError(t, err)
	require.Len(t, locks, 2)

	// Now extend ONLY q1 and confirm q2's vt is untouched (no cross-queue bleed).
	q2Before := vtOf(t, conn, ctx, "q2", msgID)
	_, err = conn.SetVTBatchMulti(ctx, []postgremq.MultiExtension{
		{Queue: "q1", ID: m1.ID, Token: m1.ConsumerToken(), VTSec: 240},
	})
	require.NoError(t, err)
	q2After := vtOf(t, conn, ctx, "q2", msgID)
	assert.True(t, q2Before.Equal(q2After), "extending q1 must not change q2's vt")

	// Settle both so the deferred Stop() drains cleanly (Stop waits for all
	// in-flight messages to complete).
	require.NoError(t, m1.Ack(ctx))
	require.NoError(t, m2.Ack(ctx))
}

func vtOf(t *testing.T, conn *postgremq.Connection, ctx context.Context, queue string, id int64) time.Time {
	t.Helper()
	msgs, err := conn.ListMessages(ctx, queue)
	require.NoError(t, err)
	for _, m := range msgs {
		if m.MessageID == id {
			return m.VT
		}
	}
	t.Fatalf("message %d not found in queue %s", id, queue)
	return time.Time{}
}
