// Keep-alive actor tests: cover the connection-level keep-alive actor that
// batches every exclusive queue's keep-alive into one
// extend_queue_keep_alive_multi call per tick.
package postgremq_test

import (
	"context"
	"errors"
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

// keepAliveSpyPool records every extend_queue_keep_alive_multi call and lets a
// test control which queues are reported "kept" (and inject a transient error).
type keepAliveSpyPool struct {
	mu sync.Mutex
	// calls is the queue-name slice from each extend_queue_keep_alive_multi call.
	calls [][]string
	// keptFn decides, per call, which queues to report as kept. nil => all.
	keptFn func(names []string) []string
	// errOnce, when set, makes the next extend call fail and is then cleared.
	errOnce error
}

func (p *keepAliveSpyPool) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (p *keepAliveSpyPool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	// Only the keep-alive multi query is interesting here.
	names, ok := args[0].([]string)
	if !ok {
		return &MockRows{}, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.errOnce != nil {
		err := p.errOnce
		p.errOnce = nil
		return nil, err
	}
	recorded := make([]string, len(names))
	copy(recorded, names)
	p.calls = append(p.calls, recorded)

	kept := names
	if p.keptFn != nil {
		kept = p.keptFn(names)
	}
	scans := make([]func(dest ...any) error, len(kept))
	for i := range kept {
		qn := kept[i]
		scans[i] = func(dest ...any) error {
			*(dest[0].(*string)) = qn
			if len(dest) > 1 {
				var ms int64
				for j, name := range names {
					if name == qn {
						ms = args[1].([]int64)[j]
					}
				}
				until := time.Now().Add(time.Duration(ms) * time.Millisecond)
				*(dest[1].(**time.Time)) = &until
				*(dest[2].(*string)) = "extended"
			}
			return nil
		}
	}
	return &MockRows{ScanFuncs: scans}, nil
}

type generationRow struct{}

func (generationRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = "00000000-0000-0000-0000-000000000001"
	return nil
}
func (p *keepAliveSpyPool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	if sql == discoverySQL {
		return discoveryRow{}
	}
	return generationRow{}
}
func (p *keepAliveSpyPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) { return nil, nil }
func (p *keepAliveSpyPool) Close()                                             {}

func (p *keepAliveSpyPool) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// maxBatch returns the size of the largest single call recorded so far.
func (p *keepAliveSpyPool) maxBatch() int {
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

// everReferenced reports whether queue appeared in any recorded call.
func (p *keepAliveSpyPool) everReferenced(queue string) bool {
	return p.referencedSince(0, queue)
}

// referencedSince reports whether any call at index >= start referenced queue.
func (p *keepAliveSpyPool) referencedSince(start int, queue string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := start; i < len(p.calls); i++ {
		for _, n := range p.calls[i] {
			if n == queue {
				return true
			}
		}
	}
	return false
}

// TestKeepAliveBatchesAllQueuesInOneCall verifies that several exclusive queues
// are coalesced into ONE extend_queue_keep_alive_multi call per tick.
func TestKeepAliveBatchesAllQueuesInOneCall(t *testing.T) {
	t.Parallel()
	spy := &keepAliveSpyPool{}
	conn, err := postgremq.DialFromPool(context.Background(), spy)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	for _, q := range []string{"qa", "qb", "qc"} {
		require.NoError(t, conn.CreateQueue(ctx, q, "t", true,
			postgremq.WithKeepAliveInterval(200*time.Millisecond)))
	}

	// Coalescing: due queues are extended in ONE batched call (not one call
	// per queue). At least one flush must carry multiple queues, and all three
	// must be kept alive. (We don't require all three in the very first tick —
	// per-registration stagger can split the first flush; the guarantee is that
	// concurrently-due queues share a single round-trip.)
	require.Eventually(t, func() bool {
		return spy.maxBatch() >= 2 &&
			spy.everReferenced("qa") && spy.everReferenced("qb") && spy.everReferenced("qc")
	}, 3*time.Second, 10*time.Millisecond, "expected queues to be batched into one call")
}

// TestKeepAlivePermanentFailureDropsAndNotifies verifies that a queue omitted
// from a successful result is dropped from the schedule and escalates to
// queueFatal — firing WithQueueFatalHandler exactly once (with ErrQueueGone),
// with no further keep-alive calls referencing it.
func TestKeepAlivePermanentFailureDropsAndNotifies(t *testing.T) {
	t.Parallel()
	spy := &keepAliveSpyPool{
		// "gone" is never reported kept => permanent failure.
		keptFn: func(names []string) []string {
			var kept []string
			for _, n := range names {
				if n != "gone" {
					kept = append(kept, n)
				}
			}
			return kept
		},
	}
	failures := make(chan string, 8)
	conn, err := postgremq.DialFromPool(context.Background(), spy,
		postgremq.WithQueueFatalHandler(func(queue string, err error) {
			assert.ErrorIs(t, err, postgremq.ErrQueueGone)
			failures <- queue
		}))
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	require.NoError(t, conn.CreateQueue(ctx, "alive", "t", true,
		postgremq.WithKeepAliveInterval(200*time.Millisecond)))
	require.NoError(t, conn.CreateQueue(ctx, "gone", "t", true,
		postgremq.WithKeepAliveInterval(200*time.Millisecond)))

	select {
	case q := <-failures:
		assert.Equal(t, "gone", q)
	case <-time.After(3 * time.Second):
		t.Fatal("expected queue-fatal handler for 'gone'")
	}

	// No second failure, and later calls must not reference the dropped queue.
	callsAtDrop := spy.callCount()
	require.Eventually(t, func() bool { return spy.callCount() > callsAtDrop+1 },
		3*time.Second, 10*time.Millisecond, "expected continued keep-alive for 'alive'")
	assert.False(t, spy.referencedSince(callsAtDrop, "gone"), "dropped queue must not be referenced again")
	select {
	case q := <-failures:
		t.Fatalf("unexpected second failure: %s", q)
	default:
	}
}

// TestKeepAliveTransientErrorRetriesNoFailure verifies a transient error is
// retried on the next tick and does not escalate to queueFatal.
func TestKeepAliveTransientErrorRetriesNoFailure(t *testing.T) {
	t.Parallel()
	spy := &keepAliveSpyPool{errOnce: errors.New("transient boom")}
	failures := make(chan string, 8)
	conn, err := postgremq.DialFromPool(context.Background(), spy,
		postgremq.WithoutRetries(), // make the transient error reach the actor immediately
		postgremq.WithQueueFatalHandler(func(queue string, err error) {
			failures <- queue
		}))
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	require.NoError(t, conn.CreateQueue(ctx, "q", "t", true,
		postgremq.WithKeepAliveInterval(200*time.Millisecond)))

	// After the injected error, a later tick must succeed (a recorded call).
	require.Eventually(t, func() bool { return spy.callCount() >= 1 },
		3*time.Second, 10*time.Millisecond, "expected a successful retry")

	select {
	case q := <-failures:
		t.Fatalf("unexpected failure for a transient error: %s", q)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestKeepAliveAdvancesKeepAliveUntil verifies (against a real DB) that the
// actor actually advances keep_alive_until for a registered exclusive queue.
func TestKeepAliveAdvancesKeepAliveUntil(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "exq", "topic", true,
		postgremq.WithKeepAliveInterval(400*time.Millisecond)))

	readKA := func() time.Time {
		qs, err := conn.ListQueues(ctx)
		require.NoError(t, err)
		for _, q := range qs {
			if q.QueueName == "exq" {
				require.NotNil(t, q.KeepAliveUntil)
				return *q.KeepAliveUntil
			}
		}
		t.Fatal("queue not found")
		return time.Time{}
	}

	first := readKA()
	require.Eventually(t, func() bool { return readKA().After(first) },
		3*time.Second, 50*time.Millisecond, "keep_alive_until should advance")
}

// TestKeepAliveDeleteQueueDeregisters verifies that DeleteQueue deregisters the
// queue so an intentional delete does not fire the queue-fatal handler.
func TestKeepAliveDeleteQueueDeregisters(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	failures := make(chan string, 8)
	conn, err := postgremq.DialFromPool(context.Background(), pool,
		postgremq.WithQueueFatalHandler(func(queue string, err error) {
			failures <- queue
		}))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "exq", "topic", true,
		postgremq.WithKeepAliveInterval(200*time.Millisecond)))
	require.NoError(t, conn.DeleteQueue(ctx, "exq"))

	select {
	case q := <-failures:
		t.Fatalf("intentional delete fired keep-alive failure for %s", q)
	case <-time.After(600 * time.Millisecond):
	}
}

// TestKeepAliveCreateCloseRace interleaves CreateQueue(exclusive) with Close()
// to shake out shutdown races (run with -race).
func TestKeepAliveCreateCloseRace(t *testing.T) {
	t.Parallel()
	spy := &keepAliveSpyPool{}
	conn, err := postgremq.DialFromPool(context.Background(), spy)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = conn.CreateQueue(context.Background(),
				"q", "t", true, postgremq.WithKeepAliveInterval(50*time.Millisecond))
		}(i)
	}
	// Close concurrently with the creates.
	go func() { _ = conn.Close() }()
	wg.Wait()
	_ = conn.Close()
}
