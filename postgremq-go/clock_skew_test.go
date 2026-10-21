// Clock-skew tests: lease renewal and keep-alive timing must not depend on the
// application and database clocks agreeing. The database clock is shifted by
// shadowing clock_timestamp() (the only clock the schema reads) for the
// connections of one pool.
package postgremq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// setupSkewedDatabase returns a plain pool and a pool whose database clock
// reads skew ahead of the real one (behind, if negative). The schema is
// installed through the skewed pool, so its column defaults use that clock too.
func setupSkewedDatabase(t *testing.T, skew time.Duration) (plain, skewed *pgxpool.Pool, ctx context.Context) {
	t.Helper()
	ctx = context.Background()
	plain, dbName, err := createTestDatabase(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		plain.Close()
		_, _ = sharedContainer.adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName))
	})
	_, err = plain.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA skew;
		CREATE FUNCTION skew.clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE
			AS $$ SELECT pg_catalog.clock_timestamp() + %d * interval '1 ms' $$;`, skew.Milliseconds()))
	require.NoError(t, err)
	cfg := plain.Config().Copy()
	cfg.ConnConfig.RuntimeParams["search_path"] = "skew, pg_catalog, public"
	skewed, err = pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(skewed.Close)
	_, err = skewed.Exec(ctx, latestSQL(t))
	require.NoError(t, err)
	var offset float64
	require.NoError(t, skewed.QueryRow(ctx,
		"SELECT extract(epoch FROM clock_timestamp() - pg_catalog.clock_timestamp())").Scan(&offset))
	require.InDelta(t, skew.Seconds(), offset, 0.5, "database clock is not skewed")
	return plain, skewed, ctx
}

// settleOnCleanup releases msg if a failed assertion left it unsettled, so
// the consumer's Stop (an earlier-registered cleanup) does not wait on it.
func settleOnCleanup(t *testing.T, msg *postgremq.Message) {
	t.Cleanup(func() { _ = msg.Release(context.Background()) })
}

func consumeOneSkewed(t *testing.T, skew time.Duration, vtSec int) (*pgxpool.Pool, context.Context, *postgremq.Message) {
	t.Helper()
	plain, skewed, ctx := setupSkewedDatabase(t, skew)
	conn, err := postgremq.Dial(ctx, skewed.Config())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.CreateTopic(ctx, "t"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "t", false))
	_, err = conn.Publish(ctx, "t", json.RawMessage(`{}`))
	require.NoError(t, err)
	consumer, err := conn.Consume("q", postgremq.WithVT(vtSec))
	require.NoError(t, err)
	t.Cleanup(consumer.Stop)
	select {
	case msg := <-consumer.Messages():
		settleOnCleanup(t, msg)
		return plain, ctx, msg
	case <-time.After(5 * time.Second):
		t.Fatal("no message delivered")
		return nil, nil, nil
	}
}

// A database clock ahead of the application must not delay renewal past the
// real expiry: the lease ends vt seconds after the claim, whatever the
// database's timestamps read on the application's clock.
func TestClockSkew_DatabaseAheadRenewsBeforeExpiry(t *testing.T) {
	t.Parallel()
	plain, ctx, msg := consumeOneSkewed(t, 10*time.Second, 3)
	time.Sleep(4 * time.Second) // past the unrenewed 3 s lease
	var live bool
	require.NoError(t, plain.QueryRow(ctx,
		"SELECT vt > clock_timestamp() + interval '10 s' FROM postgremq.queue_messages WHERE message_id = $1",
		msg.ID).Scan(&live))
	assert.True(t, live, "lease expired before it was renewed")
	assert.NoError(t, msg.StoppedCtx.Err())
	assert.NoError(t, msg.Ack(ctx))
}

// Message.VT is on the app's clock, so a handler comparing it with time.Now()
// sees the lease it really has, not one 10 s longer.
func TestClockSkew_DatabaseAheadReportsLeaseEndOnAppClock(t *testing.T) {
	t.Parallel()
	_, ctx, msg := consumeOneSkewed(t, 10*time.Second, 30)
	assert.InDelta(t, 30, time.Until(msg.GetVT()).Seconds(), 2, "lease end after claim")
	vt, err := msg.SetVT(ctx, 20)
	require.NoError(t, err)
	assert.InDelta(t, 20, time.Until(vt).Seconds(), 2, "lease end after SetVT")
	assert.Equal(t, vt, msg.GetVT())
}

func TestClockSkew_DatabaseAheadReportsRenewedLeaseEndOnAppClock(t *testing.T) {
	t.Parallel()
	_, ctx, msg := consumeOneSkewed(t, 10*time.Second, 2)
	first := msg.GetVT()
	require.Eventually(t, func() bool { return msg.GetVT().After(first) },
		3*time.Second, 50*time.Millisecond, "lease was not renewed")
	assert.InDelta(t, 2, time.Until(msg.GetVT()).Seconds(), 1, "lease end after renewal")
	assert.NoError(t, msg.Ack(ctx))
}

// A database clock behind the application must not make a live lease look
// expired: a busy renewal is retried within the lease, not reported lost.
func TestClockSkew_DatabaseBehindKeepsBusyLease(t *testing.T) {
	t.Parallel()
	plain, ctx, msg := consumeOneSkewed(t, -10*time.Second, 8)
	time.Sleep(3 * time.Second)
	tx, err := plain.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	_, err = tx.Exec(ctx, "SELECT 1 FROM postgremq.queue_messages WHERE message_id = $1 FOR UPDATE", msg.ID)
	require.NoError(t, err)
	time.Sleep(1500 * time.Millisecond) // the renewal due at 4 s reports busy meanwhile
	require.NoError(t, tx.Rollback(ctx))
	assert.Eventually(t, func() bool {
		var renewed bool
		err := plain.QueryRow(ctx,
			"SELECT vt > clock_timestamp() - interval '10 s' + interval '4 s' FROM postgremq.queue_messages WHERE message_id = $1",
			msg.ID).Scan(&renewed)
		return err == nil && renewed
	}, 3*time.Second, 50*time.Millisecond, "lease not renewed once the row was unlocked")
	assert.NoError(t, msg.StoppedCtx.Err(), "a live lease was reported lost")
	assert.NoError(t, msg.Ack(ctx))
}

func createSkewedExclusiveQueue(t *testing.T, skew, interval time.Duration) (*pgxpool.Pool, context.Context, *atomic.Value) {
	t.Helper()
	plain, skewed, ctx := setupSkewedDatabase(t, skew)
	var fatal atomic.Value
	conn, err := postgremq.Dial(ctx, skewed.Config(), postgremq.WithQueueFatalHandler(func(queue string, err error) {
		fatal.Store(err)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.CreateTopic(ctx, "t"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "t", true, postgremq.WithKeepAliveInterval(interval)))
	return plain, ctx, &fatal
}

// A database clock ahead of the application must not space keep-alives past
// the queue's real expiry.
func TestClockSkew_DatabaseAheadKeepsQueueAlive(t *testing.T) {
	t.Parallel()
	plain, ctx, fatal := createSkewedExclusiveQueue(t, 9*time.Second, 3*time.Second)
	time.Sleep(6 * time.Second) // unfixed, the queue expires at 4.5 s; the next keep-alive is due at 7.5 s
	assert.Nil(t, fatal.Load(), "exclusive queue reported gone")
	var live bool
	require.NoError(t, plain.QueryRow(ctx,
		"SELECT keep_alive_until > clock_timestamp() + interval '9 s' FROM postgremq.queues WHERE name = 'q'").Scan(&live))
	assert.True(t, live, "exclusive queue expired")
}

// A database clock behind the application must not make a live queue look
// expired: a busy keep-alive is retried within the lease.
func TestClockSkew_DatabaseBehindKeepsBusyQueue(t *testing.T) {
	t.Parallel()
	plain, ctx, fatal := createSkewedExclusiveQueue(t, -10*time.Second, 4*time.Second)
	time.Sleep(3500 * time.Millisecond) // keep-alives due at 2 s and 4 s
	tx, err := plain.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	_, err = tx.Exec(ctx, "SELECT 1 FROM postgremq.queues WHERE name = 'q' FOR UPDATE")
	require.NoError(t, err)
	time.Sleep(1200 * time.Millisecond) // the keep-alive due at 4 s reports busy meanwhile
	require.NoError(t, tx.Rollback(ctx))
	assert.Eventually(t, func() bool {
		var renewed bool
		err := plain.QueryRow(ctx,
			"SELECT keep_alive_until > clock_timestamp() - interval '10 s' + interval '2 s' FROM postgremq.queues WHERE name = 'q'").Scan(&renewed)
		return err == nil && renewed
	}, 1500*time.Millisecond, 50*time.Millisecond, "keep-alive not renewed once the row was unlocked")
	assert.Nil(t, fatal.Load(), "a live exclusive queue was reported gone")
}

// slowDeclarePool answers create_queue only after a delay.
type slowDeclarePool struct {
	keepAliveSpyPool
	delay time.Duration
}

func (p *slowDeclarePool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "create_queue") {
		time.Sleep(p.delay)
	}
	return p.keepAliveSpyPool.QueryRow(ctx, sql, args...)
}

// The database starts an exclusive queue's keep-alive when it runs
// create_queue, so a slow response must not push the first keep-alive later.
func TestClockSkew_SlowDeclarationKeepsFirstKeepAliveOnTime(t *testing.T) {
	t.Parallel()
	spy := &slowDeclarePool{delay: time.Second}
	conn, err := postgremq.DialFromPool(context.Background(), spy)
	require.NoError(t, err)
	defer conn.Close()
	start := time.Now()
	require.NoError(t, conn.CreateQueue(context.Background(), "q", "t", true,
		postgremq.WithKeepAliveInterval(2*time.Second)))
	require.Eventually(t, func() bool { return spy.callCount() > 0 }, 3*time.Second, 10*time.Millisecond)
	// Due 1 s after the statement was sent, not 1 s after it returned (2 s).
	assert.Less(t, time.Since(start), 1700*time.Millisecond, "first keep-alive counted from the response")
}

func dialSkewed(t *testing.T, skewed *pgxpool.Pool, ctx context.Context) *postgremq.Connection {
	t.Helper()
	conn, err := postgremq.Dial(ctx, skewed.Config())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.CreateTopic(ctx, "t"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "t", false))
	return conn
}

func receive(t *testing.T, consumer *postgremq.Consumer, within time.Duration) *postgremq.Message {
	t.Helper()
	select {
	case msg := <-consumer.Messages():
		settleOnCleanup(t, msg)
		return msg
	case <-time.After(within):
		t.Fatalf("no message delivered within %s", within)
		return nil
	}
}

// A database clock ahead of the application must not make the consumer wait
// past the next visible time: it waits the time the database reports.
func TestClockSkew_DatabaseAheadWakesForDelayedMessage(t *testing.T) {
	t.Parallel()
	_, skewed, ctx := setupSkewedDatabase(t, 10*time.Second)
	conn := dialSkewed(t, skewed, ctx)
	_, err := skewed.Exec(ctx,
		"SELECT postgremq.publish_message('t', '{}', p_deliver_after => clock_timestamp() + interval '1 s')")
	require.NoError(t, err)
	consumer, err := conn.Consume("q", postgremq.WithCheckTimeout(30*time.Second))
	require.NoError(t, err)
	t.Cleanup(consumer.Stop)
	// Unfixed, the consumer sleeps until 11 s on the application's clock.
	msg := receive(t, consumer, 4*time.Second)
	assert.NoError(t, msg.Ack(ctx))
}

// A nack delay is the time remaining on the application's clock, added to the
// database's: it does not grow or shrink with the skew.
func TestClockSkew_DatabaseBehindNackDelayKeepsItsLength(t *testing.T) {
	t.Parallel()
	_, skewed, ctx := setupSkewedDatabase(t, -10*time.Second)
	conn := dialSkewed(t, skewed, ctx)
	_, err := conn.Publish(ctx, "t", json.RawMessage(`{}`))
	require.NoError(t, err)
	consumer, err := conn.Consume("q", postgremq.WithCheckTimeout(30*time.Second))
	require.NoError(t, err)
	t.Cleanup(consumer.Stop)
	msg := receive(t, consumer, 5*time.Second)
	nacked := time.Now()
	require.NoError(t, msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(1500*time.Millisecond))))
	again := receive(t, consumer, 5*time.Second)
	assert.Equal(t, msg.ID, again.ID)
	assert.GreaterOrEqual(t, time.Since(nacked), 1400*time.Millisecond, "redelivered before the delay")
	assert.NoError(t, again.Ack(ctx))
}
