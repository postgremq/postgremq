package postgremq

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// heartbeatPool answers either heartbeat function after a few milliseconds,
// renewing every requested row for a minute except those in gone, which it
// leaves out as the SQL does a lease or queue that has already expired. Like
// pgx, it fails a call whose context ends before it completes.
type heartbeatPool struct{ gone map[string]bool }

func (heartbeatPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (p heartbeatPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	until := time.Now().Add(time.Minute)
	names := args[0].([]string)
	rows := &heartbeatRows{}
	for i, name := range names {
		if strings.Contains(sql, "set_vt_batch_multi") {
			id := args[1].([]int64)[i]
			if !p.gone[fmt.Sprintf("%s/%d", name, id)] {
				rows.values = append(rows.values, []any{name, id, &until, args[2].([]string)[i], "extended"})
			}
		} else if !p.gone[name] {
			rows.values = append(rows.values, []any{name, &until, "extended"})
		}
	}
	return rows, nil
}

func (heartbeatPool) QueryRow(context.Context, string, ...any) pgx.Row { return infoRow{} }
func (heartbeatPool) Acquire(context.Context) (*pgxpool.Conn, error)   { return nil, nil }
func (heartbeatPool) Close()                                           {}

type infoRow struct{}

func (infoRow) Scan(dest ...any) error {
	*(dest[0].(*[]byte)) = []byte(`{"schema_version": 10001, "protocol_major": 1}`)
	return nil
}

type heartbeatRows struct {
	values [][]any
	next   int
}

func (r *heartbeatRows) Close()                                       {}
func (r *heartbeatRows) Err() error                                   { return nil }
func (r *heartbeatRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *heartbeatRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *heartbeatRows) Next() bool                                   { r.next++; return r.next <= len(r.values) }
func (r *heartbeatRows) Values() ([]any, error)                       { return nil, nil }
func (r *heartbeatRows) RawValues() [][]byte                          { return nil }
func (r *heartbeatRows) Conn() *pgx.Conn                              { return nil }
func (r *heartbeatRows) TypeMap() *pgtype.Map                         { return nil }
func (r *heartbeatRows) Scan(dest ...any) error {
	for i, v := range r.values[r.next-1] {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(v))
	}
	return nil
}

func TestExpiringDeliveryDoesNotFailItsRenewalBatch(t *testing.T) {
	conn, err := DialFromPool(context.Background(), heartbeatPool{gone: map[string]bool{"q/1": true}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	s := newExtScheduler(conn, 100)
	now := time.Now()
	// Retried after busy until its deadline, which passes before the SQL runs.
	lost := false
	s.add(&extEntry{queue: "q", id: 1, token: "t", vtSec: 30, threshold: .5, extendAt: now, expiresAt: now.Add(2 * time.Millisecond), cancel: func() { lost = true }})
	extended := map[int64]bool{}
	for id := int64(2); id <= 4; id++ {
		s.add(&extEntry{queue: "q", id: id, token: "t", vtSec: 30, threshold: .5, extendAt: now, expiresAt: now.Add(time.Minute),
			cancel:     func() { t.Errorf("healthy delivery %d cancelled", id) },
			onExtended: func(time.Time) { extended[id] = true }})
	}
	res := s.flush(context.Background(), s.collectDue(time.Now()))
	if res.err != nil {
		t.Fatalf("batch failed: %v", res.err)
	}
	s.apply(res)
	if !lost {
		t.Fatal("expired delivery not reported lost")
	}
	if len(extended) != 3 {
		t.Fatalf("healthy deliveries renewed in the same tick: %v", extended)
	}
}

func TestExpiringQueueDoesNotFailItsKeepAliveBatch(t *testing.T) {
	fatal := make(chan string, 4)
	conn, err := DialFromPool(context.Background(), heartbeatPool{gone: map[string]bool{"expiring": true}},
		WithQueueFatalHandler(func(queue string, _ error) { fatal <- queue }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	s := newKeepAliveScheduler(conn)
	now := time.Now()
	s.add(&kaEntry{queue: "expiring", generation: "g", intervalMs: 60000, nextAt: now, expiresAt: now.Add(2 * time.Millisecond)})
	healthy := []string{"a", "b", "c"}
	for _, queue := range healthy {
		s.add(&kaEntry{queue: queue, generation: "g", intervalMs: 60000, nextAt: now, expiresAt: now.Add(time.Second)})
	}
	res := s.flush(context.Background(), s.collectDue(time.Now()))
	if res.err != nil {
		t.Fatalf("batch failed: %v", res.err)
	}
	s.apply(res)
	for _, queue := range healthy {
		if e := s.entries[queue]; e == nil || !e.expiresAt.After(now.Add(30*time.Second)) {
			t.Fatalf("queue %s not kept alive in the same tick", queue)
		}
	}
	select {
	case queue := <-fatal:
		if queue != "expiring" {
			t.Fatalf("queue-fatal for %s", queue)
		}
	case <-time.After(time.Second):
		t.Fatal("expired queue not reported fatal")
	}
}

func TestHeartbeatDeadlineBounds(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		name           string
		earliest, want time.Time
	}{
		{"expired", now.Add(-time.Second), now.Add(minHeartbeatBudget)},
		{"about to expire", now.Add(time.Millisecond), now.Add(minHeartbeatBudget)},
		{"within bounds", now.Add(3 * time.Second), now.Add(3 * time.Second)},
		{"far away", now.Add(time.Minute), now.Add(maxHeartbeatBudget)},
		{"unknown", time.Time{}, now.Add(maxHeartbeatBudget)},
	} {
		if got := heartbeatDeadline(now, c.earliest); !got.Equal(c.want) {
			t.Errorf("%s: deadline %v, want %v", c.name, got.Sub(now), c.want.Sub(now))
		}
	}
}
