package postgremq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// Run explicitly: POSTGREMQ_SOAK_SECONDS=60 go test -race -run TestProductionTopologySoak -timeout=3m -v
func TestProductionTopologySoak(t *testing.T) {
	seconds, _ := strconv.Atoi(os.Getenv("POSTGREMQ_SOAK_SECONDS"))
	if seconds < 1 {
		t.Skip("set POSTGREMQ_SOAK_SECONDS to run the topology soak")
	}
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	c, err := postgremq.DialFromPool(context.Background(), pool, postgremq.WithShutdownTimeout(5*time.Second))
	require.NoError(t, err)
	defer c.Close()
	for i := 0; i < 20; i++ {
		require.NoError(t, c.CreateTopic(ctx, fmt.Sprintf("t%d", i)))
	}
	var mu sync.Mutex
	deliveries := make(map[string]int)
	processingErrors := make(chan error, 1)
	for q := 0; q < 100; q++ {
		name := fmt.Sprintf("q%d", q)
		require.NoError(t, c.CreateQueue(ctx, name, fmt.Sprintf("t%d", q%20), false))
		for worker := 0; worker < 2+q%5; worker++ {
			_, err = c.ConsumeHandler(name, func(_ context.Context, msg *postgremq.Message) {
				if err := msg.Ack(ctx); err != nil {
					select {
					case processingErrors <- err:
					default:
					}
					return
				}
				mu.Lock()
				deliveries[fmt.Sprintf("%s/%d", name, msg.ID)]++
				mu.Unlock()
			}, postgremq.WithBatchSize(1), postgremq.WithMaxInFlight(1), postgremq.WithVT(3), postgremq.WithCheckTimeout(time.Second))
			require.NoError(t, err)
		}
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
	maintenanceDone := make(chan struct{})
	maintenanceErrors := make(chan error, 1)
	go func() {
		defer close(maintenanceDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-maintenanceCtx.Done():
				return
			case <-ticker.C:
				if _, err := pool.Exec(maintenanceCtx, "SELECT postgremq.pmq_maintenance_fast(); SELECT postgremq.cleanup_completed_messages(0,1000)"); err != nil && maintenanceCtx.Err() == nil {
					select {
					case maintenanceErrors <- err:
					default:
					}
					return
				}
			}
		}
	}()
	defer func() { stopMaintenance(); <-maintenanceDone }()
	sizes := []int{1024, 16 * 1024, 256 * 1024}
	count := seconds * 10
	started := time.Now()
	for i := 0; i < count; i++ {
		payload, _ := json.Marshal(map[string]any{"sequence": i, "data": strings.Repeat("x", sizes[i%len(sizes)])})
		_, err = c.Publish(ctx, fmt.Sprintf("t%d", i%20), payload)
		require.NoError(t, err)
		if wait := time.Until(started.Add(time.Duration(i+1) * 100 * time.Millisecond)); wait > 0 {
			time.Sleep(wait)
		}
	}
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(deliveries) == count*5 }, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, c.Close())
	mu.Lock()
	for key, attempts := range deliveries {
		require.Equal(t, 1, attempts, "duplicate in healthy run: %s", key)
	}
	mu.Unlock()
	require.Eventually(t, func() bool {
		var remaining int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM postgremq.messages").Scan(&remaining)
		return err == nil && remaining == 0
	}, 5*time.Second, 20*time.Millisecond)
	stopMaintenance()
	<-maintenanceDone
	select {
	case err := <-maintenanceErrors:
		t.Fatal(err)
	default:
	}
	select {
	case err := <-processingErrors:
		t.Fatal(err)
	default:
	}
	for i := 0; i < 20; i++ {
		require.Zero(t, c.EventListener().SubscriberCount(fmt.Sprintf("pmq:t:t%d", i)))
	}
	t.Logf("%d publications, %d deliveries, 20 topics, 100 queues, 400 consumers, payloads 1/16/256 KiB; payload storage drained to zero", count, count*5)
}
