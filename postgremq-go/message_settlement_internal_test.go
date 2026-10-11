package postgremq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSettlementOwnsCompletionUntilOperationFinishes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "failure", err: errors.New("settlement failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var operations, completions atomic.Int32
			msg := &Message{onComplete: func(*Message) { completions.Add(1) }}
			blocked, unblock := context.WithCancel(context.Background())
			defer unblock()
			entered := make(chan struct{})
			ownerResult := make(chan error, 1)
			go func() {
				ownerResult <- msg.settle(func() error {
					operations.Add(1)
					close(entered)
					<-blocked.Done()
					return tc.err
				})
			}()
			<-entered

			// Competing terminal calls must finish without waiting for the
			// blocked SQL, issuing more SQL, or ending the owner's renewal.
			const competitors = 16
			results := make(chan error, competitors)
			for i := 0; i < competitors; i++ {
				go func() {
					results <- msg.settle(func() error { operations.Add(1); return nil })
				}()
			}
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for i := 0; i < competitors; i++ {
				select {
				case err := <-results:
					require.ErrorIs(t, err, ErrLeaseLost)
				case <-deadline.C:
					t.Fatal("competing settlement waited for the owner")
				}
			}
			require.EqualValues(t, 1, operations.Load())
			require.Zero(t, completions.Load())

			unblock()
			require.ErrorIs(t, <-ownerResult, tc.err)
			require.EqualValues(t, 1, completions.Load())
			require.ErrorIs(t, msg.settle(func() error { operations.Add(1); return nil }), ErrLeaseLost)
			require.EqualValues(t, 1, operations.Load())
			require.EqualValues(t, 1, completions.Load())
		})
	}
}
