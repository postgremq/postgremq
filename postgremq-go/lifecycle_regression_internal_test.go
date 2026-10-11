package postgremq

import (
	"context"
	"errors"
	"testing"
	"time"
)

var _ scheduler[int, int, int] = (*joinScheduler)(nil)

func TestSettlementDuringExtensionCannotResurrectDelivery(t *testing.T) {
	for _, failure := range []bool{false, true} {
		s := newExtScheduler(&Connection{logger: NoopLogger{}}, 100)
		e := &extEntry{queue: "q", id: 1, token: "old", threshold: .5, extendAt: time.Now().Add(-time.Second)}
		s.add(e)
		due := s.collectDue(time.Now())
		s.remove(e.key())
		res := extResult{popped: due, locks: []MultiLock{{Queue: "q", ID: 1, Token: "old", VT: time.Now().Add(time.Minute)}}}
		if failure {
			res.err = errors.New("disconnected")
		}
		s.apply(res)
		if s.h.Len() != 0 || len(s.live) != 0 {
			t.Fatal("settled delivery resurrected")
		}
	}
}
func TestOldDeliveryCannotRemoveNewDelivery(t *testing.T) {
	s := newExtScheduler(&Connection{logger: NoopLogger{}}, 100)
	old := &extEntry{queue: "q", id: 1, token: "old", extendAt: time.Now().Add(-time.Second)}
	fresh := &extEntry{queue: "q", id: 1, token: "new", extendAt: time.Now().Add(time.Second)}
	s.add(old)
	due := s.collectDue(time.Now())
	s.add(fresh)
	s.remove(old.key())
	s.apply(extResult{popped: due, err: errors.New("disconnected")})
	if s.h.Len() != 1 || s.h.peek() != fresh {
		t.Fatal("old delivery changed new delivery")
	}
}
func TestActorStopJoinsFlush(t *testing.T) {
	s := &joinScheduler{entered: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	a := newActor[int, int, int](s, 1, 1)
	a.start()
	<-s.entered
	stopped := make(chan struct{})
	go func() { a.stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop did not join flush")
	case <-time.After(20 * time.Millisecond):
	}
	close(s.release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not finish")
	}
	select {
	case <-s.finished:
	default:
		t.Fatal("flush still running")
	}
}

type joinScheduler struct {
	entered, release, finished chan struct{}
	collected                  bool
}

func (s *joinScheduler) add(int)                     {}
func (s *joinScheduler) remove(int)                  {}
func (s *joinScheduler) earliest() (time.Time, bool) { return time.Now(), !s.collected }
func (s *joinScheduler) collectDue(time.Time) []int  { s.collected = true; return []int{1} }
func (s *joinScheduler) flush(context.Context, []int) int {
	close(s.entered)
	<-s.release
	close(s.finished)
	return 0
}
func (s *joinScheduler) apply(int) time.Time { return time.Time{} }

func TestKeepAliveRetriesUntilLeaseDeadlineAndIgnoresReplacedEntry(t *testing.T) {
	s := newKeepAliveScheduler(&Connection{logger: NoopLogger{}})
	entry := &kaEntry{queue: "q", generation: "old", intervalMs: 60000, expiresAt: time.Now().Add(time.Minute)}
	s.add(entry)
	for i := 0; i < 20; i++ {
		s.apply(kaResult{due: []*kaEntry{entry}, err: errors.New("disconnected")})
	}
	if s.entries["q"] != entry {
		t.Fatal("live queue dropped after transient errors")
	}
	fresh := &kaEntry{queue: "q", generation: "new", expiresAt: time.Now().Add(time.Minute)}
	s.add(fresh)
	s.apply(kaResult{due: []*kaEntry{entry}, leases: map[string]kaLease{}})
	if s.entries["q"] != fresh {
		t.Fatal("old result removed replacement queue")
	}
}
func TestBusyHeartbeatRetriesWithoutCancellingLiveDelivery(t *testing.T) {
	s := newExtScheduler(&Connection{logger: NoopLogger{}}, 10)
	cancelled := false
	e := &extEntry{queue: "q", id: 1, token: "t", expiresAt: time.Now().Add(time.Second), cancel: func() { cancelled = true }}
	s.add(e)
	due := s.collectDue(time.Now())
	s.apply(extResult{popped: due, locks: []MultiLock{{Queue: "q", ID: 1, Token: "t", Busy: true}}})
	if cancelled || s.live[e.key()] != e {
		t.Fatal("contention treated as lease loss")
	}
	e.expiresAt = time.Now().Add(-time.Second)
	e.extendAt = time.Now().Add(-time.Second)
	due = s.collectDue(time.Now())
	s.apply(extResult{popped: due, err: errors.New("disconnected")})
	if !cancelled || len(s.live) != 0 {
		t.Fatal("expired local lease kept alive")
	}
}
