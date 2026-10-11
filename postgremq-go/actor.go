package postgremq

import (
	"context"
	"sync"
	"time"
)

// Connection-level background actor — the shared skeleton behind both the
// keep-alive actor (keepalive.go) and the vt-extension actor (extender.go).
//
// One goroutine owns its schedule with no mutex. A select loop applies
// register/deregister synchronously and, on each timer tick, dispatches ONE
// batched DB call on a SEPARATE goroutine so registration and deregistration remain responsive during I/O.
// stop cancels and joins both the loop and its outstanding flush. A `flushing` flag prevents
// overlapping calls; `tryAfter` is an optional global backoff floor a scheduler
// may request after a persistent failure.
//
// The actor owns its lifetime: it runs on its own context (NOT conn.ctx) so it
// outlives the consumer drain and is stopped last
// in Connection.Close() via stop().
//
// Type parameters:
//   - Entry:  the register payload, a freshly-built schedule entry (there is no
//     separate "register" struct — the registrant builds the entry directly).
//   - Key:    identifies an entry for deregistration.
//   - Result: one flush's outcome; it carries whatever context the scheduler
//     needs back in apply (e.g. which entries were sent).

// scheduler is the per-actor schedule + behaviour the generic loop drives. Every
// method runs ON the actor goroutine EXCEPT flush, which runs off-loop.
type scheduler[Entry any, Key comparable, Result any] interface {
	// add inserts or overrides an entry (the register payload).
	add(Entry)
	// remove deletes the entry for a key (no-op if absent).
	remove(Key)
	// earliest reports when the next work is due; ok=false when idle.
	earliest() (at time.Time, ok bool)
	// collectDue claims the entries due at `now` (nil/empty when nothing is due).
	// The returned entries must be safe to read off the actor goroutine: the
	// scheduler must not mutate them between here and the matching apply.
	collectDue(now time.Time) []Entry
	// flush runs the batched DB call for `due` OFF the actor goroutine, so it
	// must only read `due` — never the live schedule.
	flush(ctx context.Context, due []Entry) Result
	// apply folds one flush outcome back into the schedule and returns a backoff
	// floor (zero = none) governing the earliest next tick.
	apply(Result) (tryAfter time.Time)
}

type actorCommand[E any, K comparable] struct {
	entry  E
	key    K
	remove bool
}

type actor[Entry any, Key comparable, Result any] struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	commands chan actorCommand[Entry, Key]
	results  chan Result
	sched    scheduler[Entry, Key, Result]
}

func newActor[Entry any, Key comparable, Result any](sched scheduler[Entry, Key, Result], regBuf, deregBuf int) *actor[Entry, Key, Result] {
	a := &actor[Entry, Key, Result]{
		commands: make(chan actorCommand[Entry, Key], regBuf+deregBuf),
		results:  make(chan Result, 1),
		sched:    sched,
	}
	// Own context (from Background, not conn.ctx) so the actor outlives the
	// consumer drain and is stopped explicitly via stop().
	a.ctx, a.cancel = context.WithCancel(context.Background())
	return a
}

func (a *actor[Entry, Key, Result]) start() {
	a.wg.Add(1)
	go a.run()
}

// stop cancels the actor and joins its goroutine. Called last in
// Connection.Close(), after consumers have drained.
func (a *actor[Entry, Key, Result]) stop() {
	a.cancel()
	a.wg.Wait()
}

// register and deregister share one FIFO, preserving causal command order.
// Sends apply backpressure and unblock when the actor stops.
func (a *actor[Entry, Key, Result]) register(entry Entry) {
	select {
	case a.commands <- actorCommand[Entry, Key]{entry: entry}:
	case <-a.ctx.Done():
	}
}

// deregister enqueues removal of an exact identity.
func (a *actor[Entry, Key, Result]) deregister(key Key) {
	select {
	case a.commands <- actorCommand[Entry, Key]{key: key, remove: true}:
	case <-a.ctx.Done():
	}
}

func (a *actor[Entry, Key, Result]) run() {
	defer a.wg.Done()
	var flushWG sync.WaitGroup
	defer flushWG.Wait()

	flushing := false
	var tryAfter time.Time
	var timerC <-chan time.Time

	// arm recomputes the timer to fire at the earliest pending work (or the
	// backoff floor). While a flush is in-flight we don't arm — the result
	// handler re-arms, so ticks can't pile up overlapping DB calls.
	arm := func() {
		if flushing {
			timerC = nil
			return
		}
		fireAt, ok := a.sched.earliest()
		if !tryAfter.IsZero() {
			fireAt = maxTime(fireAt, tryAfter)
			ok = true
		}
		if !ok {
			timerC = nil
			return
		}
		wait := time.Until(fireAt)
		if wait < 0 {
			wait = 0
		}
		timerC = time.After(wait)
	}
	arm()

	for {
		select {
		case <-a.ctx.Done():
			return

		case command := <-a.commands:
			if command.remove {
				a.sched.remove(command.key)
			} else {
				a.sched.add(command.entry)
			}
			arm()

		case <-timerC:
			due := a.sched.collectDue(time.Now())
			if len(due) == 0 {
				// Timer fired on a stale backoff with nothing due; clear it.
				tryAfter = time.Time{}
				arm()
				continue
			}
			flushing = true
			flushWG.Add(1)
			go func() {
				defer flushWG.Done()
				result := a.sched.flush(a.ctx, due)
				select {
				case a.results <- result:
				case <-a.ctx.Done():
				}
			}()

		case result := <-a.results:
			flushing = false
			tryAfter = a.sched.apply(result)
			arm()
		}
	}
}

// minTime returns the earlier of two times, treating zero as "unset".
func minTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if a.After(b) {
		return b
	}
	return a
}

// maxTime returns the later of two times, treating zero as "unset".
func maxTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if a.After(b) {
		return a
	}
	return b
}
