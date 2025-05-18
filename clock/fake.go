package clock

import (
	"sync"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
)

// epoch is an arbitrary but fixed start point. Using a real-looking instant
// rather than the zero time keeps failure messages readable and avoids tests
// accidentally passing because some duration arithmetic underflowed.
var epoch = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)

// Fake is a raft.Clock whose time only moves when a test calls Advance.
//
// The whole point is to remove wall-clock dependence from the consensus tests:
// an election timeout of 300ms costs nothing to simulate, and a test that
// advances exactly past one node's timeout but not another's can force a
// specific node to campaign instead of hoping the scheduler cooperates.
//
// The zero value is usable; NewFake only picks a nicer starting instant.
// All methods are safe for concurrent use.
type Fake struct {
	mu sync.Mutex

	now time.Time

	// pending holds every armed timer. Order is not maintained here; Advance
	// sorts, because arming is far more frequent than advancing.
	pending []*fakeTimer

	// seq breaks ties between timers with identical deadlines so that firing
	// order is creation order rather than map/slice happenstance.
	seq uint64

	blockers []*blocker
}

type blocker struct {
	n  int
	ch chan struct{}
}

var _ raft.Clock = (*Fake)(nil)

func NewFake() *Fake {
	return &Fake{now: epoch}
}

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer arms a timer for d of virtual time from now. A non-positive d fires
// immediately, matching time.NewTimer, so callers that compute a deadline that
// has already passed are not left waiting for an Advance that never comes.
func (f *Fake) NewTimer(d time.Duration) raft.Timer {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.seq++
	t := &fakeTimer{
		f: f,
		// Capacity 1 is what makes firing non-blocking, which is what keeps
		// Advance from deadlocking against the goroutines it wakes. See Advance.
		c:   make(chan time.Time, 1),
		seq: f.seq,
	}
	f.armLocked(t, d)
	f.notifyBlockersLocked()
	return t
}

// Sleep blocks until virtual time has advanced by at least d.
//
// Deadlock hazard: the obvious implementation has Advance hand off to each
// sleeper and wait for it to acknowledge, which deadlocks the moment a sleeper
// is the goroutine that called Advance's caller, or simply blocks forever if a
// woken goroutine goes on to do anything other than acknowledge. This
// implementation avoids the hazard entirely by never having Advance wait: Sleep
// is just an armed timer plus a receive, and Advance's wake-up is a
// non-blocking send into that timer's buffered channel. Advance therefore
// returns as soon as virtual time has moved and every expired timer has been
// signalled, whether or not the sleepers have been scheduled yet.
//
// The cost of not waiting is that Advance alone does not establish a
// happens-before edge with the woken goroutines' subsequent work. Tests that
// need that edge use BlockUntil to rendezvous before advancing again.
func (f *Fake) Sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	<-f.NewTimer(d).C()
}

// Advance moves virtual time forward by d and fires every timer whose deadline
// the move passes.
func (f *Fake) Advance(d time.Duration) {
	f.advance(d)
}

// advance is Advance plus the fired timers, in the order they fired, which is
// what the ordering tests assert on. Firing order is not otherwise observable:
// each timer has its own channel, and the order in which the woken goroutines
// get scheduled is up to the runtime.
func (f *Fake) advance(d time.Duration) []*fakeTimer {
	if d < 0 {
		panic("clock: Fake.Advance with negative duration")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	target := f.now.Add(d)
	var fired []*fakeTimer

	// Fire in deadline order, not creation order, and step `now` to each
	// deadline as we go. A single Advance can cross several nodes' election
	// timeouts, and the node with the earliest deadline is the one Raft expects
	// to campaign first; firing in creation order would make which node wins
	// depend on construction order instead of on the timeouts under test.
	for {
		t := f.earliestLocked(target)
		if t == nil {
			break
		}
		f.now = t.deadline
		t.active = false
		f.removeLocked(t)
		// Non-blocking: capacity-1 channel, and a timer is only ever armed
		// once per fire. A full buffer means an earlier value was never
		// consumed, which is exactly when a real time.Timer also drops.
		select {
		case t.c <- t.deadline:
		default:
		}
		fired = append(fired, t)
	}

	f.now = target
	f.notifyBlockersLocked()
	return fired
}

// BlockUntil waits until at least n timers are armed on this clock.
//
// This is the primitive that removes flakiness from cluster tests. Without it a
// test has to guess that every node has reached its select on the election
// timer before advancing time; guess early and Advance fires nothing, the nodes
// arm their timers afterwards, and the election never happens. BlockUntil(len(nodes))
// turns that guess into a wait. A goroutine parked in Sleep counts, because
// Sleep arms a timer.
func (f *Fake) BlockUntil(n int) {
	f.mu.Lock()
	if len(f.pending) >= n {
		f.mu.Unlock()
		return
	}
	b := &blocker{n: n, ch: make(chan struct{})}
	f.blockers = append(f.blockers, b)
	f.mu.Unlock()

	<-b.ch
}

// Blocked reports how many timers are currently armed.
func (f *Fake) Blocked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending)
}

func (f *Fake) armLocked(t *fakeTimer, d time.Duration) {
	if d <= 0 {
		t.active = false
		t.deadline = f.now
		select {
		case t.c <- f.now:
		default:
		}
		return
	}
	t.deadline = f.now.Add(d)
	t.active = true
	f.pending = append(f.pending, t)
}

func (f *Fake) earliestLocked(notAfter time.Time) *fakeTimer {
	var best *fakeTimer
	for _, t := range f.pending {
		if t.deadline.After(notAfter) {
			continue
		}
		if best == nil || t.deadline.Before(best.deadline) ||
			(t.deadline.Equal(best.deadline) && t.seq < best.seq) {
			best = t
		}
	}
	return best
}

func (f *Fake) removeLocked(t *fakeTimer) {
	for i, p := range f.pending {
		if p == t {
			f.pending = append(f.pending[:i], f.pending[i+1:]...)
			return
		}
	}
}

func (f *Fake) notifyBlockersLocked() {
	n := len(f.pending)
	kept := f.blockers[:0]
	for _, b := range f.blockers {
		if n >= b.n {
			close(b.ch)
			continue
		}
		kept = append(kept, b)
	}
	// Clear the tail so released blockers are not retained by the backing array.
	for i := len(kept); i < len(f.blockers); i++ {
		f.blockers[i] = nil
	}
	f.blockers = kept
}

type fakeTimer struct {
	f   *Fake
	c   chan time.Time
	seq uint64

	// Guarded by f.mu.
	deadline time.Time
	active   bool
}

var _ raft.Timer = (*fakeTimer)(nil)

func (t *fakeTimer) C() <-chan time.Time { return t.c }

// Reset reschedules the timer and reports whether it was still armed, matching
// time.Timer. A previously delivered value is deliberately left in the channel,
// also matching time.Timer: draining it here would silently fix the classic
// reset-without-drain bug in callers instead of exposing it in tests.
func (t *fakeTimer) Reset(d time.Duration) bool {
	f := t.f
	f.mu.Lock()
	defer f.mu.Unlock()

	was := t.active
	if was {
		f.removeLocked(t)
		t.active = false
	}
	f.armLocked(t, d)
	f.notifyBlockersLocked()
	return was
}

func (t *fakeTimer) Stop() bool {
	f := t.f
	f.mu.Lock()
	defer f.mu.Unlock()

	was := t.active
	if was {
		f.removeLocked(t)
		t.active = false
		f.notifyBlockersLocked()
	}
	return was
}
