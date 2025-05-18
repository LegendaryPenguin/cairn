package clock

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
)

// recv asserts that a timer has already fired and returns the delivered time.
// It never blocks, so a missing fire is a failure rather than a hung test.
func recv(t *testing.T, tm raft.Timer) time.Time {
	t.Helper()
	select {
	case v := <-tm.C():
		return v
	default:
		t.Fatal("expected timer to have fired")
		return time.Time{}
	}
}

func assertQuiet(t *testing.T, tm raft.Timer, name string) {
	t.Helper()
	select {
	case v := <-tm.C():
		t.Fatalf("%s fired unexpectedly at %v", name, v)
	default:
	}
}

func TestAdvanceFiresInDeadlineOrderNotCreationOrder(t *testing.T) {
	f := NewFake()
	start := f.Now()

	// Creation order is deliberately the reverse of deadline order.
	third := f.NewTimer(30 * time.Millisecond).(*fakeTimer)
	first := f.NewTimer(10 * time.Millisecond).(*fakeTimer)
	second := f.NewTimer(20 * time.Millisecond).(*fakeTimer)

	fired := f.advance(100 * time.Millisecond)

	want := []*fakeTimer{first, second, third}
	if len(fired) != len(want) {
		t.Fatalf("fired %d timers, want %d", len(fired), len(want))
	}
	for i := range want {
		if fired[i] != want[i] {
			t.Fatalf("fire position %d: got the timer with deadline %v, want %v",
				i, fired[i].deadline.Sub(start), want[i].deadline.Sub(start))
		}
	}

	// Each timer must observe virtual time as of its own deadline, not the
	// Advance target, or a handler that reads Now() sees the future.
	for i, tm := range want {
		got := recv(t, tm)
		wantAt := start.Add(time.Duration(10*(i+1)) * time.Millisecond)
		if !got.Equal(wantAt) {
			t.Errorf("timer %d delivered %v, want %v", i, got, wantAt)
		}
	}

	if f.Now().Sub(start) != 100*time.Millisecond {
		t.Errorf("after Advance, Now moved %v, want 100ms", f.Now().Sub(start))
	}
}

func TestTiedDeadlinesFireInCreationOrder(t *testing.T) {
	f := NewFake()
	a := f.NewTimer(5 * time.Millisecond).(*fakeTimer)
	b := f.NewTimer(5 * time.Millisecond).(*fakeTimer)

	fired := f.advance(5 * time.Millisecond)
	if len(fired) != 2 || fired[0] != a || fired[1] != b {
		t.Fatalf("tied deadlines did not fire in creation order: %v", fired)
	}
}

func TestAdvanceShortOfDeadlineFiresNothing(t *testing.T) {
	f := NewFake()
	tm := f.NewTimer(100 * time.Millisecond)

	f.Advance(99 * time.Millisecond)
	assertQuiet(t, tm, "timer")
	if f.Blocked() != 1 {
		t.Fatalf("timer should still be armed, Blocked()=%d", f.Blocked())
	}

	// Exactly reaching the deadline must fire: a deadline is inclusive, so a
	// heartbeat interval of exactly the election timeout is not a coin flip.
	f.Advance(time.Millisecond)
	recv(t, tm)
}

func TestAdvanceZeroFiresNothing(t *testing.T) {
	f := NewFake()
	tm := f.NewTimer(time.Millisecond)
	f.Advance(0)
	assertQuiet(t, tm, "timer")
}

func TestResetReschedulesAndOldDeadlineDoesNotFire(t *testing.T) {
	f := NewFake()
	tm := f.NewTimer(50 * time.Millisecond)

	if !tm.Reset(200 * time.Millisecond) {
		t.Fatal("Reset on an armed timer should report it was active")
	}

	// Past the original deadline, nowhere near the new one.
	f.Advance(100 * time.Millisecond)
	assertQuiet(t, tm, "timer at its abandoned deadline")

	f.Advance(100 * time.Millisecond)
	recv(t, tm)
}

func TestResetAfterFireReportsInactive(t *testing.T) {
	f := NewFake()
	tm := f.NewTimer(10 * time.Millisecond)
	f.Advance(10 * time.Millisecond)
	recv(t, tm)

	if tm.Reset(10 * time.Millisecond) {
		t.Error("Reset on an already-fired timer should report it was inactive")
	}
	f.Advance(10 * time.Millisecond)
	recv(t, tm)
}

func TestStopPreventsFiring(t *testing.T) {
	f := NewFake()
	tm := f.NewTimer(10 * time.Millisecond)

	if !tm.Stop() {
		t.Fatal("Stop on an armed timer should return true")
	}
	if f.Blocked() != 0 {
		t.Fatalf("stopped timer still counted as armed: %d", f.Blocked())
	}

	f.Advance(time.Hour)
	assertQuiet(t, tm, "stopped timer")

	if tm.Stop() {
		t.Error("Stop on an already-stopped timer should return false")
	}
}

func TestStopAfterFireReturnsFalse(t *testing.T) {
	f := NewFake()
	tm := f.NewTimer(10 * time.Millisecond)
	f.Advance(10 * time.Millisecond)
	recv(t, tm)

	if tm.Stop() {
		t.Error("Stop on a fired timer should return false")
	}
}

func TestNonPositiveTimerFiresImmediately(t *testing.T) {
	f := NewFake()
	for _, d := range []time.Duration{0, -time.Second} {
		tm := f.NewTimer(d)
		if got := recv(t, tm); !got.Equal(f.Now()) {
			t.Errorf("NewTimer(%v) delivered %v, want %v", d, got, f.Now())
		}
		if f.Blocked() != 0 {
			t.Errorf("NewTimer(%v) left a timer armed", d)
		}
	}
}

func TestSleepWaitsForEnoughVirtualTime(t *testing.T) {
	f := NewFake()
	woke := make(chan time.Time, 1)
	go func() {
		f.Sleep(100 * time.Millisecond)
		woke <- f.Now()
	}()

	// Rendezvous instead of guessing: without this the Advance below could run
	// before the sleeper arms its timer, and the sleeper would wait forever.
	f.BlockUntil(1)

	f.Advance(99 * time.Millisecond)
	select {
	case at := <-woke:
		t.Fatalf("Sleep(100ms) returned after only 99ms of virtual time (at %v)", at)
	default:
	}

	f.Advance(time.Millisecond)
	<-woke
}

func TestSleepNonPositiveReturnsImmediately(t *testing.T) {
	f := NewFake()
	f.Sleep(0)
	f.Sleep(-time.Hour)
	if f.Blocked() != 0 {
		t.Fatalf("non-positive Sleep armed a timer: %d", f.Blocked())
	}
}

func TestBlockUntilUnblocksWhenSleepersArrive(t *testing.T) {
	f := NewFake()
	const sleepers = 4

	var wg sync.WaitGroup
	wg.Add(sleepers)
	for i := 0; i < sleepers; i++ {
		go func() {
			defer wg.Done()
			f.Sleep(time.Second)
		}()
	}

	f.BlockUntil(sleepers)
	if n := f.Blocked(); n < sleepers {
		t.Fatalf("BlockUntil(%d) returned with only %d armed", sleepers, n)
	}

	// Every sleeper is parked, so one Advance releases all of them.
	f.Advance(time.Second)
	wg.Wait()
}

func TestBlockUntilReturnsImmediatelyWhenAlreadySatisfied(t *testing.T) {
	f := NewFake()
	f.NewTimer(time.Second)
	f.NewTimer(time.Second)

	f.BlockUntil(2)
	f.BlockUntil(1)
	f.BlockUntil(0)
}

func TestBlockUntilCountsArmedTimers(t *testing.T) {
	f := NewFake()
	done := make(chan struct{})
	go func() {
		f.BlockUntil(2)
		close(done)
	}()

	tm := f.NewTimer(time.Second)
	f.NewTimer(time.Second)
	<-done

	// Stopping drops the count back, so a later BlockUntil(2) must wait again.
	tm.Stop()
	if f.Blocked() != 1 {
		t.Fatalf("Blocked()=%d after Stop, want 1", f.Blocked())
	}
}

func TestConcurrentTimerChurnUnderAdvance(t *testing.T) {
	f := NewFake()

	const (
		workers = 16
		rounds  = 200
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < rounds; i++ {
				tm := f.NewTimer(time.Duration(1+rng.Intn(5)) * time.Millisecond)
				switch rng.Intn(4) {
				case 0:
					tm.Reset(time.Duration(1+rng.Intn(5)) * time.Millisecond)
					tm.Stop()
				case 1:
					tm.Stop()
				case 2:
					// Leave it armed and let Advance collect it.
					<-tm.C()
				default:
					// Sleep exercises the wake path from another goroutine.
					f.Sleep(time.Duration(1+rng.Intn(3)) * time.Millisecond)
					tm.Stop()
				}
				_ = f.Now()
			}
		}(int64(w) + 1)
	}

	// The advancer runs until the workers are finished, which guarantees every
	// armed timer and every Sleep eventually gets its deadline crossed.
	workersDone := make(chan struct{})
	advancerDone := make(chan struct{})
	go func() {
		defer close(advancerDone)
		for {
			select {
			case <-workersDone:
				// Drain anything armed between the last advance and shutdown.
				f.Advance(time.Hour)
				return
			default:
				f.Advance(time.Millisecond)
				runtime.Gosched()
			}
		}
	}()

	wg.Wait()
	close(workersDone)
	<-advancerDone

	if n := f.Blocked(); n != 0 {
		t.Fatalf("%d timers still armed after shutdown", n)
	}
}

func TestZeroValueFakeIsUsable(t *testing.T) {
	var f Fake
	tm := f.NewTimer(time.Second)
	f.Advance(time.Second)
	recv(t, tm)
}
