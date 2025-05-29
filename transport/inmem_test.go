package transport

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
)

// recorder is a stand-in for *raft.Node: it records what it was handed and
// returns canned replies, so a transport test can assert both delivery and
// fidelity without pulling in the consensus layer.
type recorder struct {
	id raft.NodeID

	mu       sync.Mutex
	votes    []raft.RequestVoteArgs
	appends  []raft.AppendEntriesArgs
	installs []raft.InstallSnapshotArgs

	voteReply    raft.RequestVoteReply
	appendReply  raft.AppendEntriesReply
	installReply raft.InstallSnapshotReply
}

func newRecorder(id raft.NodeID) *recorder {
	return &recorder{
		id:           id,
		voteReply:    raft.RequestVoteReply{Term: 1, VoteGranted: true},
		appendReply:  raft.AppendEntriesReply{Term: 1, Success: true},
		installReply: raft.InstallSnapshotReply{Term: 1},
	}
}

func (r *recorder) HandleRequestVote(a *raft.RequestVoteArgs) *raft.RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votes = append(r.votes, *a)
	reply := r.voteReply
	return &reply
}

func (r *recorder) HandleAppendEntries(a *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.appends = append(r.appends, *a)
	reply := r.appendReply
	return &reply
}

func (r *recorder) HandleInstallSnapshot(a *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.installs = append(r.installs, *a)
	reply := r.installReply
	return &reply
}

func (r *recorder) counts() (votes, appends, installs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.votes), len(r.appends), len(r.installs)
}

func TestSwitchboardDeliversToNamedHandler(t *testing.T) {
	sb := NewSwitchboard(1)
	a, b, c := newRecorder("a"), newRecorder("b"), newRecorder("c")
	sb.Register("a", a)
	sb.Register("b", b)
	sb.Register("c", c)
	b.voteReply = raft.RequestVoteReply{Term: 9, VoteGranted: true}

	reply, err := sb.Transport("a").RequestVote("b", &raft.RequestVoteArgs{Term: 9, CandidateID: "a", LastLogIndex: 3, LastLogTerm: 2, PreVote: true})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if reply.Term != 9 || !reply.VoteGranted {
		t.Errorf("reply %+v did not come from b", *reply)
	}

	if v, _, _ := b.counts(); v != 1 {
		t.Errorf("b saw %d votes, want 1", v)
	}
	if v, _, _ := c.counts(); v != 0 {
		t.Errorf("c saw %d votes, want 0", v)
	}
	b.mu.Lock()
	got := b.votes[0]
	b.mu.Unlock()
	if got.CandidateID != "a" || got.LastLogIndex != 3 || got.LastLogTerm != 2 || !got.PreVote {
		t.Errorf("args mangled in transit: %+v", got)
	}

	if _, err := sb.Transport("a").AppendEntries("nobody", &raft.AppendEntriesArgs{}); !errors.Is(err, ErrUnknownPeer) {
		t.Errorf("unregistered peer: got %v, want ErrUnknownPeer", err)
	}
}

func TestSwitchboardDeliversAllThreeRPCs(t *testing.T) {
	sb := NewSwitchboard(2)
	b := newRecorder("b")
	sb.Register("a", newRecorder("a"))
	sb.Register("b", b)
	tr := sb.Transport("a")

	if _, err := tr.RequestVote("b", &raft.RequestVoteArgs{Term: 1, CandidateID: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.AppendEntries("b", &raft.AppendEntriesArgs{Term: 1, LeaderID: "a",
		Entries: []raft.LogEntry{{Index: 1, Term: 1, Data: []byte("v")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.InstallSnapshot("b", &raft.InstallSnapshotArgs{Term: 1, LeaderID: "a", Data: []byte("snap"), Done: true}); err != nil {
		t.Fatal(err)
	}

	v, ap, in := b.counts()
	if v != 1 || ap != 1 || in != 1 {
		t.Fatalf("handler counts: votes=%d appends=%d installs=%d, want 1 each", v, ap, in)
	}

	st := sb.Stats()
	for _, typ := range []msgType{msgRequestVoteArgs, msgAppendEntriesArgs, msgInstallSnapshotArgs} {
		if st.ByType[typ.String()] != 1 {
			t.Errorf("ByType[%s] = %d, want 1", typ, st.ByType[typ.String()])
		}
	}
	if st.ByPair[Pair{From: "a", To: "b"}] != 3 {
		t.Errorf("ByPair[a->b] = %d, want 3", st.ByPair[Pair{From: "a", To: "b"}])
	}
	if st.Attempts != 3 || st.Delivered != 3 || st.Dropped != 0 {
		t.Errorf("unexpected stats: %+v", st)
	}
}

// A handler must not be able to reach back into the caller's argument memory,
// because over a socket it could not.
func TestSwitchboardCopiesArguments(t *testing.T) {
	sb := NewSwitchboard(3)
	b := newRecorder("b")
	sb.Register("b", b)

	data := []byte("original")
	args := &raft.AppendEntriesArgs{Term: 1, LeaderID: "a",
		Entries: []raft.LogEntry{{Index: 1, Term: 1, Data: data}}}
	if _, err := sb.Transport("a").AppendEntries("b", args); err != nil {
		t.Fatal(err)
	}

	b.mu.Lock()
	received := b.appends[0].Entries[0].Data
	b.mu.Unlock()
	received[0] = 'X'
	if string(data) != "original" {
		t.Errorf("handler mutated the caller's entry data: %q", data)
	}
}

func TestPartitionBlocksBothDirections(t *testing.T) {
	sb := NewSwitchboard(4)
	for _, id := range []raft.NodeID{"a", "b", "c"} {
		sb.Register(id, newRecorder(id))
	}
	sb.Partition([][]raft.NodeID{{"a", "b"}, {"c"}})

	if _, err := sb.Transport("a").AppendEntries("b", &raft.AppendEntriesArgs{}); err != nil {
		t.Errorf("same group should be reachable: %v", err)
	}
	if _, err := sb.Transport("a").AppendEntries("c", &raft.AppendEntriesArgs{}); !errors.Is(err, ErrUnreachable) {
		t.Errorf("a->c: got %v, want ErrUnreachable", err)
	}
	if _, err := sb.Transport("c").RequestVote("a", &raft.RequestVoteArgs{}); !errors.Is(err, ErrUnreachable) {
		t.Errorf("c->a: got %v, want ErrUnreachable", err)
	}
	if _, err := sb.Transport("c").InstallSnapshot("b", &raft.InstallSnapshotArgs{}); !errors.Is(err, ErrUnreachable) {
		t.Errorf("c->b: got %v, want ErrUnreachable", err)
	}

	// The blocked RPCs must never have touched the far handler.
	cRec := sb.handlerFor("c").(*recorder)
	if v, ap, in := cRec.counts(); v != 0 || ap != 0 || in != 0 {
		t.Errorf("partitioned handler invoked: votes=%d appends=%d installs=%d", v, ap, in)
	}

	sb.Heal()
	if _, err := sb.Transport("a").AppendEntries("c", &raft.AppendEntriesArgs{}); err != nil {
		t.Errorf("after Heal: %v", err)
	}
}

// A node left out of every group is cut off, so a test can describe a minority
// partition by naming only the majority side.
func TestPartitionOmittedNodeIsCutOff(t *testing.T) {
	sb := NewSwitchboard(5)
	for _, id := range []raft.NodeID{"a", "b", "c"} {
		sb.Register(id, newRecorder(id))
	}
	sb.Partition([][]raft.NodeID{{"a", "b"}})
	if _, err := sb.Transport("a").AppendEntries("c", &raft.AppendEntriesArgs{}); !errors.Is(err, ErrUnreachable) {
		t.Errorf("a->c: got %v, want ErrUnreachable", err)
	}
	if _, err := sb.Transport("c").AppendEntries("a", &raft.AppendEntriesArgs{}); !errors.Is(err, ErrUnreachable) {
		t.Errorf("c->a: got %v, want ErrUnreachable", err)
	}
}

func TestIsolateAndRejoin(t *testing.T) {
	sb := NewSwitchboard(6)
	for _, id := range []raft.NodeID{"a", "b", "c"} {
		sb.Register(id, newRecorder(id))
	}
	sb.Isolate("b")

	if _, err := sb.Transport("a").AppendEntries("b", &raft.AppendEntriesArgs{}); !errors.Is(err, ErrUnreachable) {
		t.Errorf("a->isolated b: got %v, want ErrUnreachable", err)
	}
	if _, err := sb.Transport("b").AppendEntries("a", &raft.AppendEntriesArgs{}); !errors.Is(err, ErrUnreachable) {
		t.Errorf("isolated b->a: got %v, want ErrUnreachable", err)
	}
	if _, err := sb.Transport("a").AppendEntries("c", &raft.AppendEntriesArgs{}); err != nil {
		t.Errorf("a->c should be unaffected: %v", err)
	}

	sb.Rejoin("b")
	if _, err := sb.Transport("a").AppendEntries("b", &raft.AppendEntriesArgs{}); err != nil {
		t.Errorf("after Rejoin: %v", err)
	}
}

func TestPausedNodeNeitherSendsNorReceives(t *testing.T) {
	sb := NewSwitchboard(7)
	a, b := newRecorder("a"), newRecorder("b")
	sb.Register("a", a)
	sb.Register("b", b)
	sb.Pause("b")

	if _, err := sb.Transport("a").AppendEntries("b", &raft.AppendEntriesArgs{}); !errors.Is(err, ErrPeerStopped) {
		t.Errorf("a->paused b: got %v, want ErrPeerStopped", err)
	}
	if _, err := sb.Transport("b").RequestVote("a", &raft.RequestVoteArgs{}); !errors.Is(err, ErrPeerStopped) {
		t.Errorf("paused b->a: got %v, want ErrPeerStopped", err)
	}
	if v, ap, _ := b.counts(); v != 0 || ap != 0 {
		t.Errorf("paused handler invoked: votes=%d appends=%d", v, ap)
	}
	if v, ap, _ := a.counts(); v != 0 || ap != 0 {
		t.Errorf("handler received from paused sender: votes=%d appends=%d", v, ap)
	}

	sb.Resume("b")
	if _, err := sb.Transport("a").AppendEntries("b", &raft.AppendEntriesArgs{}); err != nil {
		t.Errorf("after Resume: %v", err)
	}
	if _, ap, _ := b.counts(); ap != 1 {
		t.Errorf("resumed handler saw %d appends, want 1", ap)
	}
}

func TestDropRateMatchesOverLargeSample(t *testing.T) {
	const (
		n    = 20000
		rate = 0.25
		tol  = 0.015
	)
	sb := NewSwitchboard(8)
	b := newRecorder("b")
	sb.Register("a", newRecorder("a"))
	sb.Register("b", b)
	sb.SetDropRate(rate)
	tr := sb.Transport("a")

	dropped := 0
	for i := 0; i < n; i++ {
		if _, err := tr.AppendEntries("b", &raft.AppendEntriesArgs{Term: 1, LeaderID: "a"}); err != nil {
			if !errors.Is(err, ErrDropped) {
				t.Fatalf("unexpected error: %v", err)
			}
			dropped++
		}
	}
	observed := float64(dropped) / n
	if math.Abs(observed-rate) > tol {
		t.Errorf("drop rate %.4f, want %.2f +/- %.3f (%d of %d)", observed, rate, tol, dropped, n)
	}

	// Dropped RPCs must never reach the handler, and must still be counted as
	// attempts for message-cost accounting.
	_, delivered, _ := b.counts()
	if delivered != n-dropped {
		t.Errorf("handler saw %d appends, want %d", delivered, n-dropped)
	}
	st := sb.Stats()
	if st.Attempts != n {
		t.Errorf("attempts %d, want %d", st.Attempts, n)
	}
	if st.Dropped != uint64(dropped) {
		t.Errorf("stats dropped %d, want %d", st.Dropped, dropped)
	}
}

func TestDuplicateDeliveryInvokesHandlerTwice(t *testing.T) {
	sb := NewSwitchboard(9)
	b := newRecorder("b")
	sb.Register("a", newRecorder("a"))
	sb.Register("b", b)
	sb.SetDuplicateRate(1.0)

	reply, err := sb.Transport("a").AppendEntries("b", &raft.AppendEntriesArgs{
		Term: 2, LeaderID: "a",
		Entries: []raft.LogEntry{{Index: 1, Term: 2, Data: []byte("v")}}})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.Success {
		t.Errorf("reply %+v, want the handler's reply", *reply)
	}

	_, appends, _ := b.counts()
	if appends != 2 {
		t.Fatalf("handler saw %d appends, want 2", appends)
	}
	b.mu.Lock()
	first, second := b.appends[0], b.appends[1]
	b.mu.Unlock()
	if first.Term != second.Term || len(first.Entries) != len(second.Entries) ||
		first.Entries[0].Index != second.Entries[0].Index {
		t.Errorf("duplicate differed from original:\n%+v\n%+v", first, second)
	}
	if st := sb.Stats(); st.Duplicated != 1 {
		t.Errorf("stats duplicated %d, want 1", st.Duplicated)
	}

	sb.SetDuplicateRate(0)
	before := appends
	if _, err := sb.Transport("a").AppendEntries("b", &raft.AppendEntriesArgs{Term: 2, LeaderID: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, after, _ := b.counts(); after != before+1 {
		t.Errorf("with duplication off, handler saw %d appends, want %d", after, before+1)
	}
}

func TestLatencyIsApplied(t *testing.T) {
	sb := NewSwitchboard(10)
	sb.Register("a", newRecorder("a"))
	sb.Register("b", newRecorder("b"))
	sb.SetLatency(20*time.Millisecond, 20*time.Millisecond)

	start := time.Now()
	if _, err := sb.Transport("a").AppendEntries("b", &raft.AppendEntriesArgs{}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Errorf("delivered after %v, want at least 20ms", elapsed)
	}
}

// Reproducibility is the whole reason the seed is explicit: a fault-injection
// failure is only useful if rerunning with the same seed replays it. The
// decision function is driven directly here so the drop, duplicate and latency
// draws can be compared exactly instead of inferred from wall-clock timings.
func TestSameSeedReproducesFaultPattern(t *testing.T) {
	type outcome struct {
		err   bool
		dup   bool
		delay time.Duration
	}

	run := func(seed int64) []outcome {
		sb := NewSwitchboard(seed)
		sb.Register("a", newRecorder("a"))
		sb.Register("b", newRecorder("b"))
		sb.SetDropRate(0.3)
		sb.SetDuplicateRate(0.3)
		sb.SetLatency(time.Millisecond, 10*time.Millisecond)

		out := make([]outcome, 0, 500)
		for i := 0; i < 500; i++ {
			d, err := sb.decide("a", "b", msgAppendEntriesArgs)
			out = append(out, outcome{err: err != nil, dup: d.duplicate, delay: d.delay})
		}
		return out
	}

	first, second, other := run(1234), run(1234), run(4321)

	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("same seed diverged at call %d: %+v vs %+v", i, first[i], second[i])
		}
	}

	var drops, dups int
	var delays int
	for _, o := range first {
		if o.err {
			drops++
		}
		if o.dup {
			dups++
		}
		if o.delay > 0 {
			delays++
		}
	}
	if drops == 0 || dups == 0 || delays == 0 {
		t.Fatalf("pattern is degenerate (drops=%d dups=%d delays=%d); the test proves nothing", drops, dups, delays)
	}

	same := true
	for i := range first {
		if first[i] != other[i] {
			same = false
			break
		}
	}
	if same {
		t.Error("a different seed produced an identical pattern")
	}
}

// Every knob is documented as settable while RPCs are in flight, so the race
// detector gets a chance to prove it.
func TestFaultKnobsAreSafeDuringTraffic(t *testing.T) {
	sb := NewSwitchboard(11)
	ids := []raft.NodeID{"a", "b", "c"}
	for _, id := range ids {
		sb.Register(id, newRecorder(id))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for _, from := range ids {
		for _, to := range ids {
			if from == to {
				continue
			}
			wg.Add(1)
			go func(from, to raft.NodeID) {
				defer wg.Done()
				tr := sb.Transport(from)
				for {
					select {
					case <-stop:
						return
					default:
					}
					_, _ = tr.AppendEntries(to, &raft.AppendEntriesArgs{Term: 1, LeaderID: from})
					_, _ = tr.RequestVote(to, &raft.RequestVoteArgs{Term: 1, CandidateID: from})
				}
			}(from, to)
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			sb.SetDropRate(0.1)
			sb.Partition([][]raft.NodeID{{"a", "b"}, {"c"}})
			sb.SetLatency(0, time.Microsecond)
			sb.Isolate("a")
			sb.Pause("b")
			_ = sb.Stats()
			sb.Resume("b")
			sb.Rejoin("a")
			sb.Heal()
			sb.SetDuplicateRate(0.1)
			sb.ResetStats()
		}
		close(stop)
	}()

	wg.Wait()
}

func TestStatsResetAndStringing(t *testing.T) {
	sb := NewSwitchboard(12)
	sb.Register("a", newRecorder("a"))
	sb.Register("b", newRecorder("b"))
	if _, err := sb.Transport("a").RequestVote("b", &raft.RequestVoteArgs{}); err != nil {
		t.Fatal(err)
	}
	if s := sb.Stats().String(); s == "" {
		t.Error("Stats.String returned empty")
	}
	sb.ResetStats()
	if st := sb.Stats(); st.Attempts != 0 || len(st.ByPair) != 0 || len(st.ByType) != 0 {
		t.Errorf("after ResetStats: %+v", st)
	}
}

func TestDeregisterMakesPeerUnknown(t *testing.T) {
	sb := NewSwitchboard(13)
	sb.Register("a", newRecorder("a"))
	sb.Register("b", newRecorder("b"))
	sb.Deregister("b")
	if _, err := sb.Transport("a").RequestVote("b", &raft.RequestVoteArgs{}); !errors.Is(err, ErrUnknownPeer) {
		t.Errorf("got %v, want ErrUnknownPeer", err)
	}
}

// handlerFor is a test-only accessor used to assert that a blocked RPC never
// reached the far side.
func (s *Switchboard) handlerFor(id raft.NodeID) raft.RPCHandler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handlers[id]
}
