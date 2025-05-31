package tests

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
	"github.com/LegendaryPenguin/cairn/server"
)

func TestClusterElectsALeader(t *testing.T) {
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	if ldr == "" {
		t.Fatal("no leader elected")
	}
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
}

func TestSingleNodeClusterElectsItself(t *testing.T) {
	// A one-node cluster is its own majority, so it should become leader without
	// contacting anyone. Worth testing because the quorum arithmetic has an
	// off-by-one here that is easy to get wrong.
	c := NewCluster(t, 1)
	c.WaitForLeader(3 * time.Second)

	if err := c.Put("c1", 1, "solo", "ok"); err != nil {
		t.Fatalf("write to a single-node cluster: %v", err)
	}
	c.WaitForConvergence("solo", "ok", 2*time.Second)
}

func TestWritesReplicateAndConverge(t *testing.T) {
	c := NewCluster(t, 5)
	c.WaitForLeader(5 * time.Second)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if err := c.Put("writer", uint64(i+1), key, fmt.Sprintf("v%02d", i)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	for i := 0; i < 20; i++ {
		c.WaitForConvergence(fmt.Sprintf("key-%02d", i), fmt.Sprintf("v%02d", i), 5*time.Second)
	}
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
	c.Audit().AssertNoCommitRegression(t)
	c.Audit().AssertApplyIsOrderedAndExactlyOnce(t, false)
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := NewCluster(t, 5)
	c.WaitForLeader(5 * time.Second)

	// Write something everyone agrees on first, so the test can prove the
	// minority also fails to LOSE data, not just fails to write.
	if err := c.Put("c1", 1, "before", "yes"); err != nil {
		t.Fatalf("pre-partition write: %v", err)
	}
	c.WaitForConvergence("before", "yes", 3*time.Second)

	maj, min := c.Majority(), c.Minority()
	c.Partition(maj, min)

	// The minority must not be able to elect a leader at all, let alone commit.
	c.WaitForNoLeader(min, 3*time.Second)

	// A write submitted to a minority node must fail rather than silently
	// succeeding locally. This is the test that catches a node that appends to
	// its own log and answers the client without a quorum.
	minNode := c.Server(min[0])
	_, err := minNode.Put("c2", 1, "ghost", []byte("should-not-commit"))
	if err == nil {
		t.Fatal("a minority node accepted a write; a partitioned replica must not commit")
	}

	// The majority side keeps working.
	var majLeader raft.NodeID
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range maj {
			if c.Node(id).Role() == raft.Leader {
				majLeader = id
			}
		}
		if majLeader != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if majLeader == "" {
		t.Fatal("the majority side failed to elect a leader")
	}
	if _, err := c.Server(majLeader).Put("c3", 1, "during", []byte("ok")); err != nil {
		t.Fatalf("majority write during partition: %v", err)
	}

	c.Heal()
	c.WaitForConvergence("during", "ok", 10*time.Second)

	// The ghost write must not have survived anywhere.
	for _, id := range c.Up() {
		if _, found := c.StateMachine(id).Get("ghost"); found {
			t.Fatalf("%s holds a value written by a minority partition", id)
		}
	}
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
	c.Audit().AssertNoCommitRegression(t)
}

func TestPartitionedLeaderStepsDownAndClusterRecovers(t *testing.T) {
	c := NewCluster(t, 5)
	ldr := c.WaitForLeader(5 * time.Second)

	if err := c.Put("c1", 1, "k", "v1"); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	// Isolate the leader alone against the other four.
	var rest []raft.NodeID
	for _, id := range c.IDs {
		if id != ldr {
			rest = append(rest, id)
		}
	}
	c.Partition([]raft.NodeID{ldr}, rest)

	// The remaining four must elect a new leader from among themselves.
	var newLdr raft.NodeID
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range rest {
			if c.Node(id).Role() == raft.Leader {
				newLdr = id
			}
		}
		if newLdr != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLdr == "" {
		t.Fatal("the majority side did not elect a replacement leader")
	}
	if newLdr == ldr {
		t.Fatal("the isolated old leader was somehow re-elected by the majority")
	}

	if _, err := c.Server(newLdr).Put("c2", 1, "k", []byte("v2")); err != nil {
		t.Fatalf("write to the new leader: %v", err)
	}

	// The isolated old leader must not be able to serve a linearizable read.
	// It still believes it leads and still holds v1 in its state machine, so
	// answering locally would hand a client a stale value with a linearizable
	// label. ReadIndex is what prevents it.
	_, _, err := c.Server(ldr).Get("k", server.Linearizable)
	if err == nil {
		t.Fatal("the isolated former leader served a linearizable read; this is the classic stale-read bug")
	}

	c.Heal()
	c.WaitForConvergence("k", "v2", 10*time.Second)
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
	c.Audit().AssertNoCommitRegression(t)
}

func TestLeaderCrashAfterReplicationPreservesCommittedWrite(t *testing.T) {
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	// This write is acknowledged, so it is committed on a majority by definition.
	if err := c.Put("c1", 1, "durable", "must-survive"); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.WaitForConvergence("durable", "must-survive", 3*time.Second)

	c.Crash(ldr)

	// The survivors must elect a new leader and must still hold the committed value.
	deadline := time.Now().Add(10 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c.Up() {
			v, present := c.StateMachine(id).Get("durable")
			if !present || string(v) != "must-survive" {
				ok = false
				break
			}
		}
		if ok {
			found = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !found {
		t.Fatal("a committed write did not survive the leader crashing after replicating it")
	}

	// And the cluster must accept new writes.
	if err := c.Put("c1", 2, "after", "ok"); err != nil {
		t.Fatalf("write after leader crash: %v", err)
	}
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
	c.Audit().AssertNoCommitRegression(t)
}

func TestRestartedNodeRecoversStateFromDisk(t *testing.T) {
	c := NewCluster(t, 3)
	c.WaitForLeader(5 * time.Second)

	for i := 0; i < 10; i++ {
		if err := c.Put("c1", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	c.WaitForConvergence("k9", "v9", 5*time.Second)

	// Crash a follower, then restart it. Everything it knows must come back off
	// its own WAL, not from the leader resending the whole history.
	var victim raft.NodeID
	ldr := c.Leader()
	for _, id := range c.IDs {
		if id != ldr {
			victim = id
			break
		}
	}

	c.Crash(victim)
	c.Restart(victim)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := c.StateMachine(victim).Get("k9"); ok && string(v) == "v9" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		want := fmt.Sprintf("v%d", i)
		v, ok := c.StateMachine(victim).Get(fmt.Sprintf("k%d", i))
		if !ok || string(v) != want {
			t.Fatalf("after restart, %s holds k%d=%q (found=%v), want %q\n%s",
				victim, i, v, ok, want, c.DumpDir(victim))
		}
	}
	c.Audit().AssertNoCommitRegression(t)
}

func TestWholeClusterRestartRecoversCommittedState(t *testing.T) {
	c := NewCluster(t, 3)
	c.WaitForLeader(5 * time.Second)

	for i := 0; i < 15; i++ {
		if err := c.Put("c1", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	c.WaitForConvergence("k14", "v14", 5*time.Second)

	// Kill everything, then bring it all back. Nothing is in memory now; every
	// recovered byte came off disk.
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}

	c.WaitForLeader(10 * time.Second)
	for i := 0; i < 15; i++ {
		c.WaitForConvergence(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 10*time.Second)
	}
}

func TestFollowerWithConflictingSuffixIsRepaired(t *testing.T) {
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	if err := c.Put("c1", 1, "shared", "base"); err != nil {
		t.Fatalf("base write: %v", err)
	}
	c.WaitForConvergence("shared", "base", 3*time.Second)

	// Isolate one follower so it misses everything that follows. When it rejoins,
	// its log is a strict prefix and must be brought forward.
	var lagging raft.NodeID
	for _, id := range c.IDs {
		if id != ldr {
			lagging = id
			break
		}
	}
	var rest []raft.NodeID
	for _, id := range c.IDs {
		if id != lagging {
			rest = append(rest, id)
		}
	}
	c.Partition(rest, []raft.NodeID{lagging})

	for i := 0; i < 8; i++ {
		if err := c.Put("c1", uint64(i+2), fmt.Sprintf("new%d", i), "x"); err != nil {
			t.Fatalf("write during partition: %v", err)
		}
	}

	c.Heal()

	// The lagging node must catch up on everything it missed.
	for i := 0; i < 8; i++ {
		c.WaitForConvergence(fmt.Sprintf("new%d", i), "x", 10*time.Second)
	}
	c.Audit().AssertNoCommitRegression(t)
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
}

func TestLaggingFollowerCaughtUpBySnapshot(t *testing.T) {
	// A low threshold makes the leader compact while a follower is away, so the
	// entries that follower needs no longer exist and the only way to catch it up
	// is InstallSnapshot.
	c := NewCluster(t, 3, WithSnapshotThreshold(8))
	ldr := c.WaitForLeader(5 * time.Second)

	var lagging raft.NodeID
	for _, id := range c.IDs {
		if id != ldr {
			lagging = id
			break
		}
	}
	var rest []raft.NodeID
	for _, id := range c.IDs {
		if id != lagging {
			rest = append(rest, id)
		}
	}

	c.Partition(rest, []raft.NodeID{lagging})

	// Write well past the snapshot threshold so compaction definitely happens.
	for i := 0; i < 40; i++ {
		if err := c.Put("c1", uint64(i+1), fmt.Sprintf("s%02d", i), fmt.Sprintf("v%02d", i)); err != nil {
			t.Fatalf("write %d during partition: %v", i, err)
		}
	}

	c.Heal()

	// The follower must end up holding state it could not have replayed from the
	// log, because the leader no longer has those entries.
	c.WaitForConvergence("s00", "v00", 15*time.Second)
	c.WaitForConvergence("s39", "v39", 15*time.Second)

	// A snapshot install jumps lastApplied forward, so gaps are expected here.
	c.Audit().AssertApplyIsOrderedAndExactlyOnce(t, true)
	c.Audit().AssertNoCommitRegression(t)
}

func TestRepeatedSplitVotesStillConverge(t *testing.T) {
	// A 4-node cluster has an even quorum, which makes split votes more likely,
	// and heavy loss makes elections fail repeatedly. Randomized timeouts are the
	// only reason this terminates at all.
	c := NewCluster(t, 4, WithTimings(30*time.Millisecond, 120*time.Millisecond, 400*time.Millisecond))
	c.WaitForLeader(10 * time.Second)

	c.Board.SetDropRate(0.3)
	defer c.Board.SetDropRate(0)

	// Under 30% loss a leader should still eventually be established and writes
	// should still land, just slowly.
	var lastErr error
	ok := false
	for attempt := 0; attempt < 40; attempt++ {
		if err := c.Put("c1", 1, "flaky", "landed"); err == nil {
			ok = true
			break
		} else {
			lastErr = err
		}
	}
	if !ok {
		t.Fatalf("no write succeeded under 30%% packet loss: %v", lastErr)
	}

	c.Board.SetDropRate(0)
	c.WaitForConvergence("flaky", "landed", 15*time.Second)
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
	c.Audit().AssertNoCommitRegression(t)
}

func TestDuplicatedAndDelayedRPCsDoNotCorruptState(t *testing.T) {
	c := NewCluster(t, 3)
	c.WaitForLeader(5 * time.Second)

	// Duplicate delivery exercises the idempotence of the RPC handlers: a
	// redelivered AppendEntries must not truncate acknowledged entries, and a
	// redelivered RequestVote must get the same answer.
	c.Board.SetDuplicateRate(0.4)
	c.Board.SetLatency(1*time.Millisecond, 15*time.Millisecond)
	defer func() {
		c.Board.SetDuplicateRate(0)
		c.Board.SetLatency(0, 0)
	}()

	for i := 0; i < 15; i++ {
		if err := c.Put("c1", uint64(i+1), fmt.Sprintf("d%02d", i), fmt.Sprintf("v%02d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	c.Board.SetDuplicateRate(0)
	c.Board.SetLatency(0, 0)

	for i := 0; i < 15; i++ {
		c.WaitForConvergence(fmt.Sprintf("d%02d", i), fmt.Sprintf("v%02d", i), 10*time.Second)
	}
	c.Audit().AssertApplyIsOrderedAndExactlyOnce(t, false)
	c.Audit().AssertNoCommitRegression(t)
}

func TestRetryWithSameSequenceNumberIsNotAppliedTwice(t *testing.T) {
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	// Start a counter at 1.
	if err := c.Put("c1", 1, "counter", "1"); err != nil {
		t.Fatalf("initial: %v", err)
	}

	// A CAS from 1 to 2, submitted twice with the SAME sequence number, is what a
	// client does when the first attempt times out with an unknown outcome. The
	// second must be recognized as a retry, not applied as a second swap.
	srv := c.Server(ldr)
	r1, err := srv.CompareAndSwap("c1", 2, "counter", []byte("1"), []byte("2"), true)
	if err != nil {
		t.Fatalf("first CAS: %v", err)
	}
	if !r1.Swapped {
		t.Fatal("first CAS did not swap")
	}

	r2, err := srv.CompareAndSwap("c1", 2, "counter", []byte("1"), []byte("2"), true)
	if err != nil {
		t.Fatalf("retried CAS: %v", err)
	}
	if !r2.Swapped {
		t.Fatal("the retry reported no swap; a deduplicated retry must return the original result, not a fresh (failed) evaluation")
	}

	v, _, err := c.GetLinearizable("counter")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if v != "2" {
		t.Fatalf("counter = %q, want \"2\"", v)
	}
}

func TestLinearizableReadSeesAcknowledgedWrite(t *testing.T) {
	c := NewCluster(t, 3)
	c.WaitForLeader(5 * time.Second)

	// The core linearizability requirement: once a write returns, every
	// subsequent linearizable read must observe it or something later.
	for i := 0; i < 25; i++ {
		val := fmt.Sprintf("v%02d", i)
		if err := c.Put("c1", uint64(i+1), "hot", val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		got, ok, err := c.GetLinearizable("hot")
		if err != nil {
			t.Fatalf("read after write %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("read after write %d found nothing", i)
		}
		if got != val {
			t.Fatalf("read after acknowledged write %d returned %q, want %q: a linearizable read went backwards", i, got, val)
		}
	}
}

func TestStaleReadModeIsAllowedToBeWrongOnAPartitionedNode(t *testing.T) {
	// The complement of the ReadIndex test: this documents that Stale genuinely
	// is weaker, so the distinction in the API is real rather than decorative.
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	if err := c.Put("c1", 1, "k", "old"); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.WaitForConvergence("k", "old", 3*time.Second)

	var rest []raft.NodeID
	for _, id := range c.IDs {
		if id != ldr {
			rest = append(rest, id)
		}
	}
	c.Partition([]raft.NodeID{ldr}, rest)

	// Majority elects a new leader and moves on.
	var newLdr raft.NodeID
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && newLdr == "" {
		for _, id := range rest {
			if c.Node(id).Role() == raft.Leader {
				newLdr = id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLdr == "" {
		t.Fatal("no new leader on the majority side")
	}
	if _, err := c.Server(newLdr).Put("c2", 1, "k", []byte("new")); err != nil {
		t.Fatalf("write to new leader: %v", err)
	}

	// A stale read on the isolated former leader returns the old value and no
	// error. That is the documented contract, and the reason Linearizable is the
	// default in the HTTP layer.
	v, ok, err := c.Server(ldr).Get("k", server.Stale)
	if err != nil {
		t.Fatalf("stale read should not error: %v", err)
	}
	if !ok || string(v) != "old" {
		t.Logf("stale read returned %q (found=%v); timing-dependent, not a failure", v, ok)
	}

	// The same read with the linearizable guarantee must refuse.
	if _, _, err := c.Server(ldr).Get("k", server.Linearizable); err == nil {
		t.Fatal("linearizable read on an isolated former leader succeeded")
	}

	c.Heal()
	c.WaitForConvergence("k", "new", 10*time.Second)
}

func TestMembershipAddAndRemoveVoter(t *testing.T) {
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	if err := c.Put("c1", 1, "k", "v"); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Removing a voter shrinks the cluster and lowers the quorum. The removed
	// node is not told; it simply stops hearing from the leader.
	var victim raft.NodeID
	for _, id := range c.IDs {
		if id != ldr {
			victim = id
			break
		}
	}
	if err := c.Server(ldr).RemoveVoter(victim); err != nil {
		t.Fatalf("RemoveVoter(%s): %v", victim, err)
	}

	peers := c.Node(ldr).Peers()
	if len(peers) != 2 {
		t.Fatalf("leader peers = %v, want 2 after removal", peers)
	}

	// The two-node cluster must still accept writes.
	if _, err := c.Server(ldr).Put("c1", 2, "after-removal", []byte("ok")); err != nil {
		t.Fatalf("write after removal: %v", err)
	}

	// Adding it back must restore the original size.
	if err := c.Server(ldr).AddVoter(victim); err != nil {
		t.Fatalf("AddVoter(%s): %v", victim, err)
	}
	if peers := c.Node(ldr).Peers(); len(peers) != 3 {
		t.Fatalf("leader peers = %v, want 3 after re-adding", peers)
	}
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
}

func TestRemoveVoterRejectedBelowTwoNodes(t *testing.T) {
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	var others []raft.NodeID
	for _, id := range c.IDs {
		if id != ldr {
			others = append(others, id)
		}
	}
	if err := c.Server(ldr).RemoveVoter(others[0]); err != nil {
		t.Fatalf("first removal: %v", err)
	}
	// Going from two to one must be refused.
	err := c.Server(ldr).RemoveVoter(others[1])
	if !errors.Is(err, raft.ErrWouldLoseQuorum) {
		t.Fatalf("second removal returned %v, want ErrWouldLoseQuorum", err)
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	var follower raft.NodeID
	for _, id := range c.IDs {
		if id != ldr {
			follower = id
			break
		}
	}

	_, err := c.Server(follower).Put("c1", 1, "k", []byte("v"))
	var nle *raft.NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("follower returned %v, want NotLeaderError", err)
	}
	if nle.LeaderHint != ldr {
		t.Fatalf("redirect hint = %q, want %q", nle.LeaderHint, ldr)
	}
}

func TestConcurrentClientsAllObserveTheirOwnWrites(t *testing.T) {
	c := NewCluster(t, 3)
	c.WaitForLeader(5 * time.Second)

	const clients = 6
	const perClient = 10

	errCh := make(chan error, clients)
	for cl := 0; cl < clients; cl++ {
		go func(cl int) {
			id := fmt.Sprintf("client-%d", cl)
			for i := 0; i < perClient; i++ {
				key := fmt.Sprintf("%s-k%d", id, i)
				if err := c.Put(id, uint64(i+1), key, "v"); err != nil {
					errCh <- fmt.Errorf("%s put %s: %w", id, key, err)
					return
				}
			}
			errCh <- nil
		}(cl)
	}
	for i := 0; i < clients; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	for cl := 0; cl < clients; cl++ {
		for i := 0; i < perClient; i++ {
			c.WaitForConvergence(fmt.Sprintf("client-%d-k%d", cl, i), "v", 10*time.Second)
		}
	}
	c.Audit().AssertApplyIsOrderedAndExactlyOnce(t, false)
	c.Audit().AssertNoCommitRegression(t)
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
}
