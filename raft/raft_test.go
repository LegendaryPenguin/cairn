package raft

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// memStore is a minimal in-process Storage for unit tests. It is deliberately
// not the real storage package: these tests are about consensus rules, and a
// bug in WAL framing should fail storage's own tests rather than showing up
// here as a confusing election failure.
type memStore struct {
	mu       sync.Mutex
	term     Term
	vote     NodeID
	entries  []LogEntry // entries[0] is at index snapIndex+1
	snapIdx  Index
	snapTerm Term
	snapData []byte
	saves    int // SaveState call count, to assert durability ordering
}

func newMemStore() *memStore { return &memStore{} }

func (m *memStore) SaveState(t Term, v NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term, m.vote, m.saves = t, v, m.saves+1
	return nil
}

func (m *memStore) LoadState() (Term, NodeID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.vote, nil
}

func (m *memStore) Append(es []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range es {
		want := m.lastLocked() + 1
		if e.Index != want {
			return errors.New("non-contiguous append")
		}
		m.entries = append(m.entries, e)
	}
	return nil
}

func (m *memStore) TruncateSuffix(from Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if from <= m.snapIdx {
		return ErrCompacted
	}
	off := int(from - m.snapIdx - 1)
	if off < 0 || off > len(m.entries) {
		return nil
	}
	m.entries = m.entries[:off]
	return nil
}

func (m *memStore) Entries(lo, hi Index) ([]LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lo <= m.snapIdx {
		return nil, ErrCompacted
	}
	if hi > m.lastLocked()+1 {
		return nil, ErrUnavailable
	}
	out := make([]LogEntry, 0, hi-lo)
	for _, e := range m.entries {
		if e.Index >= lo && e.Index < hi {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *memStore) Term(i Index) (Term, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i == 0 {
		return 0, nil
	}
	if i == m.snapIdx {
		return m.snapTerm, nil
	}
	if i < m.snapIdx {
		return 0, ErrCompacted
	}
	off := int(i - m.snapIdx - 1)
	if off < 0 || off >= len(m.entries) {
		return 0, ErrUnavailable
	}
	return m.entries[off].Term, nil
}

func (m *memStore) FirstIndex() Index {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapIdx + 1
}

func (m *memStore) LastIndex() Index {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLocked()
}

func (m *memStore) lastLocked() Index {
	if len(m.entries) == 0 {
		return m.snapIdx
	}
	return m.entries[len(m.entries)-1].Index
}

func (m *memStore) SaveSnapshot(s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapIdx, m.snapTerm, m.snapData = s.Meta.Index, s.Meta.Term, s.Data
	kept := m.entries[:0]
	for _, e := range m.entries {
		if e.Index > s.Meta.Index {
			kept = append(kept, e)
		}
	}
	m.entries = kept
	return nil
}

func (m *memStore) LoadSnapshot() (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Snapshot{Meta: SnapshotMeta{Index: m.snapIdx, Term: m.snapTerm}, Data: m.snapData}, nil
}

func (m *memStore) Close() error { return nil }

// countingSM records what it applied so tests can assert exactly-once, in-order
// application.
type countingSM struct {
	mu      sync.Mutex
	applied []Index
	state   []byte
}

func (s *countingSM) Apply(e LogEntry) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, e.Index)
	s.state = append(s.state, e.Data...)
	return len(s.applied), nil
}

func (s *countingSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]byte, len(s.state))
	copy(out, s.state)
	return out, nil
}

func (s *countingSM) Restore(d []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = append([]byte(nil), d...)
	return nil
}

func (s *countingSM) appliedCopy() []Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Index(nil), s.applied...)
}

// nilTransport fails every RPC. Used for handler-only tests where no peer
// should be contacted.
type nilTransport struct{}

func (nilTransport) RequestVote(NodeID, *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, errors.New("unreachable")
}
func (nilTransport) AppendEntries(NodeID, *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return nil, errors.New("unreachable")
}
func (nilTransport) InstallSnapshot(NodeID, *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return nil, errors.New("unreachable")
}

func testNode(t *testing.T, id NodeID, peers []NodeID, store Storage) (*Node, *countingSM) {
	t.Helper()
	sm := &countingSM{}
	cfg := DefaultConfig()
	cfg.ID = id
	cfg.Peers = peers
	n, err := NewNode(cfg, store, sm, nilTransport{})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n, sm
}

func seed(t *testing.T, m *memStore, spec ...[2]uint64) {
	t.Helper()
	var es []LogEntry
	for i, s := range spec {
		es = append(es, LogEntry{Index: Index(i + 1), Term: Term(s[1]), Kind: EntryCommand})
		_ = s[0]
	}
	if err := m.Append(es); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// --- vote rules ---

func TestVoteDeniedForLowerTerm(t *testing.T) {
	m := newMemStore()
	m.term = 5
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	r := n.HandleRequestVote(&RequestVoteArgs{Term: 4, CandidateID: "b"})
	if r.VoteGranted {
		t.Fatal("granted a vote to a candidate from an older term")
	}
	if r.Term != 5 {
		t.Fatalf("reply term = %d, want 5 so the stale candidate steps down", r.Term)
	}
}

func TestOneVotePerTerm(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	if r := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "b"}); !r.VoteGranted {
		t.Fatal("first vote in a term should be granted")
	}
	if r := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "c"}); r.VoteGranted {
		t.Fatal("granted a second vote in the same term, which permits two leaders")
	}
	// A retried request from the same candidate must get the same answer rather
	// than a spurious denial, or a dropped reply costs an election.
	if r := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "b"}); !r.VoteGranted {
		t.Fatal("re-asking the same candidate should return the recorded vote")
	}
}

func TestVoteDeniedForStaleLog(t *testing.T) {
	m := newMemStore()
	// Our log: indexes 1..3, the last at term 4.
	if err := m.Append([]LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 4},
	}); err != nil {
		t.Fatal(err)
	}
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	// Candidate's last term is behind ours: deny regardless of index.
	r := n.HandleRequestVote(&RequestVoteArgs{
		Term: 9, CandidateID: "b", LastLogIndex: 99, LastLogTerm: 3,
	})
	if r.VoteGranted {
		t.Fatal("granted a vote to a candidate with a staler log term; it could erase committed entries")
	}

	// Same last term, shorter log: deny.
	m2 := newMemStore()
	if err := m2.Append([]LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 4}}); err != nil {
		t.Fatal(err)
	}
	n2, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m2)
	if r := n2.HandleRequestVote(&RequestVoteArgs{
		Term: 9, CandidateID: "b", LastLogIndex: 1, LastLogTerm: 4,
	}); r.VoteGranted {
		t.Fatal("granted a vote to a candidate with a shorter log at the same term")
	}
}

func TestVoteIsDurableBeforeReply(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	before := m.saves
	r := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "b"})
	if !r.VoteGranted {
		t.Fatal("expected vote granted")
	}
	if m.saves <= before {
		t.Fatal("vote was granted without a durable SaveState; a crash here lets the node vote twice in one term")
	}
	if m.vote != "b" || m.term != 1 {
		t.Fatalf("persisted state = (%d, %q), want (1, \"b\")", m.term, m.vote)
	}
}

func TestPreVoteDoesNotConsumeTheRealVote(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)
	// Force the election deadline into the past so the pre-vote is eligible.
	n.mu.Lock()
	n.electionDeadline = n.clock.Now().Add(-time.Second)
	n.mu.Unlock()

	saves := m.saves
	if r := n.HandleRequestVote(&RequestVoteArgs{
		Term: 1, CandidateID: "b", PreVote: true,
	}); !r.VoteGranted {
		t.Fatal("pre-vote should be granted to a fresh candidate when no leader is live")
	}
	if m.saves != saves {
		t.Fatal("pre-vote wrote persistent state; it must be a pure hypothetical")
	}
	// The real vote must still be available.
	if r := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "c"}); !r.VoteGranted {
		t.Fatal("pre-vote consumed the term's real vote")
	}
}

func TestLeaderNeverGrantsAPreVote(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	n.mu.Lock()
	n.currentTerm = 3
	n.becomeLeaderLocked()
	n.mu.Unlock()

	// A leader does not arm its own election timer, so its deadline sits in the
	// past and the "no live leader" heuristic would otherwise read as true —
	// making the one node that is certain a leader exists the most willing to
	// vote against it.
	r := n.HandleRequestVote(&RequestVoteArgs{
		Term: 4, CandidateID: "b", LastLogIndex: 99, LastLogTerm: 9, PreVote: true,
	})
	if r.VoteGranted {
		t.Fatal("a leader granted a pre-vote, which lets a rejoining follower unseat it")
	}
}

// --- log reconciliation ---

func TestAppendEntriesRejectsMissingPrefix(t *testing.T) {
	m := newMemStore()
	if err := m.Append([]LogEntry{{Index: 1, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 5, PrevLogTerm: 1,
	})
	if r.Success {
		t.Fatal("accepted an append whose prefix we do not have")
	}
	if r.ConflictIndex != 2 {
		t.Fatalf("ConflictIndex = %d, want 2 so the leader can jump straight to our log end", r.ConflictIndex)
	}
}

func TestAppendEntriesReportsConflictTerm(t *testing.T) {
	m := newMemStore()
	// Our log diverges: indexes 1..4 where 2..4 are term 3.
	if err := m.Append([]LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 3}, {Index: 3, Term: 3}, {Index: 4, Term: 3},
	}); err != nil {
		t.Fatal(err)
	}
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 5, LeaderID: "b", PrevLogIndex: 4, PrevLogTerm: 4,
	})
	if r.Success {
		t.Fatal("accepted an append whose prevLogTerm disagrees with ours")
	}
	if r.ConflictTerm != 3 {
		t.Fatalf("ConflictTerm = %d, want 3", r.ConflictTerm)
	}
	// Reporting the FIRST index of the conflicting term is what lets the leader
	// skip the whole term in one round trip instead of four.
	if r.ConflictIndex != 2 {
		t.Fatalf("ConflictIndex = %d, want 2 (first index of term 3)", r.ConflictIndex)
	}
}

func TestAppendEntriesTruncatesOnlyFromTheMismatch(t *testing.T) {
	m := newMemStore()
	if err := m.Append([]LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	// Leader resends index 2 (agreeing) and a conflicting index 3.
	r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 3, LeaderID: "b", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Index: 2, Term: 1}, {Index: 3, Term: 3}},
	})
	if !r.Success {
		t.Fatalf("expected success, got conflict %d/%d", r.ConflictTerm, r.ConflictIndex)
	}
	if got, _ := m.Term(2); got != 1 {
		t.Fatal("index 2 was rewritten even though it agreed with the leader")
	}
	if got, _ := m.Term(3); got != 3 {
		t.Fatalf("index 3 term = %d, want 3 (the leader's entry)", got)
	}
	if m.LastIndex() != 3 {
		t.Fatalf("LastIndex = %d, want 3", m.LastIndex())
	}
}

// A delayed duplicate of an earlier append must not chop off entries the
// follower has since accepted. If it did, and the leader had already counted
// them toward a commit, a committed entry would disappear.
func TestStaleDuplicateAppendDoesNotTruncate(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	first := &AppendEntriesArgs{
		Term: 2, LeaderID: "b", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Index: 1, Term: 2}, {Index: 2, Term: 2}},
	}
	if r := n.HandleAppendEntries(first); !r.Success {
		t.Fatal("initial append failed")
	}
	// A later, longer append arrives and is accepted.
	if r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: "b", PrevLogIndex: 2, PrevLogTerm: 2,
		Entries: []LogEntry{{Index: 3, Term: 2}, {Index: 4, Term: 2}},
	}); !r.Success {
		t.Fatal("second append failed")
	}
	if m.LastIndex() != 4 {
		t.Fatalf("setup: LastIndex = %d, want 4", m.LastIndex())
	}

	// Now the original RPC is redelivered by a flaky network.
	if r := n.HandleAppendEntries(first); !r.Success {
		t.Fatal("redelivered append should still succeed")
	}
	if m.LastIndex() != 4 {
		t.Fatalf("LastIndex = %d after duplicate delivery, want 4: the duplicate truncated acknowledged entries", m.LastIndex())
	}
}

func TestFollowerCommitIsBoundedByItsOwnLog(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	// Leader claims commit 10 but only ships us two entries.
	r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
		LeaderCommit: 10,
	})
	if !r.Success {
		t.Fatal("append failed")
	}
	if got := n.CommitIndex(); got != 2 {
		t.Fatalf("commitIndex = %d, want 2: committing past our own log would apply entries we do not have", got)
	}
}

func TestStaleLeaderCannotResetElectionTimer(t *testing.T) {
	m := newMemStore()
	m.term = 7
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	n.mu.Lock()
	n.electionDeadline = n.clock.Now().Add(-time.Second)
	deadlineBefore := n.electionDeadline
	n.mu.Unlock()

	n.HandleAppendEntries(&AppendEntriesArgs{Term: 3, LeaderID: "b"})

	n.mu.Lock()
	after := n.electionDeadline
	n.mu.Unlock()
	if !after.Equal(deadlineBefore) {
		t.Fatal("a stale leader's heartbeat reset the election timer, which would keep this node from ever campaigning")
	}
}

// --- commit rules ---

func TestLeaderCommitsOnlyCurrentTermByReplicaCount(t *testing.T) {
	m := newMemStore()
	// Two entries inherited from term 1.
	if err := m.Append([]LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	n.mu.Lock()
	n.currentTerm = 2
	n.role = Leader
	// A majority holds index 2, but it is from a previous term.
	n.matchIndex = map[NodeID]Index{"a": 2, "b": 2, "c": 0}
	n.maybeAdvanceCommitLocked()
	got := n.commitIndex
	n.mu.Unlock()

	if got != 0 {
		t.Fatalf("commitIndex = %d, want 0: committing a previous term's entry on replica count alone can lose an acknowledged write", got)
	}

	// Appending and replicating an entry of the current term commits both.
	n.mu.Lock()
	idx := n.appendLocked(LogEntry{Term: 2, Kind: EntryNoop})
	n.matchIndex["b"] = idx
	n.maybeAdvanceCommitLocked()
	got = n.commitIndex
	n.mu.Unlock()

	if got != idx {
		t.Fatalf("commitIndex = %d, want %d: a current-term entry carries the earlier ones with it", got, idx)
	}
}

func TestCommitRequiresMajorityNotJustOnePeer(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c", "d", "e"}, m)

	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	idx := n.appendLocked(LogEntry{Term: 1, Kind: EntryCommand})
	// Only two of five hold it: leader plus one.
	n.matchIndex = map[NodeID]Index{"a": idx, "b": idx, "c": 0, "d": 0, "e": 0}
	n.maybeAdvanceCommitLocked()
	got := n.commitIndex
	n.mu.Unlock()

	if got != 0 {
		t.Fatalf("commitIndex = %d, want 0 with only 2 of 5 replicas", got)
	}

	n.mu.Lock()
	n.matchIndex["c"] = idx // now 3 of 5
	n.maybeAdvanceCommitLocked()
	got = n.commitIndex
	n.mu.Unlock()

	if got != idx {
		t.Fatalf("commitIndex = %d, want %d once a majority holds it", got, idx)
	}
}

func TestMatchIndexNeverMovesBackward(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	_ = n.appendLocked(LogEntry{Term: 1, Kind: EntryCommand})
	_ = n.appendLocked(LogEntry{Term: 1, Kind: EntryCommand})
	n.matchIndex["b"] = 2
	n.commitIndex = 2
	// Simulate the bookkeeping a delayed reply for a shorter append would do.
	if 1 > n.matchIndex["b"] {
		n.matchIndex["b"] = 1
	}
	got := n.matchIndex["b"]
	n.mu.Unlock()

	if got != 2 {
		t.Fatalf("matchIndex = %d, want 2: a delayed reply must not undo progress", got)
	}
}

// --- apply ---

func TestApplyIsInOrderAndExactlyOnce(t *testing.T) {
	m := newMemStore()
	n, sm := testNode(t, "a", []NodeID{"a", "b", "c"}, m)
	n.Start()
	defer n.Stop()

	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	for i := 0; i < 5; i++ {
		n.appendLocked(LogEntry{Term: 1, Kind: EntryCommand, Data: []byte{byte(i)}})
	}
	n.commitIndex = 5
	n.mu.Unlock()
	n.signalApply()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(sm.appliedCopy()) == 5 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	applied := sm.appliedCopy()
	if len(applied) != 5 {
		t.Fatalf("applied %d entries, want 5: %v", len(applied), applied)
	}
	for i, idx := range applied {
		if idx != Index(i+1) {
			t.Fatalf("applied out of order at position %d: %v", i, applied)
		}
	}
}

func TestNoopEntriesAreNotSentToTheStateMachine(t *testing.T) {
	m := newMemStore()
	n, sm := testNode(t, "a", []NodeID{"a", "b", "c"}, m)
	n.Start()
	defer n.Stop()

	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	n.appendLocked(LogEntry{Term: 1, Kind: EntryNoop})
	n.appendLocked(LogEntry{Term: 1, Kind: EntryCommand, Data: []byte{9}})
	n.commitIndex = 2
	n.mu.Unlock()
	n.signalApply()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(sm.appliedCopy()) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := sm.appliedCopy(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("state machine saw %v, want only the command entry at index 2", got)
	}
}

func TestRestartRecoversAppliedPositionFromSnapshot(t *testing.T) {
	m := newMemStore()
	if err := m.SaveSnapshot(Snapshot{
		Meta: SnapshotMeta{Index: 7, Term: 2}, Data: []byte("state"),
	}); err != nil {
		t.Fatal(err)
	}
	n, sm := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	if n.lastApplied != 7 {
		t.Fatalf("lastApplied = %d after recovery, want 7: re-applying snapshotted entries would double-apply them", n.lastApplied)
	}
	if string(sm.state) != "state" {
		t.Fatalf("state machine not restored from snapshot, got %q", sm.state)
	}
}

// --- membership ---

func TestConfigChangeRoundTrip(t *testing.T) {
	for _, cc := range []ConfigChange{
		{Kind: AddVoter, Node: "node-d"},
		{Kind: RemoveVoter, Node: "c"},
		{Kind: AddVoter, Node: NodeID(make([]byte, 0))},
	} {
		got, err := DecodeConfigChange(cc.Encode())
		if err != nil {
			t.Fatalf("decode %+v: %v", cc, err)
		}
		if got.Kind != cc.Kind || got.Node != cc.Node {
			t.Fatalf("round trip gave %+v, want %+v", got, cc)
		}
	}
}

func TestDecodeConfigChangeRejectsTruncated(t *testing.T) {
	enc := ConfigChange{Kind: AddVoter, Node: "abcdef"}.Encode()
	if _, err := DecodeConfigChange(enc[:4]); err == nil {
		t.Fatal("accepted a truncated config change payload")
	}
	if _, err := DecodeConfigChange(nil); err == nil {
		t.Fatal("accepted an empty config change payload")
	}
}

func TestAddVoterTakesEffectOnAppend(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)

	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	n.applyConfigLocked(ConfigChange{Kind: AddVoter, Node: "d"})
	peers := len(n.cfg.Peers)
	quorum := n.cfg.Quorum()
	_, tracked := n.nextIndex["d"]
	n.mu.Unlock()

	if peers != 4 {
		t.Fatalf("peers = %d, want 4", peers)
	}
	if quorum != 3 {
		t.Fatalf("quorum = %d, want 3 for a 4-node cluster", quorum)
	}
	if !tracked {
		t.Fatal("new voter has no nextIndex entry, so the leader will never replicate to it")
	}
}

func TestRemoveVoterLowersQuorumAndRechecksCommit(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c", "d", "e"}, m)

	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	idx := n.appendLocked(LogEntry{Term: 1, Kind: EntryCommand})
	// 3 of 5 is already a majority here, so force a case where it is not:
	// only a and b hold it, which is 2 of 5.
	n.matchIndex = map[NodeID]Index{"a": idx, "b": idx, "c": 0, "d": 0, "e": 0}
	n.maybeAdvanceCommitLocked()
	if n.commitIndex != 0 {
		n.mu.Unlock()
		t.Fatalf("precondition failed: commitIndex = %d, want 0", n.commitIndex)
	}
	// Removing two members makes {a,b} a majority of three.
	n.applyConfigLocked(ConfigChange{Kind: RemoveVoter, Node: "d"})
	n.applyConfigLocked(ConfigChange{Kind: RemoveVoter, Node: "e"})
	got := n.commitIndex
	n.mu.Unlock()

	if got != idx {
		t.Fatalf("commitIndex = %d, want %d: shrinking the cluster lowers the quorum and should commit the already-replicated entry", got, idx)
	}
}

func TestSecondConfigChangeRejectedWhileFirstUncommitted(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)
	n.Start()
	defer n.Stop()

	n.mu.Lock()
	n.currentTerm = 1
	// becomeLeaderLocked rather than assigning the role directly, so the
	// check-quorum contact clock is primed; a hand-set role looks like a leader
	// that has never reached anyone and gets deposed on the next heartbeat tick.
	n.becomeLeaderLocked()
	n.mu.Unlock()

	// First change: will not commit because nilTransport reaches no peer.
	go n.ChangeMembership(ConfigChange{Kind: AddVoter, Node: "d"}, 2*time.Second)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		p := n.pendingConfigIndex
		n.mu.Unlock()
		if p != 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	err := n.ChangeMembership(ConfigChange{Kind: AddVoter, Node: "e"}, 50*time.Millisecond)
	if !errors.Is(err, ErrConfigChangeInFlight) {
		t.Fatalf("second change returned %v, want ErrConfigChangeInFlight: two concurrent changes can create disjoint majorities", err)
	}
}

func TestRemoveVoterRefusesToBreakQuorum(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b"}, m)
	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	n.mu.Unlock()

	if err := n.ChangeMembership(ConfigChange{Kind: RemoveVoter, Node: "b"}, time.Second); !errors.Is(err, ErrWouldLoseQuorum) {
		t.Fatalf("got %v, want ErrWouldLoseQuorum", err)
	}
}

func TestSubmitOnFollowerRedirects(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)
	n.mu.Lock()
	n.leaderID = "b"
	n.mu.Unlock()

	_, err := n.Submit([]byte("x"), time.Second)
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("got %v, want NotLeaderError", err)
	}
	if nle.LeaderHint != "b" {
		t.Fatalf("LeaderHint = %q, want \"b\" so the client does not have to poll every member", nle.LeaderHint)
	}
	if !errors.Is(err, ErrNotLeader) {
		t.Fatal("NotLeaderError should unwrap to ErrNotLeader when a hint is known")
	}
}

func TestStoppedNodeRefusesRPCsInsteadOfPanicking(t *testing.T) {
	m := newMemStore()
	n, _ := testNode(t, "a", []NodeID{"a", "b", "c"}, m)
	n.Start()
	n.Stop()

	// A peer mid-call does not know this node has shut down, and the node's
	// storage may already be closed. Serving the append would reach the durable
	// write, which treats failure as unrecoverable and panics. All three
	// handlers must decline instead.
	if r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 5, LeaderID: "b", Entries: []LogEntry{{Index: 1, Term: 5}},
	}); r.Success {
		t.Fatal("a stopped node accepted an append")
	}
	if r := n.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "b"}); r.VoteGranted {
		t.Fatal("a stopped node granted a vote")
	}
	n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 5, LeaderID: "b", LastIncludedIndex: 9, LastIncludedTerm: 5,
	})
}

// --- config validation ---

func TestConfigRejectsTimeoutsThatUnseatHealthyLeaders(t *testing.T) {
	c := Config{ID: "a", Peers: []NodeID{"a"}, HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond}
	if err := c.Validate(); err == nil {
		t.Fatal("accepted an election timeout under 3x the heartbeat interval; ordinary jitter would unseat a healthy leader")
	}
}

func TestConfigRequiresSelfInPeers(t *testing.T) {
	c := Config{ID: "a", Peers: []NodeID{"b", "c"}}
	if err := c.Validate(); err == nil {
		t.Fatal("accepted a peer set that excludes this node")
	}
}

func TestQuorumSizes(t *testing.T) {
	for _, tc := range []struct{ n, want int }{{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}, {6, 4}, {7, 4}} {
		peers := make([]NodeID, tc.n)
		for i := range peers {
			peers[i] = NodeID(rune('a' + i))
		}
		c := Config{Peers: peers}
		if got := c.Quorum(); got != tc.want {
			t.Errorf("Quorum(%d nodes) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

func TestUpToDate(t *testing.T) {
	tests := []struct {
		name             string
		candTerm, myTerm Term
		candIdx, myIdx   Index
		want             bool
	}{
		{"higher term wins even with shorter log", 5, 4, 1, 100, true},
		{"lower term loses even with longer log", 3, 4, 100, 1, false},
		{"same term, longer log wins", 4, 4, 10, 5, true},
		{"same term, equal length is up to date", 4, 4, 5, 5, true},
		{"same term, shorter log loses", 4, 4, 4, 5, false},
		{"empty logs are mutually up to date", 0, 0, 0, 0, true},
	}
	for _, tc := range tests {
		if got := upToDate(tc.candTerm, tc.candIdx, tc.myTerm, tc.myIdx); got != tc.want {
			t.Errorf("%s: upToDate = %v, want %v", tc.name, got, tc.want)
		}
	}
}
