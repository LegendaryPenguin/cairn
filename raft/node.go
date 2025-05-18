package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Node is one Raft server.
//
// Concurrency model: a single mutex guards every field in the "guarded by mu"
// block below. Four long-lived goroutines run against it — the election timer,
// the leader's replication loop, the apply loop, and the snapshot trigger — and
// each takes mu for the duration of a state decision, then releases it before
// doing I/O.
//
// The rule that keeps this tractable: no RPC and no disk write happens while mu
// is held, except the two Raft requires to be synchronous (the vote fsync in
// HandleRequestVote and the log fsync in appendLocked). Everything else copies
// what it needs under the lock, releases, and performs the I/O outside. That is
// why several methods below look like they re-check state after reacquiring —
// the world can move while an RPC is in flight, and a reply that arrives after
// a term change must be discarded rather than applied.
type Node struct {
	cfg   Config
	store Storage
	sm    StateMachine
	trans Transport
	clock Clock
	rng   *rand.Rand

	mu sync.Mutex

	// --- persistent state (durable before any dependent reply) ---
	currentTerm Term
	votedFor    NodeID

	// --- volatile state, all nodes ---
	role        Role
	commitIndex Index
	lastApplied Index
	leaderID    NodeID

	// electionDeadline is when this node will start a campaign if it has not
	// heard from a leader. Reset on every valid AppendEntries and on granting
	// a vote.
	electionDeadline time.Time

	// --- volatile state, leader only ---
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// pending maps a log index to the client waiting on it. A leader answers a
	// write only once that exact index commits AND the entry there still has
	// the term the leader appended it with — if it does not, the entry was
	// overwritten by a later leader and the client's write did not happen.
	pending map[Index]*pendingWrite

	// readIndexWaiters hold linearizable reads parked until the leader has
	// confirmed it is still the leader. See readindex.go.
	readIndexWaiters []*readRequest

	// snapshot bookkeeping, so the trigger does not re-snapshot the same state
	lastSnapshotIndex Index

	// pendingConfigIndex is the log index of the most recent EntryConfig this
	// node has appended. A second membership change is refused until this one
	// commits; see membership.go for why one-at-a-time is the safety condition.
	pendingConfigIndex Index

	applyCh     chan struct{}
	replicateCh chan struct{}
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup

	// observer, when non-nil, receives state transitions. Tests use it to
	// assert invariants like "at most one leader per term" without polling.
	observer Observer
}

type pendingWrite struct {
	term   Term
	result chan writeResult
}

type writeResult struct {
	value any
	err   error
}

// Observer receives notifications of state changes. Implementations must not
// block and must not call back into the Node.
type Observer interface {
	OnRoleChange(id NodeID, term Term, role Role)
	OnCommit(id NodeID, index Index)
	OnApply(id NodeID, index Index)
}

// NewNode constructs a node and recovers persistent state from storage. It does
// not start any goroutine; call Start.
func NewNode(cfg Config, store Storage, sm StateMachine, trans Transport) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	n := &Node{
		cfg:         cfg,
		store:       store,
		sm:          sm,
		trans:       trans,
		clock:       cfg.Clock,
		role:        Follower,
		nextIndex:   make(map[NodeID]Index),
		matchIndex:  make(map[NodeID]Index),
		pending:     make(map[Index]*pendingWrite),
		applyCh:     make(chan struct{}, 1),
		replicateCh: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
	if n.clock == nil {
		n.clock = realClock{}
	}
	// Seed per node so a cluster started in one process does not have every
	// member drawing identical election timeouts, which would defeat the
	// randomization entirely.
	n.rng = rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(len(cfg.ID))*2654435761))

	term, vote, err := store.LoadState()
	if err != nil {
		return nil, err
	}
	n.currentTerm, n.votedFor = term, vote

	// A node that restarts must not re-apply entries the snapshot already
	// covers, so lastApplied starts at the snapshot point rather than zero.
	snap, err := store.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	if snap.Meta.Index > 0 {
		if err := sm.Restore(snap.Data); err != nil {
			return nil, err
		}
		n.lastApplied = snap.Meta.Index
		n.commitIndex = snap.Meta.Index
		n.lastSnapshotIndex = snap.Meta.Index
	}

	return n, nil
}

// SetObserver installs an observer. Call before Start.
func (n *Node) SetObserver(o Observer) { n.observer = o }

// Start launches the node's goroutines.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	n.wg.Add(3)
	go n.runElectionTimer()
	go n.runLeader()
	go n.runApply()
}

// Stop shuts the node down and waits for its goroutines. Safe to call twice.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.wg.Wait()

	// Fail any client still waiting. Their writes may or may not have
	// committed; ErrShutdown says the outcome is unknown rather than failed.
	n.mu.Lock()
	for i, p := range n.pending {
		p.result <- writeResult{err: ErrShutdown}
		delete(n.pending, i)
	}
	for _, r := range n.readIndexWaiters {
		r.done <- ErrShutdown
	}
	n.readIndexWaiters = nil
	n.mu.Unlock()
}

// Role reports the current role. For tests and status endpoints.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Term reports the current term.
func (n *Node) Term() Term {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// Leader reports the last known leader, which may be stale.
func (n *Node) Leader() (NodeID, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID, n.leaderID != ""
}

// CommitIndex reports the highest index known committed.
func (n *Node) CommitIndex() Index {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// ID reports this node's identity.
func (n *Node) ID() NodeID { return n.cfg.ID }

// --- term and role transitions, all requiring mu ---

// stepDownLocked moves to a strictly greater term as a follower. Every RPC
// path calls this on seeing a higher term, which is the single mechanism that
// makes stale leaders and candidates give up.
//
// The vote is cleared because votedFor is scoped to a term; carrying it forward
// would silently deny this node's vote in the new term.
func (n *Node) stepDownLocked(term Term) {
	if term < n.currentTerm {
		return
	}
	wasLeader := n.role == Leader
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.role = Follower
	if wasLeader {
		n.failPendingLocked(ErrLeadershipLost)
	}
	n.notifyRole()
}

// becomeCandidateLocked increments the term and votes for self.
func (n *Node) becomeCandidateLocked() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	n.resetElectionTimerLocked()
	n.notifyRole()
}

// becomeLeaderLocked initializes leader state after winning an election.
//
// nextIndex starts one past the leader's last entry — an optimistic guess that
// the follower is fully caught up. The AppendEntries consistency check is what
// corrects it, walking back until the logs agree. matchIndex starts at zero
// because the leader has confirmed nothing yet, and starting it optimistically
// would let it commit entries no follower actually has.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	last := n.store.LastIndex()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	n.notifyRole()

	// Append a no-op of the new term. Without it a leader that inherits
	// uncommitted entries from a previous term cannot commit them — the
	// current-term commit rule forbids counting replicas on old-term entries —
	// so those entries and any client waiting on them would stall until
	// unrelated traffic arrived.
	n.appendLocked(LogEntry{Term: n.currentTerm, Kind: EntryNoop})
}

// appendLocked writes one entry at the next index and fsyncs it. Returns the
// index assigned. The fsync happens with mu held, which is the one place the
// no-I/O-under-lock rule is deliberately broken: the index assignment and the
// durability of that index must not interleave with another append.
func (n *Node) appendLocked(e LogEntry) Index {
	e.Index = n.store.LastIndex() + 1
	if err := n.store.Append([]LogEntry{e}); err != nil {
		// A log that cannot be written durably cannot be reasoned about. The
		// node must not continue acknowledging anything.
		panic("cairn: durable log append failed: " + err.Error())
	}
	n.matchIndex[n.cfg.ID] = e.Index
	return e.Index
}

func (n *Node) failPendingLocked(err error) {
	for i, p := range n.pending {
		p.result <- writeResult{err: err}
		delete(n.pending, i)
	}
	for _, r := range n.readIndexWaiters {
		r.done <- err
	}
	n.readIndexWaiters = nil
}

func (n *Node) resetElectionTimerLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	d := n.cfg.ElectionTimeoutMin
	if span > 0 {
		d += time.Duration(n.rng.Int63n(int64(span)))
	}
	n.electionDeadline = n.clock.Now().Add(d)
}

// persistLocked flushes term and vote. Callers must do this before sending any
// reply that depends on the new value.
func (n *Node) persistLocked() {
	if err := n.store.SaveState(n.currentTerm, n.votedFor); err != nil {
		panic("cairn: durable state save failed: " + err.Error())
	}
}

func (n *Node) notifyRole() {
	if n.observer != nil {
		n.observer.OnRoleChange(n.cfg.ID, n.currentTerm, n.role)
	}
}

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

// realClock is the default Clock. It lives here rather than in package clock so
// that package raft has no dependency on its own helper package.
type realClock struct{}

func (realClock) Now() time.Time                 { return time.Now() }
func (realClock) Sleep(d time.Duration)          { time.Sleep(d) }
func (realClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
