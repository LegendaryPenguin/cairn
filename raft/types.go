package raft

import "fmt"

// Term is a Raft logical clock. Terms increase monotonically and every node
// rejects any message carrying a term lower than the one it has already seen.
type Term uint64

// Index is a position in the replicated log. Indexes are 1-based so that zero
// can mean "no entry", which matters for prevLogIndex on the first append.
type Index uint64

// Role is the node's current position in the Raft state machine.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// NodeID identifies a cluster member. It is stable across restarts because the
// vote record on disk refers to it.
type NodeID string

// EntryKind distinguishes ordinary state-machine commands from entries the
// consensus layer appends for its own purposes.
type EntryKind uint8

const (
	// EntryCommand carries an opaque payload that the application state
	// machine interprets. Raft never inspects it.
	EntryCommand EntryKind = iota

	// EntryNoop is appended by a new leader so it can discover its true
	// commit index without waiting for client traffic. See the commit rule in
	// commitIndexFrom: a leader may not advance commitIndex on entries from
	// previous terms by counting replicas alone, so it needs one entry of its
	// own term to make progress.
	EntryNoop

	// EntryConfig carries a cluster membership change.
	EntryConfig
)

// LogEntry is one record in the replicated log. Once an entry at a given index
// is committed, no node may ever hold a different entry at that index.
type LogEntry struct {
	Index Index
	Term  Term
	Kind  EntryKind
	Data  []byte
}

func (e LogEntry) String() string {
	return fmt.Sprintf("{i=%d t=%d k=%d len=%d}", e.Index, e.Term, e.Kind, len(e.Data))
}

// RequestVoteArgs is the candidate's solicitation. LastLogIndex and LastLogTerm
// exist so a voter can refuse a candidate whose log is behind its own; without
// that check a stale candidate could win and erase committed entries.
type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term

	// PreVote marks a probe that must not cause the voter to advance its own
	// term. A partitioned node that keeps incrementing its term and forcing
	// re-elections is disruptive on rejoin, so it must first confirm it could
	// plausibly win.
	PreVote bool
}

type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

// AppendEntriesArgs doubles as the heartbeat and the replication RPC. An empty
// Entries slice is a heartbeat; the consistency check on PrevLogIndex and
// PrevLogTerm runs either way.
type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index
}

// AppendEntriesReply reports whether the consistency check passed. The hint
// fields let the leader skip a whole conflicting term in one round trip rather
// than walking nextIndex back one entry at a time.
type AppendEntriesReply struct {
	Term    Term
	Success bool

	// ConflictTerm is the term of the follower's entry at PrevLogIndex when
	// the check failed because the terms disagreed.
	ConflictTerm Term

	// ConflictIndex is the first index the follower holds for ConflictTerm,
	// or the follower's log length when it simply has no entry there.
	ConflictIndex Index
}

// InstallSnapshotArgs transfers state to a follower that has fallen so far
// behind that the leader has already compacted away the entries it needs.
type InstallSnapshotArgs struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Offset            uint64
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term Term
}

// upToDate implements Raft's log-freshness comparison: a longer term wins, and
// within the same term a longer log wins.
func upToDate(candLastTerm Term, candLastIndex Index, myLastTerm Term, myLastIndex Index) bool {
	if candLastTerm != myLastTerm {
		return candLastTerm > myLastTerm
	}
	return candLastIndex >= myLastIndex
}
