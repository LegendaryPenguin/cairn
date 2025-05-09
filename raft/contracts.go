package raft

// Storage is the durability contract the consensus layer depends on. Every
// method that returns nil error must have made its effect survive a crash;
// the implementation in package storage does that with an fsync before return.
//
// The split between SaveState and AppendEntries is deliberate. Raft requires
// the term and vote to be durable *before* a vote is granted, and requires log
// entries to be durable before they are acknowledged, but those are two
// different fsync points and conflating them costs a flush on every heartbeat.
type Storage interface {
	// SaveState durably records the term and vote. Called before replying to
	// a RequestVote so a crashed node cannot restart having forgotten a vote
	// it already granted and then vote a second time in the same term.
	SaveState(term Term, votedFor NodeID) error

	// LoadState returns the persisted term and vote, or (0, "") on a fresh node.
	LoadState() (Term, NodeID, error)

	// Append writes entries and fsyncs. Entries must be contiguous and must
	// begin at exactly LastIndex+1.
	//
	// Appending into the middle of the existing log is not permitted: the
	// consensus layer calls TruncateSuffix first when a follower's log diverges,
	// because only that layer can tell an ordinary divergent tail from an
	// attempt to overwrite a committed entry. Storage has no way to make that
	// distinction and must not silently paper over it.
	Append(entries []LogEntry) error

	// TruncateSuffix discards every entry at or after from. Used when a
	// follower's log diverges from the leader's and the conflicting tail has
	// to go. Entries below the snapshot point cannot be truncated.
	TruncateSuffix(from Index) error

	// Entries returns entries in [lo, hi). Returns ErrCompacted if lo is at or
	// below the snapshot point.
	Entries(lo, hi Index) ([]LogEntry, error)

	// Term returns the term of the entry at i. It answers for the snapshot
	// point itself, since that index's term is retained after compaction.
	Term(i Index) (Term, error)

	// FirstIndex is the lowest index still readable from the log: one past the
	// snapshot point.
	FirstIndex() Index

	// LastIndex is the highest index in the log, or the snapshot point when
	// the log is empty.
	LastIndex() Index

	// SaveSnapshot atomically persists a state-machine snapshot and compacts
	// log entries at or below lastIncludedIndex.
	SaveSnapshot(snap Snapshot) error

	// LoadSnapshot returns the newest persisted snapshot. Meta.Index is zero
	// when none exists.
	LoadSnapshot() (Snapshot, error)

	Close() error
}

// SnapshotMeta identifies the log position a snapshot replaces. Both fields are
// needed: the index tells the leader what it may compact, and the term is
// required to answer the AppendEntries consistency check for that index after
// the entry itself is gone.
type SnapshotMeta struct {
	Index Index
	Term  Term
}

// Snapshot is a point-in-time state-machine image plus the log position it
// stands in for.
type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// StateMachine is the deterministic application the log drives.
//
// Determinism is a correctness requirement rather than a style preference: two
// nodes replaying the same log must reach byte-identical Snapshot output, or a
// follower restored from a leader's snapshot diverges silently. In Go the usual
// way to violate this by accident is to serialize a map without sorting its keys.
//
// Linearizable client semantics need one thing from the implementation that the
// consensus layer cannot provide: deduplication. A client whose write times out
// does not know whether it committed, so it retries, and the retry can commit a
// second copy. The state machine must therefore track a per-client sequence
// number and ignore commands at or below the mark — and that table has to be
// inside Snapshot(), or a restored node re-applies the duplicates it had
// already filtered.
type StateMachine interface {
	// Apply is called exactly once per committed EntryCommand, in index order,
	// from a single goroutine. Entries of other kinds are consumed by the
	// consensus layer and never reach here, so implementations do not need to
	// recognize EntryNoop or EntryConfig payloads.
	Apply(e LogEntry) (result any, err error)

	// Snapshot serializes the full state. Called from the apply goroutine, so
	// it does not need its own locking against Apply.
	//
	// The returned bytes are labelled with the caller's lastApplied index. That
	// correspondence is an invariant the type system does not enforce: a
	// snapshot tagged with the wrong index is silent divergence, which is why
	// only the apply goroutine may call this.
	Snapshot() ([]byte, error)

	// Restore replaces all state with a snapshot's contents.
	Restore(data []byte) error
}

// Transport sends RPCs to peers. Implementations must be safe for concurrent
// use and must not block indefinitely; the consensus layer relies on calls
// returning so it can retry on the next tick.
type Transport interface {
	RequestVote(to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(to NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// RPCHandler is the receiving half, implemented by *Node and served by a
// transport listener.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}
