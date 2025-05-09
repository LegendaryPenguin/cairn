package raft

import "errors"

var (
	// ErrCompacted means the requested index is at or below the snapshot point
	// and the entry no longer exists. A leader seeing this for a follower's
	// nextIndex must send a snapshot instead of an append.
	ErrCompacted = errors.New("cairn: log index compacted into snapshot")

	// ErrUnavailable means the requested index is past the end of the log.
	ErrUnavailable = errors.New("cairn: log index not yet written")

	// ErrNotLeader is returned to a client that submitted a write to a node
	// that is not the leader. LeaderHint carries the redirect target.
	ErrNotLeader = errors.New("cairn: not leader")

	// ErrNoLeader means this node does not currently know of a leader, so it
	// cannot even redirect. Distinct from ErrNotLeader because the client's
	// correct response differs: retry here, redirect there.
	ErrNoLeader = errors.New("cairn: no known leader")

	// ErrLeadershipLost means an entry was appended locally but the node lost
	// leadership before it committed. The write's outcome is genuinely unknown
	// to the client: it may still commit under the next leader.
	ErrLeadershipLost = errors.New("cairn: leadership lost before commit")

	// ErrShutdown means the node is closing.
	ErrShutdown = errors.New("cairn: node shut down")

	// ErrTimeout means the operation did not complete within its deadline.
	ErrTimeout = errors.New("cairn: operation timed out")
)

// NotLeaderError carries a redirect hint alongside ErrNotLeader so a client
// does not have to poll every member to find the leader.
type NotLeaderError struct {
	LeaderHint NodeID
}

func (e *NotLeaderError) Error() string {
	if e.LeaderHint == "" {
		return ErrNoLeader.Error()
	}
	return "cairn: not leader, try " + string(e.LeaderHint)
}

func (e *NotLeaderError) Unwrap() error {
	if e.LeaderHint == "" {
		return ErrNoLeader
	}
	return ErrNotLeader
}
