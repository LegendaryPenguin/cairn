package raft

import (
	"encoding/binary"
	"errors"
	"time"
)

// Membership changes use the one-server-at-a-time rule from the Raft
// dissertation rather than joint consensus.
//
// The reason that rule is safe is worth stating, because it is the whole
// argument: any majority of a cluster of size N and any majority of a cluster
// of size N±1 must share at least one member. That overlap means the old and
// new configurations cannot independently elect two leaders in the same term,
// so the cluster can switch configurations without the two-phase handoff joint
// consensus needs. Change two members at once and the guarantee is gone — a
// 3-node cluster going to 5 can have {a,b} commit under the old config while
// {c,d,e} commit under the new one, with no shared member to notice.
//
// Two further rules this implements:
//
//   - A configuration takes effect on each node the moment the EntryConfig is
//     APPENDED, not when it commits. A leader that waits for commit would be
//     counting votes under a configuration a majority may have already left.
//   - Only one uncommitted configuration change may be in flight. The second
//     change is rejected until the first commits, because applying a change on
//     top of an uncommitted one can produce the same disjoint-majority problem.

// ConfigChangeKind distinguishes adding from removing a voter.
type ConfigChangeKind uint8

const (
	AddVoter ConfigChangeKind = iota + 1
	RemoveVoter
)

// ConfigChange is the payload of an EntryConfig log entry.
type ConfigChange struct {
	Kind ConfigChangeKind
	Node NodeID
}

var (
	// ErrConfigChangeInFlight means an earlier membership change has not yet
	// committed. Retry once it has.
	ErrConfigChangeInFlight = errors.New("cairn: configuration change already in flight")

	// ErrAlreadyMember / ErrNotMember mean the change would be a no-op.
	ErrAlreadyMember = errors.New("cairn: node is already a voter")
	ErrNotMember     = errors.New("cairn: node is not a voter")

	// ErrWouldLoseQuorum means the removal would leave a cluster that cannot
	// elect a leader.
	ErrWouldLoseQuorum = errors.New("cairn: removal would leave no viable quorum")
)

// Encode serializes a ConfigChange. Hand-rolled rather than gob so the on-disk
// log format does not change when the Go version does.
func (c ConfigChange) Encode() []byte {
	b := make([]byte, 0, 3+len(c.Node))
	b = append(b, byte(c.Kind))
	b = binary.LittleEndian.AppendUint16(b, uint16(len(c.Node)))
	return append(b, c.Node...)
}

// DecodeConfigChange parses a ConfigChange from an EntryConfig payload.
func DecodeConfigChange(b []byte) (ConfigChange, error) {
	if len(b) < 3 {
		return ConfigChange{}, errors.New("cairn: config change payload too short")
	}
	n := int(binary.LittleEndian.Uint16(b[1:3]))
	if len(b) < 3+n {
		return ConfigChange{}, errors.New("cairn: config change payload truncated")
	}
	return ConfigChange{Kind: ConfigChangeKind(b[0]), Node: NodeID(b[3 : 3+n])}, nil
}

// ChangeMembership appends a configuration change and waits for it to commit.
// Must be called on the leader.
func (n *Node) ChangeMembership(cc ConfigChange, timeout time.Duration) error {
	n.mu.Lock()

	if n.role != Leader {
		hint := n.leaderID
		n.mu.Unlock()
		return &NotLeaderError{LeaderHint: hint}
	}

	// One at a time. pendingConfigIndex is cleared when the change commits.
	if n.pendingConfigIndex != 0 && n.pendingConfigIndex > n.commitIndex {
		n.mu.Unlock()
		return ErrConfigChangeInFlight
	}

	isMember := false
	for _, p := range n.cfg.Peers {
		if p == cc.Node {
			isMember = true
			break
		}
	}

	switch cc.Kind {
	case AddVoter:
		if isMember {
			n.mu.Unlock()
			return ErrAlreadyMember
		}
	case RemoveVoter:
		if !isMember {
			n.mu.Unlock()
			return ErrNotMember
		}
		// Refuse to shrink below a cluster that can still elect. Removing the
		// second-to-last member leaves a single node that is trivially its own
		// majority, which is legal but almost always a mistake, so the floor
		// here is two remaining voters.
		if len(n.cfg.Peers)-1 < 2 {
			n.mu.Unlock()
			return ErrWouldLoseQuorum
		}
	default:
		n.mu.Unlock()
		return errors.New("cairn: unknown config change kind")
	}

	idx := n.appendLocked(LogEntry{
		Term: n.currentTerm,
		Kind: EntryConfig,
		Data: cc.Encode(),
	})
	n.pendingConfigIndex = idx

	// Apply to the live configuration immediately on append, per the rule above.
	n.applyConfigLocked(cc)

	p := &pendingWrite{term: n.currentTerm, result: make(chan writeResult, 1)}
	n.pending[idx] = p
	n.mu.Unlock()

	n.signalReplicate()

	timer := n.clock.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-p.result:
		return r.err
	case <-timer.C():
		n.mu.Lock()
		delete(n.pending, idx)
		n.mu.Unlock()
		return ErrTimeout
	case <-n.stopCh:
		return ErrShutdown
	}
}

// applyConfigLocked mutates the live peer set.
//
// A removed node is not told to stop. It will simply stop hearing from the
// leader, time out, campaign, and fail to win because the remaining majority no
// longer counts it. That is noisy but safe; the alternative, having the leader
// shut a peer down, puts a liveness decision in the hands of a node that may
// itself be about to lose leadership.
func (n *Node) applyConfigLocked(cc ConfigChange) {
	switch cc.Kind {
	case AddVoter:
		for _, p := range n.cfg.Peers {
			if p == cc.Node {
				return
			}
		}
		n.cfg.Peers = append(n.cfg.Peers, cc.Node)
		if n.role == Leader {
			// A new voter starts with an optimistic nextIndex and an empty
			// matchIndex, exactly like a follower after an election: the
			// consistency check will walk it back to wherever its log actually
			// agrees, or trigger a snapshot if it has nothing.
			n.nextIndex[cc.Node] = n.store.LastIndex() + 1
			n.matchIndex[cc.Node] = 0
		}
	case RemoveVoter:
		kept := n.cfg.Peers[:0]
		for _, p := range n.cfg.Peers {
			if p != cc.Node {
				kept = append(kept, p)
			}
		}
		n.cfg.Peers = kept
		delete(n.nextIndex, cc.Node)
		delete(n.matchIndex, cc.Node)

		// Shrinking the cluster lowers the quorum, which can retroactively make
		// an already-replicated entry committed.
		if n.role == Leader {
			n.maybeAdvanceCommitLocked()
		}
	}
}

// Peers returns the current voter set.
func (n *Node) Peers() []NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]NodeID, len(n.cfg.Peers))
	copy(out, n.cfg.Peers)
	return out
}
