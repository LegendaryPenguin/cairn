// Package server exposes the client-facing key-value API on top of the
// consensus layer.
//
// Its job is the translation that the Raft layer deliberately does not do:
// turning a client call into a replicated command, deciding what a read is
// allowed to observe, and telling a client where the leader is when it has
// guessed wrong.
package server

import (
	"errors"
	"time"

	"github.com/LegendaryPenguin/cairn/kv"
	"github.com/LegendaryPenguin/cairn/raft"
)

// ReadMode selects what guarantee a read carries.
type ReadMode uint8

const (
	// Linearizable routes the read through ReadIndex: the leader confirms with a
	// quorum that it is still the leader, then waits for its state machine to
	// catch up to the confirmed commit index. Costs one round of heartbeats and
	// no disk writes.
	Linearizable ReadMode = iota

	// Stale reads local applied state with no coordination. Fast, and wrong
	// whenever this node has been partitioned away and does not know it yet.
	// Exposed because "wrong but fast" is a legitimate choice for some callers,
	// and because having both makes the cost of correctness measurable.
	Stale
)

// Server binds a Raft node to a KV state machine.
type Server struct {
	node  *raft.Node
	store *kv.Store

	// opTimeout bounds a single client operation: the replication round trip for
	// a write, or the leadership confirmation for a linearizable read.
	opTimeout time.Duration
}

func New(node *raft.Node, store *kv.Store, opTimeout time.Duration) *Server {
	if opTimeout <= 0 {
		opTimeout = 2 * time.Second
	}
	return &Server{node: node, store: store, opTimeout: opTimeout}
}

// Put replicates a write and returns once it has committed and applied.
func (s *Server) Put(clientID string, seq uint64, key string, value []byte) (*kv.Result, error) {
	return s.submit(kv.Command{
		Op: kv.OpPut, ClientID: clientID, SeqNo: seq, Key: key, Value: value,
	})
}

// Delete replicates a removal.
func (s *Server) Delete(clientID string, seq uint64, key string) (*kv.Result, error) {
	return s.submit(kv.Command{
		Op: kv.OpDel, ClientID: clientID, SeqNo: seq, Key: key,
	})
}

// CompareAndSwap replicates a conditional write. Pass expectExists=false to mean
// "only if the key is absent"; a nil expected value cannot express that on its
// own, because absent and empty are different states.
func (s *Server) CompareAndSwap(clientID string, seq uint64, key string, expected, value []byte, expectExists bool) (*kv.Result, error) {
	return s.submit(kv.Command{
		Op: kv.OpCAS, ClientID: clientID, SeqNo: seq, Key: key,
		Expected: expected, Value: value, ExpectExists: expectExists,
	})
}

// Get reads a key under the requested guarantee.
//
// A Linearizable read still has to run on the leader, because only the leader
// can establish a current commit index. Followers return a redirect rather than
// answering from their own applied state, which would be a stale read wearing a
// linearizable label.
func (s *Server) Get(key string, mode ReadMode) ([]byte, bool, error) {
	if mode == Linearizable {
		if err := s.node.ReadIndex(s.opTimeout); err != nil {
			return nil, false, err
		}
	}
	v, ok := s.store.Get(key)
	return v, ok, nil
}

func (s *Server) submit(cmd kv.Command) (*kv.Result, error) {
	data, err := cmd.Encode()
	if err != nil {
		return nil, err
	}

	out, err := s.node.Submit(data, s.opTimeout)
	if err != nil {
		// A timeout or lost leadership means the outcome is genuinely unknown,
		// not failed: the entry may still commit under a later leader. The
		// client's correct response is to retry with the SAME SeqNo, which the
		// state machine's dedup table turns into a safe no-op if the first
		// attempt did land. Returning the error unwrapped keeps that
		// distinction visible to the caller.
		return nil, err
	}

	res, ok := out.(*kv.Result)
	if !ok {
		return nil, errors.New("cairn: state machine returned an unexpected result type")
	}
	return res, nil
}

// Status reports what this node believes about the cluster. Intended for the
// CLI and for tests; every field may be stale the instant it is read.
type Status struct {
	ID          raft.NodeID
	Role        string
	Term        uint64
	Leader      raft.NodeID
	CommitIndex uint64
	Keys        int
	Peers       []raft.NodeID
}

func (s *Server) Status() Status {
	leader, _ := s.node.Leader()
	return Status{
		ID:          s.node.ID(),
		Role:        s.node.Role().String(),
		Term:        uint64(s.node.Term()),
		Leader:      leader,
		CommitIndex: uint64(s.node.CommitIndex()),
		Keys:        s.store.Len(),
		Peers:       s.node.Peers(),
	}
}

// AddVoter and RemoveVoter expose membership changes. Both must be called on the
// leader and both block until the change commits.
func (s *Server) AddVoter(id raft.NodeID) error {
	return s.node.ChangeMembership(raft.ConfigChange{Kind: raft.AddVoter, Node: id}, s.opTimeout)
}

func (s *Server) RemoveVoter(id raft.NodeID) error {
	return s.node.ChangeMembership(raft.ConfigChange{Kind: raft.RemoveVoter, Node: id}, s.opTimeout)
}
