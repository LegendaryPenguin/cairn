package raft

import (
	"sync"
	"time"
)

// readRequest is a linearizable read parked until the leader has proven it is
// still the leader and the state machine has caught up to the read's index.
type readRequest struct {
	index Index
	done  chan error
}

// ReadIndex establishes a log index that is safe to read at, then blocks until
// this node's state machine has applied through it.
//
// Serving a read from a leader's local state without this is the classic stale-read
// bug. A leader that has been partitioned away does not know it: it still thinks
// it is leader, its state machine still holds data, and it will happily answer a
// read with values that a new leader has since overwritten. Nothing in the local
// state reveals the problem, so the only fix is to ask the cluster.
//
// The protocol:
//  1. Confirm an entry from the current term has committed. Until that happens
//     the leader does not reliably know its own commit index, because entries
//     inherited from a previous term cannot be committed by replica count alone.
//     becomeLeaderLocked appends a no-op precisely so this resolves quickly.
//  2. Record commitIndex as the read index.
//  3. Exchange heartbeats with a quorum. If a majority still accepts this node
//     as leader at this term, then no other leader could have committed anything
//     at a higher index during step 2, so the recorded index is current.
//  4. Wait for lastApplied to reach the read index, then read.
//
// This costs one round of heartbeats per read batch and no disk writes, which is
// why it is preferred over committing a no-op entry per read.
func (n *Node) ReadIndex(timeout time.Duration) error {
	n.mu.Lock()
	if n.role != Leader {
		hint := n.leaderID
		n.mu.Unlock()
		return &NotLeaderError{LeaderHint: hint}
	}

	// Step 1. A leader that has not yet committed an entry of its own term has
	// an unknown commit index, so there is no safe index to read at yet.
	commitTerm, err := n.store.Term(n.commitIndex)
	if err != nil || commitTerm != n.currentTerm {
		n.mu.Unlock()
		return ErrNoLeader
	}

	term := n.currentTerm
	readIdx := n.commitIndex // step 2
	n.mu.Unlock()

	// Step 3.
	if err := n.confirmLeadership(term, timeout); err != nil {
		return err
	}

	// Step 4.
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		hint := n.leaderID
		n.mu.Unlock()
		return &NotLeaderError{LeaderHint: hint}
	}
	if n.lastApplied >= readIdx {
		n.mu.Unlock()
		return nil
	}
	r := &readRequest{index: readIdx, done: make(chan error, 1)}
	n.readIndexWaiters = append(n.readIndexWaiters, r)
	n.mu.Unlock()

	n.signalApply()

	timer := n.clock.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-r.done:
		return err
	case <-timer.C():
		return ErrTimeout
	case <-n.stopCh:
		return ErrShutdown
	}
}

// confirmLeadership round-trips a heartbeat to a quorum and reports whether this
// node is still leader for the given term.
//
// The heartbeat carries the real PrevLogIndex/PrevLogTerm so a follower that has
// diverged rejects it rather than silently confirming leadership over a log it
// does not share.
func (n *Node) confirmLeadership(term Term, timeout time.Duration) error {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return &NotLeaderError{LeaderHint: n.leaderID}
	}
	prevIndex := n.store.LastIndex()
	prevTerm, err := n.store.Term(prevIndex)
	if err != nil {
		n.mu.Unlock()
		return err
	}
	commit := n.commitIndex
	quorum := n.cfg.Quorum()
	peers := make([]NodeID, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			peers = append(peers, p)
		}
	}
	n.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: commit,
	}

	var (
		mu      sync.Mutex
		acks    = 1 // self
		decided bool
		ok      bool
		done    = make(chan struct{})
		wg      sync.WaitGroup
	)

	for _, peer := range peers {
		wg.Add(1)
		go func(peer NodeID) {
			defer wg.Done()
			reply, err := n.trans.AppendEntries(peer, args)
			if err != nil {
				return
			}
			if reply.Term > term {
				n.mu.Lock()
				n.stepDownLocked(reply.Term)
				n.persistLocked()
				n.mu.Unlock()
				mu.Lock()
				if !decided {
					decided, ok = true, false
					close(done)
				}
				mu.Unlock()
				return
			}
			// A rejected heartbeat still proves the peer accepts us as leader at
			// this term — it only means its log needs repair — so it counts
			// toward the leadership quorum.
			mu.Lock()
			acks++
			if acks >= quorum && !decided {
				decided, ok = true, true
				close(done)
			}
			mu.Unlock()
		}(peer)
	}

	go func() {
		wg.Wait()
		mu.Lock()
		if !decided {
			decided = true
			close(done)
		}
		mu.Unlock()
	}()

	timer := n.clock.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C():
		return ErrTimeout
	case <-n.stopCh:
		return ErrShutdown
	}

	mu.Lock()
	defer mu.Unlock()
	if !ok {
		return &NotLeaderError{}
	}
	return nil
}

// releaseReadWaitersLocked wakes reads whose index has now been applied. Called
// whenever lastApplied or commitIndex advances.
func (n *Node) releaseReadWaitersLocked() {
	if len(n.readIndexWaiters) == 0 {
		return
	}
	kept := n.readIndexWaiters[:0]
	for _, r := range n.readIndexWaiters {
		if n.lastApplied >= r.index {
			r.done <- nil
			continue
		}
		kept = append(kept, r)
	}
	n.readIndexWaiters = kept
}
