package raft

import "time"

// runApply feeds committed entries into the state machine.
//
// A single goroutine owns this, which is what makes "applied exactly once, in
// index order" hold without the state machine needing its own locking against
// concurrent Apply calls. Everything that wants to advance application signals
// applyCh; only this loop ever moves lastApplied.
func (n *Node) runApply() {
	defer n.wg.Done()

	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		}

		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			next := n.lastApplied + 1

			// The entry may have been compacted by a snapshot installed from the
			// leader while this loop was between iterations; the install already
			// moved lastApplied past it, so re-check rather than failing.
			if next < n.store.FirstIndex() {
				n.lastApplied = n.store.FirstIndex() - 1
				n.mu.Unlock()
				continue
			}

			entries, err := n.store.Entries(next, n.commitIndex+1)
			if err != nil || len(entries) == 0 {
				n.mu.Unlock()
				break
			}
			n.mu.Unlock()

			for _, e := range entries {
				// Apply outside the lock: a state machine command is arbitrary
				// application code and must not be able to stall the consensus
				// layer by holding mu.
				var result any
				var applyErr error
				if e.Kind == EntryCommand {
					result, applyErr = n.sm.Apply(e)
				}

				n.mu.Lock()
				n.lastApplied = e.Index
				if n.observer != nil {
					n.observer.OnApply(n.cfg.ID, e.Index)
				}
				// A linearizable read parked at or below this index is now safe
				// to serve.
				n.releaseReadWaitersLocked()

				// Answer the waiting client, but only if the entry at this index
				// still carries the term we appended it with. If it does not, a
				// later leader overwrote this index and the client's write did
				// not happen, even though something committed here.
				if p, ok := n.pending[e.Index]; ok {
					if p.term == e.Term {
						p.result <- writeResult{value: result, err: applyErr}
					} else {
						p.result <- writeResult{err: ErrLeadershipLost}
					}
					delete(n.pending, e.Index)
				}
				n.mu.Unlock()
			}

			n.maybeSnapshot()
		}
	}
}

// maybeSnapshot compacts the log once enough entries have been applied past the
// last snapshot point.
//
// The threshold is measured against applied rather than committed entries,
// because a snapshot must capture state the machine has actually reached.
func (n *Node) maybeSnapshot() {
	n.mu.Lock()
	if n.lastApplied < n.lastSnapshotIndex+Index(n.cfg.SnapshotThreshold) {
		n.mu.Unlock()
		return
	}
	upto := n.lastApplied
	uptoTerm, err := n.store.Term(upto)
	if err != nil {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	// Serializing state is potentially expensive, so it happens outside the
	// lock. The apply loop is the only writer to the state machine and it is
	// this goroutine, so no Apply can interleave with this Snapshot call.
	data, err := n.sm.Snapshot()
	if err != nil {
		return
	}

	if err := n.store.SaveSnapshot(Snapshot{
		Meta: SnapshotMeta{Index: upto, Term: uptoTerm},
		Data: data,
	}); err != nil {
		return
	}

	n.mu.Lock()
	n.lastSnapshotIndex = upto
	n.mu.Unlock()
}

// Submit replicates a command and returns once it has committed and been
// applied on this node.
//
// It returns ErrNotLeader with a redirect hint when called on a follower.
// Callers must treat a timeout or ErrLeadershipLost as an UNKNOWN outcome
// rather than a failure: the entry may still commit under a later leader. This
// is why the kv state machine deduplicates on client sequence number — a client
// that retries an unknown write must not apply it twice.
func (n *Node) Submit(data []byte, timeout time.Duration) (any, error) {
	n.mu.Lock()
	if n.role != Leader {
		hint := n.leaderID
		n.mu.Unlock()
		return nil, &NotLeaderError{LeaderHint: hint}
	}

	idx := n.appendLocked(LogEntry{Term: n.currentTerm, Kind: EntryCommand, Data: data})
	p := &pendingWrite{term: n.currentTerm, result: make(chan writeResult, 1)}
	n.pending[idx] = p
	n.mu.Unlock()

	n.signalReplicate()

	timer := n.clock.NewTimer(timeout)
	defer timer.Stop()

	select {
	case r := <-p.result:
		return r.value, r.err
	case <-timer.C():
		n.mu.Lock()
		delete(n.pending, idx)
		n.mu.Unlock()
		return nil, ErrTimeout
	case <-n.stopCh:
		return nil, ErrShutdown
	}
}
