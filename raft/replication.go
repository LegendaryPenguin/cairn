package raft

import "sort"

// replicateCh nudges the replication loop when there is new work, so a client
// write does not wait for the next heartbeat tick to be sent.
func (n *Node) signalReplicate() {
	select {
	case n.replicateCh <- struct{}{}:
	default:
	}
}

// runLeader drives replication. It wakes on the heartbeat interval or whenever
// a new entry is appended, and fans out one AppendEntries per peer.
//
// Each peer gets its own in-flight slot rather than a shared one: a slow or
// unreachable follower must not delay heartbeats to the others, since that
// would let healthy followers time out and unseat a working leader.
func (n *Node) runLeader() {
	defer n.wg.Done()

	timer := n.clock.NewTimer(n.cfg.HeartbeatInterval)
	defer timer.Stop()

	inflight := make(map[NodeID]bool)

	for {
		select {
		case <-n.stopCh:
			return
		case <-timer.C():
			timer.Reset(n.cfg.HeartbeatInterval)
		case <-n.replicateCh:
		}

		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			continue
		}

		// A single-node cluster has no peer whose reply would trigger commit
		// advancement, so the leader has to do it for itself. Calling this
		// unconditionally is also correct for larger clusters: it is idempotent
		// and only ever moves commitIndex forward.
		n.maybeAdvanceCommitLocked()

		if n.lostQuorumLocked() {
			// Step down into the current term rather than a higher one: this node
			// has no evidence anyone has moved on, and inflating the term would
			// disrupt a healthy leader on the other side of the partition.
			n.stepDownLocked(n.currentTerm)
			n.resetElectionTimerLocked()
			n.mu.Unlock()
			continue
		}

		// Claim the in-flight slot while still holding mu, in the same critical
		// section that read it. Marking it after the unlock would race with the
		// completing goroutine's delete, which runs under mu.
		peers := make([]NodeID, 0, len(n.cfg.Peers))
		for _, p := range n.cfg.Peers {
			if p != n.cfg.ID && !inflight[p] {
				inflight[p] = true
				peers = append(peers, p)
			}
		}
		n.mu.Unlock()

		for _, p := range peers {
			go func(peer NodeID) {
				n.replicateTo(peer)
				n.mu.Lock()
				delete(inflight, peer)
				n.mu.Unlock()
			}(p)
		}
	}
}

// lostQuorumLocked reports whether a majority of peers has gone silent for
// longer than one full election timeout.
//
// The window is ElectionTimeoutMax rather than the heartbeat interval because a
// single dropped heartbeat round is normal; what matters is a peer being
// unreachable for long enough that it would have started its own election.
func (n *Node) lostQuorumLocked() bool {
	cutoff := n.clock.Now().Add(-n.cfg.ElectionTimeoutMax)
	reachable := 1 // self
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		if t, ok := n.lastContact[p]; ok && t.After(cutoff) {
			reachable++
		}
	}
	return reachable < n.cfg.Quorum()
}

// replicateTo sends one AppendEntries (or InstallSnapshot) to a peer and
// processes the reply.
func (n *Node) replicateTo(peer NodeID) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	next := n.nextIndex[peer]
	first := n.store.FirstIndex()

	// The follower needs an entry the leader has already compacted away, so the
	// only way to catch it up is to ship the snapshot.
	if next < first {
		n.mu.Unlock()
		n.sendSnapshot(peer, term)
		return
	}

	prevIndex := next - 1
	prevTerm, err := n.store.Term(prevIndex)
	if err != nil {
		// prevIndex fell below the snapshot point between the check above and
		// here; fall back to a snapshot.
		n.mu.Unlock()
		n.sendSnapshot(peer, term)
		return
	}

	last := n.store.LastIndex()
	hi := next + Index(n.cfg.MaxEntriesPerAppend)
	if hi > last+1 {
		hi = last + 1
	}
	var entries []LogEntry
	if next <= last {
		entries, err = n.store.Entries(next, hi)
		if err != nil {
			n.mu.Unlock()
			return
		}
	}

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.trans.AppendEntries(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Discard a reply that belongs to a term we have left. Acting on it could
	// move matchIndex for a log that no longer exists.
	if n.role != Leader || n.currentTerm != term {
		return
	}

	// Any reply at all proves the peer is reachable, including a rejected one:
	// a failed consistency check means the log needs repair, not that the peer
	// is gone. Recording contact only on success would depose a leader whose
	// followers are all busy being brought up to date.
	n.lastContact[peer] = n.clock.Now()

	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		n.persistLocked()
		return
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			newMatch := args.Entries[len(args.Entries)-1].Index
			// matchIndex must never move backwards: a delayed reply from an
			// earlier, shorter append could otherwise undo progress and let the
			// leader un-commit an entry.
			if newMatch > n.matchIndex[peer] {
				n.matchIndex[peer] = newMatch
			}
			if newMatch+1 > n.nextIndex[peer] {
				n.nextIndex[peer] = newMatch + 1
			}
			n.maybeAdvanceCommitLocked()
		} else if args.PrevLogIndex > n.matchIndex[peer] {
			// A successful heartbeat also proves the follower holds everything
			// through PrevLogIndex.
			n.matchIndex[peer] = args.PrevLogIndex
			n.maybeAdvanceCommitLocked()
		}
		return
	}

	// The consistency check failed. Use the follower's hint to skip the whole
	// conflicting term in one step instead of decrementing nextIndex per entry,
	// which on a long divergence costs one round trip per conflicting entry.
	if reply.ConflictTerm == 0 {
		// The follower's log is simply shorter than PrevLogIndex.
		n.nextIndex[peer] = max(1, reply.ConflictIndex)
	} else {
		// Find the leader's last entry for a term at or below ConflictTerm and
		// resume from there.
		n.nextIndex[peer] = n.firstIndexOfConflictLocked(reply.ConflictTerm, reply.ConflictIndex)
	}
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}
	// Retry promptly rather than waiting a full heartbeat per backup step.
	n.signalReplicate()
}

// firstIndexOfConflictLocked implements the leader half of the conflict-term
// optimization. If the leader has entries for the follower's conflicting term,
// resume just past the leader's last one; otherwise skip the term entirely.
func (n *Node) firstIndexOfConflictLocked(conflictTerm Term, conflictIndex Index) Index {
	first := n.store.FirstIndex()
	// Walk back from the follower's reported conflict point looking for a term
	// the leader shares. Bounded by the snapshot point, past which the leader
	// will send a snapshot instead.
	for i := conflictIndex; i >= first; i-- {
		t, err := n.store.Term(i)
		if err != nil {
			break
		}
		if t <= conflictTerm {
			return i + 1
		}
		if i == 0 {
			break
		}
	}
	return first
}

// maybeAdvanceCommitLocked recomputes commitIndex from the replica set.
//
// Two rules together make this safe. First, an index is committed only when a
// majority reports holding it. Second — and this is the subtle one — a leader
// may only commit an entry from its OWN term by counting replicas. An entry
// inherited from a previous term can be present on a majority and still be
// overwritten by a future leader, so committing it on replica count alone can
// lose an acknowledged write. Entries from earlier terms become committed
// indirectly, carried along when an entry of the current term commits above
// them.
func (n *Node) maybeAdvanceCommitLocked() {
	if n.role != Leader {
		return
	}

	matches := make([]Index, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			matches = append(matches, n.store.LastIndex())
			continue
		}
		matches = append(matches, n.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })

	// With the replica set sorted descending, the element at quorum-1 is the
	// highest index that a majority is known to hold.
	candidate := matches[n.cfg.Quorum()-1]
	if candidate <= n.commitIndex {
		return
	}

	t, err := n.store.Term(candidate)
	if err != nil || t != n.currentTerm {
		return
	}

	n.commitIndex = candidate
	if n.observer != nil {
		n.observer.OnCommit(n.cfg.ID, n.commitIndex)
	}
	n.signalApply()
	n.releaseReadWaitersLocked()
}

// HandleAppendEntries implements the follower's side of replication.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	if n.isStopped() {
		return &AppendEntriesReply{}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		// A stale leader. Replying with our higher term is what makes it step down.
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
		n.persistLocked()
		reply.Term = n.currentTerm
	} else if n.role != Follower {
		// Same term, and someone else is leader. A candidate loses the election;
		// a leader seeing this would mean two leaders in one term, which the
		// vote rules make impossible.
		n.stepDownLocked(args.Term)
	}

	n.leaderID = args.LeaderID
	// Only reset the election timer for a leader we accept. Doing it before the
	// term check would let a stale leader keep a follower from ever campaigning.
	n.resetElectionTimerLocked()

	first := n.store.FirstIndex()
	last := n.store.LastIndex()

	// Everything through the snapshot point is already durable and committed, so
	// an append whose prefix lies entirely below it is redundant but not wrong.
	if args.PrevLogIndex+1 < first {
		// Tell the leader where we actually need entries from.
		reply.ConflictIndex = first
		return reply
	}

	if args.PrevLogIndex > last {
		// We do not have the entry the leader assumed. Report our length so the
		// leader can jump straight here.
		reply.ConflictTerm = 0
		reply.ConflictIndex = last + 1
		return reply
	}

	if args.PrevLogIndex >= first {
		myTerm, err := n.store.Term(args.PrevLogIndex)
		if err != nil {
			reply.ConflictIndex = first
			return reply
		}
		if myTerm != args.PrevLogTerm {
			// Histories diverge at PrevLogIndex. Report the first index of our
			// conflicting term so the leader can skip the whole term at once.
			reply.ConflictTerm = myTerm
			reply.ConflictIndex = n.firstIndexOfTermLocked(myTerm, args.PrevLogIndex)
			return reply
		}
	}

	// The prefix agrees. Now reconcile the entries themselves.
	//
	// Truncating unconditionally would be a correctness bug, not just waste: a
	// delayed duplicate of an earlier append would chop off entries the
	// follower has since received and acknowledged, and if the leader had
	// already counted them toward a commit, a committed entry would vanish. So
	// scan for the first genuine mismatch and truncate only from there.
	appendFrom := 0
	for i, e := range args.Entries {
		if e.Index > n.store.LastIndex() {
			break
		}
		if e.Index < first {
			appendFrom = i + 1
			continue
		}
		existing, err := n.store.Term(e.Index)
		if err != nil {
			break
		}
		if existing != e.Term {
			if err := n.store.TruncateSuffix(e.Index); err != nil {
				panic("cairn: truncate failed: " + err.Error())
			}
			break
		}
		appendFrom = i + 1
	}

	if appendFrom < len(args.Entries) {
		toAppend := args.Entries[appendFrom:]
		if err := n.store.Append(toAppend); err != nil {
			panic("cairn: durable log append failed: " + err.Error())
		}

		// A configuration takes effect on append rather than on commit, so a
		// follower must adopt it here. Waiting for the commit index to reach it
		// would leave this node voting under a configuration the rest of the
		// cluster has already left.
		for _, e := range toAppend {
			if e.Kind != EntryConfig {
				continue
			}
			cc, err := DecodeConfigChange(e.Data)
			if err != nil {
				continue
			}
			n.applyConfigLocked(cc)
			n.pendingConfigIndex = e.Index
		}
	}

	// Adopt the leader's commit index, bounded by what we actually hold. The
	// bound matters: the leader may have committed entries it has not sent us
	// yet, and applying past our own log would read entries that do not exist.
	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if l := n.store.LastIndex(); newCommit > l {
			newCommit = l
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			if n.observer != nil {
				n.observer.OnCommit(n.cfg.ID, n.commitIndex)
			}
			n.signalApply()
		}
	}

	reply.Success = true
	return reply
}

// firstIndexOfTermLocked finds the lowest index holding the given term, used to
// tell a leader how far back our conflicting term extends.
func (n *Node) firstIndexOfTermLocked(term Term, from Index) Index {
	first := n.store.FirstIndex()
	i := from
	for i > first {
		t, err := n.store.Term(i - 1)
		if err != nil || t != term {
			break
		}
		i--
	}
	return i
}

// sendSnapshot ships the current snapshot to a follower too far behind to catch
// up from the log.
func (n *Node) sendSnapshot(peer NodeID, term Term) {
	snap, err := n.store.LoadSnapshot()
	if err != nil || snap.Meta.Index == 0 {
		return
	}

	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: snap.Meta.Index,
		LastIncludedTerm:  snap.Meta.Term,
		Offset:            0,
		Data:              snap.Data,
		Done:              true,
	}

	reply, err := n.trans.InstallSnapshot(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.currentTerm != term {
		return
	}
	n.lastContact[peer] = n.clock.Now()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		n.persistLocked()
		return
	}
	// The follower now holds everything through the snapshot point.
	if snap.Meta.Index > n.matchIndex[peer] {
		n.matchIndex[peer] = snap.Meta.Index
	}
	n.nextIndex[peer] = snap.Meta.Index + 1
	n.maybeAdvanceCommitLocked()
}

// HandleInstallSnapshot replaces this node's state with the leader's snapshot.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	if n.isStopped() {
		return &InstallSnapshotReply{}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
		n.persistLocked()
		reply.Term = n.currentTerm
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()

	// A snapshot that does not advance us past what we already have is stale;
	// installing it would move lastApplied backwards.
	if args.LastIncludedIndex <= n.lastApplied {
		return reply
	}

	if err := n.sm.Restore(args.Data); err != nil {
		return reply
	}
	if err := n.store.SaveSnapshot(Snapshot{
		Meta: SnapshotMeta{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm},
		Data: args.Data,
	}); err != nil {
		panic("cairn: snapshot save failed: " + err.Error())
	}

	n.lastApplied = args.LastIncludedIndex
	n.lastSnapshotIndex = args.LastIncludedIndex
	if args.LastIncludedIndex > n.commitIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	return reply
}

func max(a, b Index) Index {
	if a > b {
		return a
	}
	return b
}
