package raft

import (
	"sync"
	"time"
)

// runElectionTimer campaigns whenever the election deadline passes without a
// leader being heard from.
//
// It polls on a fraction of the minimum timeout rather than sleeping until the
// exact deadline, because the deadline is pushed forward by every heartbeat and
// a sleeping goroutine would have to be woken to notice. Polling at a tenth of
// the shortest timeout costs a handful of lock acquisitions per second and
// removes a whole class of timer-reset races.
func (n *Node) runElectionTimer() {
	defer n.wg.Done()

	tick := n.cfg.ElectionTimeoutMin / 10
	if tick <= 0 {
		tick = time.Millisecond
	}
	timer := n.clock.NewTimer(tick)
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-timer.C():
		}
		timer.Reset(tick)

		n.mu.Lock()
		// A leader never campaigns against itself; its own heartbeats are what
		// keep the rest of the cluster quiet.
		if n.role == Leader || n.clock.Now().Before(n.electionDeadline) {
			n.mu.Unlock()
			continue
		}
		n.mu.Unlock()

		n.campaign()
	}
}

// campaign runs one election round. If PreVote is enabled it first probes
// whether it could win without incrementing its own term, which stops a node
// returning from a partition — whose term may be far ahead — from unseating a
// perfectly healthy leader just by asking.
func (n *Node) campaign() {
	if n.cfg.PreVote {
		if !n.solicit(true) {
			// Lost the probe. Back off with a fresh random deadline so the next
			// attempt is not synchronized with any other candidate.
			n.mu.Lock()
			n.resetElectionTimerLocked()
			n.mu.Unlock()
			return
		}
	}

	n.mu.Lock()
	n.becomeCandidateLocked()
	n.persistLocked() // the self-vote must be durable before soliciting others
	n.mu.Unlock()

	if n.solicit(false) {
		n.mu.Lock()
		// Re-check: the term may have moved while votes were in flight, in
		// which case this win is for an election that no longer matters.
		if n.role == Candidate {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		n.signalReplicate()
	}
}

// solicit sends RequestVote to every peer and reports whether a majority
// granted. The local node's own vote counts toward the quorum.
func (n *Node) solicit(preVote bool) bool {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return false
	}
	// A pre-vote asks "would you vote for me in term+1?" without this node
	// actually entering that term.
	term := n.currentTerm
	if preVote {
		term++
	}
	lastIdx := n.store.LastIndex()
	lastTerm, err := n.store.Term(lastIdx)
	if err != nil {
		n.mu.Unlock()
		return false
	}
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
		PreVote:      preVote,
	}
	peers := make([]NodeID, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			peers = append(peers, p)
		}
	}
	quorum := n.cfg.Quorum()
	n.mu.Unlock()

	// The self-vote can already be a majority — a single-node cluster is its own
	// quorum. Checking before fanning out matters because the quorum test below
	// only runs inside the per-peer goroutines, and with no peers none of them
	// would ever run.
	if 1 >= quorum {
		return true
	}

	var (
		wg      sync.WaitGroup
		vmu     sync.Mutex
		granted = 1 // self
		decided bool
		won     bool
		done    = make(chan struct{})
	)

	for _, peer := range peers {
		wg.Add(1)
		go func(peer NodeID) {
			defer wg.Done()
			reply, err := n.trans.RequestVote(peer, args)
			if err != nil {
				return
			}

			// A reply carrying a higher term ends this campaign outright. For a
			// pre-vote the term is hypothetical, so stepping down to it would
			// adopt a term nobody has actually reached — only a real vote's
			// reply may move our term.
			if !preVote && reply.Term > args.Term {
				n.mu.Lock()
				n.stepDownLocked(reply.Term)
				n.persistLocked()
				n.mu.Unlock()
				vmu.Lock()
				if !decided {
					decided, won = true, false
					close(done)
				}
				vmu.Unlock()
				return
			}

			if !reply.VoteGranted {
				return
			}
			vmu.Lock()
			granted++
			if granted >= quorum && !decided {
				decided, won = true, true
				close(done)
			}
			vmu.Unlock()
		}(peer)
	}

	// Return as soon as a majority is in rather than waiting for stragglers; a
	// single unreachable peer must not add its whole RPC timeout to every
	// election.
	go func() {
		wg.Wait()
		vmu.Lock()
		if !decided {
			decided = true
			close(done)
		}
		vmu.Unlock()
	}()

	select {
	case <-done:
	case <-n.stopCh:
		return false
	}
	vmu.Lock()
	defer vmu.Unlock()
	return won
}

// HandleRequestVote implements the voter's side.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{Term: n.currentTerm}

	// A pre-vote is answered without touching persistent state at all: it is a
	// hypothetical, and recording a vote for it would consume this node's one
	// real vote for a term nobody has entered.
	if args.PreVote {
		// A leader never grants a pre-vote. It has a quorum by definition, so
		// there is nothing to replace, and it must not help unseat itself.
		//
		// This case is not covered by the election-deadline check below: a leader
		// does not arm its own election timer, so its deadline sits permanently
		// in the past and reads as "no live leader". Without this branch, the
		// one node that is certain a leader exists is the one most willing to
		// vote against it.
		if n.role == Leader {
			return reply
		}

		lastIdx := n.store.LastIndex()
		lastTerm, err := n.store.Term(lastIdx)
		if err != nil {
			return reply
		}
		// Grant only if the candidate's term is genuinely ahead and its log is
		// at least as fresh, and only if we have not heard from a live leader —
		// otherwise a flapping node could still trigger churn.
		if args.Term > n.currentTerm &&
			upToDate(args.LastLogTerm, args.LastLogIndex, lastTerm, lastIdx) &&
			!n.clock.Now().Before(n.electionDeadline) {
			reply.VoteGranted = true
		}
		return reply
	}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
		reply.Term = n.currentTerm
	}

	// At most one vote per term. The equality case lets a candidate's retried
	// RequestVote get the same answer rather than a spurious denial.
	if n.votedFor != "" && n.votedFor != args.CandidateID {
		return reply
	}

	lastIdx := n.store.LastIndex()
	lastTerm, err := n.store.Term(lastIdx)
	if err != nil {
		return reply
	}
	if !upToDate(args.LastLogTerm, args.LastLogIndex, lastTerm, lastIdx) {
		// Refusing here is what prevents a node missing committed entries from
		// becoming leader and erasing them.
		return reply
	}

	n.votedFor = args.CandidateID
	// Durable before the reply. A node that grants a vote, crashes, restarts
	// having forgotten it, and votes again can produce two leaders in one term.
	n.persistLocked()
	n.resetElectionTimerLocked()
	reply.VoteGranted = true
	return reply
}
