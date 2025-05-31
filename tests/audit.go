package tests

import (
	"fmt"
	"sort"
	"sync"

	"github.com/LegendaryPenguin/cairn/raft"
)

// audit records every role change and commit the cluster reports, so a test can
// assert global safety properties after the fact.
//
// Checking these continuously from the test goroutine would miss violations:
// two leaders in one term may both exist for only a few milliseconds during a
// partition, and polling cannot be relied on to sample that window. Recording
// every transition as it happens and checking the history afterwards catches it
// regardless of timing.
type audit struct {
	mu sync.Mutex

	// leadersByTerm accumulates every node that ever claimed leadership in each
	// term. More than one entry for a term is an election-safety violation.
	leadersByTerm map[raft.Term]map[raft.NodeID]bool

	// highestCommit tracks the furthest each node ever committed. Commit indexes
	// must be monotonic per node; a decrease means a committed entry was
	// forgotten, which is the most serious failure this suite can detect.
	highestCommit map[raft.NodeID]raft.Index

	// commitRegressions records any violation of the above, with enough context
	// to identify it in a failure message.
	commitRegressions []string

	// appliedByNode records the apply sequence per node so a test can verify that
	// application is gap-free and strictly increasing.
	appliedByNode map[raft.NodeID][]raft.Index
	applyAnomaly  []string
}

func newAudit() *audit {
	return &audit{
		leadersByTerm: make(map[raft.Term]map[raft.NodeID]bool),
		highestCommit: make(map[raft.NodeID]raft.Index),
		appliedByNode: make(map[raft.NodeID][]raft.Index),
	}
}

func (a *audit) OnRoleChange(id raft.NodeID, term raft.Term, role raft.Role) {
	if role != raft.Leader {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.leadersByTerm[term] == nil {
		a.leadersByTerm[term] = make(map[raft.NodeID]bool)
	}
	a.leadersByTerm[term][id] = true
}

func (a *audit) OnCommit(id raft.NodeID, index raft.Index) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if prev, ok := a.highestCommit[id]; ok && index < prev {
		a.commitRegressions = append(a.commitRegressions,
			fmt.Sprintf("%s commit index went backwards: %d then %d", id, prev, index))
		return
	}
	a.highestCommit[id] = index
}

func (a *audit) OnApply(id raft.NodeID, index raft.Index) {
	a.mu.Lock()
	defer a.mu.Unlock()

	seq := a.appliedByNode[id]
	if n := len(seq); n > 0 {
		last := seq[n-1]
		switch {
		case index == last:
			a.applyAnomaly = append(a.applyAnomaly,
				fmt.Sprintf("%s applied index %d twice", id, index))
		case index < last:
			a.applyAnomaly = append(a.applyAnomaly,
				fmt.Sprintf("%s applied %d after %d, out of order", id, index, last))
		case index > last+1:
			// A gap is legitimate only across a snapshot install, which jumps
			// lastApplied forward. Record it; the test decides whether a snapshot
			// was expected.
			a.applyAnomaly = append(a.applyAnomaly,
				fmt.Sprintf("%s skipped from %d to %d", id, last, index))
		}
	}
	a.appliedByNode[id] = append(seq, index)
}

// AssertAtMostOneLeaderPerTerm is the headline safety property. Two leaders in
// one term means the vote rules failed, and every other guarantee collapses with
// it: both would accept writes at the same indexes and the logs would diverge
// irreconcilably.
func (a *audit) AssertAtMostOneLeaderPerTerm(t testingT) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()

	terms := make([]raft.Term, 0, len(a.leadersByTerm))
	for term := range a.leadersByTerm {
		terms = append(terms, term)
	}
	sort.Slice(terms, func(i, j int) bool { return terms[i] < terms[j] })

	for _, term := range terms {
		if len(a.leadersByTerm[term]) > 1 {
			var who []string
			for id := range a.leadersByTerm[term] {
				who = append(who, string(id))
			}
			sort.Strings(who)
			t.Errorf("term %d had %d leaders (%v); the vote rules permitted a split brain",
				term, len(who), who)
		}
	}
}

// AssertNoCommitRegression checks that no node ever lowered its commit index.
func (a *audit) AssertNoCommitRegression(t testingT) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, msg := range a.commitRegressions {
		t.Errorf("committed entry lost: %s", msg)
	}
}

// AssertApplyIsOrderedAndExactlyOnce checks per-node apply sequences. Gaps are
// reported separately because a snapshot install legitimately produces one.
func (a *audit) AssertApplyIsOrderedAndExactlyOnce(t testingT, allowGaps bool) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, msg := range a.applyAnomaly {
		if allowGaps && containsSkip(msg) {
			continue
		}
		t.Errorf("state machine application anomaly: %s", msg)
	}
}

func containsSkip(msg string) bool {
	for i := 0; i+7 <= len(msg); i++ {
		if msg[i:i+7] == "skipped" {
			return true
		}
	}
	return false
}

// LeaderTerms returns the terms in which leadership was observed, for tests that
// want to assert an election actually happened rather than assuming it did.
func (a *audit) LeaderTerms() []raft.Term {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]raft.Term, 0, len(a.leadersByTerm))
	for term := range a.leadersByTerm {
		out = append(out, term)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// HighestCommit returns the furthest index a node was ever observed to commit.
func (a *audit) HighestCommit(id raft.NodeID) raft.Index {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.highestCommit[id]
}

// testingT is the subset of *testing.T the assertions use, so the audit type does
// not have to import testing into non-test code.
type testingT interface {
	Helper()
	Errorf(format string, args ...any)
}
