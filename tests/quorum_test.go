package tests

// These two tests arrived with the mechanisms they cover, both of which were
// added after a failure rather than designed up front: check-quorum, after an
// isolated leader was found to claim leadership indefinitely, and the pre-vote
// leader guard, after a rejoining follower was found able to unseat a healthy
// leader.

import (
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
)

func TestIsolatedLeaderStepsDownOnLostQuorum(t *testing.T) {
	c := NewCluster(t, 3)
	ldr := c.WaitForLeader(5 * time.Second)

	// Plain Raft would leave this node believing it leads forever: it cannot
	// commit anything, so safety holds, but it keeps reporting itself leader and
	// keeps redirecting clients to itself. Check-quorum is what makes it give up.
	c.Board.Isolate(ldr)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.Node(ldr).Role() != raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if role := c.Node(ldr).Role(); role == raft.Leader {
		t.Fatal("an isolated leader never stepped down; clients would be redirected to a node that cannot serve them")
	}

	// It must step down within its own term rather than inflating it, so that
	// rejoining does not disrupt whatever leader the majority has elected.
	c.Board.Rejoin(ldr)
	c.WaitForLeader(10 * time.Second)
	c.Audit().AssertAtMostOneLeaderPerTerm(t)
	c.Audit().AssertNoCommitRegression(t)
}

func TestPreVoteAvoidsDisruptingAHealthyLeader(t *testing.T) {
	c := NewCluster(t, 3, WithPreVote())
	ldr := c.WaitForLeader(5 * time.Second)
	termBefore := c.Node(ldr).Term()

	// Isolate a follower so it times out repeatedly and inflates nothing, then
	// let it back. Without pre-vote its inflated term would force the healthy
	// leader to step down on rejoin.
	var victim raft.NodeID
	for _, id := range c.IDs {
		if id != ldr {
			victim = id
			break
		}
	}
	c.Board.Isolate(victim)
	time.Sleep(800 * time.Millisecond)
	c.Board.Rejoin(victim)
	time.Sleep(500 * time.Millisecond)

	if c.Node(ldr).Role() != raft.Leader {
		t.Fatal("the healthy leader was unseated by a rejoining follower despite pre-vote")
	}
	if got := c.Node(ldr).Term(); got != termBefore {
		t.Fatalf("term moved from %d to %d on rejoin; pre-vote should have prevented the election", termBefore, got)
	}
}
