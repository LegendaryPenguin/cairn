package raft

import (
	"errors"
	"time"
)

// Config holds the tunables for one node. Zero values are filled from
// DefaultConfig by Validate, so callers only set what they care about.
type Config struct {
	ID    NodeID
	Peers []NodeID // all voting members, including ID

	// HeartbeatInterval is how often a leader sends AppendEntries. It must be
	// comfortably below ElectionTimeoutMin or followers will time out under a
	// healthy leader.
	HeartbeatInterval time.Duration

	// ElectionTimeoutMin and Max bound the randomized election timer. The
	// randomization is what breaks split votes: with a fixed timeout, two
	// followers that lose a leader at the same instant campaign at the same
	// instant, split the vote, and repeat.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// SnapshotThreshold is how many applied entries past the last snapshot
	// point trigger a new snapshot and compaction.
	SnapshotThreshold uint64

	// MaxEntriesPerAppend caps replication batch size so a far-behind follower
	// does not get a single multi-megabyte RPC.
	MaxEntriesPerAppend int

	// PreVote enables the pre-vote probe. Off by default to keep the base
	// implementation closer to the paper; on, it prevents a node returning
	// from a partition with an inflated term from forcing a needless election.
	PreVote bool

	// Clock allows tests to drive time deterministically. Nil means real time.
	Clock Clock
}

// Clock abstracts time so the fault-injection tests can run a cluster faster
// than wall-clock and without flakiness.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	Sleep(d time.Duration)
}

type Timer interface {
	C() <-chan time.Time
	Reset(d time.Duration) bool
	Stop() bool
}

func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:   50 * time.Millisecond,
		ElectionTimeoutMin:  300 * time.Millisecond,
		ElectionTimeoutMax:  600 * time.Millisecond,
		SnapshotThreshold:   8192,
		MaxEntriesPerAppend: 256,
	}
}

// Validate fills zero fields from defaults and rejects combinations that cannot
// elect a stable leader.
func (c *Config) Validate() error {
	d := DefaultConfig()
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = d.HeartbeatInterval
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = d.ElectionTimeoutMin
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = d.ElectionTimeoutMax
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = d.SnapshotThreshold
	}
	if c.MaxEntriesPerAppend == 0 {
		c.MaxEntriesPerAppend = d.MaxEntriesPerAppend
	}

	if c.ID == "" {
		return errors.New("cairn: Config.ID is required")
	}
	if len(c.Peers) == 0 {
		return errors.New("cairn: Config.Peers is required")
	}
	found := false
	for _, p := range c.Peers {
		if p == c.ID {
			found = true
		}
	}
	if !found {
		return errors.New("cairn: Config.Peers must include Config.ID")
	}
	if c.ElectionTimeoutMin > c.ElectionTimeoutMax {
		return errors.New("cairn: ElectionTimeoutMin exceeds ElectionTimeoutMax")
	}
	// A leader must get several heartbeats inside the shortest election timeout,
	// otherwise ordinary scheduling jitter unseats a healthy leader.
	if c.ElectionTimeoutMin < 3*c.HeartbeatInterval {
		return errors.New("cairn: ElectionTimeoutMin must be at least 3x HeartbeatInterval")
	}
	return nil
}

// Quorum is the number of votes or acknowledgements that constitute a majority.
func (c *Config) Quorum() int {
	return len(c.Peers)/2 + 1
}
