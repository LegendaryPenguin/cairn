// Package tests holds the multi-node cluster harness and the invariant and
// fault-injection suites that run against it.
//
// The harness runs a real cluster in one process: real storage on temp
// directories, the real consensus layer, the real state machine, and the
// in-memory transport so a test can partition, delay, drop, duplicate, and pause
// links on demand. Nothing here is mocked except the network, which is the one
// thing that has to be controllable for a failure test to be a test rather than
// a hope.
package tests

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/kv"
	"github.com/LegendaryPenguin/cairn/raft"
	"github.com/LegendaryPenguin/cairn/server"
	"github.com/LegendaryPenguin/cairn/storage"
	"github.com/LegendaryPenguin/cairn/transport"
)

// Cluster is a running set of Cairn nodes sharing one in-memory network.
type Cluster struct {
	t     *testing.T
	Size  int
	IDs   []raft.NodeID
	Board *switchboardWrapper

	mu      sync.Mutex
	nodes   map[raft.NodeID]*raft.Node
	stores  map[raft.NodeID]*storage.Store
	sms     map[raft.NodeID]*kv.Store
	servers map[raft.NodeID]*server.Server
	dirs    map[raft.NodeID]string
	down    map[raft.NodeID]bool

	audit *audit
	cfg   raft.Config
}

// switchboardWrapper exists only so the harness can expose the switchboard
// without tests importing the transport package directly.
type switchboardWrapper struct{ *transport.Switchboard }

// ClusterOption tunes the cluster before it starts.
type ClusterOption func(*raft.Config)

// WithSnapshotThreshold lowers the snapshot trigger so a test can exercise
// compaction and InstallSnapshot without writing tens of thousands of entries.
func WithSnapshotThreshold(n uint64) ClusterOption {
	return func(c *raft.Config) { c.SnapshotThreshold = n }
}

// WithTimings shortens the election and heartbeat intervals so tests finish in
// seconds. The 3x ratio Config.Validate enforces still has to hold.
func WithTimings(heartbeat, electionMin, electionMax time.Duration) ClusterOption {
	return func(c *raft.Config) {
		c.HeartbeatInterval = heartbeat
		c.ElectionTimeoutMin = electionMin
		c.ElectionTimeoutMax = electionMax
	}
}

// WithPreVote enables the pre-vote probe.
func WithPreVote() ClusterOption {
	return func(c *raft.Config) { c.PreVote = true }
}

// NewCluster builds and starts an n-node cluster. It registers cleanup, so a
// test does not have to remember to stop it.
func NewCluster(t *testing.T, n int, opts ...ClusterOption) *Cluster {
	t.Helper()

	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
	}

	cfg := raft.DefaultConfig()
	cfg.Peers = ids
	// Fast but not so fast that a loaded CI machine's scheduling jitter looks
	// like a failure. 150ms minimum election timeout against a 40ms heartbeat
	// keeps the 3x ratio with margin.
	cfg.HeartbeatInterval = 40 * time.Millisecond
	cfg.ElectionTimeoutMin = 150 * time.Millisecond
	cfg.ElectionTimeoutMax = 300 * time.Millisecond
	for _, o := range opts {
		o(&cfg)
	}

	c := &Cluster{
		t:       t,
		Size:    n,
		IDs:     ids,
		Board:   &switchboardWrapper{transport.NewSwitchboard(int64(1))},
		nodes:   make(map[raft.NodeID]*raft.Node),
		stores:  make(map[raft.NodeID]*storage.Store),
		sms:     make(map[raft.NodeID]*kv.Store),
		servers: make(map[raft.NodeID]*server.Server),
		dirs:    make(map[raft.NodeID]string),
		down:    make(map[raft.NodeID]bool),
		audit:   newAudit(),
		cfg:     cfg,
	}

	for _, id := range ids {
		c.dirs[id] = t.TempDir()
		c.bringUp(id)
	}

	t.Cleanup(c.Stop)
	return c
}

// bringUp constructs and starts one node from whatever is on its disk. Used both
// for initial start and for restart-after-crash, which is the point: a restarted
// node goes through exactly the same recovery path as a fresh one.
func (c *Cluster) bringUp(id raft.NodeID) {
	c.t.Helper()

	st, err := storage.Open(c.dirs[id])
	if err != nil {
		c.t.Fatalf("storage.Open(%s): %v", id, err)
	}
	sm := kv.New()

	cfg := c.cfg
	cfg.ID = id
	cfg.Peers = append([]raft.NodeID(nil), c.cfg.Peers...)

	n, err := raft.NewNode(cfg, st, sm, c.Board.Transport(id))
	if err != nil {
		c.t.Fatalf("raft.NewNode(%s): %v", id, err)
	}
	n.SetObserver(c.audit)

	c.nodes[id] = n
	c.stores[id] = st
	c.sms[id] = sm
	c.servers[id] = server.New(n, sm, 2*time.Second)
	c.down[id] = false

	c.Board.Register(id, n)
	n.Start()
}

// Stop shuts every node down and closes its storage.
func (c *Cluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, n := range c.nodes {
		if c.down[id] {
			continue
		}
		n.Stop()
		_ = c.stores[id].Close()
		c.down[id] = true
	}
}

// Crash stops a node abruptly without a clean shutdown of its state machine,
// leaving only what reached disk. Its storage is closed, so anything the WAL had
// not fsynced is gone — which is the behaviour a real power loss produces.
func (c *Cluster) Crash(id raft.NodeID) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.down[id] {
		return
	}
	// Cut the node off the network before stopping it, so no peer observes a
	// half-stopped node answering some RPCs and not others.
	c.Board.Deregister(id)
	c.nodes[id].Stop()
	_ = c.stores[id].Close()
	c.down[id] = true
}

// Restart brings a crashed node back, recovering state from disk.
func (c *Cluster) Restart(id raft.NodeID) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.down[id] {
		c.t.Fatalf("Restart(%s): node is not down", id)
	}
	c.bringUp(id)
}

// Node returns a node by ID.
func (c *Cluster) Node(id raft.NodeID) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Server returns a node's client-facing server.
func (c *Cluster) Server(id raft.NodeID) *server.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[id]
}

// StateMachine returns a node's KV store, for asserting convergence directly.
func (c *Cluster) StateMachine(id raft.NodeID) *kv.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sms[id]
}

// Up reports the nodes currently running.
func (c *Cluster) Up() []raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []raft.NodeID
	for _, id := range c.IDs {
		if !c.down[id] {
			out = append(out, id)
		}
	}
	return out
}

// Audit exposes the recorded transition history for invariant checks.
func (c *Cluster) Audit() *audit { return c.audit }

// WaitForLeader blocks until exactly one running node reports itself leader and
// a majority agrees, or the deadline passes.
//
// Requiring agreement rather than just "somebody says leader" matters: during a
// partition two nodes can both believe they lead, and a test that accepts the
// first self-reported leader will nondeterministically pick the deposed one.
func (c *Cluster) WaitForLeader(timeout time.Duration) raft.NodeID {
	c.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := c.leaders()
		if len(leaders) == 1 {
			ldr := leaders[0]
			agree := 0
			for _, id := range c.Up() {
				if l, ok := c.Node(id).Leader(); ok && l == ldr {
					agree++
				}
			}
			if agree >= c.cfg.Quorum() {
				return ldr
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no stable leader within %v (self-reported leaders: %v)", timeout, c.leaders())
	return ""
}

// WaitForNoLeader blocks until no running node considers itself leader. Used to
// assert that a minority partition cannot sustain leadership.
func (c *Cluster) WaitForNoLeader(ids []raft.NodeID, timeout time.Duration) {
	c.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		found := false
		for _, id := range ids {
			if c.Node(id).Role() == raft.Leader {
				found = true
				break
			}
		}
		if !found {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("a node in %v still claims leadership after %v", ids, timeout)
}

func (c *Cluster) leaders() []raft.NodeID {
	var out []raft.NodeID
	for _, id := range c.Up() {
		if c.Node(id).Role() == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// Leader returns the current leader, failing the test if there is not exactly one.
func (c *Cluster) Leader() raft.NodeID {
	c.t.Helper()
	return c.WaitForLeader(5 * time.Second)
}

// Put writes through whichever node currently leads, retrying redirects.
//
// The retry carries the SAME sequence number every time, which is what makes an
// unknown outcome safe: if an earlier attempt did commit, the state machine's
// dedup table turns the retry into a no-op rather than a second write.
func (c *Cluster) Put(clientID string, seq uint64, key, value string) error {
	c.t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ldr := c.WaitForLeader(3 * time.Second)
		_, err := c.Server(ldr).Put(clientID, seq, key, []byte(value))
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("put %q did not succeed within the deadline: %w", key, lastErr)
}

// GetLinearizable reads through the leader with a ReadIndex barrier.
func (c *Cluster) GetLinearizable(key string) (string, bool, error) {
	c.t.Helper()

	ldr := c.WaitForLeader(3 * time.Second)
	v, ok, err := c.Server(ldr).Get(key, server.Linearizable)
	return string(v), ok, err
}

// WaitForConvergence blocks until every running node's state machine holds the
// expected value for key.
//
// This is the assertion that catches divergence: a cluster can accept writes,
// report success, and still end up with replicas that disagree, and nothing short
// of comparing applied state across nodes will notice.
func (c *Cluster) WaitForConvergence(key, want string, timeout time.Duration) {
	c.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c.Up() {
			v, found := c.StateMachine(id).Get(key)
			if !found || string(v) != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	var report string
	for _, id := range c.Up() {
		v, found := c.StateMachine(id).Get(key)
		report += fmt.Sprintf("\n  %s: found=%v value=%q", id, found, v)
	}
	c.t.Fatalf("replicas did not converge on %q=%q within %v:%s", key, want, timeout, report)
}

// WaitForCommitIndex blocks until a node's commit index reaches at least want.
func (c *Cluster) WaitForCommitIndex(id raft.NodeID, want raft.Index, timeout time.Duration) {
	c.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Node(id).CommitIndex() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("%s commit index reached %d, want at least %d within %v",
		id, c.Node(id).CommitIndex(), want, timeout)
}

// Partition splits the cluster into isolated groups.
func (c *Cluster) Partition(groups ...[]raft.NodeID) { c.Board.Partition(groups) }

// Heal removes all partitions.
func (c *Cluster) Heal() { c.Board.Heal() }

// Majority returns the first quorum-sized subset of IDs, and Minority the rest.
func (c *Cluster) Majority() []raft.NodeID { return c.IDs[:c.cfg.Quorum()] }
func (c *Cluster) Minority() []raft.NodeID { return c.IDs[c.cfg.Quorum():] }

// DumpDir prints a node's storage directory listing. For debugging a failure.
func (c *Cluster) DumpDir(id raft.NodeID) string {
	entries, err := os.ReadDir(c.dirs[id])
	if err != nil {
		return err.Error()
	}
	out := string(id) + ":"
	for _, e := range entries {
		info, _ := e.Info()
		out += fmt.Sprintf(" %s(%dB)", e.Name(), info.Size())
	}
	return out
}
