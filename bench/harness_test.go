// Package bench measures Cairn: the latency of a write as a function of cluster
// size, the cost of a linearizable read against a stale one, the wall-clock time
// a cluster is unavailable across a leader failure, and the storage layer's
// recovery behaviour.
//
// The harness here duplicates part of the cluster setup in package tests on
// purpose. tests is a test-only package and cannot be imported; more
// importantly, a benchmark wants a different default posture — no observer, no
// audit recording, and no assertion helpers whose polling loops would show up in
// the measured interval.
//
// Every number produced here comes from a real cluster: real fsyncs to real
// files on a temp directory, the real consensus layer, the real state machine.
// The only substitution is the network, which is the in-memory switchboard with
// zero injected latency. That substitution is what the numbers have to be read
// against: these are the costs Cairn itself imposes (fsync, quorum round trips,
// apply) with network transit removed, not the latency a deployment over real
// links would see.
package bench

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/kv"
	"github.com/LegendaryPenguin/cairn/raft"
	"github.com/LegendaryPenguin/cairn/server"
	"github.com/LegendaryPenguin/cairn/storage"
	"github.com/LegendaryPenguin/cairn/transport"
)

// benchHeartbeat and the election bounds are deliberately the same timings the
// correctness suite uses, so a failover number measured here is comparable with
// the behaviour those tests assert. They are faster than DefaultConfig but still
// hold the 3x heartbeat-to-election ratio Config.Validate requires.
const (
	benchHeartbeat   = 40 * time.Millisecond
	benchElectionMin = 150 * time.Millisecond
	benchElectionMax = 300 * time.Millisecond

	// opTimeout bounds one client call. Generous relative to the measured
	// latencies so that a timeout in a benchmark means something went wrong
	// rather than that the budget was tight.
	opTimeout = 5 * time.Second
)

// cluster is a running set of nodes on one in-memory network.
type cluster struct {
	tb    testing.TB
	size  int
	ids   []raft.NodeID
	board *transport.Switchboard

	mu      sync.Mutex
	nodes   map[raft.NodeID]*raft.Node
	stores  map[raft.NodeID]*storage.Store
	sms     map[raft.NodeID]*kv.Store
	servers map[raft.NodeID]*server.Server
	dirs    map[raft.NodeID]string
	down    map[raft.NodeID]bool

	cfg raft.Config
}

type clusterOption func(*raft.Config)

func withSnapshotThreshold(n uint64) clusterOption {
	return func(c *raft.Config) { c.SnapshotThreshold = n }
}

// withDefaultTimings restores the shipped DefaultConfig election and heartbeat
// intervals. The throughput benchmarks need them: a write is fsync-bound and
// dozens of concurrent writes contend on the leader's mutex long enough that a
// 150ms election timeout fires on a perfectly healthy leader, so the tighter
// test timings turn a throughput measurement into a measurement of repeated
// elections.
func withDefaultTimings() clusterOption {
	d := raft.DefaultConfig()
	return func(c *raft.Config) {
		c.HeartbeatInterval = d.HeartbeatInterval
		c.ElectionTimeoutMin = d.ElectionTimeoutMin
		c.ElectionTimeoutMax = d.ElectionTimeoutMax
	}
}

// newCluster starts an n-node cluster and registers its shutdown with tb.
func newCluster(tb testing.TB, n int, opts ...clusterOption) *cluster {
	tb.Helper()

	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
	}

	cfg := raft.DefaultConfig()
	cfg.Peers = ids
	cfg.HeartbeatInterval = benchHeartbeat
	cfg.ElectionTimeoutMin = benchElectionMin
	cfg.ElectionTimeoutMax = benchElectionMax
	for _, o := range opts {
		o(&cfg)
	}

	c := &cluster{
		tb:      tb,
		size:    n,
		ids:     ids,
		board:   transport.NewSwitchboard(1),
		nodes:   make(map[raft.NodeID]*raft.Node),
		stores:  make(map[raft.NodeID]*storage.Store),
		sms:     make(map[raft.NodeID]*kv.Store),
		servers: make(map[raft.NodeID]*server.Server),
		dirs:    make(map[raft.NodeID]string),
		down:    make(map[raft.NodeID]bool),
		cfg:     cfg,
	}

	for _, id := range ids {
		c.dirs[id] = tb.TempDir()
		c.bringUp(id)
	}

	tb.Cleanup(c.stop)
	return c
}

// bringUp constructs one node from whatever is on its disk, which is the same
// path a restart after a crash takes.
func (c *cluster) bringUp(id raft.NodeID) {
	c.tb.Helper()

	st, err := storage.Open(c.dirs[id])
	if err != nil {
		c.tb.Fatalf("storage.Open(%s): %v", id, err)
	}
	sm := kv.New()

	cfg := c.cfg
	cfg.ID = id
	cfg.Peers = append([]raft.NodeID(nil), c.cfg.Peers...)

	n, err := raft.NewNode(cfg, st, sm, c.board.Transport(id))
	if err != nil {
		c.tb.Fatalf("raft.NewNode(%s): %v", id, err)
	}

	c.nodes[id] = n
	c.stores[id] = st
	c.sms[id] = sm
	c.servers[id] = server.New(n, sm, opTimeout)
	c.down[id] = false

	c.board.Register(id, n)
	n.Start()
}

// stop shuts the cluster down, taking every node off the network before closing
// any storage.
//
// The ordering is load-bearing. Stopping one node and closing its store while its
// peers are still heartbeating lets an AppendEntries land on a node whose WAL is
// already closed, and the consensus layer treats a failed durable append as
// unrecoverable and panics — correctly, since it cannot acknowledge an entry it
// did not persist. Deregistering first means the transport answers ErrUnknownPeer
// instead of invoking a handler that is about to lose its disk. The short pause
// lets RPCs already past the switchboard's dispatch decision finish.
func (c *cluster) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id := range c.nodes {
		if !c.down[id] {
			c.board.Deregister(id)
		}
	}
	time.Sleep(5 * time.Millisecond)

	for id, n := range c.nodes {
		if c.down[id] {
			continue
		}
		n.Stop()
		_ = c.stores[id].Close()
		c.down[id] = true
	}
}

// crash removes a node from the network and stops it without a clean state
// machine shutdown, leaving only what reached disk.
func (c *cluster) crash(id raft.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down[id] {
		return
	}
	c.board.Deregister(id)
	c.nodes[id].Stop()
	_ = c.stores[id].Close()
	c.down[id] = true
}

func (c *cluster) restart(id raft.NodeID) {
	c.tb.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.down[id] {
		c.tb.Fatalf("restart(%s): node is not down", id)
	}
	c.bringUp(id)
}

func (c *cluster) node(id raft.NodeID) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *cluster) server(id raft.NodeID) *server.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[id]
}

func (c *cluster) stateMachine(id raft.NodeID) *kv.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sms[id]
}

func (c *cluster) up() []raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []raft.NodeID
	for _, id := range c.ids {
		if !c.down[id] {
			out = append(out, id)
		}
	}
	return out
}

// waitForLeader blocks until exactly one running node claims leadership and a
// quorum names it. Accepting the first self-reported leader instead would
// sometimes return a deposed one, which in a failover measurement would make the
// reported duration shorter than the truth.
func (c *cluster) waitForLeader(timeout time.Duration) (raft.NodeID, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []raft.NodeID
		for _, id := range c.up() {
			if c.node(id).Role() == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			agree := 0
			for _, id := range c.up() {
				if l, ok := c.node(id).Leader(); ok && l == leaders[0] {
					agree++
				}
			}
			if agree >= c.cfg.Quorum() {
				return leaders[0], true
			}
		}
		time.Sleep(time.Millisecond)
	}
	return "", false
}

// leader returns the current leader or fails the test.
func (c *cluster) leader() raft.NodeID {
	c.tb.Helper()
	id, ok := c.waitForLeader(10 * time.Second)
	if !ok {
		c.tb.Fatalf("no stable leader within 10s")
	}
	return id
}

// put writes through the current leader, re-resolving the leader and retrying on
// any error. Retries reuse seq so a write whose outcome is unknown cannot be
// applied twice.
func (c *cluster) put(clientID string, seq uint64, key, value string) error {
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ldr, ok := c.waitForLeader(5 * time.Second)
		if !ok {
			lastErr = fmt.Errorf("no leader")
			continue
		}
		_, err := c.server(ldr).Put(clientID, seq, key, []byte(value))
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(2 * time.Millisecond)
	}
	return fmt.Errorf("put %q failed: %w", key, lastErr)
}

// payload builds a value of the given size. Benchmarks use a fixed 128-byte
// value so that the numbers reflect consensus and fsync cost rather than the
// bandwidth of a large payload.
func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

const valueSize = 128
