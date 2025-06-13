package bench

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/kv"
	"github.com/LegendaryPenguin/cairn/raft"
	"github.com/LegendaryPenguin/cairn/storage"
)

// scenarios are gated because they are wall-clock measurements, not unit tests:
// each one deliberately spends seconds crashing nodes or writing tens of
// thousands of entries, and none of them asserts anything a regression suite
// should depend on. `go test ./...` should stay fast; a measurement run asks for
// these explicitly with -bench.scenarios.
var runScenarios = flag.Bool("bench.scenarios", false, "run the wall-clock measurement scenarios in package bench")

func requireScenarios(t *testing.T) {
	if !*runScenarios {
		t.Skip("measurement scenario; pass -bench.scenarios to run")
	}
}

// stats are the order statistics of a sample of durations.
type stats struct {
	n                  int
	min, p50, p95, p99 time.Duration
	p999, max, mean    time.Duration
}

// summarize sorts in place and extracts percentiles by nearest-rank. Nearest-rank
// is used rather than interpolation so every reported figure is a duration that
// was actually observed, which matters when the sample is small enough that an
// interpolated p99 would be a number no request ever experienced.
func summarize(samples []time.Duration) stats {
	if len(samples) == 0 {
		return stats{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	var total time.Duration
	for _, s := range samples {
		total += s
	}
	pick := func(q float64) time.Duration {
		i := int(q * float64(len(samples)))
		if i >= len(samples) {
			i = len(samples) - 1
		}
		return samples[i]
	}
	return stats{
		n:    len(samples),
		min:  samples[0],
		p50:  pick(0.50),
		p95:  pick(0.95),
		p99:  pick(0.99),
		p999: pick(0.999),
		max:  samples[len(samples)-1],
		mean: total / time.Duration(len(samples)),
	}
}

func ms(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64)
}

// TestMeasureLeaderFailover measures how long a cluster is unable to accept
// writes after its leader disappears.
//
// The interval starts the instant the leader is cut off and ends when a write
// completes on its successor, so it includes the follower's election timeout, the
// election itself, the new leader's no-op commit, and the write's own replication
// round. Anything shorter would be a measurement of leader election rather than
// of availability, which is what a client actually experiences.
//
// Ten repetitions, not one. The dominant term is a randomized election timeout
// drawn uniformly from [ElectionTimeoutMin, ElectionTimeoutMax], so a single
// sample tells you almost nothing about the distribution and a lucky one would
// look like a much better system than this is.
func TestMeasureLeaderFailover(t *testing.T) {
	requireScenarios(t)

	const reps = 10
	// Five nodes so that crashing the leader still leaves a comfortable quorum
	// (3 of 4) while the crashed node is restarted between repetitions.
	c := newCluster(t, 5)
	val := payload(valueSize)

	var samples []time.Duration
	seq := uint64(0)

	for rep := 0; rep < reps; rep++ {
		ldr := c.leader()
		seq++
		if err := c.put("pre", seq, "pre"+strconv.Itoa(rep), "x"); err != nil {
			t.Fatalf("rep %d pre-write: %v", rep, err)
		}

		start := time.Now()
		c.crash(ldr)

		var elapsed time.Duration
		deadline := time.Now().Add(15 * time.Second)
		for {
			if time.Now().After(deadline) {
				t.Fatalf("rep %d: no successor served a write within 15s", rep)
			}
			next, ok := c.waitForLeader(15 * time.Second)
			if !ok {
				continue
			}
			seq++
			if _, err := c.server(next).Put("failover", seq, "post"+strconv.Itoa(rep), val); err == nil {
				elapsed = time.Since(start)
				break
			}
		}
		samples = append(samples, elapsed)

		c.restart(ldr)
		// Let the restarted node catch up before the next repetition, so each
		// measurement starts from a fully healthy five-node cluster rather than
		// from a cluster still recovering from the previous one.
		c.leader()
		time.Sleep(200 * time.Millisecond)
	}

	s := summarize(samples)
	t.Logf("leader failover to first successful write, %d repetitions, 5-node cluster", s.n)
	t.Logf("  election timeout window: [%v, %v], heartbeat %v", benchElectionMin, benchElectionMax, benchHeartbeat)
	t.Logf("  min %s ms   median %s ms   max %s ms   mean %s ms", ms(s.min), ms(s.p50), ms(s.max), ms(s.mean))
	t.Logf("  all samples (ms): %s", msList(samples))
}

func msList(ds []time.Duration) string {
	parts := make([]string, len(ds))
	for i, d := range ds {
		parts[i] = ms(d)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// TestMeasureFollowerCatchUp measures how long a follower that missed N entries
// takes to converge once its link is restored.
//
// The snapshot threshold is raised out of reach so this is a pure log-shipping
// measurement: the leader still holds every entry the follower is missing and
// catches it up with AppendEntries batches capped at MaxEntriesPerAppend, rather
// than falling back to InstallSnapshot. Those are two very different recovery
// paths and mixing them in one number would make it uninterpretable.
func TestMeasureFollowerCatchUp(t *testing.T) {
	requireScenarios(t)

	const (
		entries = 2000
		writers = 16
	)

	c := newCluster(t, 3, withSnapshotThreshold(1<<40))
	ldr := c.leader()

	// Build the partition around the current leader: it and one follower stay
	// connected, the remaining node is cut off. Partitioning the leader away
	// instead would measure an election, not catch-up.
	var keep, lag raft.NodeID
	for _, id := range c.ids {
		if id == ldr {
			continue
		}
		if keep == "" {
			keep = id
		} else {
			lag = id
		}
	}
	c.board.Partition([][]raft.NodeID{{ldr, keep}, {lag}})

	srv := c.server(ldr)
	val := payload(valueSize)

	// Concurrent writers only to build the backlog quickly; the measured interval
	// starts after they finish.
	var wg sync.WaitGroup
	perWriter := entries / writers
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := "w" + strconv.Itoa(w)
			for i := 1; i <= perWriter; i++ {
				if _, err := srv.Put(id, uint64(i), id+"-"+strconv.Itoa(i), val); err != nil {
					t.Errorf("backlog write %s-%d: %v", id, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	target := c.node(ldr).CommitIndex()
	behind := target - c.node(lag).CommitIndex()

	start := time.Now()
	c.board.Heal()

	lagNode := c.node(lag)
	lagSM := c.stateMachine(lag)
	lastKey := "w0-" + strconv.Itoa(perWriter)
	deadline := time.Now().Add(60 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("follower %s stuck at commit %d, want %d", lag, lagNode.CommitIndex(), target)
		}
		if lagNode.CommitIndex() >= target {
			if _, ok := lagSM.Get(lastKey); ok {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(start)

	t.Logf("lagging-follower catch-up, 3-node cluster, log shipping only (no snapshot)")
	t.Logf("  follower %s was %d entries behind (leader commit index %d)", lag, behind, target)
	t.Logf("  MaxEntriesPerAppend %d, so at least %d AppendEntries rounds were required",
		c.cfg.MaxEntriesPerAppend, (int(behind)+c.cfg.MaxEntriesPerAppend-1)/c.cfg.MaxEntriesPerAppend)
	t.Logf("  heal to convergence: %s ms (%.0f entries/sec)", ms(elapsed), float64(behind)/elapsed.Seconds())
}

// buildLog writes n command entries into a fresh store directory and closes it,
// returning the directory and the commands in index order.
func buildLog(t *testing.T, dir string, n int, keySpace int) [][]byte {
	t.Helper()

	st, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}

	val := payload(valueSize)
	cmds := make([][]byte, 0, n)
	const batch = 256
	buf := make([]raft.LogEntry, 0, batch)

	for i := 0; i < n; i++ {
		cmd := kv.Command{
			Op:       kv.OpPut,
			ClientID: "c" + strconv.Itoa(i%64),
			SeqNo:    uint64(i/64 + 1),
			Key:      "k" + strconv.Itoa(i%keySpace),
			Value:    val,
		}
		enc, err := cmd.Encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		cmds = append(cmds, enc)
		buf = append(buf, raft.LogEntry{
			Index: raft.Index(i + 1), Term: 1, Kind: raft.EntryCommand, Data: enc,
		})
		if len(buf) == batch {
			if err := st.Append(buf); err != nil {
				t.Fatalf("append: %v", err)
			}
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		if err := st.Append(buf); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return cmds
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// TestMeasureWALRecovery times storage.Open as a function of log length.
//
// Open is on the critical path of every node start, and it is not a constant: it
// replays the whole WAL, verifying a checksum per record and building the
// in-memory index. This is the measurement that justifies snapshotting existing
// at all, so the relationship to log length matters more than any single value —
// three sizes an order of magnitude apart show whether it is linear.
func TestMeasureWALRecovery(t *testing.T) {
	requireScenarios(t)

	const reps = 3
	for _, n := range []int{1000, 10000, 50000} {
		dir := filepath.Join(t.TempDir(), "n"+strconv.Itoa(n))
		buildLog(t, dir, n, n)
		size := dirBytes(t, dir)

		samples := make([]time.Duration, 0, reps)
		for r := 0; r < reps; r++ {
			start := time.Now()
			st, err := storage.Open(dir)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("Open(%d entries): %v", n, err)
			}
			if got := st.LastIndex(); got != raft.Index(n) {
				t.Fatalf("recovered LastIndex %d, want %d", got, n)
			}
			if tb := st.TruncatedBytes(); tb != 0 {
				t.Fatalf("clean shutdown still discarded %d bytes", tb)
			}
			_ = st.Close()
			samples = append(samples, elapsed)
		}

		s := summarize(samples)
		t.Logf("WAL recovery, %6d entries, %7.2f MiB on disk: median %s ms (min %s, max %s over %d opens), %.0f entries/ms",
			n, float64(size)/(1<<20), ms(s.p50), ms(s.min), ms(s.max), reps,
			float64(n)/(float64(s.p50)/float64(time.Millisecond)))
	}
}

// TestMeasureSnapshotVsReplay is the headline storage result: the same state,
// reached two ways, timed.
//
// Full replay is what a node without a snapshot must do — open the log and feed
// every entry through the state machine. The snapshot path opens a compacted log
// and calls Restore once. The comparison is only honest if both paths genuinely
// end at the same state, so the two state machines' serialized snapshots are
// compared byte for byte before any timing is reported; kv.Store.Snapshot sorts
// its keys, which is what makes that comparison meaningful.
func TestMeasureSnapshotVsReplay(t *testing.T) {
	requireScenarios(t)

	const (
		entries  = 50000
		keySpace = 10000
	)

	root := t.TempDir()
	replayDir := filepath.Join(root, "replay")
	snapDir := filepath.Join(root, "snapshot")

	cmds := buildLog(t, replayDir, entries, keySpace)

	// The snapshot directory starts as an identical log, then has a snapshot
	// published over it. SaveSnapshot compacts, so what remains on disk is the
	// snapshot plus an empty log — exactly the shape a node that snapshotted and
	// then restarted would find.
	buildLog(t, snapDir, entries, keySpace)
	{
		seed := kv.New()
		for i, data := range cmds {
			if _, err := seed.Apply(raft.LogEntry{
				Index: raft.Index(i + 1), Term: 1, Kind: raft.EntryCommand, Data: data,
			}); err != nil {
				t.Fatalf("seeding apply at %d: %v", i+1, err)
			}
		}
		blob, err := seed.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		st, err := storage.Open(snapDir)
		if err != nil {
			t.Fatalf("Open(snapDir): %v", err)
		}
		if err := st.SaveSnapshot(raft.Snapshot{
			Meta: raft.SnapshotMeta{Index: raft.Index(entries), Term: 1}, Data: blob,
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		_ = st.Close()
	}

	replayBytes := dirBytes(t, replayDir)
	snapBytes := dirBytes(t, snapDir)

	const reps = 3
	replaySamples := make([]time.Duration, 0, reps)
	snapSamples := make([]time.Duration, 0, reps)
	var replayState, snapState []byte

	for r := 0; r < reps; r++ {
		// Full replay: open, then stream the log through a fresh state machine in
		// the same chunk size the apply loop would use.
		start := time.Now()
		st, err := storage.Open(replayDir)
		if err != nil {
			t.Fatalf("Open(replayDir): %v", err)
		}
		sm := kv.New()
		for lo := st.FirstIndex(); lo <= st.LastIndex(); lo += 256 {
			hi := lo + 256
			if hi > st.LastIndex()+1 {
				hi = st.LastIndex() + 1
			}
			ents, err := st.Entries(lo, hi)
			if err != nil {
				t.Fatalf("Entries(%d,%d): %v", lo, hi, err)
			}
			for _, e := range ents {
				if _, err := sm.Apply(e); err != nil {
					t.Fatalf("Apply(%d): %v", e.Index, err)
				}
			}
		}
		replaySamples = append(replaySamples, time.Since(start))
		replayState, err = sm.Snapshot()
		if err != nil {
			t.Fatalf("replay Snapshot: %v", err)
		}
		_ = st.Close()

		// Snapshot path: open, load, restore.
		start = time.Now()
		st2, err := storage.Open(snapDir)
		if err != nil {
			t.Fatalf("Open(snapDir): %v", err)
		}
		snap, err := st2.LoadSnapshot()
		if err != nil {
			t.Fatalf("LoadSnapshot: %v", err)
		}
		sm2 := kv.New()
		if err := sm2.Restore(snap.Data); err != nil {
			t.Fatalf("Restore: %v", err)
		}
		snapSamples = append(snapSamples, time.Since(start))
		snapState, err = sm2.Snapshot()
		if err != nil {
			t.Fatalf("snapshot-path Snapshot: %v", err)
		}
		if snap.Meta.Index != raft.Index(entries) {
			t.Fatalf("snapshot covers index %d, want %d", snap.Meta.Index, entries)
		}
		_ = st2.Close()
	}

	if !bytes.Equal(replayState, snapState) {
		t.Fatalf("the two recovery paths did not reach the same state (%d vs %d bytes)",
			len(replayState), len(snapState))
	}

	rs := summarize(replaySamples)
	ss := summarize(snapSamples)
	t.Logf("snapshot recovery versus full-log replay, %d entries over %d keys, verified byte-identical final state",
		entries, keySpace)
	t.Logf("  full replay:   median %s ms  (min %s, max %s)  %7.2f MiB of log on disk",
		ms(rs.p50), ms(rs.min), ms(rs.max), float64(replayBytes)/(1<<20))
	t.Logf("  from snapshot: median %s ms  (min %s, max %s)  %7.2f MiB on disk",
		ms(ss.p50), ms(ss.min), ms(ss.max), float64(snapBytes)/(1<<20))
	t.Logf("  speedup: %.1fx, state %.2f MiB versus %.2f MiB of log",
		float64(rs.p50)/float64(ss.p50), float64(len(snapState))/(1<<20), float64(replayBytes)/(1<<20))
}

// TestMeasureWriteLatencyPercentiles records every write's latency under a
// sustained concurrent load and reports real order statistics.
//
// Percentiles, not an average, because the tail is the number that decides
// whether a client timeout is survivable, and a mean over a queueing system hides
// it completely. Samples are collected per goroutine and merged at the end so the
// recording itself contributes no contention to the thing being measured.
func TestMeasureWriteLatencyPercentiles(t *testing.T) {
	requireScenarios(t)

	const (
		clients  = 32
		duration = 6 * time.Second
	)

	// Shipped default timings, because the tighter test timings let a loaded
	// fsync-bound leader miss its own heartbeat deadline, and an election in the
	// middle of a latency run contaminates the tail with something that is not
	// queueing.
	c := newCluster(t, 3, withDefaultTimings())
	ldr := c.leader()
	val := payload(valueSize)

	if _, err := c.server(ldr).Put("warmup", 1, "warmup", val); err != nil {
		t.Fatalf("warm-up write: %v", err)
	}

	per := make([][]time.Duration, clients)
	var retries atomic.Uint64
	var wg sync.WaitGroup
	start := time.Now()
	stopAt := start.Add(duration)

	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			id := "p" + strconv.Itoa(ci)
			srv := c.server(ldr)
			local := make([]time.Duration, 0, 4096)
			var seq uint64
			for time.Now().Before(stopAt) {
				seq++
				key := id + "-" + strconv.FormatUint(seq, 10)
				// The sample is the whole client-observed wait, retries included:
				// a client that had to find a new leader really did wait that long.
				t0 := time.Now()
				for {
					if _, err := srv.Put(id, seq, key, val); err == nil {
						break
					}
					retries.Add(1)
					next, ok := c.waitForLeader(10 * time.Second)
					if !ok {
						t.Errorf("client %d write %d: no leader available", ci, seq)
						return
					}
					srv = c.server(next)
				}
				local = append(local, time.Since(t0))
			}
			per[ci] = local
		}(ci)
	}
	wg.Wait()
	wall := time.Since(start)
	if t.Failed() {
		return
	}

	var all []time.Duration
	for _, p := range per {
		all = append(all, p...)
	}
	s := summarize(all)

	t.Logf("write latency under sustained load, 3-node cluster, %d concurrent clients, %v of traffic",
		clients, duration.Round(time.Second))
	t.Logf("  %d writes in %s ms => %.0f writes/sec aggregate", s.n, ms(wall), float64(s.n)/wall.Seconds())
	t.Logf("  min %s  p50 %s  p95 %s  p99 %s  p99.9 %s  max %s  mean %s  (ms)",
		ms(s.min), ms(s.p50), ms(s.p95), ms(s.p99), ms(s.p999), ms(s.max), ms(s.mean))
	t.Logf("  %d writes needed a retry against a re-resolved leader", retries.Load())
	t.Logf("  note: with %d clients in flight these latencies include queueing behind "+
		"other clients' writes, so they are well above the single-client figure from BenchmarkWriteThreeNode",
		clients)
}
