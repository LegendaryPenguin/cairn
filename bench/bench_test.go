package bench

import (
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
	"github.com/LegendaryPenguin/cairn/server"
	"github.com/LegendaryPenguin/cairn/storage"
)

// benchWrites drives sequential writes against the leader of an n-node cluster.
//
// The cluster is built once, outside the timed region, and the leader is resolved
// before the timer starts: cluster startup includes an election and several
// fsyncs, and folding that into b.N iterations would understate large clusters
// (which take longer to start) rather than overstate them.
func benchWrites(b *testing.B, n int) {
	c := newCluster(b, n)
	ldr := c.leader()
	srv := c.server(ldr)
	val := payload(valueSize)

	// One warm-up write forces the new leader's no-op to commit, so the first
	// timed iteration is not paying for the leader's term establishment.
	if _, err := srv.Put("warmup", 1, "warmup", val); err != nil {
		b.Fatalf("warm-up write: %v", err)
	}

	b.SetBytes(int64(valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := srv.Put("bench", uint64(i+1), "k"+strconv.Itoa(i), val); err != nil {
			b.Fatalf("write %d: %v", i, err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
}

// BenchmarkWriteSingleNode is the floor: one node, quorum of one, no peer to
// wait for. What remains is command encoding, the WAL append and its fsync, the
// commit decision, and apply. Every replicated number should be read as this
// plus the cost of agreement.
func BenchmarkWriteSingleNode(b *testing.B) { benchWrites(b, 1) }

// BenchmarkWriteThreeNode adds the quorum round trip: the leader must hear from
// one of two followers, each of which fsyncs the entry before acknowledging.
func BenchmarkWriteThreeNode(b *testing.B) { benchWrites(b, 3) }

// BenchmarkWriteFiveNode needs two of four acknowledgements. The interesting
// question is whether the extra followers cost anything, since a quorum is still
// satisfied by the fastest responders and the slower ones only add work the
// leader does not wait on.
func BenchmarkWriteFiveNode(b *testing.B) { benchWrites(b, 5) }

// BenchmarkReadLinearizable measures the ReadIndex path on a three-node cluster:
// a heartbeat round confirming a quorum still follows this leader, then a wait
// for the state machine to catch up to the confirmed commit index. No disk
// writes are involved, which is the whole point of ReadIndex over committing a
// no-op per read.
func BenchmarkReadLinearizable(b *testing.B) { benchReads(b, server.Linearizable) }

// BenchmarkReadStale reads local applied state with no coordination. It is here
// as the control: the gap between it and the linearizable number is the price of
// the guarantee, and without both measured on the same cluster that price is
// guesswork.
func BenchmarkReadStale(b *testing.B) { benchReads(b, server.Stale) }

func benchReads(b *testing.B, mode server.ReadMode) {
	c := newCluster(b, 3)
	ldr := c.leader()
	srv := c.server(ldr)

	const key = "readkey"
	if _, err := srv.Put("bench", 1, key, payload(valueSize)); err != nil {
		b.Fatalf("seed write: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok, err := srv.Get(key, mode)
		if err != nil {
			b.Fatalf("read %d: %v", i, err)
		}
		if !ok || len(v) != valueSize {
			b.Fatalf("read %d returned ok=%v len=%d", i, ok, len(v))
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/sec")
}

// BenchmarkWriteThroughputParallel measures aggregate write throughput with many
// clients in flight at once.
//
// Concurrency is expressed through b.SetParallelism, so the goroutine count is
// the multiplier times GOMAXPROCS; the actual number is reported as a metric
// rather than left implicit, because the multiplier alone is meaningless on a
// different machine. Each goroutine uses its own client ID: the state machine
// deduplicates per client on a monotonic sequence number, so shared IDs with
// interleaved sequence numbers would have later writes silently swallowed as
// replays.
//
// Failed writes are retried against a freshly resolved leader with the same
// sequence number, and the retries are reported as a metric. Swallowing them
// silently would hide a leadership change inside a throughput figure; aborting on
// the first one would make the benchmark unrunnable at high concurrency, because
// sustained load on a fsync-bound leader does occasionally cost it leadership.
func BenchmarkWriteThroughputParallel(b *testing.B) {
	for _, mult := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("parallelism=%d", mult), func(b *testing.B) {
			c := newCluster(b, 3, withDefaultTimings())
			ldr := c.leader()
			val := payload(valueSize)

			if _, err := c.server(ldr).Put("warmup", 1, "warmup", val); err != nil {
				b.Fatalf("warm-up write: %v", err)
			}

			var clientSeq, retries atomic.Uint64
			goroutines := mult * runtime.GOMAXPROCS(0)

			b.SetParallelism(mult)
			b.SetBytes(int64(valueSize))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := "c" + strconv.FormatUint(clientSeq.Add(1), 10)
				srv := c.server(ldr)
				var seq uint64
				for pb.Next() {
					seq++
					key := id + "-" + strconv.FormatUint(seq, 10)
					for {
						if _, err := srv.Put(id, seq, key, val); err == nil {
							break
						}
						retries.Add(1)
						next, ok := c.waitForLeader(10 * time.Second)
						if !ok {
							b.Errorf("write %s: no leader available", key)
							return
						}
						srv = c.server(next)
					}
				}
			})
			b.StopTimer()

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
			b.ReportMetric(float64(goroutines), "goroutines")
			b.ReportMetric(float64(retries.Load()), "retries")
		})
	}
}

// BenchmarkWALAppend measures the storage layer alone: no consensus, no state
// machine, just entries into the write-ahead log with the fsync that makes the
// Append contract true.
//
// Batch size is varied because that is the single largest lever on this path. A
// batch of one pays a full fsync per entry and so measures device flush latency;
// a batch of 64 amortizes one flush across 64 entries and measures the encoding
// instead. Raft reaches the batched regime whenever more than one client write is
// outstanding, so both numbers describe real operating points.
func BenchmarkWALAppend(b *testing.B) {
	for _, batch := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			st, err := storage.Open(b.TempDir())
			if err != nil {
				b.Fatalf("storage.Open: %v", err)
			}
			b.Cleanup(func() { _ = st.Close() })

			data := payload(valueSize)
			buf := make([]raft.LogEntry, batch)

			b.SetBytes(int64(batch * valueSize))
			b.ResetTimer()
			next := raft.Index(1)
			for i := 0; i < b.N; i++ {
				for j := 0; j < batch; j++ {
					buf[j] = raft.LogEntry{Index: next, Term: 1, Kind: raft.EntryCommand, Data: data}
					next++
				}
				if err := st.Append(buf); err != nil {
					b.Fatalf("append at %d: %v", next, err)
				}
			}
			b.StopTimer()

			elapsed := b.Elapsed().Seconds()
			b.ReportMetric(float64(b.N*batch)/elapsed, "entries/sec")
			b.ReportMetric(float64(b.N)/elapsed, "fsyncs/sec")
		})
	}
}
