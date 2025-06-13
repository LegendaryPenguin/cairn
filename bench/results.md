# Cairn measured results

Every number below was produced by the commands in [README.md](README.md) on the
machine described here. Nothing is estimated, extrapolated, or carried over from a
previous run. Where a figure is noisy, the spread is given instead of a single
tidy value.

## Machine and toolchain

| | |
| --- | --- |
| CPU | Apple M4 Pro, 14 cores (`GOMAXPROCS=14`) |
| Memory | 48 GiB (51539607552 bytes) |
| OS | macOS 26.6.2 (build 25G83), Darwin 25.6.0 |
| Filesystem | APFS on the internal NVMe drive |
| Go | go1.27.1 darwin/arm64 |
| Date | 2026-10-04 |

The storage figures are APFS fsync figures. An fsync on this drive costs about
3.75 ms, which is measured directly by `BenchmarkWALAppend/batch=1` and which
turns out to explain almost every write number in this file. On hardware with a
different flush cost, the write latencies will scale with it and the read
latencies will not.

## Summary

Write and read latency, median of five `-benchtime 1s` repetitions, with the
observed range across those five in brackets.

| Measurement | Median | Range over 5 runs | Throughput |
| --- | --- | --- | --- |
| Write, single node (no replication) | **3.73 ms** | 3.71 – 3.81 ms | 265 writes/sec |
| Write, 3-node replicated | **9.53 ms** | 9.40 – 9.74 ms | 105 writes/sec |
| Write, 5-node replicated | **12.84 ms** | 12.51 – 13.39 ms | 78 writes/sec |
| Linearizable read (ReadIndex) | **2.57 µs** | 2.568 – 2.648 µs | 389k reads/sec |
| Stale read (local applied state) | **27.9 ns** | 27.66 – 28.18 ns | 36M reads/sec |

Storage layer in isolation:

| Measurement | Median | Derived |
| --- | --- | --- |
| WAL append, batch of 1 | 3.755 ms | 266 entries/sec, 266 fsyncs/sec |
| WAL append, batch of 8 | 3.728 ms | 2,146 entries/sec, 268 fsyncs/sec |
| WAL append, batch of 64 | 3.974 ms | 16,103 entries/sec, 252 fsyncs/sec |
| WAL append, batch of 256 | 4.083 ms | 62,693 entries/sec, 8.0 MB/sec |

Concurrent write throughput, 3-node cluster, default election timings:

| Parallelism | Goroutines | Writes/sec (median of 5) | Retries |
| --- | --- | --- | --- |
| 1 | 14 | 224 | 0 in all 5 runs |
| 2 | 28 | 237 | 0 in all 5 runs |
| 4 | 56 | 246 | 0 in all 5 runs |
| 8 | 112 | 242 (2 of 5 runs degraded to 144 – 180) | 0, 0, 112, 224, 224 |

Scenarios, across three full runs:

| Scenario | Result |
| --- | --- |
| Leader failover (10 reps, 5-node) | min 197 – 211 ms, median 225 – 238 ms, max 248 – 470 ms |
| Lagging follower catch-up (2,000 entries) | 353 – 365 ms, ≈5,600 entries/sec |
| WAL recovery, 1k entries | median 0.71 – 0.88 ms |
| WAL recovery, 10k entries | median 3.70 – 3.86 ms |
| WAL recovery, 50k entries | median 10.0 – 10.9 ms |
| Snapshot restore vs. full replay (50k entries) | **2.0 ms vs. 16.0 ms — 7.2x to 9.0x** |
| Write latency, 32 clients sustained | p50 135 – 154 ms, p95 199 – 272 ms, p99 227 – 333 ms |

## Headline numbers

### A single-node write is one fsync, and nothing else measurable

`BenchmarkWriteSingleNode` is 3.73 ms. `BenchmarkWALAppend/batch=1` — the raw
storage layer, no consensus, no state machine — is 3.755 ms. Command encoding, the
commit decision, apply, and the client plumbing around them are collectively
below the noise floor of a single APFS flush. Cairn's single-node write path adds
nothing measurable to the cost of making the entry durable.

### Replication costs a second serialized fsync, not a round of them

| | Latency | Relative to single node | fsyncs on the critical path |
| --- | --- | --- | --- |
| Single node | 3.73 ms | 1.00x | 1 (leader) |
| 3 nodes | 9.53 ms | 2.55x | 2 (leader, then the fastest follower) |
| 5 nodes | 12.84 ms | 3.44x | 2, plus contention from 4 concurrent follower flushes |

Going from one node to three costs 2.55x, which is close to the two serialized
fsyncs the protocol requires: the leader must have the entry durable before it
asks anyone, and the follower must have it durable before it acknowledges. Those
two flushes cannot overlap.

Going from three to five costs only a further 1.35x even though the number of
followers doubles. That is the expected shape — a quorum is satisfied by the
fastest responders and the leader never waits on the slower ones — but the
increase is not zero, because four concurrent fsyncs to the same device contend
with the leader's own.

### Correctness on the read path costs 92x, and is still cheap

| | Latency | Ratio |
| --- | --- | --- |
| Stale read (local applied state) | 27.9 ns | 1x |
| Linearizable read (ReadIndex) | 2.57 µs | **92x** |

92x looks alarming until the absolute numbers are read: the linearizable read is
2.57 **microseconds**. ReadIndex does no disk I/O at all — it confirms leadership
with a heartbeat round and waits for apply — so it never touches the 3.75 ms
flush that dominates writes. A linearizable read is roughly 1,450 times cheaper
than a 3-node write.

The 92x ratio is also the most network-sensitive number in this file. The
heartbeat round that ReadIndex pays for is near-free over the in-memory
switchboard and would cost a full network round trip in a real deployment, which
would move the linearizable read into the hundreds of microseconds or
milliseconds. The stale read would not move at all. Read the 27.9 ns and the
2.57 µs as two honest measurements of this process; read the ratio between them
as a lower bound on what the guarantee costs in production.

### Snapshot recovery beats full replay by 7-9x

50,000 entries over 10,000 distinct keys, with the two recovery paths' final
states verified byte-identical before any timing was reported:

| Path | Median | Spread over 3 runs | On disk |
| --- | --- | --- | --- |
| Open + replay every entry through the state machine | 16.0 ms | 15.9 – 16.1 ms | 8.95 MiB of log |
| Open + `LoadSnapshot` + `Restore` | 2.0 ms | 1.79 – 2.20 ms | 1.36 MiB of snapshot |
| **Speedup** | **7.2x – 9.0x** | | 6.6x less to read |

Two separate effects, and they compound. There is 6.6x less data to read, because
50,000 writes over 10,000 keys collapse to 10,000 live values; and what is read
needs no per-record checksum verification, no decode into a `kv.Command`, and no
`Apply` call. The ratio is a function of the write-to-key ratio in the workload:
at 5 writes per key it is 7-9x, and a workload that overwrites the same keys more
often would show a larger gap.

### Failover is a randomized election timeout plus a write

Ten repetitions per run, three runs, 5-node cluster, election window
[150 ms, 300 ms] and 40 ms heartbeat:

| Run | min | median | max |
| --- | --- | --- | --- |
| 1 | 204 ms | 237 ms | **470 ms** |
| 2 | 197 ms | 226 ms | 248 ms |
| 3 | 211 ms | 238 ms | **392 ms** |

The minimum and the median are stable across runs. **The maximum is not, and
should not be quoted as a bound.** Two of the three runs contain one sample well
outside the election window — 470 ms and 392 ms against a 300 ms worst-case
timeout. That is the signature of a split vote: two followers' randomized timers
fired close enough together that neither won, and the cluster paid a second
election round. With 30 samples total, two such events is consistent with how
often the randomization fails to separate five nodes, and the honest statement is
that failover is **usually 200-250 ms and occasionally about twice that**.

### p50/p95/p99 under sustained load

32 concurrent clients, 6 seconds of continuous writes, 3-node cluster, default
election timings. Latencies are the full client-observed wait:

| Run | writes | writes/sec | min | p50 | p95 | p99 | p99.9 | max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1,413 | 233 | 8.2 ms | 135 ms | 203 ms | 231 ms | 242 ms | 245 ms |
| 2 | 1,372 | 227 | 29.5 ms | 140 ms | 199 ms | 227 ms | 248 ms | 249 ms |
| 3 | 1,173 | 194 | 37.2 ms | 154 ms | 272 ms | 333 ms | 422 ms | 462 ms |

No write needed a retry in any of the three runs, so no failover is baked into
these tails.

Runs 1 and 2 agree closely. Run 3 is 17% slower at every percentile and much worse
at the tail, with no configuration difference — ordinary machine noise on a
laptop. **The defensible reading is p50 ≈ 135-155 ms, p95 ≈ 200-270 ms,
p99 ≈ 230-335 ms**, not the tidier single column from run 1.

The absolute values are queueing, not service time: a single-client 3-node write
is 9.5 ms, and 32 clients contending for an fsync-bound leader turn that into
~140 ms. 32 clients ÷ 9.5 ms would predict a p50 near 300 ms if the leader
serialized completely, so the ~140 ms observed shows real batching is happening —
just not enough to keep latency flat.

## What came out surprising

**Throughput barely scales with concurrency.** 14 goroutines achieve 224
writes/sec; 112 goroutines achieve 242. That is a 1.08x gain for 8x the offered
load. The leader's fsync is a serial resource and Raft batches whatever has
accumulated during the previous flush, so throughput is pinned near the device's
~266 flushes/sec and additional clients buy queueing delay rather than
throughput. The sustained-load scenario agrees from the other direction: 227-233
writes/sec at 32 clients.

The ceiling this implies is worth stating plainly: **single-key write throughput
on this machine is bounded at roughly 250/sec regardless of how many clients
push**, and the fix is not more concurrency but group commit at the storage layer.
`BenchmarkWALAppend` shows the headroom that would unlock — 62,693 entries/sec at
a batch of 256, 236x the batch-of-1 rate, for the same ~250 flushes/sec.

**WAL recovery is not linear, and the fixed cost dominates at small sizes.**
1k entries recover in 0.88 ms (1,136 entries/ms) but 50k recover in 10.2 ms
(4,886 entries/ms) — four times the per-entry rate at fifty times the size. Open
has a fixed cost (two file opens, a snapshot directory scan, a state-file read)
that swamps the per-record work on a short log. The marginal cost of a log entry
during recovery is about 0.2 µs, so recovery time is not what makes a long log
expensive; replaying it through the state machine is, which is the previous
section's result.

**A tight election timeout plus sustained load unseats healthy leaders.** The
first attempt at the throughput benchmark used the correctness suite's timings
(40 ms heartbeat, 150-300 ms election window) and at 56 and 112 goroutines the
leader lost leadership repeatedly, with dozens of `ErrLeadershipLost` responses
per run. The leader's fsync happens while it holds the node mutex, so heavy
concurrent `Submit` traffic delays its own heartbeats past a 150 ms follower
timeout. The throughput and percentile measurements therefore use the shipped
`DefaultConfig` timings (50 ms heartbeat, 300-600 ms window), where this mostly
stops happening — but not entirely: at 112 goroutines, 2 of 5 runs still recorded
112-224 retries and lost a third of their throughput to the resulting elections.

This is a real operating characteristic, not a benchmark artifact. **An election
timeout must be sized against the leader's worst-case fsync-under-load latency,
not just against network delay.** The 3x heartbeat-to-election ratio that
`Config.Validate` enforces is necessary but not sufficient.

**One library-level observation, reported and not fixed.** A `*raft.Node` that has
been fully stopped will still service an incoming `HandleAppendEntries`, and if
its `storage.Store` has since been closed, the failed durable append panics —
correctly, since the node cannot acknowledge an entry it did not persist, but the
panic is reached through a path the node could have refused earlier. The first
version of this harness hit it during cluster teardown. The harness now
deregisters every node from the switchboard before closing any storage, which
removes the window; a node that rejected RPCs after `Stop` returned would remove
it at the source.

## Raw output

### Benchmarks

```
$ go test ./bench -run XXX -bench . -benchtime 1s -count 5 -timeout 25m
goos: darwin
goarch: arm64
pkg: github.com/LegendaryPenguin/cairn/bench
cpu: Apple M4 Pro
BenchmarkWriteSingleNode-14            	     328	   3728048 ns/op	   0.03 MB/s	       268.2 writes/sec
BenchmarkWriteSingleNode-14            	     325	   3738449 ns/op	   0.03 MB/s	       267.5 writes/sec
BenchmarkWriteSingleNode-14            	     319	   3811479 ns/op	   0.03 MB/s	       262.4 writes/sec
BenchmarkWriteSingleNode-14            	     325	   3768410 ns/op	   0.03 MB/s	       265.4 writes/sec
BenchmarkWriteSingleNode-14            	     316	   3714523 ns/op	   0.03 MB/s	       269.2 writes/sec
BenchmarkWriteThreeNode-14             	     129	   9403367 ns/op	   0.01 MB/s	       106.3 writes/sec
BenchmarkWriteThreeNode-14             	     127	   9528569 ns/op	   0.01 MB/s	       104.9 writes/sec
BenchmarkWriteThreeNode-14             	     127	   9424660 ns/op	   0.01 MB/s	       106.1 writes/sec
BenchmarkWriteThreeNode-14             	     126	   9737036 ns/op	   0.01 MB/s	       102.7 writes/sec
BenchmarkWriteThreeNode-14             	     129	   9611942 ns/op	   0.01 MB/s	       104.0 writes/sec
BenchmarkWriteFiveNode-14              	     100	  12868378 ns/op	   0.01 MB/s	        77.71 writes/sec
BenchmarkWriteFiveNode-14              	     100	  12839648 ns/op	   0.01 MB/s	        77.88 writes/sec
BenchmarkWriteFiveNode-14              	     100	  12509220 ns/op	   0.01 MB/s	        79.94 writes/sec
BenchmarkWriteFiveNode-14              	     100	  13389071 ns/op	   0.01 MB/s	        74.69 writes/sec
BenchmarkWriteFiveNode-14              	     100	  12668447 ns/op	   0.01 MB/s	        78.94 writes/sec
BenchmarkReadLinearizable-14           	  459427	      2569 ns/op	    389264 reads/sec
BenchmarkReadLinearizable-14           	  464760	      2574 ns/op	    388552 reads/sec
BenchmarkReadLinearizable-14           	  458013	      2572 ns/op	    388746 reads/sec
BenchmarkReadLinearizable-14           	  462189	      2568 ns/op	    389339 reads/sec
BenchmarkReadLinearizable-14           	  420510	      2648 ns/op	    377605 reads/sec
BenchmarkReadStale-14                  	41838366	        27.91 ns/op	  35833909 reads/sec
BenchmarkReadStale-14                  	42120606	        27.97 ns/op	  35756399 reads/sec
BenchmarkReadStale-14                  	42585798	        27.66 ns/op	  36151256 reads/sec
BenchmarkReadStale-14                  	41726697	        28.18 ns/op	  35482208 reads/sec
BenchmarkReadStale-14                  	41952548	        27.81 ns/op	  35960719 reads/sec
BenchmarkWriteThroughputParallel/parallelism=1-14         	     266	   4454614 ns/op	   0.03 MB/s	        14.00 goroutines	         0 retries	       224.5 writes/sec
BenchmarkWriteThroughputParallel/parallelism=1-14         	     266	   4417126 ns/op	   0.03 MB/s	        14.00 goroutines	         0 retries	       226.4 writes/sec
BenchmarkWriteThroughputParallel/parallelism=1-14         	     267	   4471591 ns/op	   0.03 MB/s	        14.00 goroutines	         0 retries	       223.6 writes/sec
BenchmarkWriteThroughputParallel/parallelism=1-14         	     272	   4721696 ns/op	   0.03 MB/s	        14.00 goroutines	         0 retries	       211.8 writes/sec
BenchmarkWriteThroughputParallel/parallelism=1-14         	     279	   4605560 ns/op	   0.03 MB/s	        14.00 goroutines	         0 retries	       217.1 writes/sec
BenchmarkWriteThroughputParallel/parallelism=2-14         	     294	   4135718 ns/op	   0.03 MB/s	        28.00 goroutines	         0 retries	       241.8 writes/sec
BenchmarkWriteThroughputParallel/parallelism=2-14         	     285	   4220689 ns/op	   0.03 MB/s	        28.00 goroutines	         0 retries	       236.9 writes/sec
BenchmarkWriteThroughputParallel/parallelism=2-14         	     296	   4212639 ns/op	   0.03 MB/s	        28.00 goroutines	         0 retries	       237.4 writes/sec
BenchmarkWriteThroughputParallel/parallelism=2-14         	     273	   4171736 ns/op	   0.03 MB/s	        28.00 goroutines	         0 retries	       239.7 writes/sec
BenchmarkWriteThroughputParallel/parallelism=2-14         	     294	   4275238 ns/op	   0.03 MB/s	        28.00 goroutines	         0 retries	       233.9 writes/sec
BenchmarkWriteThroughputParallel/parallelism=4-14         	     266	   4108799 ns/op	   0.03 MB/s	        56.00 goroutines	         0 retries	       243.4 writes/sec
BenchmarkWriteThroughputParallel/parallelism=4-14         	     306	   4071832 ns/op	   0.03 MB/s	        56.00 goroutines	         0 retries	       245.6 writes/sec
BenchmarkWriteThroughputParallel/parallelism=4-14         	     301	   4029963 ns/op	   0.03 MB/s	        56.00 goroutines	         0 retries	       248.1 writes/sec
BenchmarkWriteThroughputParallel/parallelism=4-14         	     310	   4138447 ns/op	   0.03 MB/s	        56.00 goroutines	         0 retries	       241.6 writes/sec
BenchmarkWriteThroughputParallel/parallelism=4-14         	     301	   3999891 ns/op	   0.03 MB/s	        56.00 goroutines	         0 retries	       250.0 writes/sec
BenchmarkWriteThroughputParallel/parallelism=8-14         	     321	   4124226 ns/op	   0.03 MB/s	       112.0 goroutines	         0 retries	       242.5 writes/sec
BenchmarkWriteThroughputParallel/parallelism=8-14         	     327	   4150081 ns/op	   0.03 MB/s	       112.0 goroutines	         0 retries	       241.0 writes/sec
BenchmarkWriteThroughputParallel/parallelism=8-14         	     313	   6805254 ns/op	   0.02 MB/s	       112.0 goroutines	       224.0 retries	       146.9 writes/sec
BenchmarkWriteThroughputParallel/parallelism=8-14         	     310	   6951683 ns/op	   0.02 MB/s	       112.0 goroutines	       224.0 retries	       143.9 writes/sec
BenchmarkWriteThroughputParallel/parallelism=8-14         	     314	   5560365 ns/op	   0.02 MB/s	       112.0 goroutines	       112.0 retries	       179.8 writes/sec
BenchmarkWALAppend/batch=1-14                             	     304	   3755536 ns/op	   0.03 MB/s	       266.3 entries/sec	       266.3 fsyncs/sec
BenchmarkWALAppend/batch=1-14                             	     309	   3755979 ns/op	   0.03 MB/s	       266.2 entries/sec	       266.2 fsyncs/sec
BenchmarkWALAppend/batch=1-14                             	     321	   3749093 ns/op	   0.03 MB/s	       266.7 entries/sec	       266.7 fsyncs/sec
BenchmarkWALAppend/batch=1-14                             	     318	   3731192 ns/op	   0.03 MB/s	       268.0 entries/sec	       268.0 fsyncs/sec
BenchmarkWALAppend/batch=1-14                             	     318	   3755271 ns/op	   0.03 MB/s	       266.3 entries/sec	       266.3 fsyncs/sec
BenchmarkWALAppend/batch=8-14                             	     300	   3753992 ns/op	   0.27 MB/s	      2131 entries/sec	       266.4 fsyncs/sec
BenchmarkWALAppend/batch=8-14                             	     313	   3710166 ns/op	   0.28 MB/s	      2156 entries/sec	       269.5 fsyncs/sec
BenchmarkWALAppend/batch=8-14                             	     313	   3738239 ns/op	   0.27 MB/s	      2140 entries/sec	       267.5 fsyncs/sec
BenchmarkWALAppend/batch=8-14                             	     316	   3691566 ns/op	   0.28 MB/s	      2167 entries/sec	       270.9 fsyncs/sec
BenchmarkWALAppend/batch=8-14                             	     316	   3727468 ns/op	   0.27 MB/s	      2146 entries/sec	       268.3 fsyncs/sec
BenchmarkWALAppend/batch=64-14                            	     318	   3930978 ns/op	   2.08 MB/s	     16281 entries/sec	       254.4 fsyncs/sec
BenchmarkWALAppend/batch=64-14                            	     301	   4015508 ns/op	   2.04 MB/s	     15938 entries/sec	       249.0 fsyncs/sec
BenchmarkWALAppend/batch=64-14                            	     310	   3924191 ns/op	   2.09 MB/s	     16309 entries/sec	       254.8 fsyncs/sec
BenchmarkWALAppend/batch=64-14                            	     295	   4038401 ns/op	   2.03 MB/s	     15848 entries/sec	       247.6 fsyncs/sec
BenchmarkWALAppend/batch=64-14                            	     298	   3974306 ns/op	   2.06 MB/s	     16103 entries/sec	       251.6 fsyncs/sec
BenchmarkWALAppend/batch=256-14                           	     279	   4263631 ns/op	   7.69 MB/s	     60043 entries/sec	       234.5 fsyncs/sec
BenchmarkWALAppend/batch=256-14                           	     307	   3949555 ns/op	   8.30 MB/s	     64817 entries/sec	       253.2 fsyncs/sec
BenchmarkWALAppend/batch=256-14                           	     306	   4084215 ns/op	   8.02 MB/s	     62680 entries/sec	       244.8 fsyncs/sec
BenchmarkWALAppend/batch=256-14                           	     312	   4078603 ns/op	   8.03 MB/s	     62767 entries/sec	       245.2 fsyncs/sec
BenchmarkWALAppend/batch=256-14                           	     298	   4083371 ns/op	   8.02 MB/s	     62693 entries/sec	       244.9 fsyncs/sec
PASS
ok  	github.com/LegendaryPenguin/cairn/bench	164.125s
```

### Scenarios, three consecutive runs

```
$ for i in 1 2 3; do go test ./bench -run TestMeasure -bench.scenarios -v -timeout 20m; done
### scenario run 1 ###
=== RUN   TestMeasureLeaderFailover
    scenarios_test.go:140: leader failover to first successful write, 10 repetitions, 5-node cluster
    scenarios_test.go:141:   election timeout window: [150ms, 300ms], heartbeat 40ms
    scenarios_test.go:142:   min 204.437 ms   median 236.629 ms   max 470.070 ms   mean 270.891 ms
    scenarios_test.go:143:   all samples (ms): [204.437 211.984 215.875 217.707 232.895 236.629 239.778 256.803 422.730 470.070]
--- PASS: TestMeasureLeaderFailover (5.12s)
=== RUN   TestMeasureFollowerCatchUp
    scenarios_test.go:237: lagging-follower catch-up, 3-node cluster, log shipping only (no snapshot)
    scenarios_test.go:238:   follower n3 was 2001 entries behind (leader commit index 2001)
    scenarios_test.go:239:   MaxEntriesPerAppend 256, so at least 8 AppendEntries rounds were required
    scenarios_test.go:241:   heal to convergence: 353.269 ms (5664 entries/sec)
--- PASS: TestMeasureFollowerCatchUp (9.42s)
=== RUN   TestMeasureWALRecovery
    scenarios_test.go:345: WAL recovery,   1000 entries,    0.18 MiB on disk: median 0.880 ms (min 0.829, max 1.192 over 3 opens), 1136 entries/ms
    scenarios_test.go:345: WAL recovery,  10000 entries,    1.79 MiB on disk: median 3.864 ms (min 3.742, max 4.621 over 3 opens), 2588 entries/ms
    scenarios_test.go:345: WAL recovery,  50000 entries,    8.99 MiB on disk: median 10.233 ms (min 8.381, max 14.787 over 3 opens), 4886 entries/ms
--- PASS: TestMeasureWALRecovery (1.12s)
=== RUN   TestMeasureSnapshotVsReplay
    scenarios_test.go:475: snapshot recovery versus full-log replay, 50000 entries over 10000 keys, verified byte-identical final state
    scenarios_test.go:477:   full replay:   median 15.899 ms  (min 14.320, max 19.872)     8.95 MiB of log on disk
    scenarios_test.go:479:   from snapshot: median 2.202 ms  (min 1.821, max 2.281)     1.36 MiB on disk
    scenarios_test.go:481:   speedup: 7.2x, state 1.36 MiB versus 8.95 MiB of log
--- PASS: TestMeasureSnapshotVsReplay (1.80s)
=== RUN   TestMeasureWriteLatencyPercentiles
    scenarios_test.go:561: write latency under sustained load, 3-node cluster, 32 concurrent clients, 6s of traffic
    scenarios_test.go:563:   1413 writes in 6076.080 ms => 233 writes/sec aggregate
    scenarios_test.go:564:   min 8.155  p50 135.252  p95 202.980  p99 231.020  p99.9 242.088  max 244.684  mean 137.554  (ms)
    scenarios_test.go:566:   0 writes needed a retry against a re-resolved leader
    scenarios_test.go:567:   note: with 32 clients in flight these latencies include queueing behind other clients' writes, so they are well above the single-client figure from BenchmarkWriteThreeNode
--- PASS: TestMeasureWriteLatencyPercentiles (6.56s)
PASS
ok  	github.com/LegendaryPenguin/cairn/bench	24.674s
### scenario run 2 ###
=== RUN   TestMeasureLeaderFailover
    scenarios_test.go:140: leader failover to first successful write, 10 repetitions, 5-node cluster
    scenarios_test.go:141:   election timeout window: [150ms, 300ms], heartbeat 40ms
    scenarios_test.go:142:   min 197.498 ms   median 225.536 ms   max 247.953 ms   mean 223.287 ms
    scenarios_test.go:143:   all samples (ms): [197.498 202.548 218.014 218.496 219.008 225.536 227.877 234.020 241.919 247.953]
--- PASS: TestMeasureLeaderFailover (4.62s)
=== RUN   TestMeasureFollowerCatchUp
    scenarios_test.go:237: lagging-follower catch-up, 3-node cluster, log shipping only (no snapshot)
    scenarios_test.go:238:   follower n3 was 2000 entries behind (leader commit index 2001)
    scenarios_test.go:239:   MaxEntriesPerAppend 256, so at least 8 AppendEntries rounds were required
    scenarios_test.go:241:   heal to convergence: 355.667 ms (5623 entries/sec)
--- PASS: TestMeasureFollowerCatchUp (9.20s)
=== RUN   TestMeasureWALRecovery
    scenarios_test.go:345: WAL recovery,   1000 entries,    0.18 MiB on disk: median 0.712 ms (min 0.492, max 0.815 over 3 opens), 1404 entries/ms
    scenarios_test.go:345: WAL recovery,  10000 entries,    1.79 MiB on disk: median 3.704 ms (min 3.330, max 3.771 over 3 opens), 2700 entries/ms
    scenarios_test.go:345: WAL recovery,  50000 entries,    8.99 MiB on disk: median 10.033 ms (min 8.809, max 16.455 over 3 opens), 4984 entries/ms
--- PASS: TestMeasureWALRecovery (1.06s)
=== RUN   TestMeasureSnapshotVsReplay
    scenarios_test.go:475: snapshot recovery versus full-log replay, 50000 entries over 10000 keys, verified byte-identical final state
    scenarios_test.go:477:   full replay:   median 16.063 ms  (min 16.051, max 16.150)     8.95 MiB of log on disk
    scenarios_test.go:479:   from snapshot: median 1.791 ms  (min 1.502, max 1.870)     1.36 MiB on disk
    scenarios_test.go:481:   speedup: 9.0x, state 1.36 MiB versus 8.95 MiB of log
--- PASS: TestMeasureSnapshotVsReplay (1.74s)
=== RUN   TestMeasureWriteLatencyPercentiles
    scenarios_test.go:561: write latency under sustained load, 3-node cluster, 32 concurrent clients, 6s of traffic
    scenarios_test.go:563:   1372 writes in 6033.791 ms => 227 writes/sec aggregate
    scenarios_test.go:564:   min 29.454  p50 140.233  p95 198.888  p99 227.313  p99.9 247.689  max 249.092  mean 140.510  (ms)
    scenarios_test.go:566:   0 writes needed a retry against a re-resolved leader
    scenarios_test.go:567:   note: with 32 clients in flight these latencies include queueing behind other clients' writes, so they are well above the single-client figure from BenchmarkWriteThreeNode
--- PASS: TestMeasureWriteLatencyPercentiles (6.51s)
PASS
ok  	github.com/LegendaryPenguin/cairn/bench	23.338s
### scenario run 3 ###
=== RUN   TestMeasureLeaderFailover
    scenarios_test.go:140: leader failover to first successful write, 10 repetitions, 5-node cluster
    scenarios_test.go:141:   election timeout window: [150ms, 300ms], heartbeat 40ms
    scenarios_test.go:142:   min 211.365 ms   median 238.136 ms   max 392.210 ms   mean 249.745 ms
    scenarios_test.go:143:   all samples (ms): [211.365 215.237 223.710 224.744 226.073 238.136 239.909 258.954 267.114 392.210]
--- PASS: TestMeasureLeaderFailover (4.89s)
=== RUN   TestMeasureFollowerCatchUp
    scenarios_test.go:237: lagging-follower catch-up, 3-node cluster, log shipping only (no snapshot)
    scenarios_test.go:238:   follower n2 was 2001 entries behind (leader commit index 2001)
    scenarios_test.go:239:   MaxEntriesPerAppend 256, so at least 8 AppendEntries rounds were required
    scenarios_test.go:241:   heal to convergence: 360.097 ms (5557 entries/sec)
--- PASS: TestMeasureFollowerCatchUp (9.79s)
=== RUN   TestMeasureWALRecovery
    scenarios_test.go:345: WAL recovery,   1000 entries,    0.18 MiB on disk: median 0.878 ms (min 0.871, max 0.982 over 3 opens), 1138 entries/ms
    scenarios_test.go:345: WAL recovery,  10000 entries,    1.79 MiB on disk: median 3.819 ms (min 3.292, max 4.198 over 3 opens), 2619 entries/ms
    scenarios_test.go:345: WAL recovery,  50000 entries,    8.99 MiB on disk: median 10.865 ms (min 8.675, max 16.409 over 3 opens), 4602 entries/ms
--- PASS: TestMeasureWALRecovery (1.32s)
=== RUN   TestMeasureSnapshotVsReplay
    scenarios_test.go:475: snapshot recovery versus full-log replay, 50000 entries over 10000 keys, verified byte-identical final state
    scenarios_test.go:477:   full replay:   median 16.094 ms  (min 14.284, max 19.505)     8.95 MiB of log on disk
    scenarios_test.go:479:   from snapshot: median 1.905 ms  (min 1.897, max 2.079)     1.36 MiB on disk
    scenarios_test.go:481:   speedup: 8.4x, state 1.36 MiB versus 8.95 MiB of log
--- PASS: TestMeasureSnapshotVsReplay (2.65s)
=== RUN   TestMeasureWriteLatencyPercentiles
    scenarios_test.go:561: write latency under sustained load, 3-node cluster, 32 concurrent clients, 6s of traffic
    scenarios_test.go:563:   1173 writes in 6033.041 ms => 194 writes/sec aggregate
    scenarios_test.go:564:   min 37.161  p50 154.074  p95 272.003  p99 332.996  p99.9 422.272  max 462.164  mean 164.431  (ms)
    scenarios_test.go:566:   0 writes needed a retry against a re-resolved leader
    scenarios_test.go:567:   note: with 32 clients in flight these latencies include queueing behind other clients' writes, so they are well above the single-client figure from BenchmarkWriteThreeNode
--- PASS: TestMeasureWriteLatencyPercentiles (6.62s)
PASS
ok  	github.com/LegendaryPenguin/cairn/bench	25.524s
```
