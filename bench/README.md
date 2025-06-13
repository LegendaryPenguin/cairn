# Cairn benchmarks

Everything here runs a real cluster: real `storage.Store` directories with real
fsyncs, the real consensus layer, the real `kv` state machine. The only
substitution is the network — the in-memory `transport.Switchboard` with zero
injected latency.

That substitution is the single most important thing to know when reading the
numbers. These are the costs **Cairn itself** imposes — durable log append,
quorum round trips, apply, snapshot restore — with network transit removed. A
deployment over real links adds one round trip of network latency per write and
per linearizable read on top of what is reported here. Nothing in this directory
should be quoted as the latency of a distributed deployment.

Measured values and the machine they came from are in [results.md](results.md).

## Layout

| File | What it is |
| --- | --- |
| `harness_test.go` | Cluster setup: start/crash/restart nodes, resolve the leader, write through it. Deliberately separate from the `tests` package harness, which is test-only and cannot be imported. |
| `bench_test.go` | `go test -bench` microbenchmarks. |
| `scenarios_test.go` | Wall-clock measurements that are not microbenchmarks. Gated behind `-bench.scenarios`. |

## Benchmarks (`bench_test.go`)

All writes use a 128-byte value, so the numbers reflect consensus and fsync cost
rather than payload bandwidth.

| Benchmark | What it measures |
| --- | --- |
| `BenchmarkWriteSingleNode` | The floor. One node, quorum of one, no peer to wait for: command encode, WAL append + fsync, commit, apply. |
| `BenchmarkWriteThreeNode` | Adds the quorum round trip. The leader needs one of two followers, and that follower fsyncs before acknowledging. |
| `BenchmarkWriteFiveNode` | Two of four acknowledgements. Tests whether extra followers cost the leader anything, given that a quorum is satisfied by the fastest responders. |
| `BenchmarkReadLinearizable` | The ReadIndex path: one heartbeat round confirming a quorum still follows this leader, then a wait for `lastApplied` to reach the confirmed commit index. No disk writes. |
| `BenchmarkReadStale` | Local applied state, no coordination. The control: the gap between this and the linearizable figure is the price of the guarantee. |
| `BenchmarkWriteThroughputParallel` | Aggregate write throughput with many clients in flight, at four concurrency levels. |
| `BenchmarkWALAppend` | The storage layer alone, no consensus. Batch sizes 1/8/64/256, because batching is the single largest lever on this path: batch 1 measures device flush latency, batch 256 measures the encoding. |

Two notes on the throughput benchmark:

- Concurrency comes from `b.SetParallelism`, so the goroutine count is the
  multiplier times `GOMAXPROCS`. The actual count is reported as a `goroutines`
  metric rather than left implicit, since the multiplier alone means nothing on a
  different machine.
- Each goroutine uses its own client ID. The state machine deduplicates per
  client on a monotonic sequence number, so goroutines sharing an ID with
  interleaved sequence numbers would have writes silently swallowed as replays.
- A failed write is retried against a re-resolved leader with the same sequence
  number, and the retry count is reported as a `retries` metric. Sustained load
  on an fsync-bound leader does occasionally cost it leadership; hiding that
  inside a throughput figure would be dishonest, and aborting on it would make
  the benchmark unrunnable.

## Scenarios (`scenarios_test.go`)

These are `TestMeasureX` functions that log a report. They are gated because they
are wall-clock measurements, not unit tests: each one deliberately spends seconds
crashing nodes or writing tens of thousands of entries, and none asserts anything
a regression suite should depend on. Without `-bench.scenarios` they skip, so
`go test ./...` stays fast.

| Scenario | What it measures |
| --- | --- |
| `TestMeasureLeaderFailover` | Wall clock from cutting the leader off to a write completing on its successor — election timeout, election, the new leader's no-op commit, and the write itself. 10 repetitions on a 5-node cluster, reported as min/median/max plus every sample. The dominant term is a randomized election timeout, so a single sample would be meaningless. |
| `TestMeasureFollowerCatchUp` | A follower is partitioned off, 2000 entries are written, the partition heals, and the time to converge is measured. The snapshot threshold is raised out of reach so this is pure log shipping via `AppendEntries`, not `InstallSnapshot` — two very different paths that would be uninterpretable mixed into one number. |
| `TestMeasureWALRecovery` | `storage.Open` at 1k, 10k and 50k entries. Open is on the critical path of every node start and is not a constant: it replays the whole WAL, verifying a checksum per record. Three sizes an order of magnitude apart show whether it is linear. |
| `TestMeasureSnapshotVsReplay` | The headline storage result. 50k entries over 10k keys, reached two ways: open-and-replay-every-entry versus open-and-`Restore`. The two resulting state machines' serialized snapshots are compared byte for byte before any timing is reported, so "same state" is verified rather than assumed. |
| `TestMeasureWriteLatencyPercentiles` | Every write's latency under 32 concurrent clients for 6 seconds, with real order statistics (p50/p95/p99/p99.9) computed from the recorded samples. Percentiles rather than an average, because a mean over a queueing system hides exactly the tail that decides whether a client timeout is survivable. |

Percentiles use nearest-rank, not interpolation, so every figure reported is a
duration that was actually observed.

## Reproducing

From the repository root, with Go 1.27 or later:

```sh
# Build and vet, including this package.
go build ./... && go vet ./...

# All benchmarks, five repetitions each. ~3 minutes.
go test ./bench -run XXX -bench . -benchtime 1s -count 5 -timeout 25m

# All scenarios, verbose so the reports are visible. ~25 seconds.
go test ./bench -run TestMeasure -bench.scenarios -v -timeout 20m
```

Narrower runs:

```sh
# Just the write-latency ladder.
go test ./bench -run XXX -bench 'BenchmarkWrite(Single|Three|Five)Node' -count 5

# Just the linearizable-versus-stale read comparison.
go test ./bench -run XXX -bench 'BenchmarkRead' -count 5

# Just the headline storage result.
go test ./bench -run TestMeasureSnapshotVsReplay -bench.scenarios -v
```

`-run XXX` on the benchmark commands matches no test, which keeps the gated
scenarios from running alongside them.

Do not run the benchmarks under `-race`. The race detector's instrumentation
changes the timings being measured, and the correctness suite is where race
cleanliness is established.
