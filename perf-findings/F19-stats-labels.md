# F19-stats-labels: zero-alloc multi-label stats updates

## Verdict
Do it. It is small and self-contained in go/stats, the public API does not change, output keys stay byte-identical, and it removes 1 alloc (16-32 B) from every CountersWithMultiLabels.Add/Reset, GaugesWithMultiLabels.Set and MultiTimings.Add/Record on an existing key.

## Finding re-verification
- `safeJoinLabels` (go/stats/export.go) builds a new string with strings.Builder for >=2 labels, so it always makes 1 alloc. The byte-by-byte copy only happens when a label contains '.'; otherwise it scans first and then uses WriteString. The 1-label case was already zero-alloc (strings.ReplaceAll returns the input when there is no '.').
- Where the key is used:
  - `counters.counts map[string]*atomic.Int64`, looked up under RWMutex.RLock (getOrCreate).
  - `Timings.histograms map[string]*Histogram`, looked up under RLock.
  - Both are plain map index expressions, so the `m[string(b)]` no-alloc conversion applies.
- Other consumers of the key:
  - Prometheus/OpenTSDB/statsd backends read `Counts()`/`Histograms()` at scrape time. They have no per-Add callbacks.
  - The one per-Add consumer is `defaultStatsdHook.timerHook` in Timings.Add, which needs a string. The new byte path falls back to `Add(string(key))` only when that hook is registered and the Timings is named. Statsd users therefore keep today's 1 alloc and nothing else changes.
  - The histogram hook gets no label.
- `--stats_combine_dimensions`: handled inside the join through combinedLabels (the value becomes "all"), and ported unchanged. `Timings.labelCombined` is always false for MultiTimings, but the byte path still routes through Add() if it is set.
- `--stats_drop_variables` only affects publish(), not the join.

## Change (go/stats, ~95 LOC prod plus tests)
- export.go:
  - New `appendSafeJoinedLabels(dst []byte, labels, combined) []byte`, which uses `strings.IndexByte` and bulk appends.
  - `appendSafeLabel` now appends to `[]byte`.
  - `safeJoinLabels` keeps its 0/1-label fast paths and otherwise does `string(appendSafeJoinedLabels(stackbuf[:0], ...))`. It is now used only by GetLabelName and NewMultiTimings.
  - New const `labelKeyBufSize = 128`.
- counters.go:
  - New `getOrCreateBytes(key []byte)`, which does an RLock lookup with `c.counts[string(key)]` and copies the key into a string only on insert.
  - The slow path is split out as `create(key string)` and shared with getOrCreate.
  - CountersWithMultiLabels.Add/Reset and GaugesWithMultiLabels.Set use a `[128]byte` stack buffer.
- timings.go:
  - New `addBytes(name []byte, elapsed)`, which does a no-alloc lookup and falls back to Add(string) for statsd/labelCombined.
  - Add is refactored into `createHistogram` and `record` helpers, with the same semantics.
  - MultiTimings.Add/Record use addBytes. Record computes time.Since itself, which is equivalent to before.
- `go build -gcflags=-m` confirms that no `buf` is moved to heap and that `getOrCreateBytes`'s key does not leak.

## Benchmarks (measured; interleaved old/new test binaries; shared noisy 4 vCPU)
### Uncontended, -cpu=1, n=10
```
CountersWithMultiLabelsAdd/2labels    64.96n ± 15%   35.00n ± 10%  -46.13% (p=0.000)
CountersWithMultiLabelsAdd/3labels    80.44n ± 14%   41.31n ± 13%  -48.64% (p=0.000)
CountersWithMultiLabelsAdd/4labels    98.55n ± 19%   43.92n ± 16%  -55.44% (p=0.000)
MultiTimingsAdd/2labels               81.34n ±  7%   49.88n ± 14%  -38.67% (p=0.000)
MultiTimingsAdd/3labels               93.31n ±  6%   49.98n ± 24%  -46.44% (p=0.000)
MultiTimingsAdd/4labels              109.20n ± 11%   63.35n ± 10%  -41.98% (p=0.000)
geomean                               86.77n         46.44n        -46.48%
allocs/op: 1 -> 0 in all cases; B/op 16/24/32 -> 0
```

### RunParallel on the same key, -cpu=4, n=8
There is no significant change: ~230-280 ns in both versions, with ±50-80% noise. Contention on the RWMutex readerCount cache line and on the shared atomic dominates here. The saving under contention is GC and allocation pressure, not latency.

## End-to-end estimate (estimated, not measured)
### vtgate, per single-shard Execute
- vtgate.go: timings.Record plus rowsReturned, rowsAffected and queryTextCharsProcessed, which is 4 calls.
- executor.go:1571-1574: queryExecutions, queryRoutes, and queryExecutionsByTable per table, which is 3 or more calls.
- scatter_conn.go: timings.Record per shard call, which is 1 or more calls.
- errorCounts adds 1 more call on errors.

That is about 8 multi-label updates per query, so about 8 allocs and ~170 B less per query and roughly 250-350 ns of CPU. A vtgate query costs tens of µs, so this is about 0.5-1% of vtgate CPU, plus less GC work. Scatter queries add one call per shard.

### vttablet, per query
- query_engine.AddStats: 5-8 calls per table.
- UserTableQueryCount and UserTableQueryTimesNs: 2 calls per table.
- TableACL: 1 call.

That is about 8-11 calls per query, a similar saving.

## Gotchas
- **Statsd timer hook:** when it is registered, MultiTimings still allocates the string (the hook API takes a string). This is intentional and tested.
- **Long keys:** keys longer than 128 bytes (long table names or usernames) spill to the heap. That is 1 alloc, the same as today. The 128 B stack buffer per call is negligible.
- **Compiler dependency:** the change relies on the compiler's `m[string(b)]` no-alloc lookup. The lookup must stay a direct index expression; passing `string(b)` to a helper would allocate. The AllocsPerRun test guards this.
- **Pre-existing bug, not touched:** `Timings.Reset` replaces `t.histograms` while holding only RLock, which is a data race with concurrent Add. It is test-only usage.
- **Unchanged behaviour:** GaugesWithMultiLabels ignores combined dimensions, as before.
- **Release compatibility:** no API, flag or output change. Key bytes are identical, which the equivalence test covers.
- **Portability:** portable; no unsafe and no arch-specific code.

## Tests
- `go test ./go/stats/...` passes, including prometheusbackend, statsd and opentsdb.
- `go test -race ./go/stats` passes.
- go vet is clean. vtgate and tabletserver build.
- New in go/stats/multilabel_key_test.go:
  - Equivalence of appendSafeJoinedLabels and safeJoinLabels against a strings.ReplaceAll+Join reference over 11^3 label triples × 4 combined masks (dots in all positions, empty labels, 200-byte labels). It also checks that dst is not clobbered.
  - `TestMultiLabelUpdatesDoNotAllocate`: AllocsPerRun==0 for Counters Add/Reset, Gauges Set and MultiTimings Add. This fails on main, where each is 1 alloc.
  - Long keys over 128 bytes for counters and timings.
  - The statsd timer hook receives the same joined name from MultiTimings Add/Record.
- New benchmarks in go/stats/multilabel_bench_test.go (2/3/4 labels, serial and parallel). These are optional for upstreaming; `BenchmarkMultiCounters` already covers the parallel 3-label case.

Patch: findings/F19-stats-labels.patch
