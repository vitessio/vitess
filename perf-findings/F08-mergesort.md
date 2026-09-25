# F08 - MergeSort per-row channel + per-row Result

## Verdict
DO IT. Real, self-contained (1 file + tests), 3.4x faster merge for realistic chunk sizes, -99% allocs.

## Current behaviour (verified at aa9ccf9)
go/vt/vtgate/engine/merge_sort.go
- runOneStream (l.218-256): per-shard goroutine; fields sent once on a cap-1 chan; then `for _, row := range qr.Rows { select { case handle.row <- row: case <-ctx.Done(): return io.EOF } }` with row chan cap 10.
- TryStreamExecute (l.69-159): primes loser tree with one row per stream, then per row: `callback(&sqltypes.Result{Rows: [][]sqltypes.Value{row}})` (Result + 1-elem slice alloc) and a `select` receive on the popped stream's chan.
- Callers: Route.mergeSort (streaming scatter with ORDER BY, via vcursor.StreamExecutePrimitive -> Route truncate callback -> wrapCallback -> executor storeResultStats (mutex) -> executor re-buffering callback (mutex) which re-batches rows to 32KB (stream-buffer-size)); vdiff (tabletmanager/vdiff and wrangler) newMergeSorter over shardStreamer, whose output is pushed through ANOTHER channel per Result by primitiveExecutor (so vdiff paid 2 channel hops per row).
- Nothing downstream depends on 1-row results: Limit handles multi-row results (truncates, returns io.EOF); executor re-buffers; vdiff primitiveExecutor iterates qr.Rows. Only observable change: VEXPLAIN TRACE interOpStats (per-callback row counts) would show fewer/larger calls for a streaming MergeSort.

## Change (prototype in worktree)
- Stream goroutine sends one chunk per shard result: `handle.rows <- slices.Clone(qr.Rows)` (chan [][]Value, cap 1). Empty results skipped.
- Merger keeps a per-stream cursor into its current chunk; ReplaceMin from the chunk without touching channels; only when a chunk is exhausted fetch next: non-blocking receive first; if it would block, FLUSH pending output, then blocking select with ctx.Done().
- Output batched into Results of up to mergeSortBatchSize=256 rows; new rows slice per Result (callers retain it: vdiff, executor), capacity = previous batch length (adaptive; avoids 6KB alloc per row when inputs trickle in 1 row at a time).
- Final flush at end; errors/ScatterErrorsAsWarnings/fields logic unchanged.

## Why the Clone is required (aliasing)
vttablet's streaming path uses pooled Results (query_executor.go streamResultPool / returnStreamResult): after the callback returns, `result.Rows[:0]` is put back into the pool and refilled. With vtcombo's internalTabletConn (in-process vtgate+vttablet) that pooled Result reaches MergeSort's callback directly. Individual row []Value are freshly allocated per row (why the old code was safe), but the outer slice is not. TestMergeSortManyRows (reusingShardResult) fails without the Clone (verified by mutation).

## Benchmarks (measured; noisy shared 4 vCPU; benchstat n=8, -benchtime=5x)
BenchmarkMergeSort: N shards x 10000 rows each (int64 + varchar), shard results of `chunk` rows, int64 ORDER BY, no-op consumer. Old impl kept as mergeSortOld in merge_sort_old_bench_test.go.

```
                                │     old      │                 new                 │
                                │   sec/row    │   sec/row     vs base               │
MergeSort/shards=4/chunk=1-4      542.6n ± 14%   629.9n ± 40%  +16.10% (p=0.010 n=8)
MergeSort/shards=4/chunk=256-4    517.5n ± 18%   152.3n ± 20%  -70.57% (p=0.000 n=8)
MergeSort/shards=16/chunk=1-4     861.1n ± 20%   999.1n ± 27%        ~ (p=0.161 n=8)
MergeSort/shards=16/chunk=256-4   883.4n ± 29%   259.1n ± 10%  -70.66% (p=0.000 n=8)
                                │     B/op      │     B/op      vs base  │
shards=4/chunk=256                 6.412Mi         2.106Mi       -67.16%
shards=16/chunk=256               25.647Mi         7.934Mi       -69.07%
                                │  allocs/op    │  allocs/op             │
shards=4/chunk=1                    80.04k          76.06k        -4.98%
shards=4/chunk=256                 80041.0           607.5       -99.24%
shards=16/chunk=1                   320.1k          267.7k       -16.39%
shards=16/chunk=256               320.130k          2.112k       -99.34%
```
(first version allocated cap-256 output per batch: chunk=1 regressed 3-4x -> fixed with adaptive capacity.)

Profile after change (shards=16 chunk=256): ~55% evalengine Comparison.Less (ParseInt64 of text values each compare), i.e. remaining cost is the actual comparison work; channel/select/runtime scheduling gone from the top.

chunk=1 (every shard result has 1 row) is roughly par / slightly slower (cap-1 chan vs cap-10 row chan gives less slack). Realistic tablets send ~32KB (stream-buffer-size) results, i.e. hundreds of rows per chunk; 1-row chunks only for rows >32KB where per-row overhead is irrelevant.

End-to-end (estimated, not measured): microbench measures only MergeSort; in vtgate each 1-row Result additionally went through Route Truncate (alloc), 2x wrapCallback, storeResultStats (mutex), executor streaming callback (mutex) - est. another ~100-200ns/row saved. For a streaming scatter ORDER BY returning many rows, vtgate CPU for the merge portion drops ~3x; overall vtgate CPU/row for such queries plausibly -30..50% (gRPC decode + MySQL protocol write remain). vdiff: also removes one channel hop per row in primitiveExecutor (now per batch).

## Gotchas
- Aliasing: must Clone qr.Rows (pooled vttablet results under vtcombo). 1 alloc/chunk.
- Memory: per stream up to ~3 chunks live (merging + chan(1) + goroutine blocked on send) vs ~1 chunk + 10 rows before; ~96KB/shard at default 32KB stream buffer. Chan cap 0 would reduce to 2.
- Latency/early termination: output flushed before any blocking receive, so a slow shard never holds back mergeable rows and Limit's io.EOF arrives as soon as possible (TestMergeSortFlushesBeforeWaiting deadlocks/timeouts if the pre-block flush is removed). At most 255 extra rows merged beyond a LIMIT - negligible.
- Output batching is timing-dependent (non-deterministic result boundaries); existing tests that asserted 1-row results now coalesce rows before comparing.
- On mid-stream shard error, rows merged but not yet flushed are dropped (error returned instead); old code had already emitted them. Query fails either way.
- ctx cancellation checked only when fetching a new chunk (bounded by chunk merge time, microseconds).
- VEXPLAIN TRACE per-call row counts change for streaming MergeSort (cosmetic).
- No proto/API/config change; release compatible.

## Tests
- go/vt/vtgate/engine: all pass (also -race -count=5 for MergeSort tests).
- go/vt/vtgate, planbuilder, wrangler (VDiff tests exercise MergeSort via shardStreamer): pass.
- go/vt/vtexplain TestUsingKeyspaceShardMap flaky (1/8 fails on main version too) - unrelated.
- tabletmanager/vdiff needs mysqld (not available here).
- Added: TestMergeSortManyRows (5 shards x1000 rows, reusing sender buffer, batch <=256, sorted), TestMergeSortFlushesBeforeWaiting, TestMergeSortStopsOnCallbackError, BenchmarkMergeSort. Note: these guard the new design; they pass on main too (no main bug), so per CLAUDE.md framing they are regression tests for the rewrite.
- merge_sort_old_bench_test.go is A/B scaffolding only - drop before merging.

## Difficulty
S: ~100 LOC net in merge_sort.go, + tests. No generated code (cached_size unaffected: MergeSort struct unchanged).
