# F10: per-row allocations in RowsToProto3 / proto3ToRows

## Call paths (verified)

Send side (vttablet `grpcqueryservice` Execute/StreamExecute/ExecuteBatch/BeginExecute/ReserveExecute...,
vtgate `grpcvtgateservice`, tabletmanager ExecuteFetch*, vdiff): `ResultToProto3` -> `RowsToProto3` ->
`RowToProto3` per row = 3 allocs/row (`&querypb.Row{}`, Lengths, Values). Streaming uses exactly the same
`ResultToProto3(reply)` per chunk (chunks bounded by `queryserver-config-stream-buffer-size`, default 32KiB).
No pooled path: the vtproto `Row` pool (`RowFromVTPool`) is only used by vstreamer's rowstreamer
(`RowToProto3Inplace` into pooled rows of `VStreamRowsResponse`). The consolidator path caches
`proto3Rows` (`CacheProto3Rows`) on the shared Result; that also goes through `RowsToProto3`.

gRPC marshal: `go/vt/servenv/grpc_codec.go` registers a vtproto codec (`SizeVT` + `MarshalToSizedBufferVT`), so the
`querypb.Row` objects are transient after `Send` (except the consolidator cache, which lives exactly as long as the Result).

Receive side (vtgate `grpctabletconn` Execute -> `Proto3ToResult`; StreamExecute/BeginStreamExecute/ReserveStreamExecute/
MessageStream -> `CustomProto3ToResult`; also vtgateconn client, vtctl/wrangler): `proto3ToRows` -> `MakeRowTrusted` per row
= 1 alloc/row. `QueryResult.UnmarshalVT` (not pooled) already allocates 3/row (`&Row{}`, packed `Lengths` make, and
`Values = append(m.Values[:0], ...)` copy, needed because the codec frees the pooled receive buffer). `MakeTrusted`
aliases each `Row.Values` byte slice. So on receive, the slab removes 1 of 4 per-row allocs; the other 3 are in generated code.

## Change (prototype, uncommitted in worktree)

- `RowsToProto3`: pre-pass computing total cols and bytes; allocate one `[]querypb.Row`, one `[]*querypb.Row`, one `[]int64`,
  one `[]byte`; each row gets `lengths[a:b:b]` / `values[s:e:e]` (3-index, so appends reallocate instead of clobbering the
  neighbour). 4 allocs total regardless of row count. Non-nil empty slices preserved for 0-col / all-NULL rows (same as before).
- `proto3ToRows`: one `[]Value` slab, rows are `values[:n:n]`; `MakeRowTrusted` refactored into `makeRowTrustedInto`
  (public API unchanged; other `MakeRowTrusted` callers unaffected). 2 allocs total.
- ~55 LOC in `go/sqltypes/proto3.go`, `go/sqltypes/result.go`. No generated code involved.
- Tests: `go/sqltypes/proto3_slab_test.go` (random equivalence vs per-row reference incl. NULLs/empty/0-col rows, cap==len
  checks, append isolation) + micro benchmarks; `go/vt/vttablet/grpctabletconn/bench_rows_test.go` (loopback gRPC E2E bench).

## Micro A/B (measured; noisy shared machine, benchstat n=8)

```
RowsToProto3   rows=1,cols=5     468n -> 507n  (~, p=0.96)   4 -> 4 allocs
               rows=10,cols=5    5.3µ -> 4.8µ  (~)          31 -> 4
               rows=100,cols=5   59µ  -> 31µ   -47%        301 -> 4
               rows=1000,cols=5  643µ -> 297µ  -54%       3001 -> 4
               rows=10000,cols=20 7.6ms -> 4.1ms -46%     30001 -> 4    B/op -3..-9%
proto3ToRows   rows=1,cols=5     362n -> 375n  (~)           2 -> 2
               rows=10,cols=5    2.7µ -> 3.6µ  (~, p=0.33)  11 -> 2
               rows=100,cols=5   34µ  -> 24µ   -28%        101 -> 2
               rows=1000,cols=5  491µ -> 257µ  -48%       1001 -> 2
               rows=10000,cols=20 6.9ms -> 4.4ms -36%    10001 -> 2    B/op +2..+10% small, -9% large
```
(Earlier quieter scratch run: RowsToProto3 1000x5 162µs->104µs, Proto3ToResult 130µs->100µs, i.e. ~1.3-1.6x.)
Tiny results: no regression in allocs (1 row = same count), time within noise.

## End-to-end (measured, loopback gRPC client+server in one process, interleaved old/new binaries, n=6)

```
                                   sec/op (noise ±15-80%)   B/op     allocs/op
Execute/rows=1,cols=5              ~                         ~        192 -> 192
Execute/rows=10,cols=5             ~                         ~        259 -> 223  (-14%)
Execute/rows=1000,cols=5           1.72ms -> 1.24ms (~)      ~        7207 -> 3211 (-55%)
StreamExecute/rows=1000,cols=5     ~                        -2.6%     7261 -> 3268 (-55%)
Execute/rows=10000,cols=20         ~                        -4.7%    70.4k -> 30.4k (-57%)
StreamExecute/rows=10000,cols=20   ~                        -2.4%    72.0k -> 32.2k (-55%)
```
Wall time differences are not statistically significant under the shared-CPU noise.

CPU profile (old, Execute+StreamExecute rows=1000,cols=5, both sides in one process): `RowsToProto3` 7.8%,
`proto3ToRows` 4.2%, `mallocgc` 13%, GC mark 5%. New: `RowsToProto3` 4.6% (roughly halved), `proto3ToRows` ~4.9%
(not clearly improved; the big pointerful `[]Value` slab needs zeroing and goes to the large-object path),
`mallocgc` 11.3%, GC mark 4.6%.
Estimate: in production vttablet, `RowsToProto3` is a few % of CPU for large-result workloads (MySQL protocol parsing,
query execution dominate); the change saves maybe 1-3% vttablet CPU and ~1% vtgate CPU on large-result OLTP/OLAP reads,
~0 for point queries (1 row). Allocation count reduction (-55% E2E) reduces GC pressure proportionally more under load.

## Gotchas

1. **Memory retention on vtgate (the main risk).** After the change, retaining a single `Row` (the `[]Value` slice, not a
   copied `Value`) pins the whole chunk's `[]Value` slab, which in turn references every row's `Values` byte buffer, i.e.
   the whole chunk. Before, a retained row pinned only itself. Relevant consumer: streaming `MemorySort` with LIMIT
   (`evalengine.Sorter` heap keeps top-N rows across chunks) -> can pin up to N chunks (~32KiB data + ~32 B/value headers
   each) instead of N rows. E.g. `ORDER BY ... LIMIT 1000` over a large scatter stream could hold tens of MB instead of
   ~100 KB. Bounded by min(N, #chunks) * chunk size; also affects OrderedAggregate/vdiff merge (1 row per stream, negligible).
   Non-streaming results are mostly already all-or-nothing (Rows sub-slicing already pins everything). Copied `Value`s
   (e.g. `Rows[0][0]` in sequences) do not pin the slab. Mitigation options: only slab in `Proto3ToResult`
   (non-streaming), keep per-row in `CustomProto3ToResult`; or cap slab chunks (e.g. 64-128 rows per slab), which keeps
   >95% of the alloc win with bounded pinning.
2. Send side has no retention issue: proto rows are transient after vtproto marshal; consolidator cache lives with the Result.
3. Aliasing: all sub-slices are 3-index capped, so `append` to a row's Lengths/Values or to a `[]Value` row reallocates
   (same semantics as before, since the old per-row allocations also had cap==len). In-place `row[i] = x` stays row-local.
   Covered by `TestRowsToProto3SlabAppendIsolation`.
4. `ReturnToVTPool` on a slab `querypb.Row` would put an interior pointer into the pool (memory-safe, but pins the slab).
   Nobody calls it on rows produced by `RowsToProto3` today (only VStreamRows client path, which uses its own pooled rows).
5. `CachedSize` of `proto3Rows`/Rows still sums to the slab sizes, so accounting is unchanged.
6. nil vs empty: preserved (non-nil empty Lengths/Values/row) so `reflect.DeepEqual`-based tests behave identically.
7. Tiny results: same alloc count, time within noise; decode B/op +10% for 10 rows (slab size class rounding).
8. The bigger remaining cost is generated `QueryResult.UnmarshalVT` (3 allocs/row) and marshaling via intermediate
   `querypb.Row` at all; a custom codec that marshals `sqltypes.Result` directly or unmarshals rows into slabs would be a
   larger (generator/hand-written codec) project.
9. Release compatibility: no wire change, no API change.

## Tests
- `go test ./go/sqltypes/...` pass (plus related packages, see final report).
- New equivalence test passes on main too (it is an equivalence/regression guard, not a bug-fix test).

Test run: sqltypes, vttablet/grpctabletconn, vtgate/grpcvtgateconn, vttablet/grpcqueryservice (no tests), vtgate/engine: pass.
vtctl/workflow: TestConcurrentKeyspaceRoutingRulesUpdates fails with 10s timeout, also on baseline (unrelated).
tabletmanager/vdiff: needs mysqld (env), not run.
