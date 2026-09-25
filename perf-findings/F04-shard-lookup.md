# F04: shard lookup for a keyspace id (GetShardForKeyspaceID), plus ResolveDestinations and insert side items

Worktree: /home/user/vitess/.claude/worktrees/agent-a96e04415a8bc6029 (base aa9ccf9)
Patch: findings/F04-shard-lookup.patch (uncommitted in the worktree)

## 1. Is the finding real?

Yes. `go/vt/key/destination.go:217` `GetShardForKeyspaceID` does a linear scan calling
`KeyRangeContains` (which calls `KeyRangeIsComplete`, then `Compare` = 2x `Normalize` + `bytes.Compare`,
up to twice) on every shard. It is the only resolver for `DestinationKeyspaceID` and `DestinationKeyspaceIDs`,
which srvtopo `Resolver.ResolveDestinations` (resolver.go:248) and `ResolveDestinationsMultiCol` (:167) call once per
destination. Callers: sharded INSERT (engine/insert.go, one destination per row), INSERT...SELECT
(insert_select.go), IN-lists / multi-equal (routing.go resolveShards / resolveShardsMultiCol), point selects,
lookup vindex verification, and so on.

## 2. Can we rely on sortedness? (No, not everywhere.)

Where the `[]*ShardReference` comes from: `Resolver.GetKeyspaceShards` -> `srvtopo.Server.GetSrvKeyspace`
(the ResilientServer watcher caches the proto that it reads from the topo, so the pointer stays the same until the next change)
-> `SrvKeyspaceGetPartition(...).ShardReferences`. No sorting happens on the read path.

Writers of SrvKeyspace partitions:
| writer | sorted? | disjoint? |
|---|---|---|
| topotools.RebuildKeyspace (rebuild_keyspace.go:161) | yes (OrderAndCheckPartitions) | yes, contiguous; `allowPartial` allows a missing tail |
| topo.MigrateServedType (traffic switch, reshard cutover) | yes (OrderAndCheckPartitions) | yes, contiguous |
| topo.AddSrvKeyspacePartitions (legacy `UpdateSrvKeyspacePartition` vtctl command via wrangler/shard.go:51) | **no, appends** | only deduped on exact KeyRange equality, so **an operator can create overlaps** |
| topo.DeleteSrvKeyspacePartitions (RemoveShardCell, and the same legacy command) | keeps order | leaves **gaps** |
| direct topo writes, vtexplain, and test fakes (sandbox, faketopo, PassthroughSrvTopoServer) | not guaranteed | usually |
| custom sharding / unsharded | all KeyRanges nil or empty | "all contain everything" |

Also note that `processExactKeyRange` (destination.go:129) **sorts the shared cached slice in place** with
`sort.SliceStable`. On a sorted slice it never swaps, so there are no writes. On an unsorted slice (the AddSrvKeyspacePartitions path) it is
a real data race on shared topo cache state, and it changes the "first match" order that the linear scan depends on.
That is a pre-existing bug, and worth its own finding. Fix: sort a copy.

Conclusion: the code can't assume sortedness. It CAN rely on disjointness for every partition that was produced by rebuild
or traffic switching. Overlap requires a manual legacy command, or direct topo writes.

## 3. Design chosen (prototype): binary search, verified, with a linear fallback (key package only, no API change)

```go
func GetShardForKeyspaceID(allShards, ksid) (string, error) {
    if len(allShards) == 0 { same UNAVAILABLE error }
    if i := searchShardForKeyspaceID(allShards, ksid); i >= 0 { return allShards[i].Name, nil }
    <unchanged linear scan + unchanged INVALID_ARGUMENT error>
}
func searchShardForKeyspaceID(allShards, ksid) int {
    if KeyRangeContains(allShards[0].KeyRange, ksid) { return 0 }   // same as linear; covers nil/unsharded/custom
    id := Normalize(ksid)
    binary search over [1,n) for first Start > id (Start compared normalized; nil Start = min)
    c := lo-1; if c > 0 && KeyRangeContains(allShards[c].KeyRange, ksid) { return c }
    return -1
}
```
Properties:
- The candidate is always verified with `KeyRangeContains`, so an unsorted list can at worst miss, and then falls back to the linear scan.
  It can never return a shard that doesn't contain the ksid.
- Error messages and codes are identical: both come from the unchanged fallback path.
- The result is the same shard as the linear scan whenever at most one shard contains the ksid, meaning disjoint ranges: sorted, unsorted,
  gapped, unnormalized boundaries (trailing 0x00) and custom sharding (first shard wins, as before).
- With **overlapping** ranges (only from the legacy manual command or direct writes), it returns *a* containing shard, but not necessarily the
  first one in slice order. `TestGetShardForKeyspaceIDOverlapping` documents this.
- Cost of an unsorted but disjoint list: one extra probe before the linear scan, about 1 compare plus log n. That's negligible.

Alternative (B), which is exact even under overlap: cache a "sorted & disjoint" verdict, or a sorted copy, per
`*SrvKeyspace_KeyspacePartition` pointer in `srvtopo.Resolver`, keyed by (keyspace, tabletType), in a sync.Map with
an atomic entry. In `ResolveDestinations`, add a type switch on `key.DestinationKeyspaceID` / `DestinationKeyspaceIDs`
that uses the binary search only when the verdict is true. That costs about 20-30 ns per call for the map lookup. A cache miss (test fakes
that return fresh protos) costs an O(n) validation, but it's still correct. It needs about 60 more LOC and couples srvtopo to the concrete
destination types. I only recommend B if reviewers insist on first-match semantics under overlap. Validating per call without a cache
would regress single-row point lookups (O(n) validation is about as expensive as the scan), so don't do that.

## 4. Measurements (shared noisy 4 vCPU box; interleaved A/B; ratios matter, absolute numbers are noisy)

### Micro: GetShardForKeyspaceID, random 8-byte ksids, evenly split shards (measured)
```
                                   │    linear     │               current               │
GetShardForKeyspaceID/shards=2-4      52.53n ± 47%   37.48n ± 58%        ~ (p=0.328 n=8)
GetShardForKeyspaceID/shards=4-4     132.80n ± 70%   55.69n ± 28%  -58.06% (p=0.001 n=8)
GetShardForKeyspaceID/shards=8-4     112.02n ± 41%   56.83n ± 10%  -49.27% (p=0.000 n=8)
GetShardForKeyspaceID/shards=32-4    291.00n ± 20%   85.69n ± 32%  -70.55% (p=0.000 n=8)
GetShardForKeyspaceID/shards=128-4   1083.0n ± 23%   110.0n ± 59%  -89.84% (p=0.000 n=8)
GetShardForKeyspaceID/shards=256-4   1493.5n ± 24%   146.3n ± 34%  -90.20% (p=0.000 n=8)
geomean                               267.7n         73.96n        -72.37%
```
0 allocations in both. (The earlier quiet-machine scratch numbers were 4: 30->13 ns, 32: 248->20, 256: 1646->75, which is consistent.)

### End-to-end: Resolver.ResolveDestinations, one DestinationKeyspaceID per row (new BenchmarkResolveDestinations, measured)
old = HEAD, mid = closure hoist only, new = closure hoist + binary search:
```
                                           │      old      │        mid           │        new           │
ResolveDestinations/shards=4/rows=1-4         1.211µ ± 29%    1.285µ   ~              1.490µ   ~ (noise)
ResolveDestinations/shards=4/rows=1000-4      264.5µ ± 32%    124.7µ  -52.86%        169.1µ  -36.07%
ResolveDestinations/shards=32/rows=1-4        2.071µ ± 34%    1.601µ   ~              1.668µ   ~
ResolveDestinations/shards=32/rows=1000-4     692.6µ ± 48%    406.0µ  -41.39%        234.0µ  -66.21%
ResolveDestinations/shards=256/rows=1-4       5.160µ ± 26%    4.334µ   ~              1.356µ  -73.73%
ResolveDestinations/shards=256/rows=1000-4   2472.7µ ± 17%   2343.8µ   ~              500.0µ  -79.78%
allocs/op rows=1000: 4 shards 1055 -> 56; 32 shards 1292 -> 293; 256 shards 2304 -> 1305 (all from the closure hoist)
B/op rows=1000:      4 shards 112Ki -> 18.5Ki; 32: 125Ki -> 31Ki; 256: 187Ki -> 93Ki
```
A separate full run (old vs new, 9 configurations) gave: rows=100 -43%/-37%/-77% (4/32/256 shards), rows=1000 -66%/-72%/-75%.
Attribution: at 4 shards most of the gain comes from the closure hoist (1 alloc and about 100 ns per destination). At 32 shards and above the binary search dominates.
rows=1 is unchanged except at 256 shards, where the linear scan itself was the main cost.

### Side item: insert row-index round trip (measured)
`insert.go` / `insert_select.go` allocated, per row, a `*querypb.Value` plus the `strconv.AppendInt(nil, …)` bytes, and
boxed a `DestinationKeyspaceID` into an interface (another 24 B alloc). The new `rowDestinations` helper in insert_common.go
carves the Values and their bytes out of 2 backing arrays, and presizes the slices:
```
                  │     old      │                 new                 │
RowDestinations-4   195.1µ ± 12%   109.6µ ± 11%  -43.83% (1000 rows, including ParseInt decode)
B/op                160.8Ki        122.4Ki       -23.83%
allocs/op           3.017k         1.005k        -66.69%
```
The remaining 1 alloc per row is the interface boxing of `key.DestinationKeyspaceID`, which can't be avoided without an API change.
`ParseInt(string(b))` doesn't allocate (strconv clones for errors), so the decode side is fine. A bigger refactor that returns
`[][]int` row indexes from ResolveDestinations would need changes to the VCursor interface and all its fakes, and isn't worth it.

### End-to-end relevance (estimate)
- A 1000-row sharded INSERT at 32 shards saves about 0.25-0.45 ms of resolve work plus about 2000 allocs (resolver) and
  2000 allocs (insert). Per row in insert.go, the remaining costs (sqlparser.String of each Mid tuple, 2 Walks, bindvar map inserts,
  the vindex Map) are several µs, so for insert-heavy workloads this is roughly a 3-10% cut of the vtgate CPU per row at 32 shards,
  and more at 128-256 shards.
- Point queries (rows=1) only benefit noticeably with ≥128 shards (256 shards: about 4 µs less per query, measured).
- IN-lists with many values benefit proportionally, since they go through resolveShards -> DestinationKeyspaceIDs/ID per value.

## 5. Difficulty
S. Production code is about 110 LOC across 5 files: key/destination.go (+43), srvtopo/resolver.go (a restructure, net ±0),
engine/insert_common.go (+35), insert.go and insert_select.go (-10 each). No generated code.
Tests and benchmarks: key/destination_bench_test.go (reference linear implementation, benchmark, randomized equivalence test,
overlap test), srvtopo/resolver_bench_test.go, engine/insert_rowdest_test.go.

## 6. Gotchas
- Overlapping partitions: the binary search returns a containing shard but not necessarily the first one in slice order.
  Overlaps are only reachable through the legacy `UpdateSrvKeyspacePartition` command or direct topo writes, and first-match order is already
  unstable there because processExactKeyRange sorts in place. If exactness is required, use design B (per-partition cached verdict in srvtopo).
- Pre-existing bug found: `processExactKeyRange` mutates the shared cached `ShardReferences` slice (sort.SliceStable
  in place). This is a data race when the partition is unsorted. Separate fix: sort a copy.
- `searchShardForKeyspaceID` must compare *normalized* Start values (trailing 0x00 bytes). The prototype normalizes both sides, which
  matches `KeyRangeContains`/`Compare`. The randomized test includes unnormalized boundaries.
- Closure hoist: the closure now captures the loop index `i` by reference, which is correct because Resolve calls the callback synchronously.
  An implementation of `ShardDestination` that retained the callback and called it later would now see the wrong index.
  No in-tree implementation does that (the interface contract implies a synchronous call). `resultAcc.resolveShard` changed from
  `func(idx) func(string) error` to a method `func(string) error` plus an `idx` field. It is unexported, and has no other users.
- rowDestinations: the Values share one byte buffer, so each value is capped with a 3-index slice. That's tested with len==cap,
  so an append by a consumer can't clobber its neighbour. The slab is retained as long as any `*querypb.Value` is referenced
  (short-lived, per query). The loggingVCursor test fake prints values, and output is unchanged.
- No wire, proto or release-compatibility impact. Pure in-process changes. Portable, since there's no unsafe or SIMD.
- Flakes seen while testing, unrelated: `srvtopo` failed once out of 9 runs when run in parallel with other packages under load
  (timing-based watcher tests), and `engine` TestMirror is latency-based (failed once, passed on 5 reruns).

## 7. Tests
- Existing tests pass: go/vt/key, go/vt/srvtopo, go/vt/vtgate/engine, go/vt/vtgate, go/vt/vtexplain, go/vt/topo,
  go/vt/vtgate/executorcontext, go/vt/vtgate/vindexes.
- Added `TestGetShardForKeyspaceIDMatchesLinearScan`: about 211 shard layouts (even splits of 1..256 shards, 200 random
  layouts with unnormalized boundaries, nil or empty KeyRange, custom sharding), each also shuffled and gapped (prefix, suffix,
  and middle removed). It probes boundaries ±1, boundaries with trailing 0x00/0x01, nil/empty/0xff.. and random ksids, and asserts the same
  shard, or the same error string and code, as the reference linear implementation. Mutation check: removing the candidate verification
  makes it fail.
- Added `TestGetShardForKeyspaceIDOverlapping` (documents the overlap semantics), `TestRowDestinations` (equivalence with the
  old loop, including nil ksids and digit-width boundaries 9/10/99/100), and benchmarks `BenchmarkGetShardForKeyspaceID`,
  `BenchmarkResolveDestinations` and `BenchmarkRowDestinations`.
- Note on "must fail on main": these are equivalence and regression tests for a performance rewrite. They pass on main by design,
  and they protect behaviour rather than a bug fix.
