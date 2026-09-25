# F17: vstreamer vindex filter fast path (+ charsets / mapValues allocations)

Patch: `findings/F17-vstreamer-vindex-filter.patch` (worktree agent-a189e7b22bedc3f63, uncommitted).
Benchmark harness (not in the patch): `scratchpad/F17/f17_bench_test.go.txt`, `scratchpad/F17/ab.sh`, `scratchpad/F17/prof.sh`.

## 1. Is the finding real? Yes.

- `planbuilder.go getKeyspaceID` (was ~380) builds `vindexValues`, wraps it in `[][]sqltypes.Value{...}` and calls
  `vindexes.Map(context.TODO(), vindex, nil, ...)`. For a SingleColumn vindex, `vindexes.Map` then allocates a
  `firstColsOnly` slice, the vindex's `Map` allocates `[]key.ShardDestination`, and each `DestinationKeyspaceID`
  is boxed into an interface. 6 allocs / 136-144 B per call beyond the hash itself.
- Callers: `Plan.shouldFilter` (VindexMatch filter, i.e. `in_keyrange(...)`, the filter every reshard / MoveTables-to-sharded
  stream uses) runs it for **every before and every after image** in `vstreamer.processRowEvent`, and for every row
  in `rowStreamer.streamQuery` (copy phase). `Plan.mapValues` runs it again for a `keyspace_id()` column.
- During a reshard, each target shard has its own stream against the source, so the source tablet runs this per row
  once per target shard, and rows for other targets are rejected only after paying the full cost.
- `vstreamer.getValues` allocated a `charsets` slice per image (constant per plan). Confirmed: the only reader is
  `shouldFilter -> compare`, which returns before using the charset when the value is NULL; for non-NULL values the
  entry was always `Fields[colNum].Charset`, so a per-plan slice is equivalent.
- `Plan.mapValues` allocated its result per image; both callers copy it immediately (`RowToProto3` in vstreamer,
  `RowToProto3Inplace` in rowstreamer). Its doc comment even still described a caller-supplied `result` argument.

## 2. Semantics of Map vs Hash (checked every `vindexes.Hashing` implementer)

hash, xxhash, numeric, numeric_static_map, binary, binary_md5, reverse_bits, unicode_loose_md5, unicode_loose_xxhash:
every `Map` is exactly `for id in ids { ksid, err := vind.Hash(id); ... }`. Differences are only in the error case:
hash/numeric/numeric_static_map/reverse_bits turn a Hash error into `DestinationNone` (then getKeyspaceID reports
"could not map ... got destination DestinationNone"), xxhash/binary/binary_md5 return the error, unicode_loose_* wrap it.
Empty ksid (binary vindex on '') is rejected by getKeyspaceID's `len(ksid)==0` check.
None of them need a VCursor; lookup vindexes do not implement Hashing; region_experimental is MultiColumn.

Fast path: `len(vindexColumns)==1 && vindex is Hashing && !MultiColumn` -> `Hash(values[col])`; if `err==nil && len(ksid)>0`
return it, **otherwise fall back to the old generic path**, which reproduces the exact old error text (Hash is pure).
So results and errors are byte-identical; only the success path changes.

## 3. Measured A/B (interleaved, 8 runs each, shared noisy 4 vCPU box; allocs are exact, ns are ±20-50%)

Micro (getKeyspaceID, 1024 rotating int64 ids):

| case | old | new | delta | allocs |
|---|---|---|---|---|
| hash | 289 ns / 144 B | 127 ns / 16 B | -56% | 6 -> 1 |
| xxhash | 178 ns / 136 B | 27 ns / 8 B | -85% | 6 -> 1 |
| numeric | 207 ns / 136 B | 34 ns / 8 B | -84% | 6 -> 1 |
| unicode_loose_md5 | 559 ns / 208 B | 322 ns / 80 B | -43% | 7 -> 2 |
| region_experimental (multicol, unchanged path) | 374 ns | 335 ns | ~ (noise) | 6 -> 6 |

shouldFilter, 3-column table, `in_keyrange('-80')`: hash 334 -> 158 ns (-53%), xxhash 214 -> 48 ns (-77%), 6 -> 1 allocs.

processRowEvent (binlog cell decode of 2 images + filter + mapValues + RowToProto3), 1-row UPDATE events,
3 columns (bigint, varchar(64), bigint), ids rotating so ~half the rows pass (one target of a 1:2 split),
500ms x 8 interleaved:

| case | old | new | delta | allocs/op | B/op |
|---|---|---|---|---|---|
| nofilter (`select *`) | 1.40 us | 1.30 us | ~ (p=0.33) | 20 -> 17 | 1191 -> 1079 |
| hash in_keyrange | 1.61 us | 1.19 us | -26% | 26 -> 13 | 986 -> 668 |
| xxhash in_keyrange | 1.47 us | 0.97 us | -34% | 26 -> 13 | 984 -> 665 |
| hash in_keyrange + keyspace_id() col | 2.11 us | 1.42 us | -33% | 31 -> 14 | 1177 -> 722 |

CPU profile (old, hash+xxhash keyrange cases): getKeyspaceID = 1.63 s of 4.61 s in processRowEvent (35%), of which
vindexes.Map overhead ~1.1 s and the actual Hash only ~0.5 s; mallocgc 35% of samples and GC mark workers 20% on top.
New: getKeyspaceID 0.90 s (almost all of it is Hash itself: the `hash` vindex is a 3DES block encrypt + ToString/ParseInt
for signed ints), mallocgc 27%.

## 4. End-to-end estimate (estimated, not measured)

The benchmark covers cell decoding + filter + proto conversion but not binlog network read / event header parsing,
`ev.Rows()` splitting, VEvent batching, or gRPC marshalling. Those roughly double the per-row cost for small rows, so the
realistic saving is ~10-20% of source-tablet vstreamer CPU per filtered stream for small rows during reshard / MoveTables
into a sharded keyspace (more for the xxhash vindex, less for wide rows where decoding dominates), multiplied by the
number of target shards streaming from the source. Plus 13 fewer allocs per row -> less GC pressure. Unfiltered streams
(unsharded targets, CDC with `select *`) gain only the charsets/mapValues allocs (3 allocs/row, ~10% bytes, time within noise).

## 5. Difficulty: S

~40 LOC of production change in 3 files (planbuilder.go, vstreamer.go, rowstreamer.go) + ~80 LOC tests. No generated code,
no proto/wire changes, no config. Unexported API only (`mapValues` gained a `buf` parameter).

## 6. Sketch

```go
if len(vindexColumns) == 1 {
    if hasher, ok := vindex.(vindexes.Hashing); ok {
        if _, multi := vindex.(vindexes.MultiColumn); !multi {
            if ksid, err := hasher.Hash(values[vindexColumns[0]]); err == nil && len(ksid) > 0 {
                return ksid, nil
            }
        }
    }
}
return getKeyspaceIDSlow(values, vindex, vindexColumns) // old code, also produces the old error
```
- `streamerPlan.charsets` computed lazily once in `getValues` from `Table.Fields`; getValues returns it.
- `mapValues(values, buf)` reuses `buf` when `cap >= len(ColExprs)`; processRowEvent keeps one buffer per event
  (shared by before/after images), rowstreamer keeps one for the whole copy query.

Runtime type assertions (Go per-call-site type-assert cache) cost ~1-2 ns, so no plan-time caching was added; that
would add a field to Filter/ColExpr and break the `require.Equal(plan)` expectations in TestPlanBuilder /
TestPlanBuilderFilterComparison.

## 7. Gotchas

- Equivalence relies on each Hashing vindex's Map being "Hash per id". True for all 9 in-tree; a future/plugin vindex
  that implements Hashing but whose Map differs would diverge. The `Hashing` doc says it exists so multicol vindexes can
  compute a ksid from it, so that contract already implies consistency; the new test enumerates the in-tree types.
- Error messages are preserved by falling back to the slow path on any Hash error or empty ksid (double Hash only on the
  error path, which then aborts the stream anyway).
- `streamerPlan.charsets` is mutated lazily: safe because a vstreamer's plans are used only by its own event loop
  goroutine and `rebuildPlans`/table-map events create fresh streamerPlans (so schema changes recompute it).
- NULL / omitted (NOBLOB partial image) columns now have a non-zero charset entry; harmless since compare() returns
  before using the charset for NULL values (omitted columns are zero Values = NULL).
- mapValues buffer: the returned slice aliases buf and holds references to the raw binlog bytes until the next row;
  both callers copy immediately. `mapped` is local to processRowEvent / streamQuery, so no retention beyond the call.
- Only `hash`'s Hash itself remains expensive (3DES + `ToString`+`ParseInt` for signed ints -> 1 alloc); a follow-up in
  `vindexes.Hash.Hash` could use `id.ToInt64()` for signed ints to drop that alloc (separate change, out of scope).
- Release compatibility: none of this is visible on the wire; keyspace ids unchanged.

## 8. Tests

- Added `TestGetKeyspaceIDHashingFastPath` (planbuilder_test.go): for 8 Hashing vindex types x 15 values (NULL, ints,
  negative, uint64 max, float, decimal, empty/trailing-space/invalid-UTF-8 strings, binary) asserts same ksid or same
  error text as the generic path.
- Added `TestMapValuesReusesBuffer`: stale buffer contents are fully overwritten (plain column, fixed value, keyspace_id,
  NULL), buffer reused when large enough, not when too small.
- Ran (with a fake `mysqld --version` under VT_MYSQL_ROOT and a temporary TestMain bypass, both reverted):
  TestMustSendDDL, TestPlanBuilder, TestPlanBuilderFilterComparison, TestCompare, TestFindColumn, TestPlanMapBitmap,
  and the two new tests: all PASS. `go vet` clean, `scripts/fmt` applied.
- NOT run: the mysqld-backed vstreamer/rowstreamer/uvstreamer integration tests (no mysqld in this environment) - CI
  must run them (they cover in_keyrange / keyspace_id() end to end).
