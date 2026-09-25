# F09-insert-per-row: precompute the plan-constant parts of sharded INSERT queries

## Finding (verified)
`go/vt/vtgate/engine/insert.go` `(*Insert).getInsertShardedQueries` (lines ~250-290 on main) does the following for every row routed
to a shard, on every execution:
- allocates a new `walkFunc` closure.
- calls `sqlparser.String(ins.Mid[index])` to format the row tuple. Typed arguments print `/* INT64 */` comments, which makes this expensive.
- runs `sqlparser.Walk` over each expr of the row to collect bind var names.
- runs `sqlparser.Walk` over `ins.Suffix` **again per row**. This is redundant because the suffix is the same for every row.
- calls `sqlparser.String(ins.Suffix)` once per shard.
- builds the query with `strings.Join(mids)` plus concatenation, which makes extra copies. The shardBindVars map is not presized, and rehashing shows up in the profile.

Two more per-row costs:
- `InsertVarName` (fmt.Sprintf + CompliantName) runs per row, per vindex column.
- The row-index round trip allocates 2 objects per row: `&querypb.Value{}` and `strconv.AppendInt(nil, ...)`.

`ins.Mid`, `ins.Suffix`, `ins.Prefix` and `ins.Alias` are set only in the planbuilder's `generateInsertShardedQuery`
(operator_transformers.go:694) and never change afterwards, so they are plan constants.
- Mid always holds the rows as planned. Vindex columns become `:_<col>_<row>` and the autoinc column becomes `:__seq<row>`. Other values are literals or normalized `:vtgN` args.
- Sequence values and vindex values are injected via **bind vars** at execution: processGenerateFromValues sets `__seqN` and getInsertShardedQueries sets `_id_N`. So the Mid text and arg names do not change.
- INSERT ... SELECT uses the separate InsertSelect primitive (insert_select.go), which this change does not touch. Its rows are built at execution time and are not plan constants.
- Unsharded inserts use `ins.Query`.
- ON DUPLICATE KEY (Suffix) is also a plan constant; it is CopyOnRewrite'd at plan time.

## Profile of old code (100 rows x 8 cols, benchmark below)
Within getInsertShardedQueriesOld:

| Callee | Share |
|---|---|
| sqlparser.String | 34% |
| sqlparser.Walk | 28% |
| buildVindexRowsValues | 9% |
| processVindexes | 5% |
| InsertVarName | 5% |
| map assign / growslice | rest |

On top of that, GC is heavy (bgsweep/mark) because of 2758 allocs/op.

## Prototype (uncommitted in worktree; patch F09-insert-per-row.patch)
- **Cached plan-constant parts:** a new unexported `insertShardedParts{mids []string; midArgs [][]string; suffix string; suffixArgs []string; vindexVarNames [][][]string}`. It is computed lazily, once per plan, via `sync.Once` fields on `engine.Insert` (`partsOnce`, `parts`).
  - Lazy computation keeps all tests and struct literals working.
  - Plans are shared between goroutines, hence `sync.Once`.
- **Per-shard query building:** the bind var map is presized to the sum of arg counts, and a strings.Builder to the sum of mid lengths.
  - Bind vars are copied by name with `copyInsertBindVars`. It returns VT03026 on the first missing name, in the same order as the old walk: row args first, then suffix args.
  - The suffix is checked once, after the first row of each shard. This is equivalent to the old per-row check because bindVars does not change in between.
- **Row indexes:** one `[]querypb.Value` slab plus one shared digit buffer (3-index sub-slices), instead of 2 allocations per row. ParseInt is kept (cheap, no allocation). The ResolveDestinations API is unchanged.
- **Generated code:** cached_size.go was regenerated with the `make sizegen` command line. It adds insertShardedParts.CachedSize, and the Insert struct grows from 224 to 256 bytes.
- **Dead check removed:** `if keyspaceIDs[index] != nil` could never be false, because only non-nil ksids are passed to ResolveDestinations.

## Benchmark
Measured on a noisy, shared 4 vCPU machine. Old and new were interleaved via -count=8, with the old implementation kept verbatim in insert_old_test.go.

```
                                             │     old      │                 new                  │
                                             │    sec/op    │    sec/op      vs base               │
InsertShardedQueries/rows=1/ondup=false-4      5.893µ ±  9%   3.168µ ±  12%  -46.25% (p=0.000 n=8)
InsertShardedQueries/rows=1/ondup=true-4       9.191µ ± 28%   4.158µ ±  20%  -54.76% (p=0.000 n=8)
InsertShardedQueries/rows=100/ondup=false-4    434.9µ ± 19%   137.1µ ±  12%  -68.47% (p=0.000 n=8)
InsertShardedQueries/rows=100/ondup=true-4     453.7µ ± 11%   153.6µ ±  15%  -66.15% (p=0.000 n=8)
InsertShardedQueries/rows=1000/ondup=false-4   6.146m ± 12%   1.832m ±  12%  -70.19% (p=0.000 n=8)
InsertShardedQueries/rows=1000/ondup=true-4    7.240m ± 24%   2.414m ± 213%  -66.66% (p=0.015 n=8)
geomean                                        279.4µ         103.5µ         -62.97%

                                             │     B/op     │     B/op      vs base               │
InsertShardedQueries/rows=1/ondup=false-4      2.727Ki ± 0%   1.672Ki ± 0%  -38.68% (p=0.000 n=8)
InsertShardedQueries/rows=1/ondup=true-4       3.461Ki ± 0%   1.945Ki ± 0%  -43.79% (p=0.000 n=8)
InsertShardedQueries/rows=100/ondup=false-4    250.0Ki ± 0%   100.3Ki ± 0%  -59.87% (p=0.000 n=8)
InsertShardedQueries/rows=100/ondup=true-4     261.6Ki ± 0%   100.3Ki ± 0%  -61.64% (p=0.000 n=8)
InsertShardedQueries/rows=1000/ondup=false-4   2.598Mi ± 0%   1.059Mi ± 0%  -59.23% (p=0.000 n=8)
InsertShardedQueries/rows=1000/ondup=true-4    2.705Mi ± 0%   1.059Mi ± 0%  -60.85% (p=0.000 n=8)
geomean                                        128.7Ki        58.11Ki       -54.85%

                                             │  allocs/op   │  allocs/op   vs base               │
InsertShardedQueries/rows=1/ondup=false-4        53.00 ± 0%    33.00 ± 0%  -37.74% (p=0.000 n=8)
InsertShardedQueries/rows=1/ondup=true-4         64.00 ± 0%    35.00 ± 0%  -45.31% (p=0.000 n=8)
InsertShardedQueries/rows=100/ondup=false-4     2758.0 ± 0%    771.0 ± 0%  -72.04% (p=0.000 n=8)
InsertShardedQueries/rows=100/ondup=true-4      3174.0 ± 0%    771.0 ± 0%  -75.71% (p=0.000 n=8)
InsertShardedQueries/rows=1000/ondup=false-4   26.103k ± 0%   7.107k ± 0%  -72.77% (p=0.000 n=8)
InsertShardedQueries/rows=1000/ondup=true-4    30.119k ± 0%   7.107k ± 0%  -76.40% (p=0.000 n=8)
geomean                                         1.691k         571.1       -66.22%
```

Per row, old is ~4.3-6 us and new is ~1.4-1.8 us, so ~3-4 us is saved per row. That is more than the earlier ~1 us
estimate, for three reasons:
- typed-argument formatting prints type comments.
- the suffix was walked once per row.
- the extra allocations add GC pressure.

The benchmark measures getInsertShardedQueries, which is all of Insert.TryExecute except processGenerate and ExecuteMultiShard.

## End-to-end relevance (estimated)
For comparison, parsing the same 100-row and 1000-row 8-column insert text on this machine takes ~5.7-8 us/row
(568-878 us and 5.9-8.4 ms; measured with a throwaway benchmark). A sharded bulk insert therefore costs roughly:
- parse: ~6 us/row
- normalize and plan-cache key: not measured
- this primitive: ~4.5 us/row before the change

Saving ~3-4 us/row is plausibly ~20-30% of vtgate CPU for bulk sharded INSERTs. This is an estimate; there is no
end-to-end profile. Single-row inserts save ~2.7 us per statement (5.9 -> 3.2 us), which also matters for
high-QPS single-row insert workloads.

## Difficulty
S/M: ~120 LOC of logic in insert.go, plus generated cached_size.go (sizegen). No proto changes.

## Gotchas
- **Memory:** the cached parts add ~85% to the Insert primitive's size, mostly the rendered mid strings.

  | Plan | Insert primitive | Cached parts |
  |---|---|---|
  | 1000 rows x 8 cols | 530 KB | 449 KB |
  | 100 rows x 8 cols | 54 KB | 45 KB |

  Arg-name strings share backing with the AST, so the counted size is somewhat too high. Two ways to shrink it:
  store mids as one string plus offsets and midArgs flat plus offsets, or drop vindexVarNames.
  A lower-memory alternative is to skip caching the mid strings and format the rows straight into the per-shard
  builder. It keeps about half the CPU cost.
- **Plan cache accounting:** the plan cache (theine) computes CachedSize when the plan is inserted, before the first
  execution, so lazily computed parts are NOT counted. The fix is to compute them eagerly in the planbuilder
  (operator_transformers.go, right after generateInsertShardedQuery) through an exported method. The lazy
  fallback would stay for tests and hand-built plans.
- **Copying:** sync.Once makes engine.Insert non-copyable (go vet copylocks). vet passes today; no value copies were found.
- **Generated code:** sizegen must be rerun because CI checks generated code. The regenerated file is included in the patch.
- **Error behaviour:** kept. A missing arg in a row or in the suffix still returns VT03026 with the same first-missing name. There was no test for this before; one is added.
- **Plan output:** `description()` and the planbuilder testdata are unchanged, because description uses only exported fields. The planbuilder tests pass.
- **InsertSelect:** untouched; its rows are dynamic.
- **Release compatibility:** purely internal, no wire or format change. The SQL sent to tablets is byte-identical (checked by TestInsertShardedQueriesMatchOld).

## Tests
- **Existing tests:**
  - go/vt/vtgate/engine passes. One full run failed in TestMirror, a timing-sensitive test that looks unrelated. It passed on the full-package rerun and on -count=3.
  - go/vt/vtgate/planbuilder/... passes.
  - go/vt/vtgate passes.
- **Added (insert_bench_test.go):**
  - BenchmarkInsertShardedQueries: 1/100/1000 rows, with and without ON DUP, old vs new.
  - TestInsertShardedQueriesMatchOld: byte-identical SQL and bind vars vs the old implementation, including a second execution from the cache.
  - TestInsertShardedMissingBindVar: VT03026 for a missing row arg and a missing suffix arg, before and after caching.
  - TestInsertShardedConcurrentFirstExecution: race detector on the lazy init; passes with -race.
- **For a real PR:** drop insert_old_test.go and turn MatchOld into a golden-SQL test. Keep the benchmark and the
  error and concurrency tests as guards. They are not bug-fix tests, so they also pass on main.
