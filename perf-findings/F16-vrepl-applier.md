# F16 – vreplication applier: per-row, per-column work that is constant per plan

File: `go/vt/vttablet/tabletmanager/vreplication/replicator_plan.go`
Patch: `findings/F16-vrepl-applier.patch` (uncommitted in worktree agent-ab66f11f4772837a5)
Scratch harness: `scratchpad/f16/` (bench_scratch_test.go, equiv_scratch_test.go, bench.sh, equiv.sh, prof.sh)

## 1. Is the finding real? (checked item by item)

| Item | Real? | Notes |
|---|---|---|
| `bindvars["b_"+field.Name]` / `"a_"+...` in applyChange, bindAfterJSONFieldVals, partial rebuild loop, applyBulkInsertChanges | **Yes** | Map keys escape, so this is 1 heap alloc per column per image: 20 allocs for an insert/delete and 40 for an update on a 20-col table. |
| `sqltypes.ValueBindVariable` per column (not in the finding) | **Yes, the biggest item** | 1 `*querypb.BindVariable` alloc per column per image. In the pprof alloc profile it is 57% of alloc objects on the update path. |
| `tp.FieldsToSkip[strings.ToLower(field.Name)]` | **Partly** | `strings.ToLower` returns its input without allocating when the name is already lowercase ASCII, so there is no alloc, only CPU. In the vcopier path (`appendFromRow`, run per column per row) `strings.ToLower` takes **15% of CPU** (pprof). In the vplayer paths it runs only for JSON columns and in the bulk-insert loop. |
| `ConvertCharset[field.Name]`, `ConvertIntToEnum[field.Name]` | Minor | These maps are usually nil or empty, so the lookup returns early. Small CPU cost, no allocs. |
| applyBulkInsertChanges: per-row `strings.Builder` + map + `Append`, then `values.WriteString(rowValues.String())` and `insertPrefix + vals.String()` | **Yes** | Per row: 1 map, about 7 builder growth allocs, and a copy of each row. At the end, a full copy of the whole statement. |
| `valsEqual` uses `ToString()==ToString()` | **No** | `Value.ToString()` is `hack.String(v.val)`: zero-copy, no allocation. `bytes.Equal` would gain nothing. (`pkChanged`'s `"b_"+pkref` lookup keys don't escape and fit the 32-byte stack buffer, so there's no alloc either.) Nothing changed here. |

### When is TablePlan rebuilt / is caching safe?
- `TablePlan.Fields` is set **only** in `ReplicatorPlan.buildExecutionPlan`. That runs for every FIELD event in the vplayer (`vp.tablePlans[name] = tplan`, so any DDL or new FIELD event gives a fresh plan) and when vcopier / vcopier_atomic start copying a table.
- `FieldsToSkip` (from `generate()`), `ConvertCharset` and `ConvertIntToEnum` (from the rule) never change after the plan is built.
- So precomputing in `buildExecutionPlan`, right after `Fields` is assigned, invalidates correctly: a new FIELD event gives a new TablePlan and new metadata.
- Concurrency: vcopier's parallel insert workers share one TablePlan (`vbc.tablePlan.applyBulkInsert`). The metadata is built eagerly and is read-only afterwards. It is never lazily stored, so there is no race.
- The `tplanv := *prelim` copy: prelim plans have no Fields, so `fieldInfos` is nil there and is then built for the copy.

## 2. Prototype (implemented, compiles, vet clean)

- New unexported `TablePlan.fieldInfos []fieldInfo`, where `fieldInfo{beforeName, afterName, skip, charsetConversion, intToEnum}`. It is built by `buildFieldInfos()` in both branches of `buildExecutionPlan`. Bind names use the backtick-trimmed name, `skip` uses `FieldsToSkip[strings.ToLower(name)]`, and the conversions are keyed by the exact name, which keeps the old semantics.
- `getFieldInfos()` falls back to computing the metadata without storing it when a TablePlan was built by hand (tests), i.e. when `len(fieldInfos) != len(Fields)`.
- `bindFieldVal(fi, field, val, dst)` writes into a caller-provided `*BindVariable` (or allocates if `dst == nil`). The charset-converted bytes are used directly instead of `string(out)` → `[]byte`, which saves 2 copies.
- `applyChange`: bind variables for all images come from one `[]querypb.BindVariable` slab, and the map is presized to `nimages*len(Fields)` (it was `len(Fields)`, which made updates grow the map).
- JSON paths, the partial rebuild loop, `checkInsert/UpdateJSONRowSize` and `appendFromRow` all use `fieldInfos` (skip / names / charsetConversion) instead of `ToLower` plus map lookups.
- `applyBulkInsertChanges`:
  - the map and a BindVariable slab are reused across rows, since every row binds the same names and `Append` consumes them right away;
  - rows are rendered straight into one statement builder that already holds the prefix and is pre-sized from the row sizes;
  - the ON DUP suffix is appended in place, so the common case has no final `prefix + values` copy;
  - when a row overflows `maxQuerySize`, the statement is executed up to the row start and the row is carried into a new builder. This gives the same statements and the same `BulkQueryCount` as before.

Diff: 2 files, +376/-52 (about 160 LOC of production code; the rest is tests and benchmarks). No generated code.

## 3. Measurements (MEASURED, interleaved A/B, shared noisy 4-vCPU box)

The package's TestMain needs a local mysqld, which isn't installed here. So I copied the package's non-test files into two throwaway sibling packages, A (HEAD) and B (patched), with a tiny benchmark, then ran the two alternately.

Table: 20 columns (int, varchar, datetime, decimal, text, blob), no JSON. Plan built through the real `buildReplicatorPlan` + `buildExecutionPlan`. The executor is a no-op.

`-cpu=1`, n=10, benchtime 400ms:
```
                              A (HEAD)        B (patched)
ApplyChange20Cols/insert      5.33µs ±14%     4.51µs ±12%   -15%  (p=0.009)
ApplyChange20Cols/update      9.99µs ±11%     6.41µs ±14%   -36%  (p=0.000)
ApplyBulkInsertChanges (100r) 663.8µs ±6%     211.5µs ±20%  -68%  (p=0.000)  => 6.6µs -> 2.1µs per row
```
GOMAXPROCS=4, n=8 (noisier):
```
update/star  -35%, update/cols -38%, pkchange/cols -32%, insert/delete ~ -15..25% (not significant at ±40% noise)
```
allocs/op (deterministic):
```
insert    48 -> 9     (-81%)     B/op 4.87Ki -> 4.84Ki
update    91 -> 10    (-89%)     B/op 9.37Ki -> 8.40Ki
delete    47 -> 8     (-83%)
pkchange  94 -> 13    (-86%)
bulk 100 rows: 5217 -> 108 allocs (52 -> ~1 per row), 656KiB -> 112KiB (-83%)
```
vcopier copy path (`applyBulkInsert` → `appendFromRow`, 100 rows, only the ToLower/map lookups removed):
```
ApplyBulkInsertCopy20Cols  104µs -> 73µs (-30%, p=0.000) and 118µs -> 75µs (-37%, p=0.005) in a 2nd run; allocs unchanged (2)
```
Baseline pprof of that path: `strings.ToLower` = 15.1% flat, and appendFromRow's own flat time is 14% (mostly map lookups).

Where the remaining time goes after the patch (insert): `ParsedQuery.GenerateQuery`/`Append` is about 40% of applyChange (a name→map lookup per bind location plus encoding), then `MakeRowTrusted`, the map assigns, and the slab.

## 4. End-to-end relevance (ESTIMATED, no mysqld available)

- **vplayer (running phase / MoveTables catch-up):** apply is serial, one goroutine per stream, and each transaction waits on a MySQL round trip plus the MySQL execution of single-row DML, typically tens to hundreds of µs per row. On the vttablet side, per row event we have gRPC/proto decode (roughly 1–3µs for a 20-col row), relay log, `applyChange` (5–10µs before the patch), stats, and batching string appends. `applyChange` is therefore probably the largest vttablet-side CPU item per row, perhaps 40–60%. The patch cuts it by 15–36%, so **target vttablet CPU per applied row drops roughly 8–20%**, and allocations per row drop 80–90%, which lowers GC pressure.
  - **Throughput / lag** only improves meaningfully when the tablet is CPU-bound, for example many streams on one target (Reshard fan-in, several workflows, many shards merging) or VPlayerBatching with multi-row events where MySQL does little work per row. With a single stream it stays bound by MySQL latency, so expect about 0–3%.
- **Multi-row INSERT events** use the bulk path because `VPlayerBatching` is on by default (flags=7). Here the patch is a clear win: 6.6 → 2.1µs and 52 → 1 allocs per row. MySQL applies a batched multi-row INSERT at a few µs per row, so vttablet CPU is a larger share on this path.
- **vcopier (copy phase):** `appendFromRow` is 30–37% faster at the micro level, i.e. about 0.3–0.4µs saved per 20-col row. The copy phase also pays rowstream decode and MySQL bulk-insert time, so this is estimated at a low single-digit % of target vttablet CPU during copy.

## 5. Gotchas / review notes

- **Stale-cache guard:** `getFieldInfos` trusts `fieldInfos` when the lengths match. If code ever reassigned `tp.Fields` to a different slice of the same length after `buildExecutionPlan`, the metadata would go stale. No such code exists today (only buildExecutionPlan sets Fields). This contract is documented on the field. An alternative is to also compare `&Fields[0]`.
- **Fallback cost:** hand-built TablePlans (tests, `BenchmarkAppendFromRowLargeJSON`) rebuild the metadata on every call. That is correct, and it's allocs only in tests.
- **Aliasing:** bind variables still alias the row buffer, exactly as `ValueBindVariable` did. A charset-converted value now aliases the fresh `charset.Convert` output instead of a copy. IntToEnum still copies.
- **Nil map values:** `ConvertCharset` with a key mapping to a nil `*CharsetConversion` used to nil-deref (panic). It is now treated as "no conversion". This is strictly safer.
- **Slab retention:** the slab lives only as long as the per-row bindvars map, so nothing is retained after the row. BindVariable contains `protoimpl.MessageState`; there are no struct copies (fields are set in place), so vet copylocks is clean.
- **Pre-existing, not changed (FYI):**
  - (a) In the `select *` path, `buildFromFields` → `generate()` drops the rule's `ConvertCharset`/`ConvertIntToEnum`. The prelim plan had them, but the execution plan does not. OnlineDDL uses explicit columns, so this is probably harmless, but it looks like a latent bug.
  - (b) With an explicit column list that selects a target-generated column, `appendFromRow` indexes past `Fields` and panics. Workflows never generate such filters.
  - (c) The partial-query cache key `hex.EncodeToString(dataColumns.Cols)` allocates per partial row. `m[string(bytes)]` would not allocate. This matters only for NOBLOB/MINIMAL images.
- **Further (not done):** render SQL directly by column index instead of going through the name→bindvar map plus `ParsedQuery` (the `appendFromRow` approach). That is a bigger refactor touching the plan builder and partial queries. It would remove the remaining roughly 40% spent in GenerateQuery.
- Release compatibility: internal-only change. There are no wire, flag or proto changes, and generated SQL is byte-identical.

## 6. Tests

- **Equivalence (scratch, MEASURED):** A randomized driver ran against A and B and dumped every generated SQL string and every error; the two dumps were **byte-identical** (58,501 lines, about 20k SQL statements, 2,287 errors).
  - Plan variants: select * and explicit columns; mixed-case and lowercase colInfo names; generated INT and JSON columns; utf8mb4→latin1 charset conversion including failing conversions (日本); int→enum; MaxRowJSONBytes 0 and 40.
  - Row changes: insert, update, delete and pk-change; NULLs; JSON docs including the `null` literal; partial DataColumns bitmaps; PARTIAL_JSON diffs, both empty and JSON_SET/JSON_REMOVE.
  - Bulk insert with maxQuerySize 0 / 250 / 400 / 1MiB, the vcopier `applyBulkInsert`, and bulk delete.
- **Added to replicator_plan_test.go:**
  - `TestBuildExecutionPlanFieldInfos`: explicit columns with backtick trimming plus charset and enum conversions, including an end-to-end applyChange; select * with case-insensitive skip of a generated column.
  - `TestTablePlanFieldInfosFallback`: on-demand metadata is computed and not stored.
  - `TestApplyBulkInsertChangesSplitCarriesRow`: 5 rows split 2/2/1 with the ON DUP suffix.
  - `BenchmarkApplyChange20Cols` and `BenchmarkApplyBulkInsertChanges20Cols`.
- The real package can't run here (TestMain needs mysqld). The copied `replicator_plan_test.go` + `vplayer_test.go` (all existing pure tests plus the new ones) **pass with -race** against the patched code. `go vet` on the real package is clean.
- The bench, equivalence and fmt harness files are in `scratchpad/f16/`.
