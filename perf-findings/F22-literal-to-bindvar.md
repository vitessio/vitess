# F22 literal-to-bindvar: normalizer literal/IN-list/alias allocations and ValidateBindVariable

Patch: `findings/F22-literal-to-bindvar.patch` (uncommitted in worktree agent-ab8f4840cdf74c743)
Files: go/vt/sqlparser/normalizer.go, go/vt/sqlparser/normalizer_test.go, go/sqltypes/bind_variables.go, go/sqltypes/bind_variables_test.go

## Verdict: do it
The finding is real. The fix is small and local, and it drops allocations substantially. Aliasing the query string's bytes is safe after an audit (see below).

## What was wrong (verified)
- `literalToBindvar` (normalizer.go ~496): `lit.Bytes()` = `[]byte(node.Val)` escapes, so every literal gets a full copy. HexNum/HexVal did `make` + `bytes.ToUpper` (a second alloc) + append. BitNum also made a copy.
- `rewriteInComparisons` (~445): for each IN element it called `literalToBindvar` (1 BindVariable + 1 []byte copy, both thrown away), then allocated a `&querypb.Value`. That is 3 allocs per element. Then `bvals.MarshalVT()` serialised the whole tuple for the dedup key, and `string(key)` made a second full-size copy on insert.
- `noteAliasedExprName` (~206): a closure plus a map entry per unaliased select expr. It also formatted `ColName` exprs, because the parser leaves `InputExpression` empty for plain columns. That Format was most of its cost in the profile.
- `ValidateBindVariable` (sqltypes ~359): `&querypb.BindVariable{...}` escapes (the function is recursive), so there is 1 heap alloc (96 B) per tuple element on every gRPC Execute with IN-list bind vars.

## Change
1. `literalToValue(lit) (typ, []byte, ok)`: for Str/Int/Float/Decimal/Date/Time/Timestamp, and BitNum with a `0b` prefix, the value aliases `lit.Val` through `hack.StringBytes`. An empty string maps to `[]byte{}`, which keeps the old non-nil value so DeepEqual-based tests still pass. HexNum/HexVal build one buffer and do an in-place ASCII uppercase. `literalToBindvar` wraps it.
2. `rewriteInComparisons`: one `[]querypb.Value` slab plus one `[]*Value`. The dedup key is a `maphash` (per-process seed) over (type, len, bytes) for each element. `map[uint64]string` holds the keys, and a hit is confirmed with `tupleValuesEqual`, so a hash collision only costs a missed dedup and never gives a wrong result.
3. `onLeave` changes from a closure map to a `[]pendingAlias` stack, matched by the pointer at the top in walkUp. Unaliased `*ColName` select exprs are no longer tracked: walkDown never descends into ColName, so nothing under it can be rewritten. `shouldParameterize`'s `len(onLeave)` still means the same thing.
4. `validateValue(typ, val)`, which does not allocate, is used for tuple elements.

## Benchmarks (A/B = old vs new test binaries run interleaved, n=10, shared 4 vCPU, so time numbers are noisy)
New `BenchmarkNormalizeLiterals` (Normalize only; the parse runs with the timer stopped):
```
                          old sec/op     new sec/op    delta      old allocs  new allocs
in1000int                 180.7µ ±20%    59.8µ ±18%    -67%       3036        24
in1000str                 209.2µ ± 7%    49.8µ ±11%    -76%       3036        24
in1000hex                 207.0µ ± 8%    71.1µ ±17%    -66%       4036        1024
insert500 (2500 lits)     1072.6µ ±14%   903.2µ ±15%   -16%       16.97k      14.46k   (B/op -7%)
twoSameIn (2x10 IN)       8.73µ ± 7%     4.61µ ±18%    -47%       100         29
selectExpr                7.38µ ±10%     6.01µ ±10%    -19%       58          43
```
Existing end-to-end benchmarks (parse + normalize + String for each query of lobsters / TPCC traces):
```
                 sec/op (noise ±35-75%, n.s.)   B/op     allocs/op
NormalizeVTGate  114.2m -> 105.9m  (~ -7%, p=0.09)  -5.8%   -7.8%
NormalizeTPCCIns 115.2m -> 102.1m  (n.s.)           -7.3%   -12.3%
NormalizeTPCC    92.7m  -> 81.6m   (n.s.)           -11.0%  -17.3%
```
CPU profile of NormalizeVTGate+TPCC: `literalToBindvar` went from 2.89% to 1.21% of samples (12.9% to 5.4% of `Normalize` cum). `noteAliasedExprName` was 2.30% before the ColName skip and 1.56% after the stack change alone; the ColName skip removes most of what remains, since that remainder was ColName.Format.

Dedup key alone (temporary microbenchmark, 1000 x ~30B strings): MarshalVT + string(key) takes 60-130µs with 80 KB and 2 allocs. maphash takes about 17µs with 0 allocs. For a 10-element tuple it is 0.75-1.5µs (768 B) vs 0.15-0.2µs.

`BenchmarkValidateBindVariables` (1000-int tuple): 165µs (noisy) to 15.7µs, and 93.75 KiB with 1000 allocs to 0 B with 0 allocs.

End-to-end estimate: normalize is roughly 20-25% of vtgate's parse+normalize CPU for typical OLTP queries, and the literal/alias parts were about 5% of it, so a few % of vtgate query-prep CPU plus 8-17% fewer allocs there. For workloads with large IN lists or bulk INSERTs the gain is much larger: 3-4x faster normalize of IN lists, and 1000 fewer allocs per 1000-element IN list on gRPC ValidateBindVariables.

## Aliasing audit (hack.StringBytes(lit.Val))
- **Is the query string owned?** Yes. MySQL protocol: `parseComQuery` returns `string(data[1:])`, a copy. gRPC: vtproto `UnmarshalVT` copies strings (`UnmarshalVTUnsafe` is not used anywhere outside generated code). The tokenizer's `buf` is the query string itself: `Literal.Val` is a substring of the query except for `scanStringSlow` (escapes), which builds its own string, and literals synthesised in code (`NewIntLiteral(strconv.Itoa(...))`, constants).
- **In-place writes to bind var bytes:** I grepped vtgate, vttablet, sqltypes, sqlparser and mysql for index-assignments / copy / decode into `.Value`, `.val`, `.bytes`, `Raw()`. Found:
  - `evalengine.parseHexNumber` writes `val[1]='0'` and then restores it. It is reached on HEXNUM bind vars (`push_hexnum`, `valueToEval`). HexNum therefore stays a private copy, and I added a code comment saying so. Separately, this is already a data race today if one HEXNUM bind var is evaluated concurrently.
  - `upcaseASCII` in evalengine only operates on freshly built output.
  - `mysql/conn.go:1342` `val.Value = append(val.Value, chunk...)` is on prepared-statement bind vars that do not come from the normalizer. Appending to an aliased slice is safe anyway because cap == len, so append reallocates.
  - vtproto `UnmarshalVT` does `m.Value = append(m.Value[:0], ...)`, which would write into the aliased memory if someone unmarshalled into an existing normalizer-built BindVariable/Value. Nothing does that today: there is no vtproto pool for these messages and vtgate never re-unmarshals into them. This is the main latent hazard to flag in review. If it ever happens with a Go string constant it segfaults (rodata); with a heap string it silently corrupts the query text.
  - `truncateInPlace` and `charset.Slice` only reslice.
- **Lifetime/retention:** bind vars now keep the whole query string alive. This adds no retention in practice. `engine.Plan.Original` already retains the first query string in the plan cache, and AST identifiers (ColName/TableName) are substrings of the query. Query logs hold the SQL alongside the bind vars. Over gRPC to vttablet the values are serialised, so nothing is shared. For in-process vttablet (vtcombo/vtexplain) the bytes are shared but only read. SET statements are not parameterized (`parameterize=false`), so session user variables don't capture aliased bytes. For multi-statement queries a piece's bind vars retain the whole multi-statement string, the same as Plan.Original.
- **Concurrency:** strings are immutable, so there are no new races.

## Gotchas
- Aliasing: see the vtproto UnmarshalVT reuse hazard above. It would be worth documenting on `literalToValue` or in the PR that normalizer bind var bytes are read-only.
- The ASCII-only uppercase differs from `bytes.ToUpper` only for non-ASCII input, which the tokenizer never produces for hex literals.
- Dedup map key changed from the serialized tuple to a 64-bit hash plus an equality check. The result is the same, including tuples that differ only in element types or in element boundaries (tests added).
- The IN-list values now live in one slab, so a single element keeps the whole slab alive. `BuildBindVariable` already does the same.
- The `pendingAlias` stack relies on walkDown/walkUp nesting. A mismatch (the node was replaced) leaves the entry in place, which matches the old map behaviour exactly.
- Release compatibility: no wire or behaviour change.

## Tests
- Existing: all of go/vt/sqlparser and go/sqltypes pass. go/vt/vtgate, vtgate/engine, vtgate/planbuilder, vtgate/evalengine, vtgate/vindexes, vtgate/executorcontext and vtexplain (in-process vttablet) pass.
- Added:
  - `TestLiteralToBindvarMatchesLegacy`: equivalence with the old implementation across all literal kinds, 0X/0B spellings, overflow and invalid numbers, empty values, and parser-produced literals.
  - `TestLiteralToBindvarDoesNotCopy`: AllocsPerRun == 1. It fails on main, where there are 2.
  - Three TestNormalize cases for IN dedup: same bytes with different types, same concatenation with different boundaries, and hex spellings deduplicating.
  - `TestValidateBindVariableTupleDoesNotAllocate`: fails on main (4 allocs).
  - Benchmarks `BenchmarkNormalizeLiterals` and `BenchmarkValidateBindVariables`.
- Note: the existing `BenchmarkNormalize` / `BenchmarkNormalizeTraces` re-normalize an already-normalized AST after the first iteration, so they barely measure literal handling. Use NormalizeVTGate/TPCC or the new benchmark instead.
