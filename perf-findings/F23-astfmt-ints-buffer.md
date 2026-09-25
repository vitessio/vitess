# F23: astfmt integer formatting, enum names, and String() buffer sizing

Patch: `F23-astfmt-ints-buffer.patch` (uncommitted in worktree agent-aa9c19adb7b878c88).
Scratch benchmarks and raw data: `scratchpad/f23/` (zz_f23_*_test.go, old.txt/new.txt, hint.txt, micro.txt, ab.sh).

## Verification of the finding

- `go/tools/astfmtgen/main.go`: `%d` was rewritten as `buf.WriteString(fmt.Sprintf("%d", x))`, which produced 24 call sites in
  `ast_format_fast.go`: Argument (DECIMAL/DATETIME/TIME size and scale, plus the `/* DECIMAL(4,2) */` comment), ColumnType,
  ConvertType, CurTimeFuncExpr, Offset, Kill, partition options, and so on. The generator handles the verbs `%s`, `%l`/`%r`/`%v`, `%d`, `%n`;
  only `%d` went through fmt. `%c` is not supported, and neither the generator nor `Myprintf` handles it.
- Correction: `fmt.Sprintf("%d", x)` does not allocate for one-digit values, because Go interns 1-byte strings. So the
  allocation only happens for sizes of 10 or more (for example `DECIMAL(12,4)`). It still costs about 50ns compared with 9ns (measured).
- `TrackedBuffer.WriteInt/WriteUint` used `strconv.FormatInt`, which allocates for values of 100 or more (0..99 are interned).
- `Argument.FormatFast` calls `node.Type.String()`, which does a protobuf reflection lookup, for every typed bind var. Every normalized literal is a typed Argument
  (`/* INT64 */`, `/* VARCHAR */`, and so on), so this runs once per bind var per query.
- `sqlparser.String` grows the buffer from zero. **Additional finding:** `String()` also records `bindLocations` for every
  `WriteArg` (a growing `[]BindLocation` slice), although only `ParsedQuery()`/`HasBindVars()` read them, and `String()` throws
  the buffer away. This turned out to be the biggest allocation source after buffer growth.
- executor.go:1434: `query = sqlparser.String(stmt)` runs for every non-prepared query where `UpdateQueryFromAST` is set.
  The result is stored as `plan.Original` in the plan cache, so buffer slack gets retained.

## Changes (prototype)

1. The generator's `%d` becomes `buf.WriteInt(int64(x))`, or `buf.WriteUint(uint64(x))` for unsigned types. It uses go/types, panics at generation time on
   non-integer or untyped args, and skips the conversion when the arg is already int64/uint64. The `fmt` import injection is removed.
   Regenerated with `go generate ./go/vt/sqlparser/generate.go`. The only diff is in ast_format_fast.go: 24 Sprintf sites plus the
   dropped `fmt` import. Re-running generate produces no further diff.
2. `WriteInt/WriteUint` use `strconv.AppendInt(scratch[:0])` with a `[20]byte` on the stack and `buf.Write`, with 0 allocs (verified with AllocsPerRun).
3. `typeName(t)` in ast_funcs.go is a `[256]` table indexed by `uint8(t)`, built at init **from `querypb.Type_name`**, so it stays in
   sync with the proto. The low byte is the unique MySQL type id. Each slot stores the full value and is checked on lookup. If a future proto
   adds a colliding low byte, that slot is disabled and falls back to `t.String()`, as do unknown values. ast_format.go uses it for the
   `/* %s` comment.
4. `StringWithSizeHint(node, hint)` is new. `String()` now delegates to it with hint=0. Both set `skipBindLocations` (a new bool on
   TrackedBuffer, false by default, so every other TrackedBuffer user is unaffected). If a hint was given and it overshot by more than 2x, the result is
   `strings.Clone`d so the plan cache does not retain a huge buffer, for example when a long IN list collapses to `::vtg1`.
5. executor.go uses `StringWithSizeHint(stmt, min(len(q)+len(q)/4+16, 64KiB))`.

## Measurements (4 vCPU shared, load average about 10, so `-cpu=1` and interleaved old/new binaries, n=10)

Micro (single binary):
```
F23TypeName/proto   15.3ns   F23TypeName/table 1.75ns          (~8.7x)
F23Int/sprintf      51.6ns 1 alloc (value 12)   F23Int/writeint 8.7ns 0 allocs
```
String(single Argument), old binary vs new binary (includes skipBindLocations):
```
F23Argument/int64      255n -> 195n  (-24%)  7 -> 6 allocs
F23Argument/decimal    373n -> 237n  (-36%)  7 -> 6 allocs
F23Argument/datetime   314n -> 205n  (-35%)  7 -> 6 allocs
```
Formatting the normalized statements of each trace (the executor step), old `String` vs new `StringWithSizeHint(len*1.25+16)`:
```
django (290 q)     264.4µ -> 194.9µ (-26%, p=0.000)  2434 -> 1292 allocs (-47%)  126.0Ki -> 79.2Ki (-37%)
lobsters (10k q)   14.82m -> 11.18m (-25%, p=0.000)  101.5k -> 42.9k allocs (-58%) 5.10Mi -> 3.52Mi (-31%)
```
Per query on lobsters: **1.48µs → 1.12µs, 10.2 → 4.3 allocs, 534 → 369 B**.

Attribution on lobsters, allocs per query: old 10.15, then skipBindLocations plus ints/names gives 7.97, then hint = len gives 4.72, then hint = 1.25*len+16 gives 4.29 (clone guard included).
Hint alone vs no-hint (new binary): lobsters 13.3ms → 11.4ms (-14%), django -20%.

End-to-end: `BenchmarkNormalizeVTGate` (parse, normalize, and String, for lobsters) on the old binary measures about 9.2µs/query, 65.7 allocs, 3.73KB.
The format step is therefore about 16% of the parse+normalize+format path. The measured saving of about 0.36µs is **about 4% of that path**, with
about 9% fewer allocs and about 4% fewer bytes. On the new binary, NormalizeVTGate, which calls String and so gets no hint, drops allocs by 3.3% (657k → 636k, p=0.000) and bytes by 2.1%.
Its time delta is lost in noise (±40% under -cpu=4). The traces contain few DECIMAL/DATETIME literals of 10 or more, so parts (a) and (b) matter mostly for
workloads with decimal or fractional-time literals, at about 50ns per such literal plus about 13ns per typed bind var.

Hint factor sweep (lobsters, allocs per 10k queries / retained slack): no hint 79.7k/572KB, hint=len 47.8k/1.06MB (before the clone guard),
1.25x+16 42.9k/(bounded by the clone guard), 1.5x 41.6k. 81% of lobsters queries get *longer* after normalization, which is why hint=len alone still regrows 81% of the time.

## Difficulty
S. About 120 LOC of non-generated code (generator 30, tracked_buffer 35, ast_funcs 35, executor 10) plus 24 generated lines and about 130 LOC of tests.
Files: go/tools/astfmtgen/main.go (+test), go/vt/sqlparser/{ast_format.go, ast_format_fast.go (generated), ast_funcs.go, tracked_buffer.go (+test)}, go/vt/vtgate/executor.go.

## Gotchas
- Generated-code drift: CI's codegen check requires ast_format_fast.go to match the generator output. It is regenerated and stable.
- The generator now *requires* type info for `%d` args (it panics at generate time otherwise). The unit tests in astfmtgen/main_test.go
  were updated to register types.
- `typeName` has to stay identical to `Type.String()`. It is built from `querypb.Type_name` and verified for every enum value plus unknown values
  in TestTypeName. A collision disables the fast path instead of returning a wrong name.
- `skipBindLocations` changes `String()`'s internal behaviour only. The output is identical, and nothing reads `bindLocations` from a String buffer.
  `FormatFast` code must never call `HasBindVars()`; none does today.
- Size hints increase retained slack for queries that shrink (IN-list collapse, bulk inserts). This is mitigated by the >2x clone guard (only
  when a hint is given) and the 64KiB cap in the executor. `plan.Original` is retained in the plan cache, and CachedSize counts `len` rather than `cap`, both
  before and after this change.
- `%d` in the slow path (`Myprintf`) already used WriteInt, so that path gets the allocation-free WriteInt too. No output change.
- Release compatibility: output bytes are unchanged. `StringWithSizeHint` is a new exported function (additive).
- Remaining allocations in String: TrackedBuffer, strings.Builder, and the buffer. You could fold TrackedBuffer and Builder into one allocation, or use a
  pooled []byte builder with an exact-size copy, but that changes the TrackedBuffer layout. Left as a follow-up.

## Tests
- Added: TestTrackedBufferWriteInt (edge values and an alloc check; fails on main), TestTypeName (all enum values and unknown ones),
  TestStringWithSizeHint (the output is equal for hints -1, 0, 1, len, and 10x len, including the clone path), and astfmtgen cases for int64 (no conversion),
  unsigned (WriteUint), a non-integer panic, and an unknown-type panic.
- These pass: go/vt/sqlparser/..., go/tools/astfmtgen, go/vt/vtgate/planbuilder/..., and go/vt/vtgate. The format output is unchanged, and
  the normalizer tests cover `/* DECIMAL(2,1) */` and `CAST(... AS DATETIME(6))`.
