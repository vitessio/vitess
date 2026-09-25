# F20 — BIT value SQL encoding (`encodeBytesSQLBits`) uses fmt per byte

## Finding re-verified
`go/sqltypes/value.go:940` (HEAD aa9ccf9):

```go
func encodeBytesSQLBits(val []byte, b BinWriter) {
	fmt.Fprint(b, "b'")
	for _, ch := range val {
		fmt.Fprintf(b, "%08b", ch)
	}
	fmt.Fprint(b, "'")
}
```

Dispatched from all three entry points for `v.Type() == Bit`:
- `Value.EncodeSQL(BinWriter)` (line 546). Callers pass `*strings.Builder` (sqlparser `encodable.go`, `parsed_query.go`,
  `vtgate/engine/set.go`), `*bytes2.Buffer`/`*strings.Builder` (binlog_streamer, rowstreamer lastpk, table_plan_builder,
  wrangler vdiff).
- `Value.EncodeSQLStringBuilder` (line 563). Callers: sqlparser `ParsedQuery.GenerateQuery` bind-var encoding (vttablet query
  generation with BIT bind vars), evalengine `format.go` (literal formatting).
- `Value.EncodeSQLBytes2` (line 591). Hot caller: vreplication `TablePlan.appendFromRow`
  (`replicator_plan.go:1132`), which is used for every row in copy phase / bulk inserts; every BIT column value goes through here.

Each call does 2 + len(val) `fmt.Fprint*` calls. With a `*strings.Builder` target each Fprint issues a separate `Write`, so the
builder grows repeatedly (5 allocs for an 8-byte value).

## Change (prototype, uncommitted in worktree)
Only `encodeBytesSQLBits` is rewritten; its dispatch lines in the three EncodeSQL* functions are untouched.

- `bitChars [256][8]byte` table (2 KiB, built at init by shift/mask) with the 8 ASCII digits of each byte, MSB first.
- `appendBytesSQLBits(dst, val []byte) []byte` appends `b'` + table rows + `'`.
- `encodeBytesSQLBits` type-switches on the writer: for `*bytes2.Buffer`, `*strings.Builder`, `*bytes.Buffer` it builds into a
  `[128]byte` stack buffer (fits values up to 15 bytes; BIT(64) = 8 bytes = 67 chars) and does one concrete `Write`; longer
  values just let `append` spill to the heap. For any other `BinWriter` it `make`s an exact-size slice and does one `Write`
  (the stack buffer cannot be used there: passing it to an interface method makes it escape, and escape analysis is
  flow-insensitive, so each case has its own local array). Verified with `-gcflags=-m`: no "moved to heap" for `tmp`.

Output is byte-identical, including the empty value -> `b''` (old code printed `b'` + `'`).

## Inner loop variants (measured, scratch benchmark appending into a reused 1 KiB buffer, 6 runs, medians)
| bytes | table `[256][8]byte` | shift/mask (8 appends) | SWAR (mul+mask, PutUint64) |
|---|---|---|---|
| 1 | ~7 ns | ~7 ns | ~4.5 ns |
| 8 | ~11 ns | ~21-80 ns (noisy) | ~11.5 ns |
| 64 | ~64 ns | ~200 ns | ~74 ns |

Table and SWAR are equivalent; shift/mask is 2-3x slower. Chose the table for readability. SWAR alternative (no table):
```go
x := uint64(ch) * 0x0101010101010101 & 0x0102040810204080
x = ((x + 0x7f7f7f7f7f7f7f7f) & 0x8080808080808080) >> 7
dst = binary.LittleEndian.AppendUint64(dst, x+0x3030303030303030)
```

## A/B micro benchmark (measured; interleaved old/new, 4x2 runs, 200ms, benchstat)
`BenchmarkEncodeSQLBits` (added to value_test.go), value bytes = 0xa5 repeated:

```
                                             old          new
EncodeSQL/strings.Builder/1        192.2n ± 37%   41.0n ± 37%  -78.67%   2 -> 1 allocs, 24 -> 16 B
EncodeSQL/bytes.Buffer/1           123.7n ± 36%   11.1n ± 13%  -91.02%   0 -> 0
EncodeSQLStringBuilder/1           182.7n ± 49%   28.4n ± 86%  -84.46%   2 -> 1 allocs
EncodeSQLBytes2/1                  118.5n ± 18%   10.9n ± 15%  -90.79%   0 -> 0
EncodeSQL/strings.Builder/8        870.4n ± 34%   87.0n ± 44%  -90.01%   5 -> 1 allocs, 248 -> 80 B
EncodeSQL/bytes.Buffer/8           517.3n ± 10%   15.2n ± 20%  -97.06%   0 -> 0
EncodeSQLStringBuilder/8           825.8n ± 42%   79.5n ± 40%  -90.37%   5 -> 1 allocs, 248 -> 80 B
EncodeSQLBytes2/8                  539.6n ± 23%   14.6n ±  7%  -97.29%   0 -> 0
geomean                            317.5n         26.1n        -91.77%
```
The remaining 1 alloc in the strings.Builder cases is the builder's own buffer (the benchmark calls `Reset()`, which drops
the builder's storage), i.e. it is not from the encoder. Ratio: ~10x (1 byte) to ~35x (8 bytes, bytes2 = vreplication path).

## End-to-end relevance (estimated, not measured)
- vreplication copy/apply (`appendFromRow`): each BIT column value costs ~520 ns -> ~15 ns. For comparison an int/varchar column
  costs tens of ns through `EncodeSQLBytes2`, so on tables with BIT columns the BIT encoding was likely the single most
  expensive column in the query-building step; e.g. a 10-column row with 1 BIT column: roughly 0.8 us -> 0.3 us of
  query-building CPU per row. Relative to the MySQL-side cost of executing the INSERT, this is a small fraction of
  wall-clock, but it is pure vttablet CPU saved at copy throughput of 100k+ rows/s.
- Query generation with BIT bind vars (vttablet `GenerateQuery`), sqlparser/evalengine formatting of BIT values: same
  per-value saving, rare in typical workloads.
- No package benchmark covers BIT columns (`BenchmarkAppendFromRowLargeJSON` is JSON only), and the vreplication test package
  needs a MySQL test env (its `testenv` init panics without one here), so no end-to-end profile was taken.

## Other '%08b'-style per-byte formatting (checked)
- `go/mysql/binlog/rbr.go` `TypeBit`: returns the raw bytes via `MakeTrusted(Type_BIT, ...)`; no formatting.
- sqlparser `BitNum` literals (`ast_format.go:1574`): printed verbatim (`%#s` of the literal text); no per-byte fmt.
- evalengine: `BIN()`/`CONV()` use strconv-based formatting; `parseBitNum` parses. BIT evals formatted via
  `EncodeSQLStringBuilder`, i.e. they benefit from this change.
- vstreamer `%b` uses (vstreamer.go:1452/1488/1501) are in error messages only.
- `go/mysql/collations/tools/makecolldata` uses `0b%08b` in a code generator (offline).
Nothing else worth changing.

## Difficulty
S. ~40 LOC in value.go (one function body + a table + a helper), ~120 LOC tests/bench in value_test.go. No generated code.

## Merge with F03 (escaping encoders rewrite)
This patch changes only the `encodeBytesSQLBits` body (after `EncodeStringSQL`, before `SQLEncodeMap`) and adds two
declarations right above it; the `case v.Type() == Bit:` dispatch lines are unchanged, so F03 can freely rewrite the
IsBinary/IsQuoted branches. A textual conflict is only possible if F03 edits the last lines of `EncodeStringSQL` (adjacent
hunk) or adds tests right before `TestIsComparable` in value_test.go; both are trivial to resolve. If F03 introduces its own
concrete-writer type switch / `append*` helper convention, `appendBytesSQLBits` fits it directly (its signature is
`append(dst, val) []byte`). `fmt` stays imported in value.go (other uses remain).

## Gotchas
- Output must remain byte-identical (it becomes SQL sent to MySQL); covered by the equivalence test against a
  `fmt.Sprintf("%08b")` reference for all 256 byte values, empty/nil, and random lengths 0..64 across all 6 entry
  point/writer combinations (strings.Builder, bytes.Buffer, bytes2.Buffer, generic writer, EncodeSQLStringBuilder,
  EncodeSQLBytes2).
- The equivalence test passes on main too (verified by running it against the original value.go) — it is a regression guard
  for a pure performance rewrite, not a bug-fix test; the benchmark carries the "fails on main" signal.
- Stack buffer is only safe with concrete-typed writes; a future refactor that funnels all cases through `b.Write` would make
  it escape (silently adding an alloc, not a correctness issue).
- Portability: table is endian-agnostic; the SWAR alternative uses explicit LittleEndian so it is too.
- No wire/format change -> no release-compatibility concern.
- The 2 KiB table is initialized at package init (256x8 loop, negligible).

## Tests
- Added `TestEncodeSQLBits` (empty, every byte value, random lengths 0..64) and `BenchmarkEncodeSQLBits` in
  `go/sqltypes/value_test.go`.
- Passed: `go test ./go/sqltypes ./go/bytes2 ./go/vt/sqlparser ./go/mysql/json`, evalengine TestFormat subset, `go vet ./go/sqltypes`,
  `scripts/fmt` clean.
- `go/vt/vttablet/tabletmanager/vreplication` could not be run: its testenv init panics without a MySQL environment (unrelated).
