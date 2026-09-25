# F03-sql-escape: run-copying SQL string escaper (sqltypes)

## Verdict
**Do it.** The finding is real. The change is small and self-contained: one production file plus tests. Output is byte-identical to the old code, verified by exhaustive, random and fuzz equivalence tests against it. It is 4-20x faster on realistic values and allocates less. On the pathological all-escapes case it is within noise of the old code.

## Finding re-verification
`go/sqltypes/value.go` (HEAD aa9ccf9), lines 852-938:
- `encodeBytesSQLBytes2` / `encodeBytesSQLStringBuilder`: one `WriteByte` per input byte, with a `SQLEncodeMap` lookup. In `\%` and `\_` the backslash is kept unescaped.
- `BufEncodeStringSQL`: `for idx, ch := range val` iterates over *runes* and calls `WriteRune` per rune.
- `encodeBytesSQL(val, BinWriter)` / `encodeBinarySQL`: build a fresh zero-capacity `bytes2.Buffer`, then copy it into the writer. That is two copies plus log2(n) growth allocations per value.
- Escape set (from `encodeRef`): 0x00, `'`, `\b`, `\n`, `\r`, `\t`, 0x1a, `\`. Confirmed. No code outside `value.go` writes `SQLEncodeMap`.

### Callers (hot paths)
- vttablet `ParsedQuery.GenerateQuery` -> `EncodeValue` -> `Value.EncodeSQLStringBuilder`. This runs for every query with bind vars. The big win is for inserts and updates with text or blob bind vars.
- vreplication `TablePlan.appendFromRow` (copy-phase bulk inserts) and the JSON walker in `mysql/json/marshal.go` -> `EncodeSQLBytes2`.
- `sqlparser` `Literal.Format/FormatFast` -> `EncodeSQL(*TrackedBuffer)` -> BinWriter path. Previously this built a temp buffer and copied it for every string literal.
- Also on the BinWriter path via `EncodeSQL(BinWriter)`: vstreamer rowstreamer lastpk, the binlog streamer, and vtgate `SET`.
- `mysql/json` `MarshalSQLTo` -> `EncodeStringSQL`.
- Otherwise `EncodeStringSQL` is used for low-frequency admin and sidecar query building.

## BufEncodeStringSQL rune semantics
For *valid* UTF-8, iterating runes with `WriteRune` gives the same bytes as a byte-wise copy:
- All escapable bytes are ASCII, and ASCII never appears inside a multi-byte sequence.
- Runes 0x80-0xFF are never escaped, and `WriteRune` re-emits the same two bytes.

The only observable effect is on **invalid UTF-8**: each invalid byte becomes U+FFFD (EF BF BD). This is almost certainly accidental, because the []byte variants keep the raw bytes. To stay byte-identical, the new code calls `utf8.ValidString(val)` first and falls back to the old rune loop for invalid input.

If maintainers agree the replacement is a bug, the fallback can be deleted later. That would be a behaviour change and needs a release note.

## Implementation (prototype in worktree, uncommitted)
- `nextSQLEscapeCandidate[T string|[]byte](val, i)`: 8-byte SWAR scan with a scalar tail. It flags bytes < 0x20, == `'` and == `\`.
  - The load is built byte by byte as little-endian. It compiles to a single MOVQ on amd64 and MOVD on arm64, and the result does not depend on host endianness.
  - The lowest flagged byte is always a true match. Borrow false positives only appear above it.
- `sqlEscapeAt(val, i)` (inlined): returns the escape char or `DontEscape`, including the `\%`/`\_` rule.
- `nextSQLEscape`: find a candidate, verify it with `sqlEscapeAt`, and continue scanning if it is a false positive.
- Encoders loop over two steps:
  1. Bulk `Write`/`WriteString` of the clean run.
  2. A tight inner loop that escapes consecutive escapable bytes. This keeps the all-escapes case at legacy speed.
- `encodeBytesSQL`/`encodeBinarySQL`: type switch on `*strings.Builder` / `*bytes2.Buffer`. The default path writes runs straight into the `BinWriter`. Escape pairs come from a global `[256][2]byte` table, so there is no temp buffer and no allocation.
- `EncodeStringSQL` calls `Grow(len+2)` on its fresh builder, so it allocates exactly once. No other path calls `Grow`:
  - I tried `Grow(len+2)` in the builder paths. It changed size-class rounding (+32 B/op on point-select).
  - Bulk `Write` already grows in one step, so it is not worth it.
  - For the same reason, `bytes2.Buffer.Grow` was not added.
- Size: ~190 LOC changed in `go/sqltypes/value.go`, ~390 LOC of sqltypes tests, and a 66-LOC benchmark in sqlparser.
- No archsimd/AVX: stdlib only, portable.

## Microbenchmarks
All numbers here are measured. The machine is a shared, noisy 4-vCPU box (load average ~15-20). Runs were interleaved, n=8.

Variants:
- **legacy**: the old code, copied into `value_escape_legacy_test.go`.
- **scalar**: the new structure with the SWAR loop disabled (byte table scan).
- **swar**: the final code.

Inputs:
- Text cases are ASCII prose.
- escN: a quote every N bytes.
- utf8: mixed Latin and CJK.
- binary: random bytes.

The EncodeSQLStringBuilder and EncodeStringSQL benchmarks allocate a fresh builder per op, so allocation dominates the short cases.

```
goos: linux
goarch: amd64
pkg: vitess.io/vitess/go/sqltypes
cpu: Intel(R) Xeon(R) Processor @ 2.10GHz
                                     │   legacy.txt   │              scalar.txt               │               swar.txt               │
                                     │     sec/op     │     sec/op      vs base               │    sec/op      vs base               │
EncodeSQLBytes2/8B-4                    13.62n ±  26%    18.28n ±  63%        ~ (p=0.105 n=8)    12.71n ± 30%        ~ (p=0.328 n=8)
EncodeSQLBytes2/32B-4                   62.83n ± 112%    39.41n ±  49%  -37.28% (p=0.028 n=8)    16.03n ± 33%  -74.49% (p=0.000 n=8)
EncodeSQLBytes2/128B-4                 219.50n ±  30%   122.20n ± 131%        ~ (p=0.065 n=8)    43.31n ± 80%  -80.27% (p=0.000 n=8)
EncodeSQLBytes2/1KiB-4                 1552.5n ±  63%    830.4n ±  78%  -46.52% (p=0.015 n=8)    235.9n ± 12%  -84.81% (p=0.000 n=8)
EncodeSQLBytes2/16KiB-4                19.204µ ±  85%   15.350µ ±  51%        ~ (p=0.083 n=8)    3.645µ ± 48%  -81.02% (p=0.000 n=8)
EncodeSQLBytes2/1KiB-esc64-4           1461.5n ±  21%   1163.0n ±  87%        ~ (p=0.328 n=8)    346.8n ± 19%  -76.27% (p=0.003 n=8)
EncodeSQLBytes2/1KiB-esc8-4             1.479µ ±  14%    1.891µ ±  61%  +27.82% (p=0.007 n=8)    1.298µ ± 30%        ~ (p=0.398 n=8)
EncodeSQLBytes2/1KiB-esc1-4             2.028µ ±  34%    2.143µ ±  99%        ~ (p=0.574 n=8)    2.244µ ± 12%        ~ (p=0.645 n=8)
EncodeSQLBytes2/1KiB-utf8-4            1323.5n ±  20%    820.3n ±  75%  -38.02% (p=0.050 n=8)    264.2n ± 51%  -80.03% (p=0.000 n=8)
EncodeSQLBytes2/1KiB-binary-4          1702.5n ±  71%   1243.5n ±  37%  -26.96% (p=0.050 n=8)    958.7n ± 70%  -43.69% (p=0.015 n=8)
EncodeSQLStringBuilder/8B-4            100.94n ±  42%    97.94n ±  77%        ~ (p=0.798 n=8)    85.95n ± 22%        ~ (p=0.645 n=8)
EncodeSQLStringBuilder/32B-4            366.8n ±  31%    146.8n ±  28%  -59.99% (p=0.001 n=8)    135.2n ± 27%  -63.15% (p=0.000 n=8)
EncodeSQLStringBuilder/128B-4           926.2n ±  41%    378.1n ±  63%  -59.18% (p=0.003 n=8)    253.1n ± 29%  -72.67% (p=0.000 n=8)
EncodeSQLStringBuilder/1KiB-4           5.832µ ±  42%    1.997µ ±  21%  -65.77% (p=0.000 n=8)    1.867µ ± 51%  -68.00% (p=0.000 n=8)
EncodeSQLStringBuilder/16KiB-4         115.92µ ±  54%    29.01µ ±  57%  -74.98% (p=0.000 n=8)    21.43µ ± 81%  -81.52% (p=0.000 n=8)
EncodeSQLStringBuilder/1KiB-esc64-4     6.536µ ±  55%    5.916µ ±  94%        ~ (p=0.798 n=8)    3.007µ ± 87%  -53.99% (p=0.028 n=8)
EncodeSQLStringBuilder/1KiB-esc8-4      4.940µ ±  33%    7.329µ ±  41%        ~ (p=0.105 n=8)    7.457µ ± 49%        ~ (p=0.105 n=8)
EncodeSQLStringBuilder/1KiB-esc1-4      9.317µ ±  26%   11.585µ ±  80%        ~ (p=0.279 n=8)   12.607µ ± 48%        ~ (p=0.382 n=8)
EncodeSQLStringBuilder/1KiB-utf8-4      5.111µ ±  78%    2.046µ ±  38%  -59.98% (p=0.000 n=8)    1.484µ ± 58%  -70.95% (p=0.000 n=8)
EncodeSQLStringBuilder/1KiB-binary-4    6.392µ ±  47%    5.045µ ±  41%        ~ (p=0.105 n=8)    5.498µ ± 53%        ~ (p=0.505 n=8)
EncodeSQLBinWriter/8B-4                101.92n ±  42%    24.75n ±  65%  -75.71% (p=0.001 n=8)    18.07n ± 69%  -82.27% (p=0.000 n=8)
EncodeSQLBinWriter/32B-4               423.50n ±  55%    57.41n ±  53%  -86.44% (p=0.000 n=8)    22.85n ± 19%  -94.61% (p=0.000 n=8)
EncodeSQLBinWriter/128B-4              809.05n ±  29%   150.05n ±  99%  -81.45% (p=0.000 n=8)    45.42n ± 66%  -94.39% (p=0.000 n=8)
EncodeSQLBinWriter/1KiB-4              5383.0n ±  86%   1059.0n ±  26%  -80.33% (p=0.000 n=8)    258.9n ± 49%  -95.19% (p=0.000 n=8)
EncodeSQLBinWriter/16KiB-4             79.470µ ±  57%   12.748µ ±  28%  -83.96% (p=0.000 n=8)    3.422µ ± 56%  -95.69% (p=0.000 n=8)
EncodeSQLBinWriter/1KiB-esc64-4        5275.0n ±  32%   1758.5n ±  46%  -66.66% (p=0.000 n=8)    467.8n ± 30%  -91.13% (p=0.000 n=8)
EncodeSQLBinWriter/1KiB-esc8-4          4.434µ ±  19%    2.172µ ±  64%  -51.01% (p=0.001 n=8)    1.621µ ± 28%  -63.45% (p=0.000 n=8)
EncodeSQLBinWriter/1KiB-esc1-4         11.302µ ±  57%    3.695µ ±  16%  -67.31% (p=0.000 n=8)    4.327µ ± 26%  -61.71% (p=0.000 n=8)
EncodeSQLBinWriter/1KiB-utf8-4         7775.5n ±  61%    873.4n ±  23%  -88.77% (p=0.000 n=8)    241.5n ± 79%  -96.89% (p=0.000 n=8)
EncodeSQLBinWriter/1KiB-binary-4        6.068µ ±  47%    1.490µ ±  34%  -75.45% (p=0.000 n=8)    1.077µ ± 30%  -82.25% (p=0.000 n=8)
EncodeStringSQL/8B-4                   144.80n ±  37%    79.98n ±  76%        ~ (p=0.065 n=8)    79.07n ± 62%  -45.39% (p=0.015 n=8)
EncodeStringSQL/32B-4                   550.0n ±  48%    137.3n ±  70%  -75.04% (p=0.000 n=8)    124.0n ± 34%  -77.45% (p=0.000 n=8)
EncodeStringSQL/128B-4                 2087.0n ±  52%    327.8n ±  22%  -84.29% (p=0.000 n=8)    236.8n ± 33%  -88.65% (p=0.000 n=8)
EncodeStringSQL/1KiB-4                 14.727µ ±  57%    2.195µ ±  41%  -85.09% (p=0.000 n=8)    1.101µ ± 42%  -92.52% (p=0.000 n=8)
EncodeStringSQL/16KiB-4                306.74µ ±  62%    37.17µ ±  27%  -87.88% (p=0.000 n=8)    16.72µ ± 26%  -94.55% (p=0.000 n=8)
EncodeStringSQL/1KiB-esc64-4           13.432µ ±  43%    2.676µ ±  42%  -80.08% (p=0.000 n=8)    1.330µ ± 29%  -90.10% (p=0.000 n=8)
EncodeStringSQL/1KiB-esc8-4            14.149µ ±  35%    5.901µ ±  44%  -58.29% (p=0.000 n=8)    5.140µ ± 50%  -63.67% (p=0.000 n=8)
EncodeStringSQL/1KiB-esc1-4            11.917µ ±  67%    8.036µ ±  96%        ~ (p=0.234 n=8)   10.901µ ± 85%        ~ (p=0.645 n=8)
EncodeStringSQL/1KiB-utf8-4            14.513µ ±  83%    3.070µ ±  36%  -78.85% (p=0.000 n=8)    2.894µ ± 36%  -80.06% (p=0.000 n=8)
EncodeStringSQL/1KiB-binary-4           21.79µ ±  53%    19.72µ ± 178%        ~ (p=0.798 n=8)    22.99µ ± 78%        ~ (p=0.382 n=8)
geomean                                 2.835µ           1.190µ         -58.02%                  716.1n        -74.74%
```

Allocations:
- BinWriter path: 1 -> 0 allocs/op (was 24 B to 83 KiB per op).
- StringBuilder and EncodeStringSQL at 1 KiB: 3.24 KiB -> 1.125 KiB per op.
- bytes2 path: unchanged (0).

SWAR vs scalar:
- SWAR is 3-4x faster than scalar on clean runs of 128 B or more. For example, Bytes2 1 KiB goes from 830 to 236 ns, and 16 KiB from 15.3 to 3.6 µs.
- They are equal on short inputs (32 B or less) and on dense-escape inputs.
- Keep SWAR.

Worst case (every byte a quote, esc1): within noise of legacy. The first version, without the inner dense loop, was 5x slower here; that is fixed.

EncodeStringSQL on invalid UTF-8 (1KiB-binary) is unchanged, because it takes the old code path.

## End-to-end: ParsedQuery.GenerateQuery
Measured with the new `BenchmarkGenerateQuery`:
```
goos: linux
goarch: amd64
pkg: vitess.io/vitess/go/vt/sqlparser
cpu: Intel(R) Xeon(R) Processor @ 2.10GHz
                                  │  e2e_old.txt  │             e2e_new.txt             │
                                  │    sec/op     │    sec/op     vs base               │
GenerateQuery/point-select-4         409.9n ± 27%   418.1n ± 37%        ~ (p=0.798 n=8)
GenerateQuery/in-list-100-4          9.611µ ± 24%   8.806µ ± 36%        ~ (p=0.505 n=8)
GenerateQuery/insert-4KiB-text-4    17.620µ ± 29%   5.293µ ± 35%  -69.96% (p=0.000 n=8)
GenerateQuery/insert-64KiB-blob-4   298.25µ ± 38%   58.34µ ± 50%  -80.44% (p=0.000 n=8)
geomean                              12.00µ         5.806µ        -51.59%

                                  │  e2e_old.txt  │              e2e_new.txt              │
                                  │     B/op      │     B/op      vs base                 │
GenerateQuery/point-select-4           272.0 ± 0%     272.0 ± 0%        ~ (p=1.000 n=8) ¹
GenerateQuery/in-list-100-4          6.922Ki ± 0%   6.922Ki ± 0%        ~ (p=1.000 n=8) ¹
GenerateQuery/insert-4KiB-text-4    17.469Ki ± 0%   4.969Ki ± 0%  -71.56% (p=0.000 n=8)
GenerateQuery/insert-64KiB-blob-4   278.72Ki ± 0%   72.09Ki ± 0%  -74.13% (p=0.000 n=8)
geomean                              9.727Ki        5.066Ki       -47.92%
¹ all samples are equal

                                  │ e2e_old.txt │             e2e_new.txt             │
                                  │  allocs/op  │ allocs/op   vs base                 │
GenerateQuery/point-select-4         3.000 ± 0%   3.000 ± 0%        ~ (p=1.000 n=8) ¹
GenerateQuery/in-list-100-4          9.000 ± 0%   9.000 ± 0%        ~ (p=1.000 n=8) ¹
GenerateQuery/insert-4KiB-text-4    11.000 ± 0%   4.000 ± 0%  -63.64% (p=0.000 n=8)
GenerateQuery/insert-64KiB-blob-4   20.000 ± 0%   3.000 ± 0%  -85.00% (p=0.000 n=8)
geomean                              8.779        4.243       -51.67%
¹ all samples are equal
```
- point-select (an int plus a 19-byte email) and an IN-list of 100 short strings: within noise. Escaping short values is a small share of GenerateQuery; map lookup, ProtoToValue and allocations dominate.
- Insert with 4 KiB of text: -70% time, -72% B/op, 11 -> 4 allocs.
- Insert with a 64 KiB blob: -80% time, -74% B/op, 20 -> 3 allocs.
- vreplication `appendFromRow`: not measured. I wrote a benchmark, but the vreplication test binary panics in `init` without a local mysqld (testenv), so I removed it.
  - Estimate: it uses the bytes2 path, which is 4-6x faster in the microbenchmarks for string or blob columns of 128 B or more.
  - Copy-phase bulk inserts of text-heavy tables should therefore use noticeably less CPU on the vttablet target side.

## Gotchas
- **Equivalence.** Output is byte-identical to the old code for every input. This covers:
  - invalid UTF-8 (via the fallback path);
  - `\%`/`\_` at run and word boundaries;
  - a trailing `\`;
  - the `\\%` sequence.

  Verification:
  - an exhaustive test over byte x position x next byte around the 8-byte word boundary;
  - 20k random inputs from an alphabet dense in special bytes;
  - ~660k fuzz execs over two runs.

  A mutation that drops the `_` rule is caught.
- **Hard-coded candidate set.** `SQLEncodeMap` is an exported, mutable array, but the SWAR candidate set is hard-coded (< 0x20, `'`, `\`). If someone changed `SQLEncodeMap` to escape another byte, the new code would miss it. No such writer exists in the repo, and `TestNextSQLEscapeCandidate` asserts the invariant. The array could be unexported, or at least commented.
- **Write granularity.** The BinWriter default path now makes several `Write` calls per value instead of one. The documented contract is in-memory buffers, and `encodeBytesSQLBits` already writes per byte via fmt. Writers such as hashers see identical bytes.
- **Extra validation pass.** `BufEncodeStringSQL` pays an extra `utf8.ValidString` pass. It is cheap for ASCII. For CJK-heavy text it is the dominant cost, but the function is still ~5x faster than legacy there.
- **sqlparser literals.** Literal formatting passes a `*TrackedBuffer`, not a `*strings.Builder`, so it takes the BinWriter path (direct writes, no temp buffer). Passing `buf.Builder` would use the builder path instead. I left it unchanged because `ast_format_fast.go` is generated by ASTFmtGen.
- **Release compatibility.** No concern: no wire or format change and no API removal.
- **Concurrency.** No concerns.
- **Portability.** The byte-composed load compiles to one 8-byte load on amd64 and arm64. It is also correct on big-endian, because the value is built explicitly as little-endian.

## Tests
- Added `go/sqltypes/value_escape_test.go`:
  - `TestSQLEscapeEquivalence`
  - `TestNextSQLEscapeCandidate`
  - `FuzzSQLEscapeEquivalence`
  - microbenchmarks, in new and Legacy variants.
- Added `go/sqltypes/value_escape_legacy_test.go`, with the old implementations as reference.
- Added `BenchmarkGenerateQuery` in `go/vt/sqlparser/parsed_query_test.go`.
- Passing packages: go/sqltypes, go/vt/sqlparser/..., go/mysql/json, go/vt/vtgate/evalengine, go/vt/vtgate/engine, go/vt/binlog and go/bytes2.
- Fuzz runs of 30 s and 40 s were clean.

Patch: findings/F03-sql-escape.patch. Raw benchmark data: scratchpad/F03/.
