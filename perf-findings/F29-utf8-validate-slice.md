# F29: utf8mb3 Validate, utf8mb3/utf8mb4 Slice, allocation-free Length

## Verdict
**Do it.** It is small (~125 LOC in one file, plus tests). It is exactly equivalent to the generic helpers: the tests are exhaustive, random and fuzz-based, and they are mutation-checked. The affected primitives get 4-30x faster. It also removes one allocation per call (len(input) bytes) that Go 1.27's `utf8.RuneCount` makes on non-ASCII input longer than 32 bytes.

## Finding re-verification
* `charset.Validate` (helpers.go:38) falls back to a `DecodeRune` loop through the `types.Charset` interface for utf8mb3. utf8mb4 already has `Validate` = `utf8.Valid`. The finding is real.
* `charset.Slice` (helpers.go:19) has no utf8mb3/utf8mb4 implementation, so it calls `DecodeRune` through the interface once per rune. The finding is real.
* Callers of `charset.Validate` that reach utf8mb3 (whole repo):
  - `CHAR(... USING utf8mb3)` (fn_string.go:2031, compiler_asm.go:4701)
  - `charset.ConvertFromBinary` (convert.go:223), i.e. `CONVERT(<binary> USING utf8mb3)`, or casting a binary value to utf8mb3-collated text.
  - NOTE: `CONVERT(<utf8mb4 col> USING utf8mb3)` does NOT call Validate; it goes through `convertFastFromUTF8`. utf8mb3->utf8mb4 is a no-op (superset).
* Callers of `charset.Slice` (all charsets; utf8mb4 is the common case):
  - SUBSTRING/SUBSTR/MID (fn_string.go:1597, compiler_asm.go:3143/3175)
  - LEFT/RIGHT (fn_string.go:1178/1180, compiler_asm.go:2908/2940)
  - LPAD/RPAD (compiler_asm.go:2974/2991/3028/3045, fn_string.go:1258/1271)
  - INSERT (fn_string.go:487/490)
  - CAST(x AS CHAR(n)) truncation (eval_bytes.go:179 truncateInPlace)
  - WEIGHT_STRING / hashing with a length (weights.go:103/174)
  - LOCATE/INSTR with an offset (colldata/collation.go:392)
* Almost every Slice caller calls `charset.Length` (= `utf8.RuneCount`) first, so Length is on the same path.

## Semantics that must be (and are) preserved
* The generic Slice decodes at most `to` runes and **stops at the first invalid sequence**. Invalid bytes do NOT count as characters for slicing, unlike in `Length`, which counts each invalid byte as 1.
  - start = the bytes of the first `min(from,to)` runes that decoded successfully.
  - A negative from is treated as 0. to <= 0 returns `input[0:0]`.
  - The result aliases the input with the same capacity. The tests check that bytes, cap and nil-ness are equal.
* The utf8mb3 decoder accepts exactly well-formed UTF-8 restricted to sequences of at most 3 bytes:
  - Overlongs are rejected (C0/C1 = xx, and E0 only accepts A0..BF).
  - Surrogates are rejected (ED only accepts 80..9F).
  - F0..FF are xx.
  - Therefore `Validate(p) == utf8.Valid(p) && no byte >= 0xF0` exactly. In valid UTF-8, a byte >= 0xF0 can only be a 4-byte lead. ED A0 80 is rejected both by the mb3 decoder and by utf8.Valid.
* mb4 Slice uses `utf8.DecodeRune` with the same ok-rule as `Charset_utf8mb4.DecodeRune`: RuneError with size<=1 is invalid, and an encoded U+FFFD is valid.
* `Length` stays equivalent to `utf8.RuneCount` (tested). A pre-existing quirk is kept as is: utf8mb3 `Length` counts a 4-byte sequence as 1 rune, while the mb3 decoder treats it as 4 invalid bytes (and Slice stops there).

## Implementation (go/mysql/collations/charset/unicode/utf8.go)
* `Charset_utf8mb3.Validate` is `utf8.Valid(p) && !hasFourByteLead(p)`. `hasFourByteLead` is a SWAR check, `w&(w<<1)&(w<<2)&(w<<3)&0x8080..`: bit 7 of a byte is set iff the top 4 bits of that byte are set. Shifts of at most 3 never carry into the tested bit.
* `Slice` for both charsets (`sliceUTF8`):
  - A SWAR ASCII prefix scan (8 bytes per step, capped at `to` bytes) gives offsets directly, because each ASCII byte is one rune.
  - After that, a loop without interface calls: an ASCII byte fast path, then `utf8.DecodeRune` or a direct `Charset_utf8mb3{}.DecodeRune` call.
* `Length` for both charsets (`runeCount`): a SWAR ASCII prefix scan, then an inline DecodeRune loop.
  - Why: Go 1.27's `utf8.RuneCount` is `for ascii...; return n + RuneCountInString(string(p[n:]))`, and that conversion **allocates**. Measured: 1 alloc of 1 KiB for a 1 KiB mixed string. It also scans the ASCII part 1 byte at a time.
  - This is worth an upstream Go issue. Other vitess callers of `utf8.RuneCount` on []byte are affected too.
  - A `for range string(p[n:])` loop was tried and also allocates here.

## Measurements
All numbers are measured, on a shared, noisy 4 vCPU box, interleaved, n=6..8.

### Micro: generic helper loop through the interface vs the new method
```
                                     │  f29_old.txt   │             f29_new.txt             │
                                     │     sec/op     │    sec/op     vs base               │
UTF8mb3Validate/ascii20-4               62.41n ±  21%   13.70n ± 10%  -78.05% (p=0.002 n=6)
UTF8mb3Validate/ascii1k-4              2813.0n ±  20%   177.8n ± 23%  -93.68% (p=0.002 n=6)
UTF8mb3Validate/mixed1k-4              2533.5n ±  11%   602.2n ± 22%  -76.23% (p=0.002 n=6)
UTF8Slice/utf8mb3/ascii20/3-13-4       50.245n ±  21%   8.436n ± 24%  -83.21% (p=0.002 n=6)
UTF8Slice/utf8mb3/ascii20/half-end-4   83.195n ±  22%   9.863n ± 29%  -88.14% (p=0.002 n=6)
UTF8Slice/utf8mb3/ascii1k/3-13-4       38.370n ±  17%   8.898n ±  9%  -76.81% (p=0.002 n=6)
UTF8Slice/utf8mb3/ascii1k/half-end-4   3609.0n ±  13%   111.3n ± 12%  -96.91% (p=0.002 n=6)
UTF8Slice/utf8mb3/mixed1k/3-13-4        48.01n ±   4%   13.61n ± 10%  -71.65% (p=0.002 n=6)
UTF8Slice/utf8mb3/mixed1k/half-end-4    2.486µ ±  20%   1.038µ ± 14%  -58.25% (p=0.002 n=6)
UTF8Slice/utf8mb4/ascii20/3-13-4       53.650n ±   7%   9.316n ±  9%  -82.63% (p=0.002 n=6)
UTF8Slice/utf8mb4/ascii20/half-end-4   80.515n ±  18%   8.373n ± 24%  -89.60% (p=0.002 n=6)
UTF8Slice/utf8mb4/ascii1k/3-13-4       49.895n ±  12%   7.054n ± 42%  -85.86% (p=0.002 n=6)
UTF8Slice/utf8mb4/ascii1k/half-end-4   3526.0n ± 123%   111.0n ± 20%  -96.85% (p=0.002 n=6)
UTF8Slice/utf8mb4/mixed1k/3-13-4        58.41n ±  20%   12.91n ± 25%  -77.90% (p=0.002 n=6)
UTF8Slice/utf8mb4/mixed1k/half-end-4    3.318µ ±  26%   1.303µ ± 35%  -60.73% (p=0.002 n=6)
geomean                                 277.8n          41.33n        -85.12%
```

### Length: utf8.RuneCount vs runeCount
```
                     │ f29_len_a.txt │            f29_len_b.txt            │
                     │    sec/op     │    sec/op     vs base               │
UTF8Length/ascii20-4    7.839n ± 28%   8.768n ± 15%        ~ (p=0.132 n=6)
UTF8Length/ascii1k-4    313.4n ± 10%   126.6n ± 34%  -59.60% (p=0.002 n=6)
UTF8Length/mixed1k-4   1699.5n ± 19%   893.6n ± 30%  -47.42% (p=0.002 n=6)
geomean                 161.0n         99.73n        -38.06%

                     │ f29_len_a.txt  │              f29_len_b.txt               │
                     │      B/op      │     B/op      vs base                    │
UTF8Length/ascii20-4     0.000 ± 0%       0.000 ± 0%         ~ (p=1.000 n=6) ¹
UTF8Length/ascii1k-4     0.000 ± 0%       0.000 ± 0%         ~ (p=1.000 n=6) ¹
UTF8Length/mixed1k-4   1.000Ki ± 0%     0.000Ki ± 0%  -100.00% (p=0.002 n=6)
geomean                             ²                 ?                      ² ³
¹ all samples are equal
² summaries must be >0 to compute geomean
³ ratios must be >0 to compute geomean

                     │ f29_len_a.txt │             f29_len_b.txt              │
                     │   allocs/op   │ allocs/op   vs base                    │
UTF8Length/ascii20-4    0.000 ± 0%     0.000 ± 0%         ~ (p=1.000 n=6) ¹
UTF8Length/ascii1k-4    0.000 ± 0%     0.000 ± 0%         ~ (p=1.000 n=6) ¹
UTF8Length/mixed1k-4    1.000 ± 0%     0.000 ± 0%  -100.00% (p=0.002 n=6)
geomean                            ²               ?                      ² ³
¹ all samples are equal
² summaries must be >0 to compute geomean
³ ratios must be >0 to compute geomean
```

### evalengine end-to-end
Benchmark: BenchmarkCompilerExpressions, with new cases added to perf_test.go. old = HEAD utf8.go, new = the patch.
Columns are VARCHAR utf8mb4: 20 B ASCII, 1 KiB ASCII, and 1012 B mixed (Latin-1 + CJK). The convert cases use VARBINARY input (binary -> utf8mb3 goes through Validate).
```
goos: linux
goarch: amd64
pkg: vitess.io/vitess/go/vt/vtgate/evalengine
cpu: Intel(R) Xeon(R) Processor @ 2.10GHz
  │ f29_eval_old.txt │  f29_eval_new.txt  │
  │  sec/op  │  sec/op  vs base  │
CompilerExpressions/substring_ascii20/eval=ast-4  132.3n ± 37%  128.3n ± 32%  ~ (p=0.574 n=8)
CompilerExpressions/substring_ascii20/eval=vm-4  124.2n ± 28%  100.8n ± 29%  ~ (p=0.195 n=8)
CompilerExpressions/substring_ascii1k/eval=ast-4  4201.0n ± 11%  334.1n ± 89%  -92.05% (p=0.000 n=8)
CompilerExpressions/substring_ascii1k/eval=vm-4  4181.5n ± 27%  295.1n ± 17%  -92.94% (p=0.000 n=8)
CompilerExpressions/substring_mixed1k/eval=ast-4  5.775µ ± 23%  2.720µ ± 11%  -52.91% (p=0.000 n=8)
CompilerExpressions/substring_mixed1k/eval=vm-4  4.991µ ± 14%  2.537µ ± 74%  -49.17% (p=0.007 n=8)
CompilerExpressions/left_ascii1k/eval=ast-4  485.0n ± 22%  247.4n ± 38%  -48.99% (p=0.000 n=8)
CompilerExpressions/left_ascii1k/eval=vm-4  502.7n ± 30%  211.6n ± 26%  -57.91% (p=0.000 n=8)
CompilerExpressions/convert_utf8mb3_ascii1k/eval=ast-4  2910.0n ± 13%  276.2n ± 37%  -90.51% (p=0.000 n=8)
CompilerExpressions/convert_utf8mb3_ascii1k/eval=vm-4  3001.0n ± 27%  314.9n ± 27%  -89.51% (p=0.000 n=8)
CompilerExpressions/convert_utf8mb3_mixed1k/eval=ast-4  2690.0n ± 21%  868.7n ± 15%  -67.71% (p=0.000 n=8)
CompilerExpressions/convert_utf8mb3_mixed1k/eval=vm-4  2432.5n ± 18%  871.0n ±  6%  -64.20% (p=0.000 n=8)
geomean  1.486µ  420.2n  -71.72%

  │ f29_eval_old.txt │  f29_eval_new.txt  │
  │  B/op  │  B/op  vs base  │
CompilerExpressions/substring_ascii20/eval=ast-4  64.00 ± 0%  64.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/substring_ascii20/eval=vm-4  32.00 ± 0%  32.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/substring_ascii1k/eval=ast-4  64.00 ± 0%  64.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/substring_ascii1k/eval=vm-4  32.00 ± 0%  32.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/substring_mixed1k/eval=ast-4  1088.00 ± 0%  64.00 ± 0%  -94.12% (p=0.000 n=8)
CompilerExpressions/substring_mixed1k/eval=vm-4  1056.00 ± 0%  32.00 ± 0%  -96.97% (p=0.000 n=8)
CompilerExpressions/left_ascii1k/eval=ast-4  64.00 ± 0%  64.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/left_ascii1k/eval=vm-4  32.00 ± 0%  32.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/convert_utf8mb3_ascii1k/eval=ast-4  64.00 ± 0%  64.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/convert_utf8mb3_ascii1k/eval=vm-4  64.00 ± 0%  64.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/convert_utf8mb3_mixed1k/eval=ast-4  64.00 ± 0%  64.00 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/convert_utf8mb3_mixed1k/eval=vm-4  64.00 ± 0%  64.00 ± 0%  ~ (p=1.000 n=8) ¹
geomean  86.08  50.80  -40.99%
¹ all samples are equal

  │ f29_eval_old.txt │  f29_eval_new.txt  │
  │  allocs/op  │ allocs/op  vs base  │
CompilerExpressions/substring_ascii20/eval=ast-4  2.000 ± 0%  2.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/substring_ascii20/eval=vm-4  1.000 ± 0%  1.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/substring_ascii1k/eval=ast-4  2.000 ± 0%  2.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/substring_ascii1k/eval=vm-4  1.000 ± 0%  1.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/substring_mixed1k/eval=ast-4  3.000 ± 0%  2.000 ± 0%  -33.33% (p=0.000 n=8)
CompilerExpressions/substring_mixed1k/eval=vm-4  2.000 ± 0%  1.000 ± 0%  -50.00% (p=0.000 n=8)
CompilerExpressions/left_ascii1k/eval=ast-4  2.000 ± 0%  2.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/left_ascii1k/eval=vm-4  1.000 ± 0%  1.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/convert_utf8mb3_ascii1k/eval=ast-4  2.000 ± 0%  2.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/convert_utf8mb3_ascii1k/eval=vm-4  2.000 ± 0%  2.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/convert_utf8mb3_mixed1k/eval=ast-4  2.000 ± 0%  2.000 ± 0%  ~ (p=1.000 n=8) ¹
CompilerExpressions/convert_utf8mb3_mixed1k/eval=vm-4  2.000 ± 0%  2.000 ± 0%  ~ (p=1.000 n=8) ¹
geomean  1.740  1.587  -8.75%
¹ all samples are equal
```
The substring_ascii20 row (SUBSTRING(col,1,10) on a 20-byte string) is not significant. That case takes ~130 ns and is dominated by evaluation overhead and allocation.

Profile of the new substring_ascii1k/mixed1k (before the runeCount change): `utf8.RuneCount` was 55% of CPU and 75% of allocated bytes. That is why Length was included.

## End-to-end relevance (estimated)
* vtgate normally pushes SUBSTRING/LEFT/CONVERT/CHAR down to MySQL inside a Route. evalengine evaluates them at vtgate only for:
  - projections that cannot be pushed down (cross-shard join output columns that mix tables, or projections over aggregations / derived tables evaluated at vtgate)
  - ORDER BY/GROUP BY on computed expressions after the merge
  - constant folding at plan time
  - weight_string/hash computations for text with a length (weights.go)
* vttablet uses evalengine in vstreamer/vreplication filters (replicator_plan.go, vstreamer/planbuilder.go) and in vdiff. The most likely hot use is a column expression in a Materialize/MoveTables filter, which is evaluated for every row event.
* For typical OLTP traffic, the absolute benefit is small. Queries that do hit these paths on long strings get 2-14x per expression evaluation. utf8mb3 Validate matters only for CHAR() and for binary->utf8mb3 conversions.
* The Length change helps every LENGTH/CHAR_LENGTH/LPAD/SUBSTRING/... evaluation on utf8 text: it removes an allocation for non-ASCII input longer than 32 B, and ASCII input gets ~2.5x faster.

## Difficulty
S:
* ~125 LOC in go/mysql/collations/charset/unicode/utf8.go
* ~250 LOC of tests (utf8_test.go)
* ~110 LOC of benchmarks (utf8_bench_test.go, optional)
* +10 LOC of evalengine benchmark cases (perf_test.go, optional)
* No generated code.

## Gotchas
* Matching the generic helpers exactly is subtle: stop-at-invalid, from>to, negative values, nil input, capacity/aliasing. The tests cover all of these.
* `Length` and `Slice` disagree on invalid input. This is pre-existing: callers such as SUBSTRING compute end=Length and then call Slice. The behaviour is unchanged.
* Pre-existing and preserved: utf8mb3 `Length` counts 4-byte sequences as one character (utf8.RuneCount semantics).
* The SWAR code uses encoding/binary.LittleEndian, which is portable. `GOARCH=arm64 go vet` and a `GOARCH=386` test build were checked. Endianness does not matter because only the top bit of each byte is tested.
* Validate makes two passes (utf8.Valid, then the 0xF0 scan). A fused single pass would be slightly faster on mixed input but needs more code.
* mb3 Slice on non-ASCII data calls the non-inlined mb3 DecodeRune once per rune. It is still ~2.4x faster than before.
* Release compatibility: this is a pure in-process optimisation, with no wire or format change.

## Tests
* Added in go/mysql/collations/charset/unicode/utf8_test.go, all compared against copies of the generic decode loops:
  - TestUTF8mb3ValidateExhaustive covers:
    - all 1/2/3-byte sequences, with ASCII prefixes of 0/3/7/8 bytes to hit every SWAR word position and the tail
    - all 4-byte sequences made of any lead byte followed by 3 of the 26 boundary bytes
  - TestUTF8mb3Validate: named cases (surrogates ED A0 80 / ED BF BF, overlongs, 4-byte, truncated, U+D7FF, U+FFFD).
  - TestUTF8SliceEquivalence: 200k random, possibly invalid strings, random from/to including negative values, both charsets. It compares bytes, cap and nil-ness.
  - TestUTF8mb3ValidateRandom.
  - TestUTF8LengthEquivalence.
  - FuzzUTF8Slice: fuzzed for 20 s, ~300k execs, no failures.
* Mutation check: each of these deliberate bugs was caught by the tests:
  - dropping the 0xF0 check
  - `>=` -> `>` in the scalar tail
  - dropping `w<<3`
  - a wrong mb4 ok-rule
  - an off-by-one on from
  - an uncapped ASCII prefix
* `go test ./go/mysql/collations/...` passes, except the integration package, which needs mysqlctl (a pre-existing environment issue).
* `go test ./go/vt/vtgate/evalengine/` passes. Its integration subpackage also needs mysqlctl.
* These are equivalence tests for a performance rewrite with no behaviour change, so they also pass on main. The CLAUDE.md rule that a test must fail on main is aimed at bug fixes.
