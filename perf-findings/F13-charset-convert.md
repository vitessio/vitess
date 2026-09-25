# F13: charset.Convert ASCII fast path

Worktree: /home/user/vitess/.claude/worktrees/agent-a0c815326c69f161f (uncommitted)
Patch: findings/F13-charset-convert.patch

## Verification of the finding

- `go/mysql/collations/charset/convert.go`: `convertFastFromUTF8` (`for _, cp := range string(src)`) and
  `convertSlow` (`srcCharset.DecodeRune` + `dstCharset.EncodeRune` per character) run one character at a
  time through interface calls, including for plain ASCII. Confirmed.
- `range string(src)` copies `src` for inputs > 32 bytes. The loop body calls an interface method that could
  modify `src`, so the compiler cannot skip the copy. The benchmark shows 2 allocs/op for utf8mb4->latin1 1KB
  (legacy) and 1 alloc/op (new).
- The buffer is always `len(src)*3`, whatever the target MaxWidth: 3x for utf8mb4->latin1, where 1x is exact.
  Growth doubles `len(dst)`. A non-nil `dst` with a capacity below 4 makes the legacy code panic: a
  zero-capacity doubling stays at 0, and `EncodeRune` indexes `dst[nDst:]`. I found no current caller that
  passes such a `dst`. `concatConvert` passes a `buf` that came from `append`, so its capacity is 8 or more.
- Dispatch: dst `binary` -> `Convertible`; `IsSuperset` shortcuts, for example utf8mb3->utf8mb4 returns
  `src` unchanged, with no copy and no work. So "utf8mb3->utf8mb4" is already free. The reverse,
  utf8mb4->utf8mb3, uses convertFastFromUTF8.
- Callers:
  - vreplication `TablePlan.convertStringCharset` (replicator_plan.go:547). It is called per row, per converted
    column, in both vplayer (`bindFieldVal`, :572) and the vcopier bulk-insert path (:1123).
  - evalengine: eval_bytes.go:95 (CONVERT/CAST/coercion), fn_string.go:1773 (CONCAT, with an append-style
    `buf`), fn_compare.go, eval_result.go, eval_json.go.
  - colldata coercion closures (collation.go:375/381).

## Implementation (convert.go, about +180/-25 LOC; tests are separate)

- `asciiDecoder(cs)` / `asciiEncoder(cs)`: type switch whitelists.
  - Decoders: utf8mb3/4, latin1, gb2312, gb18030, euckr, sjis, cp932, ujis, eucjpms.
  - Encoders: the same list **minus sjis**, because sjis `EncodeRune('\\')` = 0x815F, two bytes (a COMPAT
    quirk in encodeSJIS).
  - `*Charset_8bit`: flags are computed once per instance by probing all 128 code points, and cached in a
    `sync.Map`. Not every 8-bit charset is an ASCII superset: **swe7** remaps `@[\]^`{|}~`. ascii, latin2,
    cp125x, koi8r and macroman qualify.
  - Not ASCII-compatible: utf16, utf16le, ucs2, utf32, and binary (binary never reaches these loops anyway).
- Why multibyte sources (sjis/gbk/euckr trail bytes < 0x80) are safe: the fast path fires only when the
  **current position** (a character boundary, as the legacy loop sees it) holds a byte < 0x80. At such a
  position every whitelisted decoder returns `(b, 1, true)` without looking ahead. It checks `c0 <
  RuneSelf` first, and the test verifies this with trailing 0x80/0xA1/0xFF bytes. A trail byte < 0x80 is
  only ever at a boundary after the legacy loop resyncs (skip 1 on invalid), and there the legacy loop also
  decodes it as ASCII. So the output is identical byte for byte.
- `asciiPrefix`: 8-byte SWAR (`binary.LittleEndian.Uint64 & 0x8080..`, `bits.TrailingZeros64/8` for the
  position; portable, no unsafe). The run is bulk-copied with `copy`.
- Buffer sizing (`convertBuffer`):
  - dst MaxWidth 1 -> `len(src)`.
  - ASCII pair -> `len(src) + countNonASCII(src)*(MaxWidth-1)`. This is a proven upper bound: every non-ASCII
    character contains at least one byte >= 0x80 and emits at most MaxWidth bytes. It uses a popcount SWAR
    pre-pass.
  - Otherwise -> `len(src)*min(MaxWidth,3)`, the legacy value for wide charsets.
  - Plus `utf8.UTFMax` slack. Some encoders bounds-check `dst[3]` even for ASCII (for example gb18030
    `_ = dst[3]`), so the loop keeps the legacy "4 free bytes before EncodeRune" invariant.
  - A nil dst uses `make`. A non-nil dst uses `slices.Grow` once upfront.
- The per-character path is unchanged. convertFastFromUTF8 now uses `utf8.DecodeRune` instead of
  `range string`, which gives the same RuneError/width-1 semantics (invalid UTF-8 -> U+FFFD, **not**
  counted as a failure, as before). The ASCII-run copy is in a non-inlined helper (`copyASCIIRun`), and
  growth is legacy-style make+copy. With `slices.Grow` inlined into the loop, the non-ASCII pairs were
  15-20% slower because of register pressure; that is fixed.
- Error values and messages are unchanged: `failedConversionError(from, to, original input)`.

## Micro A/B (measured; interleaved legacy vs new, 8 runs x 100ms, shared noisy host)

`CONVERT_IMPL=legacy` switches BenchmarkConvertPairs to the verbatim legacy copy in
convert_legacy_test.go. Input: "lorem ipsum ..." words. "mostly" = one non-ASCII character per 12 words.

```
                                          │    legacy     │      new      vs base
latin1-utf8mb4/ascii/10B                    125.90n ± 15%   47.57n ± 10%  -62.22%
latin1-utf8mb4/ascii/1KB                    7567.0n ±  8%   868.3n ± 18%  -88.53%
latin1-utf8mb4/mostly/10B                   122.20n ±  6%   69.77n ± 34%  -42.90%
latin1-utf8mb4/mostly/1KB                    8.139µ ± 19%   1.088µ ± 54%  -86.63%
utf8mb4-latin1/ascii/10B                     89.97n ± 14%   38.98n ± 12%  -56.67%
utf8mb4-latin1/ascii/1KB                    5333.5n ± 11%   691.1n ± 21%  -87.04%
utf8mb4-latin1/mostly/10B                    92.16n ± 28%   58.28n ± 17%  -36.76%
utf8mb4-latin1/mostly/1KB                   5601.0n ± 21%   895.7n ±  9%  -84.01%
utf8mb4-utf8mb3/mostly/10B                  122.35n ±  9%   82.28n ±  7%  -32.75%
utf8mb4-utf8mb3/mostly/1KB                   6.665µ ± 16%   1.246µ ± 10%  -81.31%
ascii-utf8mb4/ascii/10B                     123.20n ± 16%   52.37n ± 18%  -57.49%
ascii-utf8mb4/ascii/1KB                     7269.5n ± 12%   907.2n ±  8%  -87.52%
utf8mb4-ascii/ascii/10B                      94.21n ± 12%   50.80n ± 10%  -46.08%
utf8mb4-ascii/ascii/1KB                     6442.5n ± 18%   714.9n ± 28%  -88.90%
sjis-utf8mb4/cjk/10B                         218.9n ± 10%   177.5n ± 13%  -18.91%
sjis-utf8mb4/cjk/1KB                         7.199µ ±  8%   1.789µ ± 14%  -75.15%
utf16-utf8mb4/ascii/10B (no fast path)       135.5n ± 10%   157.6n ± 12%  +16.31% (p=0.005)
utf16-utf8mb4/ascii/1KB (no fast path)       9.497µ ± 22%   9.872µ ±  4%  ~
latin1-utf16/ascii/10B (no fast path)        119.0n ±  8%   132.8n ± 12%  +11.64% (p=0.038)
latin1-utf16/ascii/1KB (no fast path)        8.642µ ± 16%   8.495µ ±  9%  ~
geomean                                      926.4n         340.3n        -63.26%

B/op: 1KB latin1->utf8mb4 3.125Ki -> 1.125Ki; utf8mb4->latin1 4.25Ki -> 1.125Ki; 10B 48 -> 16.
allocs/op: utf8mb4->{latin1,ascii,utf8mb3} 1KB 2 -> 1 (hidden range-string copy gone).
```

Throughput for 1KB: latin1->utf8mb4 is about 130 MB/s legacy and 1.1 GB/s new; utf8mb4->latin1 is about
190 MB/s legacy and 1.4 GB/s new.

## End-to-end relevance (estimated, not measured)

- vreplication with a charset conversion rule: Online DDL that changes a column charset (latin1 -> utf8mb4),
  and MoveTables/Reshard into a different charset. The copy phase converts every non-NULL value of each
  converted column (replicator_plan.go:1123), then SQL-escapes it into the bulk INSERT buffer. That escape is
  a byte loop of about 1-2 ns/B. At about 7.5 µs/KB, the legacy conversion was probably the largest
  per-value CPU cost in vttablet for those columns. It is now about 0.9 µs/KB, the same order as the escape.
  For text-heavy tables with conversion, I estimate a 2-4x reduction of the vttablet CPU share for those
  columns. MySQL-side insert cost dominates wall time, so the wall-clock gain is smaller. The retained
  buffers are also 3-4x smaller: the returned slice capacity is now close to the output length, where the
  legacy capacity was 3x the input.
- evalengine: CONVERT()/CAST ... CHARSET, cross-charset comparisons and CONCAT. The input is usually short
  (10-100B), where the gain is about 1.5-2.5x per call, with 3x fewer bytes allocated.
- The common case utf8mb3 <-> utf8mb4 superset is already free and is unaffected.

## Difficulty

S/M. convert.go is about +180 LOC. Tests: about 260 LOC for equivalence, property and fuzz tests; about
115 LOC for a verbatim legacy copy used as the test oracle; and a 98 LOC benchmark. No generated code, no
API change.

## Gotchas

- sjis must not be an ASCII *encoder* (0x5C -> 0x815F). swe7, and possibly other 8-bit charsets, are not
  ASCII-compatible at all. That is why 8-bit charsets are probed per instance rather than whitelisted.
  TestASCIICompatibility probes every charset and pins the expected classification. Mutation check:
  adding sjis to the encoder list fails both TestASCIICompatibility and TestConvertMatchesLegacy.
- The 8-bit probe cache is a package-level `sync.Map` keyed by `*Charset_8bit`. It is bounded, because the
  instances are static in colldata/mysqldata.go, about one per collation. A caller that builds
  Charset_8bit values dynamically would grow the map. None exists today; if that is a concern, the
  makecolldata generator could emit the flag instead.
- `convertBuffer` bounds depend on each charset's MaxWidth being honest, and on "a non-ASCII char has at
  least one byte >= 0x80", which holds for all whitelisted charsets. If the bound is ever wrong, the
  ASCII-run copy still grows (a defensive check) rather than truncating, and the per-character path keeps
  the legacy grow check.
- For a non-nil `dst`, the code now grows once upfront to the bound instead of doubling lazily. It may
  reallocate where the legacy code would have fit in place. Callers must use the returned slice in both
  versions, so the semantics are the same. A small-capacity non-nil `dst` no longer panics
  (TestConvertSmallDestination; the legacy version panics there).
- There is a possible small fixed-cost regression, about 10-15 ns, 11-16% at p about 0.01-0.04 on a noisy
  shared host. It affects 10-byte inputs for pairs that get no fast path (utf16/ucs2/utf32 on either side,
  swe7, sjis as the destination). At 1KB it is in the noise.
- Output bytes and error strings are identical to the legacy version for all charset pairs, including
  invalid input: invalid source -> '?' + error; invalid UTF-8 -> U+FFFD without an error; an unencodable
  character -> '?' + error. The code is portable, with no unsafe and no assembly: `binary.LittleEndian` is
  byte-order-defined, so it is correct on arm64 and on big-endian machines. Release compatibility: pure
  internal change.

## Tests

- New `convert_ascii_test.go` (package charset_test, imports colldata to reach every 8-bit charset):
  - TestASCIICompatibility.
  - TestASCIIPrefix (SWAR boundary positions 0..39).
  - TestConvertMatchesLegacy. It runs every pair of the 42 charsets over ASCII, 2%- and 50%-non-ASCII native
    text, and random, often invalid, bytes. Lengths are 0..200 and cross the 8-byte boundaries, with nil
    and non-nil dst.
  - TestConvertSmallDestination.
  - FuzzConvertMatchesLegacy: 45 s, 285k execs, no diffs.
- New `export_test.go` and `convert_legacy_test.go` (the oracle), and `convert_bench_test.go`.
- Passed: go/mysql/collations, charset, colldata, go/vt/vtgate/evalengine. The collations/integration
  package fails for environmental reasons only: it needs a local mysqlctl.
