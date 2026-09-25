# F18: JSON escaping/unescaping in go/mysql/json

Patch: `findings/F18-json-escaping.patch` (worktree agent-a87db14230687b551, uncommitted).

## Verdict
**Do it.** Real, self-contained, byte-identical rewrite. 1.8-4x faster escaping, 3-4x faster
closing-quote scan, ~10x faster unescape; no allocations for invalid UTF-8 anymore (old code
allocated a []rune + a string, ~35x slower). End-to-end: MarshalTo (binlog row path) -40..-60%,
AppendMarshalSQL (vreplication) -17..-24% today, larger once F03 fixes the SQL encoder.

## Finding re-verification
- `parser.go` `escapeString` (used by `Value.MarshalTo` / `Object.MarshalTo`, i.e. the binlog
  row path `rbr.go:~700 ParseBinaryJSON(...).MarshalTo(nil)`, evalengine JSON output,
  `Value.String`): **four** passes over the input when there is nothing to escape:
  `utf8.ValidString`, two `strings.IndexByte`, a scalar `< 0x20` loop, then append. On the slow
  path a per-byte switch + append; for invalid UTF-8 `string([]rune(s))` (2 allocs).
- `marshal.go` `sqlWriter.writeStringContent` (vreplication `MarshalSQLValue` /
  `AppendMarshalSQL`, replicator_plan.go 653/789/988/1109): byte-at-a-time scan for `"`/`\`.
- `marshal.go` `unescapeJSON`: appends one byte at a time between escapes.
- Note: text-parsed values (`Parser.Parse`) keep strings as `typeRawString` and `MarshalTo`
  copies them verbatim; escaping only happens for keys and for values built via `NewString`
  (binlog decoding, evalengine) or after `Type()` materialized them.
- `unescapeStringBestEffort` (parser) already copies runs with IndexByte - left as is.
  `parseRawKey` is scalar but keys are tiny - left as is.

## Escaping semantics (checked, unchanged)
Escapes `"`, `\`, control chars < 0x20 (`\b \f \n \r \t` short forms, others `\u00XX`). Does NOT
escape `/`, `<`, `>`, `&`, 0x7f, U+2028/2029 - this matches MySQL's JSON text output, so kept.
Invalid UTF-8 bytes -> U+FFFD exactly as `[]rune` conversion (one per invalid byte, incl. encoded
surrogates and truncated sequences).

## Implementation (new file go/mysql/json/escape.go, ~170 LOC)
- SWAR over 8-byte little-endian words (`load64` combines byte loads -> 1 MOVQ; portable, correct
  on big-endian too): `haszero(x^0x22) | haszero(x^0x5c) | hasless(x,0x20) [| x for >=0x80]`,
  lowest set bit is exact -> `TrailingZeros64>>3`. Scalar tail uses a 256-entry class table.
- `escapeString`: single pass; bulk `append(dst, s[start:i]...)` of clean runs. UTF-8 validated
  lazily: on the first non-ASCII byte call `utf8.ValidString(s[i:])` once; if valid, switch to the
  ASCII-only mask for the rest (keeps CJK text from regressing); if invalid, decode rune-by-rune
  from there and emit U+FFFD without allocating. Pure-ASCII strings never run ValidString.
- `writeStringContent`: loop `nextQuoteOrBackslash` (SWAR) + skip 2 on backslash. Same errors.
- `unescapeJSON`: `bytes.IndexByte` for next `\`, bulk copy runs. Same errors/outputs.
- Removed old `escapeString`/`hasSpecialChars`/`hexDigits` from parser.go.

Tried a 16-byte unrolled scan: no measurable gain in the noise, reverted.

## Benchmarks (4 vCPU shared Xeon, noisy; ratios matter)
Micro (same binary, ref = old copy in escape_old_test.go, -count=8):
```
EscapeString/short10-4                18.98n ± 14%   10.36n ± 18%  -45.43% (p=0.000 n=8)
EscapeString/medium200-4             109.05n ± 14%   55.80n ± 11%  -48.83% (p=0.000 n=8)
EscapeString/long10K-4                3.633µ ± 16%   2.050µ ± 10%  -43.57% (p=0.000 n=8)
EscapeString/medium200_escapes-4     330.95n ± 16%   79.81n ± 15%  -75.89% (p=0.000 n=8)
EscapeString/long10K_escapes-4       12.377µ ± 46%   2.864µ ± 12%  -76.86% (p=0.000 n=8)
EscapeString/medium_cjk-4             243.9n ± 18%   218.9n ± 21%        ~ (p=0.234 n=8)
EscapeString/medium_invalid_utf8      ~2.6µ, 1120 B, 2 allocs -> ~78ns, 0 allocs (count=4)
UnescapeJSON/medium200_escapes-4     207.35n ± 37%   44.62n ± 26%  -78.48% (p=0.000 n=8)
UnescapeJSON/long10K_escapes-4       7426.0n ± 26%   683.0n ± 19%  -90.80% (p=0.000 n=8)
ScanJSONString/short10-4             12.050n ± 14%   3.526n ± 15%  -70.73% (p=0.000 n=8)
ScanJSONString/medium200-4           124.65n ± 27%   37.75n ± 13%  -69.71% (p=0.000 n=8)
ScanJSONString/long10K-4              6.032µ ± 15%   1.273µ ± 13%  -78.89% (p=0.000 n=8)
```
(Unescape/Scan "no-escape" rows are less relevant: unescapeJSON only runs when an escape exists.)

End-to-end, separate old/new test binaries run interleaved, -count=10:
```
MarshalTo/user-4                                   521.8n ± 11%   292.1n ±  8%  -44.02% (p=0.000 n=10)
MarshalTo/user_escapes-4                           864.2n ± 10%   338.1n ± 12%  -60.88% (p=0.000 n=10)
MarshalTo/long_text-4                              3.800µ ± 11%   2.240µ ± 16%  -41.03% (p=0.000 n=10)
AppendMarshalSQL/user-4                           1021.0n ± 10%   851.1n ± 14%  -16.64% (p=0.009 n=10)
AppendMarshalSQL/user_escapes-4                    1.715µ ± 18%   1.391µ ± 12%  -18.89% (p=0.001 n=10)
AppendMarshalSQL/long_text-4                       18.34µ ±  8%   13.99µ ± 11%  -23.73% (p=0.000 n=10)
AppendMarshalSQL/*/MarshalSQLValue                 ~ (dominated by bytes2.Buffer growth allocs, noise ±30%)
ParseBinaryJSONMarshal/user-4 (binlog)             2.724µ ± 38%   2.362µ ± 27%        ~ (p=0.247 n=10)
ParseBinaryJSONMarshal/user_escapes-4              3.069µ ± 33%   2.565µ ± 22%        ~ (p=0.089 n=10)
ParseBinaryJSONMarshal/long_text-4                 12.70µ ± 17%   10.63µ ± 21%        ~ (p=0.123 n=10)
geomean binlog                                                                  -15%
```
MarshalTo benchmarks use a tree whose strings were materialized to TypeString (as binlog decode
produces). The binlog benchmark (new, hand-encodes MySQL binary JSON) is dominated by
ParseBinaryJSON allocations (29 allocs/op for a 6-field object), so escaping is ~10-15% of it.

CPU profile AppendMarshalSQL/user (old): writeStringContent flat 19%; encodeBytesSQLBytes2 +
Buffer.WriteByte ~45% (F03's territory). New: scan ~13% (nextQuoteOrBackslash + mask), SQL
encoder ~60%. Once F03 lands, this change's relative share on the vreplication path grows.

## Gotchas
- Byte-identical output verified; keep MySQL-compatible escape set (do not add `<>&`/U+2028).
- `hack.String(w.data)` in writeStringContent is read-only aliasing of the caller's buffer - safe.
- Lazy UTF-8 check: a string that is invalid late calls ValidString once then per-rune decode;
  no quadratic behaviour (state machine unknown/valid/invalid).
- SWAR correctness hinges on "lowest set bit exact" - documented; tests put every special byte at
  every offset 0..15 plus random/fuzz inputs.
- No release-compatibility impact (pure internal, same output).
- F03 (sqltypes SQL escaping SWAR) uses the same technique; `load64`/mask helpers could be shared
  in a small internal package later. Bigger follow-up: fuse JSON-unescape + SQL-escape into one
  pass in writeStringContent (currently scanned twice: JSON scan then EncodeSQLBytes2).
- Separate cheap win noticed: `MarshalSQLValue` allocates a fresh `bytes2.Buffer` with no size
  hint (17 allocs / 45KB for a 10KB doc); `buf.Grow(len(raw)+len(raw)/8+32)` would cut growth.

## Tests
- Existing go/mysql/json, go/mysql/binlog, go/vt/vtgate/evalengine tests: pass.
  (evalengine/integration and vreplication packages need a local MySQL/testenv - not runnable here.)
- Added escape_test.go: equivalence vs old reference (escape_old_test.go) for escapeString,
  writeStringContent (output, pos, error text) and unescapeJSON on edge cases, 20k biased random
  strings each, 20k arbitrary byte strings; FuzzEscapeString (ran 45s, ~190k execs, no diffs).
  Mutation check: dropping the <0x20 term from the mask makes the tests fail.
- Added benchmarks: escape_bench_test.go (micro), marshal_bench_test.go (MarshalTo /
  AppendMarshalSQL / MarshalSQLValue on realistic docs), binlog_json_bench_test.go
  (ParseBinaryJSON + MarshalTo, with a small binary-JSON encoder helper).
