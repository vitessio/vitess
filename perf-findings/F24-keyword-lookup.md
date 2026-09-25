# F24: keyword lookup (sqlparser `caseInsensitiveTable.LookupString`)

## Verdict
Do it. Small, self-contained change (one file plus a one-line change in formatID) that cuts keyword lookup
cost by 1.6-13x depending on input, and it is exhaustively testable against a reference map.

## Finding re-verified
- `keywords.go`: `caseInsensitiveTable` was a `map[uint64]keyword` keyed by a case-folded FNV-1a hash of
  the whole name. A lookup therefore paid for (1) FNV over every byte, with a multiply per byte,
  (2) `runtime.mapaccess2_fast64` plus `memHash64AES`, and (3) `keywordASCIIMatch` on a hit.
- Callers: `Tokenizer.scanIdentifier` (token.go:396) for every unquoted identifier or keyword token, and
  `formatID` (ast_funcs.go:1149) for every identifier that gets formatted (String(), normalizer output,
  planbuilder queries sent to shards).
- There are no version-dependent keywords. The table is static, and MySQL version only affects the
  grammar and the parser options, not the lookup. `UNUSED` keywords are also in the table, so that formatID
  backquotes them. The new table keeps them.
- Case folding is ASCII only (`keywordASCIIMatch`); the new code keeps exactly that. Unquoted identifiers
  are `[A-Za-z0-9_$]` (isLetter/isDigit). formatID can receive arbitrary bytes, including non-ASCII, from
  backquoted identifiers.
- Keyword stats: 757 keywords, lengths 2..33, all `[a-z0-9_]`.

## Implementation (prototype in the worktree, patch F24-keyword-lookup.patch)
- The table is an open-addressing table with 2048 slots (`[2048]uint64`, 16 KB) and linear probing.
  It has a load factor of 0.37. A hit takes 1.34 probes on average and at most 6.
- The key is `keywordKey(s) = len | (s[0]|0x20)<<8 | (s[1]|0x20)<<16 | (s[n-2]|0x20)<<24 | (s[n-1]|0x20)<<32`:
  40 bits, always O(1), and it never reads the middle of the name. The slot is `key * golden >> 53`.
  Each slot packs `key | (idx+1)<<40`, with 0 meaning empty. Only 20 keywords share a key with another
  keyword, and probing handles those.
- A lookup first returns early when the length is below the shortest keyword or above the longest
  (`< 2`, `< minLen`, `> maxLen`). This rejects long identifiers without reading them. It then probes, and
  on a 40-bit key match it runs the exact `keywordASCIIMatch`.
- The key's `|0x20` folding is lossy on purpose: it maps `_` (0x5F) and 0x7F together, digits and control
  bytes together, and so on. The exact compare after a key match removes those false candidates. The key
  never produces a false negative: for a letter byte both cases map to the same value, and for any other
  byte an exact match gives the same key.
- `formatID`: the `escapeAllIdentifiers` check now runs before the lookup, through short-circuit
  evaluation of `isKeyword(original)`. The result is identical because all three conditions are pure.
- The patch removes the dead `keyword.match([]byte)` and `keyword.matchStr`, and the FNV helpers,
  which no longer have callers.
- token.go is unchanged. With an O(1) key, folding a hash into the scanIdentifier loop no longer buys
  anything, so this change cannot conflict with F02.
- `buildCaseInsensitiveTable` panics at init on a duplicate keyword, on a keyword length outside 2..255,
  or when there are more than 1023 keywords. It replaces the old panic on an FNV collision.

## Measurements (4 vCPU shared box, load average 4-12; wall-time end-to-end A/B was pure noise at ±15-70%)

### Micro (interleaved, count=6, benchtime=200ms). Each op looks up the whole list of names.
```
                              │     old      │                 new                 │
KeywordLookup/keywords-4         261.2n ± 11%   165.5n ± 21%  -36.65% (p=0.002 n=6)   20 names, 13.1 -> 8.3 ns/lookup
KeywordLookup/long_keywords-4    180.4n ±  6%   107.5n ± 16%  -40.43% (p=0.002 n=6)
KeywordLookup/short_idents-4    139.05n ± 23%   37.00n ± 11%  -73.39% (p=0.002 n=6)   11.6 -> 3.1 ns/lookup
KeywordLookup/idents-4          125.90n ± 18%   55.03n ± 51%  -56.29% (p=0.002 n=6)
KeywordLookup/long_idents-4     158.45n ± 17%   12.25n ± 15%  -92.27% (p=0.002 n=6)   (rejected by maxLen)
KeywordLookup/lookalikes-4      115.65n ± 18%   40.60n ± 22%  -64.89% (p=0.002 n=6)
geomean                          157.3n         51.20n        -67.44%
```
Allocations: 0 before and after.

### End-to-end: instruction counts (callgrind, GOGC=off, django_queries trace, 12 iterations)
Wall-clock A/B was unusable on the shared machine, where the load average was about 11. The numbers
below are deterministic per-function instruction counts. Totals still vary with runtime noise such as
memprofile sampling and stack growth, so the deltas come only from the functions this change touches:
LookupString, mapaccess2_fast64 and memHash64AES.

| benchmark | old instr/iter (approx) | saved instr/iter | share |
|---|---|---|---|
| ParseTraces/django | ~8.4M | ~0.19M (LookupString -0.82M, mapaccess -1.32M, memhash -0.12M over 12 iters) | **~2.2%** |
| StringTraces/django (formatting) | ~3.3M | ~0.25M (LookupString -1.64M, mapaccess -1.23M, memhash -0.12M) | **~7.5%** |
| NormalizeTraces/django | ~5.9M | ~0.08M (LookupString -0.45M, mapaccess -0.49M) | **~1.3%** |

For ParseTraces, scanIdentifier's inclusive count dropped from 7.04M to 4.71M, and LookupString's
inclusive count from 4.48M to 2.18M.
Instruction counts understate the win a little, because the old map probe also had more cache misses and
branch mispredictions. A realistic end-to-end estimate is 1-2% for normalize and parse and 5-7% for pure
formatting (String()/formatID-heavy paths, such as planbuilder rewriting queries to send to shards).

## Tests
- Existing: `TestKeywordTable` (every keyword, lowercase only), `TestCompatibility`, `TestKeywords`.
- Added `TestKeywordLookupMatchesReference` in keywords_test.go, about 650k names in 0.2s. It compares
  the table with a plain `map[string]int` under ASCII-only lowering, using:
  - every keyword in all upper/lower case combinations when len <= 12, and 256 random combinations otherwise;
  - every byte of every keyword replaced by a set of bytes that look alike under `|0x20`
    (0x7f, 0x10, 0x19, '@', '[', ']', '^') or are non-ASCII (0xc0, 0xe1, 0xff), plus '$', '0', 'Z';
  - every keyword truncated at either end, extended at either end, and followed by the Kelvin sign
    (U+212A, which Unicode lowercases to 'k');
  - 200k random identifiers; the empty string, a 1-byte name, non-ASCII names, and a 300-byte name.
- Added `BenchmarkKeywordLookup`.
- Scratch only, not in the patch (kept at scratchpad/f24_src/keywords_old_test.go.txt): a copy of the old
  table, an old-vs-new equivalence test on 1M random byte strings plus 64 mutations of every keyword
  (passes), `BenchmarkKeywordLookupOld`, and the probe statistics.
- `go test ./go/vt/sqlparser/... ./go/vt/vtgate/planbuilder/...` passes, so the planbuilder expected
  outputs are unchanged.

## Gotchas
- CLAUDE.md asks for "a test must fail on main without the fix". This is a performance rewrite: the new
  test is an equivalence guard, and it passes on main by design. Say so in the PR.
- Table invariants are enforced at init by panic: no keyword shorter than 2 bytes (a hypothetical 1-letter
  keyword would need `keywordKey` changed), at most 1023 keywords (a load factor below 0.5 guarantees
  that probing terminates), and no duplicates.
- The 16 KB slot array plus the keywords slice replaces the Go map (about 30-40 KB). Memory is not larger.
- `LookupString` now returns `(0, false)` on a miss. The old code could return `(candidate.id, false)`
  after an FNV collision, but every caller ignores the id when the result is false.
- It is portable: the code has no unsafe and no unaligned loads, and it behaves the same on arm64.
- No release-compatibility impact: this is internal code with no behavior change.
- `golangci-lint` could not run locally because the binary was built with go1.25 and the module targets
  go1.27. `go vet` is clean.
