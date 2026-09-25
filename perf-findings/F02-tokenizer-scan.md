# F02 — sqlparser tokenizer: byte-at-a-time scan loops

Worktree: /home/user/vitess/.claude/worktrees/agent-af85e8ea69c9b8692 (uncommitted)
Patch: findings/F02-tokenizer-scan.patch

## Verdict
**Do it.** Adopt the stdlib-only `strings.IndexByte` rewrite. Large literals and comments get 4x to 12x faster, typical
traces get about 20-25% faster at the tokenizer level, short tokens are at parity, and the output is identical
(checked against a verbatim copy of the old tokenizer with random, targeted and fuzz inputs).

## Finding re-verified
In go/vt/sqlparser/token.go these loops go one byte at a time through `cur()`/`skip(1)` (each `cur()` does an
eofChar bounds check):
- `scanString`: looks for the delimiter or `\`.
- `scanStringSlow`: its inner run loop looks for the delimiter or `\`, then writes one byte per escape.
- `scanLiteralIdentifier` / `scanLiteralIdentifierSlow`: look for backticks. The slow path writes one byte at a time.
- `scanCommentType1`: looks for `\n`.
- `scanCommentType2`: looks for `*/`.
- `scanMySQLSpecificComment`: its "version not satisfied" skip loop tracks nested comments.

The earlier experiment was slower on `escaped` because the second `IndexByte(delim)` was thrown away on every escape.
If the code searched for the delimiter again after each escape, the cost would be O(n²) in the number of escapes.

## Design (final)
- `indexQuoteOrBackslash(s, from, quote, nextQuote) (i, nextQuote)` does
  `IndexByte(s[from:], quote)` only when the cached `nextQuote` is unknown or already consumed, then
  `IndexByte(s[from:nextQuote], '\\')`. The caller keeps the delimiter position between calls
  (`scanString` → `scanStringSlow`), so a string with many escapes is scanned in about 2 linear passes,
  whatever the number of escapes.
- `scanStringSlow` bulk-writes clean runs with `WriteString`. Escape handling is unchanged
  (`\%` and `\_` keep their backslash, SQLDecodeMap applies, doubled delimiters work as before).
- When a long string has an escape before a known delimiter, `scanString` calls `buffer.Grow(nextDelim-start)`
  once. This removes the repeated growth of the builder.
- Backticks use `IndexByte('`')` plus a bulk `WriteString` in the slow path. Line comments use `IndexByte('\n')`.
  `/* */` comments loop on `IndexByte('*')` and check for a following `/`. `strings.Index("*/")` cost about
  100 instructions per short comment.
- The versioned-comment skip also loops on `IndexByte('*')`. A '*' preceded by an unconsumed '/' (i>0)
  starts a nested comment. Otherwise a following '/' ends the comment.
- Behaviour kept exactly: every `Pos` value, including the odd `Pos = len(buf)+1` after an unterminated string
  whose run reaches EOF. This matters because `Error()` reports `Pos+1`. Returned values, LEX_ERROR cases and
  `inVersionedComment` are also unchanged.
- `skipBlank` and `scanIdentifier` are left as they are. Whitespace runs are about 1 byte, and identifiers would
  need a character-class table. That is a different change.

### Kernel choice (measured, kern microbench, loaded host, ns/op)
| needle at | byte loop | 2×IndexByte | SWAR 8B | SWAR≤32+2×IB | archsimd 128b ×4 |
|---|---|---|---|---|---|
| 3 | 4.2 | 7.0 | 3.5 | 4.8 | 2.4 |
| 15 | 10.3 | 7.7 | 6.8 | 11.6 | 2.6 |
| 64 | 92 | 11.5 | 28.6 | 20.8 | 4.8 |
| 4096 | 2207 | 168 | 1310 | 168 | 124 |
| 100000 | 43500 | 3429 | 26500 | 3240 | 2910 |

I also tried a byte-loop prefix of 8 or 16 bytes in front of `IndexByte`, measured interleaved in-process with 20
samples. It made no difference on short-token-heavy input and made django slightly worse (0.80 without the prefix,
0.82 with 8, 0.83 with 16). The final version uses plain `IndexByte`, which is also the simplest.

## Benefit
### Tokenizer only: old copy vs new, interleaved in one process, 20 samples, new/old time
| input | new/old |
|---|---|
| django_queries.txt | **0.77** ±10% |
| lobsters.sql.gz (2000 queries) | **0.74** ±8% |
| synthetic, every token short (`col1`, 'short1', "ab", 'it''s', 'a\'b', short comments) | 1.02 ±20% (parity) |
| INSERT with a 1 KB string literal | **0.23** (4.4x) |
| INSERT with 1 KB JSON-escaped string (`\"` every ~6 bytes) | **0.81** |

### Parser-level benchmarks: old.test vs new.test binaries run alternately, 10 rounds
| benchmark | old | new | Δ |
|---|---|---|---|
| Parse3/normal | 713µs | 60µs | **−91.6%** (p=0.000) |
| Parse3/escaped | 5.54ms | 2.51ms | **−54.7%**, B/op 4.91MiB→1.10MiB (−78%), allocs 296→65 |
| ParseTraces, ParseStress, Normalize* | | | no significant difference (±30-80% host noise; 4 vCPU, load average 15-22) |

The host was too noisy to resolve small effects, so I also counted instructions deterministically
(cachegrind, per iteration):
- ParseTraces/django: −1.0%
- ParseTraces/lobsters: −2.9%
- NormalizeTraces/django and ParseStress: within the ±3% run-to-run noise
- Parse3/normal: 23.4M → 2.28M instructions (−90%)
- Parse3/escaped: 40.1M → 30.0M instructions (−25%)

End-to-end: the tokenizer is about 5% of `Parse` on typical OLTP traces, so expect a gain of about 1% there (estimate).
The real win is queries with large string literals or comments (big INSERTs of TEXT/JSON/blob literals, long
comments), where tokenizing becomes about 10x faster and allocations drop sharply for escaped payloads. vtgate
parses every query, so this matters for write-heavy workloads with large payloads.

### archsimd (GOEXPERIMENT=simd) headroom
A single-pass two-needle kernel with 128-bit vectors (4×16B unrolled) is about 2.5x faster than 2×`IndexByte`
for short strings (~2.5ns vs ~6.5ns) and about 15% faster for long ones. In parse terms that saves a few ns
per string literal, which is negligible end to end. It is not worth an experimental, amd64-only build-tagged
code path. The kernel is in scratchpad/F02/kern/simd.go and is not in the patch.

## Difficulty
S. token.go is about +127/−121 lines and has no generated code.
The tests are new files:
- token_ref_test.go (~620 lines): a verbatim copy of the old scan methods on `type oldTokenizer Tokenizer`, used as
  the reference.
- token_equivalence_test.go (~170 lines).

## Gotchas
- **Pos semantics.** The old `scanStringSlow` leaves `Pos = len+1` when a run reaches EOF, and `Error()` reports
  `Pos+1`. The patch keeps this on purpose. The equivalence test catches a mutation that "fixes" it.
- **Builder.Grow.** For long escaped strings the returned string may keep up to about 1 byte per escape of slack,
  bounded by the raw literal length. For unterminated strings Grow may reserve the rest of the buffer, but only on
  the LEX_ERROR path. Fast-path results are still substrings of the input, so there is no new aliasing or retention.
- **Linearity.** Without the cached `nextDelim`, a long string with many escapes and a far delimiter becomes
  quadratic. The earlier regression was a mild form of this.
- **Recursion.** No new recursion (see the AGENTS.md warning). Helpers still return to the `Scan` loop.
- **Portability.** `strings.IndexByte` is assembly on amd64, arm64, s390x and others, with a generic fallback
  elsewhere. There is no unsafe code and no build tags.
- **Tokens with many short parts.** The worst case, where every token is 2-8 bytes, is at parity by time.
  Cachegrind shows about 2-3% more instructions there, but they are cheap and predictable.
- **Test policy.** The equivalence test cannot "fail on main without the fix" because this is a refactor for
  speed. A reviewer may object to the 620-line reference copy. Options are to keep it (strongest guarantee),
  trim it to the scan helpers, or drop it after merge and keep only targeted tests.

## Tests
- Existing `go test ./go/vt/sqlparser/...`: PASS.
- Added `TestTokenizerEquivalence`: 50k random SQL-like inputs built from fragments (quotes, doubled
  quotes, `\'`, `\\`, `\%`, `\_`, backticks, `/* */`, `/*!50000`, `/*!90000`, `--`, `#`, long runs, high bytes).
  Each run compares (typ, val, Pos, inVersionedComment) after every `Scan` with both `SkipSpecialComments` settings.
- Added `TestTokenizerEquivalenceLongTokens`: long strings, identifiers and comments with a special sequence at
  every third offset, plus unterminated variants.
- Added `FuzzTokenizerEquivalence` with 24 seeds. Fuzzing ran for 60s + 90s (820k executions) without failures.
- Mutation checks: removing the `len+1` quirk, or disabling nested-comment detection, makes the tests fail.
