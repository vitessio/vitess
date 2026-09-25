# F14 collate-fastpath: common-prefix skip + devirtualized decode in general_ci / 8bit simple_ci Collate

## Verdict
Do it. Correct by construction (validated prefix skip, generic fallback), 5-60x faster for the realistic
utf8mb4/utf8mb3_general_ci cases, 2-4x for latin1 ci. Small, self-contained change (2 files, ~135 LOC plus tests).

## Finding verified
- `colldata/unicode.go` `Collation_unicode_general_ci.Collate`: two interface `cs.DecodeRune` calls per rune,
  plus `unicodeSort` for both sides even when the runes are equal. Used by utf8mb3/utf8mb4/ucs2/utf16/utf16le/utf32 _general_ci (6 collations).
- `colldata/8bit.go` `Collation_8bit_simple_ci.Collate`: byte loop with 2 table lookups per byte (39 collations incl. latin1_swedish_ci).
- Callers: evalengine `NullsafeCompare`/`compare` (api_compare.go:63), used by MemorySort/MergeSort/Route ORDER BY (Comparison),
  OrderedAggregate group-key comparisons (ordered_aggregate.go:344), aggregations (DISTINCT), compiled `=`/`<` on text (compiler_asm.go:712/724),
  IN/FIELD/GREATEST/LEAST/STRCMP, and the LIKE matcher (wildcard.go:97, literal patterns and `abc%` with isPrefix=true).
- general_ci has NO TinyWeightString (only 8bit_simple_ci, binary and 0900 do), so MemorySort on a general_ci text column
  calls Collate for every comparison: n log n calls per sort. Equal/shared-prefix keys are exactly the common case in
  ORDER BY / GROUP BY key comparisons (adjacent sorted keys share prefixes; group keys are equal).
- Other collations: *_bin (unicode_bin, 8bit_bin, 0900_bin, binary) already use collationBinary (bytes.Compare).
  0900 already has FastForward32. uca_legacy (utf8mb4_unicode_ci etc., 121 collations) is iterator based; a byte-prefix
  skip is NOT trivially safe there because of contractions (e.g. Spanish "ch", "ll") and context-dependent weights. Not changed.
  multibyte.go (sjis/euckr/gb*) not changed (rune-boundary detection is not self-synchronizing in those charsets).
- Note (pre-existing, preserved): neither Collate implements PAD SPACE: `Collate("a", "a ")` != 0 even though MySQL
  general_ci is PAD SPACE. Existing behaviour, unchanged by this patch; worth a separate issue.

## Implementation (prototype in worktree, uncommitted)
unicode.go:
- `Collate` type-switches on the charset: utf8mb4 / utf8mb3 go to `collateUTF8`, everything else to `collateGeneric`
  (the old loop, byte-for-byte unchanged).
- `commonPrefix(a, b)`: 8 bytes at a time (LittleEndian.Uint64, XOR, TrailingZeros64>>3), also ORs the scanned bytes
  to report whether the prefix is pure ASCII.
- Back up n to a rune start (`utf8.RuneStart` on left[n] / right[n]).
- If the prefix is not ASCII, validate it: `utf8.Valid` (== utf8mb4 DecodeRune semantics incl. surrogates/overlongs),
  plus for utf8mb3 a word-wise "no byte >= 0xF0" check (x&(x<<1)&(x<<2)&(x<<3)&0x80..80). If invalid, fall back to
  `collateGeneric` from the start (the old algorithm would have hit the invalid sequence inside the shared prefix and
  returned bytes.Compare there; falling back gives the identical result trivially).
- Tail loop: ASCII fast path when both bytes < 0x80 (no decode); otherwise static (devirtualized, inlinable)
  `Charset_utf8mb4{}.DecodeRune` / `Charset_utf8mb3{}.DecodeRune`; skips unicodeSort when runes are equal.
  isPrefix / final length semantics unchanged (the remaining slices are the same suffixes as in the old loop).
8bit.go: `if cmpLen > 0 && left[0] == right[0] { start, _ = commonPrefix(left, right) }` then the old byte loop from `start`;
return values and rightIsPrefix handling unchanged (uses original slice lengths).

Why skipping is safe: identical bytes that form complete, valid runes decode to identical runes -> identical weights,
so the old loop would pass through the shared prefix without returning. Validity + rune boundary guarantee the old
loop's decode positions coincide with n. (The RuneStart backup is only a perf step: without it the prefix would end
mid-rune, fail utf8.Valid and fall back to the generic path; the mutation test confirms correctness does not depend on it.)

WeightString / Hash / Wildcard's per-rune `equals` are untouched.

## Tests
New `colldata/collate_fastpath_test.go` (package colldata):
- `TestCollateUnicodeGeneralCIFastPathEquivalence`: all 6 general_ci collations; `Collate` vs `collateGeneric` (old loop),
  exact return value equality, isPrefix false/true, ~40k corpus cross pairs + 60k random pairs built from pieces (ASCII
  upper/lower, spaces/trailing spaces, NUL, accented Latin, CJK, U+FFFD, U+FFFF, 4-byte emoji, invalid bytes 0xFF/0x80,
  truncated sequences, surrogates ED A0 80, overlongs C0 80 / E0 80 AF, out-of-range F4 90 80 80), mutations
  (byte flip, truncation, append, ToUpper, insert, trailing space) to create long shared prefixes that diverge mid-rune,
  plus fully random bytes. Non-UTF8 charsets get their encoded forms.
- `TestCollate8bitSimpleCIFastPathEquivalence`: all 8bit_simple_ci collations vs a copy of the old loop.
- `TestCommonPrefix`: boundaries at 7/8/9/15/17, ASCII flag.
- Mutation checks: disabling the prefix validation -> fails (utf8mb3/mb4); disabling the mb3 >=0xF0 check (byte or word variant) -> fails.
- Existing: go/mysql/collations/... all pass (golden, fuzz seeds, wildcard, uca). `integration` needs a live mysqld
  (bin/mysqlctl) -> cannot run offline (fails identically on main). go/vt/vtgate/evalengine and engine: pass.
- `BenchmarkCollateFastPath` with impl=old/new sub-benchmarks (old = collateGeneric / reference 8bit loop).

## Benchmarks (measured, noisy shared 4 vCPU box; benchtime=100ms, count=6/8; benchstat -col /impl)
utf8mb4_general_ci                old        new      delta
  ascii/equal/8                   79.0n      8.9n     -89%
  ascii/equal/44                 389n       16.1n     -96%  (24x)
  ascii/equal/256               2214n       34.1n     -98%  (65x)
  ascii/diff-end/44              423n       18.0n     -96%
  ascii/diff-start/44            10.2n      10.4n     ~
  ascii/case/44 (ci-equal)       457n      170n       -63%  (no prefix to skip; gain from devirtualized/ASCII path)
  latin(accented)/equal/44       426n       38.1n     -91%
  latin/equal/256               2239n       90.1n     -96%
  latin/diff-end/44              474n       35.1n     -93%
  latin/case/256                2367n     1082n       -54%
utf8mb3_general_ci: similar; latin/equal/44 346n -> 54n (-84%), 256: 2211n -> 157n (-93%) (extra mb3 validation pass).
latin1_swedish_ci (8bit simple ci)
  ascii/equal/44                  34.7n      12.5n    -64%
  ascii/equal/256                168n        46.8n    -72%
  latin/diff-end/256             174n        53.6n    -69%
  ascii/diff-start/44             1.7n        2.9n    +1.2ns (call overhead; reference is inlined into the bench loop)
  latin/case/8..44               ~7-30n     +2..7ns   (first byte equal -> commonPrefix call returns after 1 byte)
geomean over all 63 cases: -71%.

End-to-end (estimated, not measured): for MemorySort / MergeSort / OrderedAggregate over utf8mb4_general_ci or
utf8mb3_general_ci text keys, Collate is the per-comparison cost (no tiny weights for general_ci); a 44-byte key
compare drops from ~400ns to ~20-40ns when keys share a prefix or are equal, so sorting 100k rows (~1.7M compares)
drops by up to ~0.6s CPU. For 0900_ai_ci (MySQL 8 default) columns this change has no effect.

## Gotchas
- Exactness: new code returns the exact same int (not just sign) as the old loop, including isPrefix's `len(right)`
  (note general_ci isPrefix returns remaining len(right) > 0 for "not a prefix", while 8bit returns <= 0; both preserved).
- Invalid UTF-8 inside the shared prefix: must fall back (verified by mutation test); invalid after the prefix is
  handled by the tail loop identically (bytes.Compare of the remaining suffixes == bytes.Compare of originals minus shared prefix).
- utf8mb3 differs from utf8.Valid only by rejecting 4-byte sequences; handled by the >=0xF0 check.
- ucs2/utf16/utf16le/utf32 general_ci keep the old generic loop (could be extended: fixed-width boundary rounding for ucs2/utf32).
- Tiny regression (~1-7ns) for latin1 strings differing in the first couple of bytes; could be avoided by comparing
  8-byte words only when len>=8, or inlining a first-word check. Negligible vs call overhead through the interface.
- Big-endian (s390x): binary.LittleEndian.Uint64 is still correct, just a byte-swapping load. arm64 fine.
- No struct/generator changes (mysqldata.go is generated by makecolldata; the type switch avoids adding fields).
- No allocations; no aliasing.
