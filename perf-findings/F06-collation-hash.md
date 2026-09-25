# F06: collation Hash feeds Metro128 one codepoint at a time

Patch: `F06-collation-hash.patch` (uncommitted changes in worktree `agent-a79a9d2f3c5aa2ec3`).

## Verdict
**Do it.** 2-3.5x faster collation hashing for latin1/8-bit `_ci`, `utf8mb4_general_ci`, `utf8mb4_bin` (and multibyte),
hashes bit-identical (verified), ~200 LOC non-test, low risk. The DISTINCT probe on 3 general_ci text columns is 45% faster.
Caveat: the MySQL 8 default `utf8mb4_0900_ai_ci` is already batched and is unaffected, and legacy UCA
(`utf8mb4_unicode_ci`) shows no measurable gain because its iterator dominates.

## 1. Is the finding real? Yes.
- `8bit.go` `Collation_8bit_simple_ci.Hash`: `hasher.Write8(sortOrder[ch])` per byte.
- `unicode.go` general_ci: `Write16` per rune, and `DecodeRune` through the `charset.Charset` interface per rune.
- `unicode.go` `Collation_unicode_bin.hashUnicode/hashBMP`: `Write32`/`Write16` per rune.
- `uca.go` `Collation_uca_legacy.Hash` and the non-fast path of `Collation_utf8mb4_uca_0900.Hash`: `Write16` per weight.
- `multibyte.go`: `Write8` per ASCII byte.
- `metro.go` `Write8/16/32/64`: they build a scratch array and go through the generic `Write`, which does `%32`, a `copy` and branches.

Callers: `evalengine.NullsafeHashcode128` (vtgate `engine/hash_join.go` probe/build per row and `engine/distinct.go`
per row per column), `evalBytes.Hash` (evalengine `IN` tables and the VM), and enum/set hashing.

### Do hashes leave the process? No.
`vthash.Hasher` (Metro128) is used only in memory: vtgate engine hash join and DISTINCT maps, evalengine `InExpr` tables,
and the VM. Vindexes (`vindexes/unicode.go` and the others) use the separate `collations/vindex/collate` package with
md5/xxhash, not `colldata.Hash`. The plan cache uses `vthash.New256` (highway) or theine hashing. `NullsafeHashcode` (64-bit) has only test callers.
So cross-version identity is not required. It is still preserved exactly: the hasher consumes a byte stream, so
batching the same bytes cannot change the result.

## 2. Benefit

### Micro, instruction counts (cachegrind, deterministic; instructions per op including vthash.New plus Sum128)
`old` = main, `a` = Metro128 WriteN fast path only, `new` = full patch.

| case | old | a (metro only) | new | old/new |
|---|---|---|---|---|
| latin1_swedish_ci/ascii8 | 1183 | 588 | 430 | 2.8x |
| latin1_swedish_ci/ascii44 | 4947 | 1704 | 908 | 5.4x |
| latin1_swedish_ci/latin44 | 4762 | 1602 | 912 | 5.2x |
| utf8mb4_general_ci/ascii8 | 1968 | 1340 | 795 | 2.5x |
| utf8mb4_general_ci/ascii44 | 8736 | 5643 | 2221 | 3.9x |
| utf8mb4_general_ci/latin8 | 2002 | 1513 | 1056 | 1.9x |
| utf8mb4_general_ci/latin44 | 8811 | 5615 | 2794 | 3.2x |
| utf8mb4_bin/ascii8 | 1833 | 1140 | 916 | 2.0x |
| utf8mb4_bin/ascii44 | 7855 | 4717 | 2511 | 3.1x |
| utf8mb4_bin/latin44 | 7848 | 4830 | 3021 | 2.6x |
| utf8mb4_unicode_ci/ascii44 | ~14300 | ~11500 | ~11100 | 1.3x (not visible in wall clock) |
| utf8mb4_0900_ai_ci/ascii44 | 2371 | 2446 | 2399 | 1.0x |

(The runs have about ±5% noise from GC scanning of the large collation tables at init.)

### Micro, wall clock (benchstat, 10 interleaved rounds; the machine had load ~15-20 on 4 vCPU, so high variance)
```
CollationHashValues/latin1_swedish_ci/ascii8     165.5n ± 69%  121.2n ± 60%  -26.75% (p=0.023)
CollationHashValues/latin1_swedish_ci/ascii44    382.6n ± 10%  141.1n ± 43%  -63.13% (p=0.002)
CollationHashValues/latin1_swedish_ci/latin44    387.7n ± 50%  145.2n ± 59%  -62.55% (p=0.000)
CollationHashValues/utf8mb4_general_ci/ascii8    202.7n ± 31%  134.9n ± 28%  -33.42% (p=0.011)
CollationHashValues/utf8mb4_general_ci/ascii44   647.4n ± 47%  236.3n ± 19%  -63.50% (p=0.000)
CollationHashValues/utf8mb4_general_ci/latin8    183.5n ± 14%  149.5n ± 26%  -18.51% (p=0.019)
CollationHashValues/utf8mb4_general_ci/latin44   611.6n ± 19%  255.0n ± 25%  -58.30% (p=0.000)
CollationHashValues/utf8mb4_bin/ascii8           191.9n ± 11%  143.8n ± 53%  -25.09% (p=0.043)
CollationHashValues/utf8mb4_bin/ascii44          518.7n ± 21%  303.1n ± 32%  -41.55% (p=0.001)
CollationHashValues/utf8mb4_bin/latin44          520.1n ± 22%  299.4n ± 20%  -42.44% (p=0.000)
CollationHashValues/utf8mb4_unicode_ci/*         ~ (no significant change)
CollationHashValues/utf8mb4_0900_ai_ci/*         ~ (ascii44 +14% in one run, p=0.009; the instruction count is unchanged, so this is noise;
                                                   the evalengine run below shows no change)
```
There are 0 allocations before and after. The 64-byte hashBuffer stays on the stack (checked with `-gcflags=-m`).

### End to end: evalengine NullsafeHashcode128 on a VARCHAR (new benchmark `BenchmarkNullsafeHashcode128Text`)
```
utf8mb4_general_ci/ascii8    197.0n  137.8n  -30.03% (p=0.004)
utf8mb4_general_ci/ascii44   608.0n  260.8n  -57.12% (p=0.000)
utf8mb4_general_ci/latin44   643.4n  292.7n  -54.51% (p=0.003)
utf8mb4_bin/ascii8           190.9n  138.8n  -27.29% (p=0.002)
utf8mb4_bin/ascii44          504.8n  274.2n  -45.67% (p=0.000)
utf8mb4_bin/latin44          549.0n  287.3n  -47.67% (p=0.000)
utf8mb4_0900_ai_ci/*         ~ (p>0.28)
```

### End to end: vtgate DISTINCT probe table (new benchmark `BenchmarkDistinctProbeText`: 1000 rows, 3 VARCHAR columns, fresh probe map per iteration)
```
DistinctProbeText/utf8mb4_general_ci   812.4µ ± 9%   442.6µ ± 26%  -45.52% (p=0.000)
DistinctProbeText/utf8mb4_0900_ai_ci   549.4µ ± 38%  562.3µ ± 24%  ~
```
This is roughly 0.37µs saved per row for 3 text columns. HashJoin hashes one join key per build or probe row, saving
~0.1-0.35µs per row depending on the string length (estimated from the NullsafeHashcode128 numbers).
The share of total vtgate CPU for such queries depends on row decoding and network costs (estimated at a few % up to ~10-20% for
DISTINCT or hash-join-heavy queries over general_ci/bin/latin1 text). It is **zero for utf8mb4_0900_ai_ci**, the MySQL 8 default.

## 3. Difficulty: S
About 200 non-test LOC in 7 files (`vthash/metro/metro.go`, `colldata/{hashbuf.go (new), 8bit.go, unicode.go, unicase.go, uca.go, multibyte.go}`).
No generated code: `mysqldata.go` is untouched, and `unicase.go` and the `Hash` methods are hand-written.
The tests and benchmarks add about 150 LOC.

## 4. Implementation (prototype is complete)
- **Metro128.Write8/16/32/64**: when the value fits in the pending 32-byte `m.input` block, they store it directly with
  `binary.LittleEndian.PutUintN(m.input[fill:], u)`, bump `count`, and call `processInput()` when the block becomes full.
  Otherwise they fall back to the old scratch-and-`Write` path. `Write` itself is unchanged; its block mixing stays inline, because
  factoring it into `processInput` made it a call there.
  On its own this gives 1.5-2.8x fewer instructions for Write8-heavy loops (column `a`).
- **colldata/hashbuf.go**: `hashBuffer{buf [64]byte; n int}` with inlinable `write8/write16/write32/writeBytes`
  (big-endian, which equals `WriteN(bits.ReverseBytesN(w))`) and an out-of-line `//go:noinline flush`, which keeps the writers under the inline budget.
  There is also `hashPadding8` for the PAD loops.
- **8bit simple_ci**: maps the input through `sortOrder` into a 64-byte stack chunk and `Write`s each chunk.
- **general_ci**: for utf8mb3/utf8mb4 only, it hashes runs of ASCII bytes through a lazily built `[128]uint16` table
  (`UnicaseInfo.asciiSort()`, `sync.Once`). It falls back to `DecodeRune` plus `unicodeSort` for non-ASCII. Other charsets
  (ucs2/utf16/utf32) use the old loop with the buffer.
- **unicode_bin (utf8mb4_bin, utf8mb3_bin)**: an ASCII-run fast path for UTF-8, with batched `write32`/`write16`.
- **uca_legacy and the uca_0900 slow path**: batched `write16`. **multibyte**: batched `write8`/`writeBytes`.

## 5. Gotchas
- **Hash identity**: verified bit-identical. A temporary dump covered every MySQL8 collation × ~610 inputs (random valid
  and invalid UTF-8 plus native-charset conversions) × numCodepoints {0,1,5,40,200}, 1.38M hashes, and `cmp` showed them identical before and after.
  The permanent test `TestHashMatchesPerCodepointHash` compares against test-only copies of the per-codepoint
  implementations. A mutation check (BE→LE in write16) confirmed that it fails.
- The hashes never leave the vtgate process, so there is no cross-version concern even if the stream were changed later.
  The utf8mb4_bin stream (4 bytes per char) could be shrunk for more speed, but that would change the hashes and is not worth it.
- `UnicaseInfo` (an exported struct) gains unexported `sync.Once` plus `[128]uint16` fields. It is only used by pointer, and vet/copylocks is clean.
  An alternative is to precompute the table at package init for the single `unicaseInfo_default`.
- ASCII fast paths are gated on `Charset_utf8mb4`/`Charset_utf8mb3`. For UTF-16/32/ucs2, a byte < 0x80 is not a codepoint.
- The `Write8` fast path cost is 81, just over the inline budget of 80. That is harmless once the collations batch.
- Legacy UCA (`utf8mb4_unicode_ci` and the other `*_unicode_ci`) shows no wall-clock gain. The time is in `uca.Iterator.Next` (~250 instr/char),
  which is a separate finding if it matters.
- **Pre-existing bug (not fixed)**: `Collation_binary.Hash` panics (slice out of range) when `0 < numCodepoints > len(src)`.
  All production callers pass `numCodepoints=0`, so it cannot be reached today.
- Portability: this is plain `encoding/binary` code with no unsafe or asm, so it is fine on arm64.

## 6. Tests
- Existing: `go test ./mysql/collations/colldata ./mysql/collations/charset ./vt/vthash/... ./vt/vtgate/evalengine ./vt/vtgate/engine`
  all pass, and `-race` on metro and colldata passes. (`collations/integration` needs a local MySQL and fails the same way on main.)
- Added: `metro.TestMetroHashFixedWidthWrites` (a random mix of Write8/16/32/64/Write versus one Write over 2000 iterations, all alignments),
  `colldata.TestHashMatchesPerCodepointHash`, and the benchmarks `BenchmarkCollationHashValues`, `BenchmarkNullsafeHashcode128Text`,
  and `BenchmarkDistinctProbeText`.
