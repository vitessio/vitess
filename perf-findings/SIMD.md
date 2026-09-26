# Can Go's SIMD experiment speed up Vitess? (the original question)

**Short answer:** not yet worth adopting. The measurable wins in the parser and escaping code come from replacing byte-at-a-time loops with stdlib primitives that are already vectorized (`strings.IndexByte`, etc.) and from bulk-copying clean runs. An `archsimd` kernel adds only a few ns per token on top (F02).

**Setup:** Go 1.27.1 (the repo's toolchain) on a 4-vCPU Xeon with AVX2 and AVX-512. The benchmark sources are in `SIMD-bench/`; run them with `GOEXPERIMENT=simd go test -bench .`.

## The API as of Go 1.27

- **Build flag:** both packages exist only with `GOEXPERIMENT=simd`, and neither is covered by the Go 1 compatibility promise.
- **`simd` (portable, vector-size agnostic) can't search efficiently.** Its masks support only `And`, `Or` and `ToInt8s`: there is no `ToBits` (PMOVMSKB) and no first-true index. Finding "the first byte that matches", which is what lexers and escapers need, therefore requires spilling the mask to memory.
- **`simd/archsimd` (amd64 only) can.** `Mask8x32.ToBits()` compiles to `VPCMPEQB` + `VPMOVMSKB` + `BSF`, and CPU features are checked with `archsimd.X86.AVX2()`.

## Measurements

**Finding the first quote or backslash**, the core of `scanString`:

| size | scalar loop | archsimd AVX2 | portable simd | stdlib IndexByte (1 needle) |
|---|---|---|---|---|
| 8 B | 4.1 ns | 8.4 | 8.8 | 2.4 |
| 1 KiB | 485 ns | 36 | 115 | 17 |
| 16 KiB | 6333 ns | 332 | 2286 | 268 |

- archsimd is 13–19x faster than the scalar loop from 32 bytes up, but slower below 16–32 bytes.
- portable `simd` is only ~3x faster, because it has to spill the mask.

**SQL escaping** (`encodeBytesSQL*`):

| payload | current | bulk run copy (no SIMD) | runs + 128-bit | runs + 256-bit AVX2 |
|---|---|---|---|---|
| 128 B | 431 ns | 109 | 76 | 186 |
| 16 KiB, no escapes | 46.7 µs | 9.2 | 5.4 | 5.1 |
| 16 KiB, 1 escape per 64 B | 51.1 µs | 12.4 | 15.2 | 38.3 |

- Most of the gain comes from copying clean runs in bulk instead of calling `WriteByte` per byte, with no SIMD at all.
- **256-bit AVX2 was slower than scalar when mixed with `strings.Builder` writes**, even though its kernel alone was 10x faster. The Go 1.27.1 compiler emitted no `VZEROUPPER` before returning, and the SSE code that ran next paid the AVX/SSE transition penalty. 128-bit vectors avoid it. This is worth an upstream Go report (also listed in `BUGS.md`).

## Is the parser worth vectorizing?

- **Where parse time goes on real traces** (django, lobsters): the goyacc state machine is ~44% flat, the whole tokenizer (`Lex`) ~15% cumulative, `scanString` ~2%, `skipBlank` ~1.4%, and `scanIdentifier` ~5%. Tokens are short, which is where SIMD loses.
- **Large literals are the exception.** A stdlib `IndexByte` fast path made `BenchmarkParse3/normal` (1 MB of literals) 739 µs → 47 µs. F02 later turned this into a full tokenizer rewrite: −92% on large literals and about −25% of tokenizer time on traces. The archsimd kernel it tried added only ~2.5x on short strings (a few ns) and ~15% on long ones, which isn't worth an experimental amd64-only path.

## Where SIMD would actually help

Only multi-needle "find the first special byte" scans: `scanStringSlow` (quote or backslash), the JSON escaper (`"`, `\`, or a byte below 0x20) and the SQL escape scan. Even there, 8-byte SWAR (F03, F18) gets most of the gain portably.

A speculative idea nobody pursued: bitsliced DES with SIMD, to batch the `hash` vindex over many ids. DES is 87 of the 120 ns per id, but it's a big effort for a small share of the insert path (round-1 engine survey #8).

## Recommendation

Don't depend on `GOEXPERIMENT=simd`: it needs build and release changes, its API is unstable, and it would conflict with N±1 release compatibility. Land the stdlib/SWAR versions instead (F02, F03, F18, F21). Revisit once portable `simd` gains a bitmask or first-true operation and the `VZEROUPPER` issue is fixed.
