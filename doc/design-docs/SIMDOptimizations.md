# SIMD fast paths on the Vitess hot query path

## Summary

Go is growing native SIMD support: the `simd` and `simd/archsimd` packages,
behind `GOEXPERIMENT=simd` since Go 1.26. This document ranks the Vitess code
paths that could use it, by measured CPU share on the per-query path, and
records a benchmark-gated prototype of the top three:

1. **Bind-variable escaping** (`sqltypes.encodeBytesSQL*`): vttablet runs it
   through `ParsedQuery.GenerateQuery` on every query with a string or binary
   bind variable.
2. **utf8mb4_0900 collation fast path**
   (`uca.(*FastIterator900).FastForward32`): vtgate runs it for ORDER BY,
   GROUP BY, DISTINCT and hash joins once the tiny-weight comparison ties.
3. **Tokenizer string-literal scanning** (`sqlparser.(*Tokenizer).scanString`):
   vtgate runs it for every literal in every query it parses.

Each fast path has a pure-Go scalar rewrite (the release path today) and a
portable-`simd` kernel that compiles only when `GOEXPERIMENT=simd` and the
`simd` build tag are both set. **A kernel ships only if it is measurably
faster** than the scalar path it replaces, with no significant regression in
any benchmark cell (§4). Regular `make build` stays on the scalar release
path; `make build-experimental-simd` and the experiment workflow supply both
opt-ins explicitly (§6). **While this pull request is a Draft that is
suspended: the Makefile aliases `build` to the experimental path so
arewefastyet measures the kernels. See the Status section.**

## 1. Go SIMD status

Verified against the go1.27.1 source tree (`$GOROOT/src/simd`).

- `simd/archsimd` (Go 1.26): architecture-specific vector types (`Uint8x32`,
  `Mask8x16`, ...) and intrinsics. amd64 has AVX, AVX2 and AVX-512 tiers plus
  permutes/shuffles (`Uint8x32.Permute`, `Uint16x32.Permute`); arm64 has NEON
  arithmetic and compares but **no permute**; wasm is emulated.
- `simd` (Go 1.27): portable, vector-size-agnostic types (`Uint8s`, `Mask8s`,
  ...) with one code path across AVX-512/AVX2/NEON and pure-Go emulation
  elsewhere. `Uint8s.Len()` is 16 on arm64 and 16/32/64 on amd64 by CPU;
  `GODEBUG=simd=<bits>` caps it. `simd.Emulated()` reports software fallback.
- Both packages exist only under `GOEXPERIMENT=simd`, are not covered by the
  Go 1 compatibility promise, and may change between releases.
- Documented caveats: package-level `var` initializers that produce vector
  values do not work; `reflect.Call` on vector functions is broken.
- Gaps found while building the prototypes:
  - **No movemask / first-set-lane operation.** Extracting a lane index from a
    `Mask8s` is `ToInt8s().ToBits().ReshapeToUint64s().Store(buf)` followed by
    a word scan and `bits.TrailingZeros64`. This costs a store and a reload per
    block, which is why the portable kernels cannot match hand-tuned assembly
    such as `bytes.IndexByte` (§4).
  - **No horizontal reduction** (any/all/max across lanes) in the portable API.
  - **Methods with a receiver that use vector values fail to compile** in
    go1.27.1 (`internal compiler error: missing Types entry: Index@simd0`).
    The kernel body must be a plain function; a thin method that only calls
    it is fine. Closures that capture vector values hit the same error.
  - **A call to a vector function costs the caller its inlining budget.**
    The compiler clones each vector function per vector width, and a call
    to one is charged like any non-inlined call, so a wrapper that does
    "short input → scalar, else → kernel" lands at cost 72–91 against the
    budget of 80. Whether it inlines decides whether every short input pays
    a call: `uca.equalASCIIPrefix` fits (72) and 16-byte compares are
    unchanged; `bytes2.(*ByteSet).Index` does not (88) and 8-byte inputs
    pay about 1ns.
  - **A kernel with a per-block mask store cannot beat stdlib assembly that
    has a movemask.** `bytes.IndexByte` runs at ~42 GB/s on arm64; the
    portable equivalent reached ~12 GB/s (§4). Where a stdlib primitive
    already covers the scan, the fast path is to call it.
  - **`LoadUint8sPart` is a real function call, not an intrinsic**, and Go's
    ABI has no callee-saved vector registers, so the compiler spills and
    reloads every live vector around it. In the eight-member `ByteSet`
    kernel that was 16 `FMOVQ`s and a 240-byte frame; a 4-byte tail cost
    more than a 16-byte block (+26% vs +5% over scalar). Read the tail as
    an overlapping full block ending at the last byte instead: bytes the
    loop already cleared cannot flag, so a hit there is real, and there is
    no zero-fill to discount.
  - **`BroadcastUint8s` from a scalar is three instructions** (`MOVBU`,
    `VMOV` to lane 0, `VDUP`). Eight of them per call was the fixed cost
    that made short inputs lose to the table walk. Pre-broadcasting each
    member into a 64-byte row at construction makes it one vector load.
  - **One 16-byte NEON block is about as expensive as 16 scalar table
    lookups.** On an M4 the scalar walk runs at ~1 byte/cycle; a block is
    8 `VCMEQ` + 7 `VORR` + the mask store, reload and word scan, ~17
    cycles. SIMD wins only when consecutive clean blocks pipeline (~6
    cycles/block on 4 KB clean, 2.3×); a scan that hits every ~32 bytes
    never gets that overlap, so it pays the wrapper call and the setup for
    parity. That is the shape of the remaining `dense` regression, and it
    is a property of 128-bit lanes without a movemask, not of the code.

## 2. Current Vitess state

- `go.mod` is `go 1.27.1`. `make build` remains scalar;
  `make build-experimental-simd` and the experiment workflow set
  `GOEXPERIMENT=simd` and `-tags simd`. Direct Go builds, `build.env` and the
  Docker images do not. No `GOAMD64` or `GOARM64` is set. _(The Draft alias
  described in the Status section temporarily makes `make build` set both.)_
- Hand-written SIMD already in tree: `go/vt/vthash/highway` (AVX2, SSE4, NEON
  and ppc64le assembly with `golang.org/x/sys/cpu` dispatch and a `noasm`
  opt-out), used only for the plan-cache key; `go/atomic2` (128-bit atomics).
- Pure-Go hashing on the hot path: `vthash.New()` is Metro128, used by
  `Distinct`, `HashJoin`, evalengine hashing and every collation's `Hash`.
- Vectorized third-party dependencies already cover backups
  (`klauspost/compress` zstd, `pgzip`, `pierrec/lz4`) and the `xxhash`
  vindex (`cespare/xxhash`). MD5/SHA use the standard library's assembly.
- CI runs unit tests on amd64 and arm64 (`unit_test.yml`).

## 3. Hot-path CPU ranking

CPU profiles of the existing benchmarks at the baseline commit (§4), arm64
(Apple M-series), `go tool pprof -top`. The benchmark corpus is the proxy for
the per-query path: `BenchmarkParse3` and `BenchmarkNormalizeVTGate` parse
and normalize the lobsters query log; `BenchmarkOLTP/TPCC/TPCH` plan them;
`BenchmarkCollation*` compare, weight and hash a mixed corpus;
`BenchmarkCompilerExpressions` and `BenchmarkScalarAggregate` cover
evalengine and aggregation. A live vtgate/vttablet profile under sysbench is
the follow-up that would replace this proxy.

Profiles were captured per package with:

```sh
go test -run '^$' -bench "$BENCH" -benchtime=2s -cpuprofile cpu.prof -o pkg.test "$PKG"
go tool pprof -top -nodecount=200 pkg.test cpu.prof
```

| Candidate | Benchmark | flat% | cum% | Notes |
|---|---|---|---|---|
| `encodeBytesSQLBytes2` / `encodeBytesSQLStringBuilder` | `EncodeSQL` | 29.8 / 7.2 | 44.0 / 11.7 | Self-benchmark, so the share is of the encoder alone. Within `GenerateQueryStringBinds` the encoder is ~100% of the substitution cost; there was no existing benchmark that covered it. |
| `(*Collation_utf8mb4_uca_0900).Collate` | `CollationCollate`, `CollateSharedPrefix` | 1.6 | **18.4** | `FastForward32` is 6.8% flat inside it; `NextWeightBlock64` 5.1% cum. |
| `(*Tokenizer).scanString` + `peek` | `NormalizeVTGate` (lobsters corpus) | 0 + 5.4 | **1.5** (`Scan` 8.5, `yyParse` 23.6) | On real queries the literals are short; `peek` is the bounds-checked read every scan routine does per byte. In `Parse3`, a 1 MB query of ten 100 KB literals, the same loop is 38.5% cum — a fair stress test of the primitive, not a corpus share. |
| `(*Tokenizer).scanIdentifier`, `LookupString` | `NormalizeVTGate` | 0.3 | 2.7, 1.1 | Identifiers are too short to vectorize. |
| planbuilder, evalengine, engine candidates | `OLTP/TPCC/TPCH`, `CompilerExpressions`, `ScalarAggregate` | — | — | Profiles are allocation-dominated (`mallocgc` 6%, `memmove` 1%); no byte-loop candidate appears above 0.5%. |

The ranking is by where the byte loop is a large share of a path that runs
per query: escaping is the whole cost of bind substitution on vttablet, the
collation is a fifth of every compare that reaches it, and the tokenizer's
string hunt is a small share of parse time on a real corpus. It stays on the
list because the scalar rewrite is a one-line change to a proven-faster
stdlib primitive, and long literals — JSON payloads, generated INSERTs — are
where vtgate parse time actually goes when it goes anywhere.

**Not candidates**, with reasons: `key.Compare` (inputs are 8 bytes,
`bytes.Compare` is already assembly); `readLenEncInt` and MySQL packet
encoding (per-field, memmove-bound); `binlog.CellValue` (branchy per-type
decode); LIKE wildcard matching (recursive, literal runs already use
`bytes.Index`); `utf8.Valid` (called on charset coercion, not per value);
`Distinct`/`HashJoin` hashing (short keys; setup dominates the hash).

## 4. Benchmark method and viability gate

### Baseline

The benchmarks were added in the first commit of this branch, before any code
moved, so before/after comparisons have a reviewable starting point. Test
binaries built at that commit are kept for the local arm64 comparison, and
the old loops are kept as `*Reference` functions in the test files so a single
test binary at HEAD can report today's code, the new scalar path and the SIMD
path side by side. The `Reference` cells drive the old loop through the same
writer as the new one. Against the frozen binary they are at parity from 256
bytes up and read 8–12 ns under it on 8–32 byte inputs; the `Value.EncodeSQL*`
type switch they bypass measured ~1 ns of that, and the remainder — the same
loop compiled into two binaries — is not traced, so the frozen-binary column
is the one the gate reads at small sizes.

Benchmarks, all with `-benchmem` and `b.SetBytes`:

- `go/sqltypes`: `BenchmarkEncodeSQL/{Bytes2,StringBuilder}/{8,32,256,4096}/{clean,sparse,dense}`.
- `go/vt/sqlparser`: `BenchmarkGenerateQueryStringBinds/{64B,1KB}`,
  `BenchmarkTokenizerScanString/{squote,dquote}/{16,64,256,4096}/{clean,escape,escape-run-{2,4,6,16}}`.
  The `escape-run-N` cells keep exactly N clean bytes between escapes, covering
  both sides of the scalar prefix and the fixed first-window cost that follows
  it.
- `go/mysql/collations/colldata`: `BenchmarkCollateSharedPrefix/utf8mb4_0900_ai_ci/{16,64,256,1024,short-16}`.
- `go/bytes2`: `BenchmarkByteSetIndex/{8,32,256,4096}/{clean,sparse,dense}`, `BenchmarkIndexAny2/{16,64,256,4096}`.
- Regression gates: `BenchmarkParse3`, `BenchmarkNormalizeVTGate`, `BenchmarkCollationCollate`.

### What real traffic looks like

The cells above are chosen sizes. To weight them, the one real workload in
the tree — `go/vt/sqlparser/testdata/lobsters.sql.gz`, 44,247 queries from a
Rails app's query log — was tokenized and measured:

| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| query length | 93 B | 151 B | 896 B | 4.7 KB |
| string literal length | 19 B | 237 B | 558 B | 862 B |
| literal bytes per query | 0 | 23 B | 683 B | 1.7 KB |
| identifier length | 7 | 10 | — | 26 |

89.3% of queries carry a literal, so `GenerateQuery` runs for ~9 in 10; 29.5%
carry a string literal (strings are 23.8% of bind variables, numbers the
rest), so the escaping loop runs for ~3 in 10; 8.7% of string literals are
≥256 B, which is where the escaping kernel's wins sit; 8.8% of literals
carry an escape, so `scanStringSlow` runs for ≤5%; identifiers are 29% of all
query bytes and every query has them. `ORDER BY` is 7.7% and `DISTINCT` 0.7%,
and vtgate only compares collations on scatter queries over text columns, so
the UCA path's frequency cannot be read from this corpus; TPCH (82% `ORDER
BY`, 68% `GROUP BY`) is the shape where it matters. There is no BIT literal.

What that does to the cells: 8/32/256 B bracket the p50–p99 literal well; the
4096 B cells, the 8 × 1 KB `GenerateQueryStringBinds` case and the 1 MB
`Parse3` query are all beyond anything in the corpus. They stand in for the
shapes a production fleet has and this log does not — bulk `INSERT`s of text
columns, ORM `IN` lists, JSON documents, bounded above by
`--grpc-max-message-size` (16 MB) and MySQL's `max_allowed_packet` — and are
reported as the tail, not as representative. Two benchmarks make the split
explicit: `BenchmarkGenerateQueryCorpus` replays the whole log through
`Parse2` → `Normalize` → `GenerateQuery`, the vtgate-to-vttablet path, and
`BenchmarkGenerateQueryTail` / `BenchmarkParseTail` run three named tail
shapes (a 100-row `INSERT` of 1 KB text, a 500-id `IN` list, a 64 KB JSON
literal).

### Procedure

```sh
# plain build → the scalar path; both opt-ins → the SIMD kernels
go test -run '^$' -bench "$BENCH_PATTERN" -count=10 -benchmem $PKGS | tee plain.txt
GOEXPERIMENT=simd go test -tags simd -run '^$' -bench "$BENCH_PATTERN" -count=10 -benchmem $PKGS | tee simd.txt
go run golang.org/x/perf/cmd/benchstat@v0.0.0-20260908200009-22c9c6c9d4da plain.txt simd.txt

# Kernel equivalence under the experiment: one package and one target per
# run, which is what the workflow's fuzz step loops over.
GOEXPERIMENT=simd go test -tags simd -run '^$' -fuzz '^FuzzByteSetIndex$' -fuzztime 30s ./go/bytes2/

# Whole-tree compile through each build path.
make build
make build-experimental-simd
```

Those two commands run one after the other, which is fine for a cell whose
delta is large but cannot resolve a small one: the machine is not the same
temperature for the second command as for the first, and whichever build runs
second absorbs the difference. Measured drift within a single 105 s series on
the reference M4 Max is +3.2%, so **a delta under ~3% from this procedure is
not yet a result.** Before a cell that close either fails the gate or counts
as a win, re-measure it interleaved: build both binaries with `go test -c`,
alternate them round by round, and alternate which goes first so position is
balanced as well.

```sh
go test -c -o plain.test $PKG                       # and the simd build likewise
for i in $(seq 10); do
  ./plain.test -test.run '^$' -test.bench "$B" -test.benchtime 2s -test.count 1 >> plain.txt
  ./simd.test  -test.run '^$' -test.bench "$B" -test.benchtime 2s -test.count 1 >> simd.txt
done
```

This is how the `GenerateQueryTail` cells in §Results were measured, after a
sequential run reported a +1.1% regression on a cell whose whole code path is
byte-identical to `main`.

**This procedure is run by hand, not in CI.** Benchmarks are not a CI check in
this repository and this branch does not make them one: `simd_experiment.yml`
runs the tests and the fuzz targets under the experiment, which is a
correctness check, and arewefastyet measures whole-system performance on a
labelled pull request. A benchstat matrix in CI would also be measuring a
shared runner, which §4's own drift finding says cannot resolve the deltas
these kernels turn on. `benchstat` at its default significance level
(p < 0.05) decides; `-count=10` per cell.

### Gate

Applied per fast path before the pull request leaves Draft, on the Go release
it merges on:

- **Scalar rewrite**: no statistically significant regression against the
  baseline in any cell on either architecture. A regressing cell is fixed
  with a length gate, or the rewrite is reverted and the old loop becomes the
  release path.
- **SIMD kernel**: judged against the scalar path it ships with (`plain.txt`
  vs `simd.txt`), because that is the path a build without the kernel runs.
  It stays if at least one representative cell (≥64 B for escaping and
  `scanString`; ≥64 B shared prefix for the collation) shows a significant
  improvement **and** no cell shows a significant regression, including
  `dense`, `8`/`16` and `short-16`. A kernel that passes on one architecture
  only keeps its `_simd.go` with the build tag narrowed to that architecture.
  A kernel that fails on both is deleted; its scalar path stays if that
  passed, and the negative result is recorded below.
- Whole-branch gates: `BenchmarkParse3`, `BenchmarkNormalizeVTGate`,
  `BenchmarkCollationCollate` and `BenchmarkGenerateQueryStringBinds` show no
  significant regression in either build mode.

If no kernel passes, the branch is reduced to the benchmarks and this document
and merges without waiting for the experiment to graduate.

## 5. Alternatives considered

The user-facing question was whether a third-party SIMD library could carry
the kernels until Go's own package graduates. The survey (repository trees
checked, not READMEs, 2026-09):

| Project | Stars | Activity | Architectures with assembly | Covers the three primitives? |
|---|---|---|---|---|
| `tphakala/simd` | 24 | active | amd64 + arm64 | No: numeric only (f64/f32/f16/i32/i16/i8/complex/crc); `i8` is quantized-ML kernels |
| `segmentio/asm` | 926 | dormant (last code commit 2022) | `mem/` amd64 only | No: `ContainsByte`, `IndexPair` (adjacent pair), `Copy`, `Mask`, `Blend` |
| `gofiber/utils/v2/simd` | 57 | active | amd64 only | Partially: `memchr_class` is a byte-set index; no equal-prefix; a web-framework utility module |
| `go-simd/*` (26 repos) | 0 each | 3 months old, single author | amd64, arm64, riscv64, loong64, ppc64le, s390x | Partially: `matchlen.MatchLen` is equal-prefix without the ASCII check |
| `mmcloughlin/avo` | ~3k | active; used by the Go standard library | amd64 codegen only | Not a library; the generator most Go SIMD projects write their kernels with |
| `bytedance/sonic`, `minio/simdjson-go`, `minio/sha256-simd`, `coregx/coregex` | 0.3k–9.6k | active | various | No: SIMD is internal to JSON, hashing or regex |

No maintained, multi-architecture library exposes "first index of any of N
bytes" or "equal-ASCII prefix length". Every mature Go SIMD project instead
commits small `.s` kernels (generated with avo on amd64, hand-written NEON on
arm64) dispatched on `x/sys/cpu` with a pure-Go fallback behind `noasm`, which
is also what `go/vt/vthash/highway` does. That remains the recorded fallback
vehicle if the `simd` experiment stalls for more than two Go releases; it was
not chosen now because kernels written against the portable `simd` API are
one readable Go source for both architectures and need no assembly review.

## 6. Adoption policy

Normative for SIMD code in this repository.

1. **Portable `simd` first.** `simd/archsimd` only where the portable API has
   no equivalent (today: permutes for table lookups), in files tagged to the
   architecture that has the instruction.
2. **Two tagged files per SIMD fast path.** `*_noasm.go`
   (`//go:build !simd || !goexperiment.simd || !(amd64 || arm64)`) and
   `*_simd.go` (`//go:build simd && goexperiment.simd && (amd64 || arm64)`)
   define the same entry point. The scalar fallback stays untagged, either as
   a helper or in the caller's existing loop.
3. **Kernel shape.** The vector body is a plain function; exported methods
   are thin wrappers (§1 compiler limitation). Set members a kernel compares
   against are pre-broadcast into byte rows at construction and loaded with
   `LoadUint8s`; the cost that rule removes is eight `MOVBU`+`VMOV`+`VDUP`
   sequences per call, so a kernel that needs one literal constant (the UCA
   kernel's `0x80` mask) may broadcast it in-function. Neither is held in a
   package-level `var`. Inputs shorter than the threshold take the scalar
   path, and inputs shorter than one vector always do. The tail is read as
   an overlapping full block ending at the last byte, not with
   `LoadUint8sPart` (§1: it is a call that spills every live vector).
4. **Equivalence is fuzzed.** Every kernel has a table test at the 16/32/64
   byte block boundaries and a fuzz target against the scalar reference; the
   fuzz targets run in the `simd_experiment` workflow under the experiment.
5. **Kernels must pass the gate in §4 to exist.** A kernel that is not
   measurably faster than its fallback is deleted, not kept for later.
6. **No SIMD type in an exported signature; no new `unsafe`.**
7. **Manual opt-in while experimental.** `make build` remains the scalar
   release path. The experiment workflow calls `build-experimental-simd`,
   which supplies both controls and remains the explicit local shortcut.
   **Suspended while this pull request is a Draft** -- the Makefile currently
   aliases `build` to the experimental path so arewefastyet measures the
   kernels. See the Status section; it is reverted before this leaves Draft.

### Graduation

When a Go release ships `simd` without `GOEXPERIMENT` and Vitess `main` has
moved `go.mod` to it: rebase; replace
`simd && goexperiment.simd && (amd64 || arm64)` with
`!noasm && (amd64 || arm64)` and the `_noasm.go` constraint with
`noasm || !(amd64 || arm64)`, so the kernels are on by default with a
`-tags noasm` opt-out, matching `highway`; remove the
`build-experimental-simd` target and drop both experiment opt-ins from the
workflow; have it compare `-tags noasm` against the default build; re-run the
gate on the final release; refresh the results below.

## 7. Roadmap

Each item is a follow-up with its own gating benchmark, in the order the
ranking in §3 suggests.

- `NextWeightBlock64` weight generation (GROUP BY / DISTINCT / HashJoin
  hashing): needs a 128-entry 16-bit table lookup, so `archsimd`
  `Uint16x32.Permute` on AVX-512BW, amd64 only.
- 8-bit collations (`latin1_*`): `Collate`, `WeightString`, `ToLower`/`ToUpper`
  are per-byte 256-entry table walks; ASCII range compares cover most inputs,
  full tables need `archsimd` permutes.
- `utf8.Valid` via a simdutf-style validator, if a profile shows charset
  coercion on a hot path.
- `HEX`/`UNHEX`/`TO_BASE64`/`FROM_BASE64` in evalengine.

## 8. Risks

- **Experiment API churn.** The kernels are small and unexported; a rename in
  Go 1.28 is a mechanical fix. The workflow runs on every Go patch and
  pre-release to surface it early.
- **Whole-tree compile under the experiment.** The workflow's
  `make build-experimental-simd` expands to
  `GOEXPERIMENT=simd go build -tags simd ./go/...`. If an unrelated dependency
  fails, it is narrowed to the SIMD packages and the reason recorded here.
- **Emulated path.** On a CPU where `simd.Emulated()` is true the kernels
  route to the scalar path at run time.
- **Release default.** `make build`, `make install`, Docker images and the
  ordinary CI workflows remain scalar; only the explicit experimental target
  and workflow supply the SIMD controls. **Suspended while this pull request
  is a Draft** by the alias in the Status section, which is why the
  whole-tree compile above currently runs in 22 workflows rather than one.

## Results

Baseline commit: the first commit of this branch ("Add benchmarks and CI
workflow for the SIMD hot-path candidates"), whose test binaries were kept
and run against HEAD. arm64: Apple M4 Max, go1.27.1, `-count=10
-benchtime=200ms`, `benchstat` default p<0.05. **amd64: pending** — the
gate needs a manual run on an amd64 host, per the procedure above; the
verdicts below are arm64 only and the
graduation gate re-runs on both.

Columns: *today* = baseline commit; *scalar* = HEAD plain build (the release
path); *simd* = HEAD with `GOEXPERIMENT=simd` and `-tags simd`.
Percentages are vs *today*.

### Bind-variable escaping (`sqltypes`, `GenerateQuery`)

| cell | today | scalar | simd |
|---|---|---|---|
| `EncodeSQL/Bytes2/8/clean` | 40.3n | 6.75n (−83%) | 7.20n (−82%) |
| `EncodeSQL/Bytes2/32/sparse` | 85.5n | 16.5n (−81%) | 18.0n (−79%) |
| `EncodeSQL/Bytes2/32/dense` | 63.2n | 16.7n (−74%) | 18.0n (−72%) |
| `EncodeSQL/Bytes2/32/clean` | 69.3n | 12.7n (−82%) | 10.8n (−84%) |
| `EncodeSQL/Bytes2/256/clean` | 462n | 76.0n (−84%) | 36.5n (−92%) |
| `EncodeSQL/Bytes2/256/dense` | 478n | 98.7n (−79%) | 102n (−79%) |
| `EncodeSQL/Bytes2/4096/clean` | 7.14µ | 1.05µ (−85%) | 468n (−93%) |
| `EncodeSQL/Bytes2/4096/sparse` | 7.18µ | 1.32µ (−82%) | 558n (−92%) |
| `EncodeSQL/Bytes2/4096/dense` | 7.34µ | 1.62µ (−78%) | 1.62µ (−78%) |
| `EncodeSQL/StringBuilder/8/clean` | 29.8n | 19.8n (−34%) | 20.5n (−31%) |
| `EncodeSQL/StringBuilder/32/dense` | 105n | 32.8n (−69%) | 33.8n (−68%) |
| `EncodeSQL/StringBuilder/256/clean` | 1.13µ | 107n (−91%) | 67.7n (−94%) |
| `EncodeSQL/StringBuilder/4096/clean` | 19.4µ | 1.27µ (−93%) | 801n (−96%) |
| `EncodeSQL` geomean | 389n | 80.7n (−79%) | 66.2n (−83%) |
| `GenerateQueryStringBinds/64B` | 1.63µ | 490n (−70%) | 461n (−72%) |
| `GenerateQueryStringBinds/1024B` | 33.1µ | 6.39µ (−81%) | 5.74µ (−83%) |

- **Scalar rewrite: passes.** Every cell improves, 3.5–15×. This is the
  release-build result.
- **SIMD `ByteSet.Index` kernel: conditional on arm64.** Against the scalar
  path it ships with, it is 1.9–2.4× faster on clean and sparse inputs of
  32 bytes and up, at parity on dense inputs of 256 bytes and up, and
  **slower by 0.4–1.5 ns on the 8-byte cells and the 32-byte sparse/dense
  cells** (+3% to +14% on the `Bytes2` path, +2% to +4% on the
  `StringBuilder` path vttablet uses; all p<0.05). The disassembly and a
  same-binary probe with the threshold pinned trace this to two costs that
  are the experiment's (§1): the `Index` wrapper is over the inlining
  budget, so 8-byte inputs pay a call the plain build does not, and one
  16-byte NEON block with a stored-mask scan costs about what 16 table
  lookups cost, so a scan that hits within its first block gains nothing
  for its setup. Two further costs that were ours are fixed: eight
  per-call broadcasts (now vector loads from pre-broadcast rows) and the
  spilled `LoadUint8sPart` tail (now an overlapping block); together they
  took the 8-byte regression from +16% to +7% and `4096/sparse` from 594 to
  558 ns. Under the gate as written the small cells are still a regression
  on arm64; the amd64 numbers (32/64-byte lanes with a cheaper block) decide
  whether the kernel is narrowed to amd64, its threshold raised to 64, or
  it is deleted.

### utf8mb4_0900 collation fast path (`colldata`, `internal/uca`)

| cell | today | scalar | simd |
|---|---|---|---|
| `CollateSharedPrefix/16` | 28.6n | 28.7n (~) | 28.4n (~) |
| `CollateSharedPrefix/short-16` | 27.3n | 27.4n (~) | 26.8n (−1.7%) |
| `CollateSharedPrefix/64` | 36.4n | 36.2n (~) | 34.3n (−5.8%) |
| `CollateSharedPrefix/256` | 70.4n | 71.2n (+1.1%) | 53.0n (−24.7%) |
| `CollateSharedPrefix/1024` | 222n | 221n (~) | 136n (−38.9%) |
| `CollationCollate/utf8mb4_0900_ai_ci` | 11.4µ | 10.8µ (−5.1%) | 10.9µ (−4.6%) |
| `CollationCollate/utf8mb4_0900_bin` | 31.9n | 34.4n (+7.9%) | 32.3n (+1.1%) |

- The scalar column is today's loop (the noasm `equalASCIIPrefix` is a
  constant 0); its ±1–8% deltas are run-to-run noise on code that did not
  change (`utf8mb4_0900_bin` is `bytes.Compare`).
- **SIMD `equalASCIIPrefix` kernel: passes on arm64.** No cell regresses
  (16 and short-16 at parity once the wrapper inlines), −5.8% at 64 B rising
  to −38.9% at 1 KB shared prefix. The threshold is 32 bytes; at 16 the
  16-byte cells lost 11–16%.

### Tokenizer string scan (`sqlparser`)

| cell | today | scalar | simd |
|---|---|---|---|
| `TokenizerScanString/squote/16/clean` | 28.5n | 10.3n (−63.9%) | 10.3n |
| `TokenizerScanString/squote/64/clean` | 106n | 10.5n (−90.2%) | 10.6n |
| `TokenizerScanString/squote/256/clean` | 417n | 12.8n (−96.9%) | 12.8n |
| `TokenizerScanString/squote/4096/clean` | 6.62µ | 102n (−98.5%) | 103n |
| `TokenizerScanString/squote/4096/escape` | 7.30µ | 4.09µ (−44.0%) | 4.18µ |
| `TokenizerScanString` geomean | 1.74µ | 356n (−78.6%) | 357n |
| `Parse3/normal` (1 MB query) | 1.50ms | 23.4µ (−98.4%) | 23.3µ |
| `Parse3/escaped` | 2.19ms | 2.20ms (~) | 2.25ms (~) |
| `NormalizeVTGate` (lobsters corpus) | 31.2ms | 31.6ms (~) | 30.6ms (−1.9%) |

- **Scalar rewrite (two `bytes.IndexByte` scans): passes.** Clean literals
  are 3–65× faster. A review sweep over 2/4/6/16-byte runs found that calling
  `IndexAny2` after every escape regressed the 4-byte cells by 43–62% and the
  6-byte cells by 13–22%. `scanStringSlow` therefore keeps the old byte loop;
  only the initial hunt uses `IndexAny2`, after an 8-byte scalar prefix. On
  arm64 (`-count=6`), the 4/6-byte cells from 16 B through 4 KB are now at
  parity or within +5% of the reference, while clean literals retain 66–99%
  wins and the single-escape cells retain 11–46% wins. The corpus benchmark is
  unchanged, as §3 predicts for a 1.5% share.
- **SIMD `IndexAny2` kernel: deleted.** Measured before removal (count=4):
  2–3× slower than the `IndexByte` fallback on every clean cell of 64 bytes
  or more (4096 B: 101 ns vs 314 ns; `IndexAny2/4096`: 90 ns vs 331 ns),
  ahead only at 16 bytes. The cause is architecture-independent (§1: no
  movemask in the portable API), so it was not held for the amd64 run. The
  *simd* column above is therefore the same code as *scalar*.

### Corpus-weighted and tail results (release build, vs `main`)

Measured per branch, so the escaping rewrite this pull request carries can
be told apart from the bind-substitution allocation work that moved to the
follow-up (lazy builder sizing, the BIT table, the identifier/digit class
tables). Plain builds against a `main` build of the same benchmark file,
arm64, `-count=8`, `main` at `e66114ee01`:

| benchmark | `main` | this branch | Δ | + follow-up | Δ vs `main` |
|---|---|---|---|---|---|
| `GenerateQueryCorpus` (all 44k lobsters queries, one pass) | 9.819 ms | 5.866 ms | **−40.3%** | 5.638 ms | **−42.6%** |
| `GenerateQueryTail/insert-100x1KB` | 408.8 µs | 100.8 µs | **−75.4%** | 64.55 µs | **−84.2%** |
| `GenerateQueryTail/in-500` | 5.715 µs | 5.779 µs | ~ (see below) | 3.687 µs | −35.5% |
| `GenerateQueryTail/json-64KB` | 280.2 µs | 71.57 µs | **−74.5%** | 45.28 µs | **−83.8%** |
| `ParseTail/insert-100x1KB` | 269.5 µs | 264.5 µs | −1.9% | 265.4 µs | −1.6% |
| `ParseTail/in-500` | 71.74 µs | 71.33 µs | ~ | 66.82 µs | −6.9% |
| `ParseTail/json-64KB` | 144.5 µs | 146.1 µs | +1.1% | 145.9 µs | +1.0% |
| `NormalizeVTGate` | 31.13 ms | 30.40 ms | −2.4% | 29.79 ms | −4.3% |

Allocations move almost entirely with the follow-up: `GenerateQueryCorpus`
88.4k → 88.3k → 86.6k, and each `GenerateQueryTail` cell 12 or 20 → the same
→ 3.

**The escaping rewrite is what produces the corpus win, not the sizing.**
This branch is −40.3 of the −42.6; the follow-up adds the remaining 2.3
points of time and nearly all of the allocation drop. The two are close to
orthogonal, which is the argument for reviewing them apart: one is a
byte-at-a-time loop replaced by a run copy, the other is a `strings.Builder`
that stops doubling.

Two rows do not say what an earlier revision of this document claimed, and
both are worth stating plainly:

- **`ParseTail` is unchanged, not −46%/−34%.** Those figures date from
  `08387fcb60`, when `scanStringSlow` also hunted with `IndexAny2`. That was
  reverted in `2e35ac3f13` because calling it after every escape regressed
  the short-run cells, and the table was never re-measured. All three
  `ParseTail` shapes carry escapes, so they route to `scanStringSlow`, which
  is now `main`'s byte loop; `scanString`'s −79% geomean is on literals that
  close without one. The `in-500` improvement is the follow-up's class
  tables — 500 integers is `scanMantissa`'s loop — which is why it appears
  only in the last column.
- **`GenerateQueryTail/in-500` is unchanged, and the `-count=8` run saying
  otherwise was measuring the machine.** That run reported +1.1% (p=0.004),
  which §4's scalar gate does not allow, so it was chased down. Re-measured
  from two prebuilt test binaries alternated round by round, it is `~`:
  p=0.739 with `main` first, p=0.631 with the branch first, p=0.551 pooled
  over both orders (n=20). The sequential run's own numbers show why — the
  `main` series drifted 5475 → 5652 ns within its 105 s, **+3.2%, about three
  times the effect it was reporting** — and every function `in-500` executes
  (`GenerateQuery`, `Append`, `FetchBindVar`, `EncodeValue`,
  `Value.EncodeSQLStringBuilder`, `ForEachValue`) is byte-identical to
  `main`, with identical inlining decisions and no read of `sqlEscapeSet` on
  the unquoted arm. There was nothing there to regress.

With `GOEXPERIMENT=simd` and `-tags simd` on top of this branch, scalar vs
simd. The `GenerateQueryTail` cells are interleaved and position-balanced
(n=10) after the `in-500` finding above: `insert-100x1KB` **−5.1%**
(p=0.000), `in-500` **`~`** (p=0.853 — the `-count=8` run's +5.6% was the
same artifact), **`json-64KB` +18.7%** (p=0.000), tail geomean **+4.1%**. The
`GenerateQueryCorpus` (−4.9%, p=0.021) and `ParseTail` (within ±2.3%) cells
are from the sequential `-count=8` run and are not re-measured; both are
small enough to sit inside the drift band, so treat them as unresolved rather
than as results.

Interleaving moved the verdict further against the kernel, not nearer it: it
removed a regression that was not real and made the one that is real half
again larger. The JSON document has an escape every ~20 bytes, so it is the
hit-dense shape where `ByteSet.Index` pays its per-call setup for one block's
work; a 64 KB JSON column is a shape fleets have. Under §4 that is a
significant regression on a representative cell and a tail geomean that is
net slower, which counts against the kernel on arm64 alongside the 8–32 B
cells.

So the honest headline for vttablet bind substitution on real OLTP traffic
is **−40%** from this branch, −43% with the follow-up on top; not the −90%
of the 8 × 1 KB cell, which is the tail, where the win is −75% here and −84%
stacked. The experiment then takes some of it back on escape-dense
documents. The escaping SIMD kernel's wins sit at ≥256 B, which is 8.7% of
the corpus's literals, and its regressions at 8–32 B, where the median
literal lives; corpus-weighted it is neutral to slightly negative on OLTP
traffic, which is a further reason its verdict waits on amd64. Nothing left
on this branch touches the p50 query: the escaping rewrite needs a literal
≥256 B to pay, and the identifier/digit tables that did touch every query
went to the follow-up.

### Whole vttablet query (release build, vs `main`)

The per-path numbers above say how much faster each loop got; this says how
much of a vttablet query those loops were. `tabletserver.BenchmarkExecuteVarBinary`
runs a whole `tsv.Execute` — plan cache, bind substitution, `fakesqldb`
standing in for MySQL — on a 1 MB query with ten 100 KB `VARBINARY` binds
that carry an escape every eleven bytes. arm64, `-count=8`, measured per
branch the same way as the table above:

| | `main` | this branch | + follow-up | this branch + experimental SIMD |
|---|---|---|---|---|
| `ExecuteVarBinary` sec/op | 6.460 ms | **3.042 ms (−52.9%)** | 2.839 ms (−56.1%) | 3.155 ms (+3.7% vs scalar) |
| B/op | 9.274 MiB | 9.194 MiB (−0.9%) | **4.577 MiB (−50.6%)** | 9.172 MiB (~) |
| allocs/op | 96 | 94 (−2.1%) | **62 (−35.4%)** | 94 (~) |

The split here is by column, not by fraction: **this branch buys the CPU and
the follow-up buys the allocations**, with almost no overlap. The escaping
rewrite halves the time while leaving B/op where it was, because it copies
clean runs instead of bytes but still lets the builder grow; the sizing then
halves the memory and 34 of the 94 allocations while adding only 3 more
points of time. Either one alone is worth having and they are easier to
judge apart.

Bind substitution was more than half of everything vttablet itself did for
that query. The experiment is +3.7% over the plain build on this shape, the
escape-dense pattern again, and moves neither B/op nor allocs. For the
corpus median (a 19 B literal) the same path saves ~90 ns against a
per-query vttablet cost in the tens of microseconds — under 1%, below
dashboard noise.

The matching vtgate instrument, `vtgate.BenchmarkWithNormalizer`, fails on
`main` as well as on this branch (`bench_test.go` is byte-identical on
both), so no whole-vtgate number exists; `NormalizeVTGate` over the corpus
is the closest proxy and is unchanged.

### Whole-branch gates

`NormalizeVTGate`, `Parse3/escaped`, `CollationCollate/*` show no significant
regression in either build mode. `BenchmarkGenerateQueryStringBinds` improves
70–83%. No cell regresses significantly on the scalar path once the cells
near the drift band are interleaved. Under the experiment, `json-64KB`
regresses 18.7% and the tail geomean is +4.1%.

### Summary

| fast path | release-build win (scalar) | SIMD kernel, arm64 verdict |
|---|---|---|
| escaping | `EncodeSQL` −79% geomean; bind substitution −40% on the corpus, −75% on the tail; whole vttablet `Execute` −53% on a large-bind query, at unchanged B/op | conditional: ~2× at ≥32 B clean; +3–14% at 8 B and 32 B sparse/dense, +14% on `json-64KB`, +3.7% on the 1 MB `Execute`, sqlparser geomean +0.6% |
| UCA prefix | none (unchanged) | **pass**: −4% at 64 B to −40% at 1 KB, no regression |
| tokenizer | `scanString` −79% geomean on literals that close without an escape; escaped literals stay on the byte loop, so `ParseTail` is unchanged | **deleted**: stdlib `IndexByte` is 2–3× faster |

Two of the three release-build wins need no experiment at all, which is
the more useful finding: the byte-at-a-time loops were the cost, and the
portable `simd` package is the right tool only where no stdlib primitive
already vectorizes the scan.

What an operator rolling this out would see, from the numbers above: on
OLTP traffic, nothing attributable — the savings are sub-1% of a query's
vttablet CPU and vtgate parse time is unchanged at the corpus level. On
tablets serving bulk writes, VReplication targets with text/blob/JSON
columns, or JSON-document workloads, a measurable CPU and allocation drop —
hundreds of microseconds and a dozen-plus allocations per statement — that
should show on `process_cpu_seconds_total` and `go_memstats_alloc_bytes_total`.
On arm64 the experiment is a wash on OLTP and a regression on the JSON
shape; the recommendation for arm64 is to ship the scalar rewrites and leave
both SIMD opt-ins off until the amd64 run says otherwise.

## Status

As of this branch (2026-09-27):

- **TEMPORARY: `make build` is aliased to the experimental path.** The
  Makefile sets `GOEXPERIMENT=simd` and appends the `simd` tag to the `build`
  target, so every consumer of `make build` -- arewefastyet, the 22 workflows
  that call it, local builds -- gets the kernels rather than the scalar
  fallbacks. This is here only to get arewefastyet numbers on the kernels
  while the pull request is a Draft, and it suspends §6 rule 7 while it
  stands. **Revert it before this leaves Draft**: drop the two `build:`
  override lines and their comment block from the Makefile, and the suspension
  notes in the Summary, §2, §6 rule 7, §8 and the two Status bullets here, all
  of which name this alias.
- **Draft POC with explicit opt-in** _(once the alias above is reverted)_.
  `make build` is the scalar release path, while `build-experimental-simd`
  and the workflow supply both opt-ins. Release, install, Docker and ordinary
  CI consumers cannot then inherit the experiment through the default target.
- **This document is the RFC.** No separate `Type: RFC` issue; discussion
  happens on the pull request.
- **Scoped to the three ranked paths.** The pure-Go allocation work that grew
  alongside the prototypes -- lazy builder sizing in `ParsedQuery.Append`, the
  BIT literal table, the identifier/digit class tables -- is a follow-up pull
  request. It is a good result on its own and it is not SIMD, so crediting it
  to the experiment overstated what the kernels do. Both Results tables are
  measured per branch, so each pull request's own numbers are visible: the
  escaping rewrite here carries the CPU win, the follow-up the allocations.
- **Kernel verdicts (arm64):** UCA prefix skip passes; escaping
  `ByteSet.Index` is conditional and the arm64 evidence is against it
  (8–32 B cells, `json-64KB`, the 1 MB `Execute`); tokenizer `IndexAny2` was
  deleted. **amd64 is unmeasured** — it needs a manual §4 run on an amd64
  host, since the `simd_experiment` workflow tests the kernels rather than
  benchmarking them — and decides whether the escaping kernel is kept,
  narrowed to amd64, or dropped.
- **Every number here is from one Apple M4 Max.** Server arm64 (Neoverse)
  and amd64 may move the ±5% experiment deltas; the scalar wins are large
  enough not to depend on it. No live cluster profile was taken; the
  whole-vttablet figure is the in-process benchmark.
- **A delta under ~3% from a sequential run is not a result on this machine.**
  Measured drift within a single 105 s series is +3.2%, so the `-count=8`
  runs behind most of the tables cannot resolve anything smaller than that.
  One cell was chased on the strength of a +1.1% "regression" that turned out
  to be the machine warming up between the two halves of the run; the same
  run understated a real +18.7% regression as +14.4%. The cells that matter
  either way have been re-measured interleaved and say so; the rest are
  large enough not to care.
- **Filed along the way:** vitessio/vitess#21242, `BufEncodeStringSQL`
  rewriting invalid UTF-8 as `U+FFFD` (pre-existing, found comparing the two
  encoders). Not yet filed: `vtgate.BenchmarkWithNormalizer` failing on
  `main`.
- **Open:** re-measure `GenerateQueryCorpus` and the `ParseTail` cells
  interleaved, since the experiment deltas reported for them sit inside the
  drift band and are currently unresolved rather than known; run §4 by hand on
  an amd64 host for the per-kernel numbers, and label the pull request
  `Benchmark me` for arewefastyet's whole-system view, which is a different
  measurement and not a substitute; collect the amd64 ranking with the §3
  profiling procedure; re-run the §4 gate on the graduating Go release and
  refresh the Results tables then.
