# F07: backup compressor (pargzip vs pgzip), plus CRC32 and xtrabackup CopyN

## Summary / verdict

**Do it: fork pargzip into `go/vt/mysqlctl/internal/pargzip` and pool its per-chunk state.** Keep `pargzip` as the default.
The fork's output is **byte-for-byte identical** to upstream `planetscale/pargzip`. The measured cost is about the same as
pgzip. It needs no manifest change, no flag default change, and no deprecation staging, and it has no compatibility risk in
either direction (N-1 and N+1).

Switching the default to pgzip would also work for compatibility. The history (#5613/#7037/#8174) and the small-file and
peak-memory numbers make that the riskier and less useful change. The pooled fork gets the CPU win without it.

Side items:
- xtrabackup striped `io.CopyN`: real, tiny, and cheap to fix. Prototyped with a test that fails on main.
- The unused backup-side CRC32: real but under 1% of backup CPU. Skip, or fold it into another change.

## 1. Is the finding real?

Yes. `go/vt/mysqlctl/compression.go:51` sets `CompressionEngineName = "pargzip"`. At `:323-328` pargzip gets
`ChunkSize=backupCompressBlockSize` (250000, backup.go:90), `Parallel=backupCompressBlocks` (2) and `CompressionLevel=1`.

Upstream `planetscale/pargzip@v0.0.0-20201116224723` is an external module, not vendored. It dates from 2020 and is a copy of
golang.org/x/build/pargzip. For **every 250 KB chunk** it:
- copies the chunk with `string(p)`,
- allocates a new `bytes.Buffer`, growing from zero,
- calls `gzip.NewWriterLevel`, which allocates a new compressor of about 1.1 MB.

The measured result is 6.0-6.8 bytes of garbage per input byte.

## 2. Measurements

The machine is shared, with load average 11-21 on 4 vCPUs during the runs, so wall times are very noisy. **CPU time
(getrusage user+sys) and alloc bytes are the reliable metrics.**

Setup:
- GOMAXPROCS=4 and Vitess defaults: block 250000, 2 blocks, level 1.
- 32 KB writes via `io.Copy` from a reader without WriteTo, as `backupPipe` does, into a 2 MB bufio writer as in the builtin engine.
- Runs interleaved across engines.
- Bench source: `scratchpad/F07/benchsrc/bench/{main.go,pooled.go}`.

Datasets:
- **innodb**: a real InnoDB `.ibd` (the vitess-resources enwiki `text.ibd`, the dataset the existing
  `compression_benchmark_test.go` uses). I took 64-128 MB from offset 64 MB of a partial download.
- **mixed**: 16 KB pages, 60% real InnoDB pages, 25% random (incompressible) pages, 15% mostly-empty pages.
- **text**: about 52 MB of Vitess Go source.

### CPU seconds per GB compressed (run2 is 128 MB × 4 reps; run1 and run4 are 64 MB × 3-4 reps)

| dataset | pargzip (upstream) | pgzip | internal pooled pargzip | pooled vs upstream |
|---|---|---|---|---|
| innodb run1 | 10.28 | 7.35 | 7.65 | −26% |
| innodb run2 | 12.30 | 6.70 | 7.10 | −42% |
| innodb run4 | 9.48 | 6.66 | 7.08 | −25% |
| mixed run1 | 8.94 | 5.78 | 6.04 | −32% |
| mixed run2 | 8.76 | 5.72 | 5.82 | −34% |
| mixed run4 | 9.99 | 5.59 | 5.73 | −43% |
| text run1 | 7.91 | 4.98 | 5.03 | −36% |
| text run4 | 7.42 | 4.74 | 5.34 | −28% |

Upstream pargzip uses **1.4-1.8× the CPU of pgzip**. The pooled fork is within about 2-8% of pgzip.

### Allocation, GC and heap (64 MB input, one compressor per 64 MB "file")

| | pargzip | pgzip | pooled |
|---|---|---|---|
| alloc per input byte | 6.0-6.8 B/B (428 MB per 64 MB) | 0.14 B/B | 0.13-0.17 B/B |
| GCs per run | 1-3 | 0 | 0 |
| peak heap objects above baseline | 200-300 MB (GC pacing on garbage) | ~10 MB | ~9-11 MB |

### Compressed ratio (compressed/original)

| dataset | pargzip | pgzip |
|---|---|---|
| innodb | 0.3444 | 0.3425 |
| mixed | 0.4926 | 0.4907 |
| text | 0.2218 | 0.2195 |

pgzip is 0.4-1% smaller because each block uses the 32 KB tail of the previous block as its dictionary. pargzip writes
independent gzip members.

### Byte identity

Upstream pargzip and the internal pooled fork produce byte-identical output on all 3 datasets: 23112258, 33055311 and
11662531 bytes.

### Go benchmark (16 MB mixed test data, `BenchmarkWriter`, 6 interleaved runs)

```
         │    old.txt     │               new.txt               │
         │      B/op      │     B/op      vs base               │
Writer-4   119.088Mi ± 0%   2.245Mi ± 9%  -98.11% (p=0.002 n=6)
         │  allocs/op  │ allocs/op   vs base               │
Writer-4   2247.0 ± 0%   269.5 ± 5%  -88.01% (p=0.002 n=6)
sec/op     85.38m ± 10%   62.48m ± 53%  ~ (p=0.132 n=6, box load ~20)
```

### Small files (a new compressor per file; CPU in cpu-s/GB)

| file size | pargzip | pgzip | pooled |
|---|---|---|---|
| 64 KB | 26.3 cpu-s/GB, 51.8 B/B | 22.8 cpu-s/GB, 61.5 B/B, peak heap 750 MB | 16.0 cpu-s/GB, 40.5 B/B |
| 1 MB | 10.5 | 10.5 | 10.1 (7.65 kp variant) |

- In the 64 KB case, about 32 B/B of every column is the harness's 2 MB bufio writer per file. The builtin engine also
  allocates one per file (`builtinBackupStorageWriteBufferSize`); see the side notes.
- pgzip's per-Writer setup (flate writers and dst pools that are per Writer, not global) costs about 1.9 MB per file. For
  small files it is no better than pargzip and has a higher peak heap.
- The pooled fork uses process-wide pools, so small files benefit too.

### Restore

Decompression of pargzip and pgzip output with the pgzip reader (the restore path) and with stdlib gunzip: throughputs are in
the same range and the noise dominates. I saw no systematic difference. The stdlib reader handles both (multistream).

## 3. Settings equivalence (pgzip vs pargzip)

- **Levels.** In Go 1.26+ (the repo uses 1.27.1), stdlib `compress/flate` contains klauspost-derived level encoders
  (`level1.go`…`level6.go`, "Copyright 2026 The Go Authors"). The pooled pargzip with stdlib gzip and a variant with
  `klauspost/compress/gzip` produced **identical output sizes**. So with this toolchain, level N means the same thing in
  both engines. With older Go toolchains, stdlib level 1 was the old deflatefast and ratio and speed differed.
- **Block size.** pgzip `SetConcurrency(250000, 2)` and pargzip `ChunkSize=250000, Parallel=2` give the same parallelism
  (2 blocks in flight).
- **Format.**
  - pgzip writes one gzip member with dictionary carry-over and a sync flush per block.
  - pargzip writes one gzip member per chunk. A write that bypasses its bufio buffer (a write of at least ChunkSize while the
    buffer is empty) produces a short final chunk, so chunk boundaries depend on write sizes. The fork preserves this.

## 4. Compatibility and history

- **Manifest.** The manifest records `CompressionEngine` (builtinbackupengine.go:153, :1219; xtrabackupengine.go:93, :361).
  Restore picks the decompressor **by manifest engine name**, not by extension:
  - builtinbackupengine.go:1569-1590 and xtrabackupengine.go:727-745; an empty name falls back to pgzip.
  - `"pargzip"` is mapped to the pgzip reader (compression.go:249; builtin:1347; xtrabackup:602).
  - Both engines use the `.gz` extension (engineExtensions).
- **pgzip-created backups on vN-1.** They restore fine. `pgzip` has been a valid manifest engine since v15, when
  `--compression-engine-name` was added (changelog/15.0/15.0.0/summary.md:190-228), and before that decompression was always
  pgzip.
- **History** (the repo is a shallow clone; I used the changelog and the GitHub API):
  - v9.0.0 #7037 (enisoc) switched compression from pgzip to pargzip to fix #5613. #5613 reported vttablet OOM and RSS
    bloat during xtrabackup backups, attributed to pgzip's short-lived allocations and per-stripe buffers.
  - v9.0.2 #8174 (Slack) reverted that on release-9.0 only: "Backups are taking twice as long with this change". The plan
    was to make the compressor configurable (#7978, which became the v15 engines). main kept pargzip.
  - The measured 1.4-1.8× CPU is consistent with #8174.
  - Ironically, upstream pargzip allocates about 45× more garbage per byte than pgzip at these settings. The pooled fork
    addresses both complaints.
- **Default flag change.** Output stays readable by N-1, so under CLAUDE.md it is not strictly breaking. It still changes
  memory behaviour, especially for small files and xtrabackup stripes (the #5613 concern), and it would need release notes.
  The pooled fork needs **none of that**: the default name, the manifest value and the bytes are all identical.

## 5. Prototype (uncommitted in the worktree; patch at findings/F07-backup-compressor.patch)

1. `go/vt/mysqlctl/internal/pargzip/pargzip.go` (~290 LOC, most of it upstream code)
   - Forked from upstream with the BSD notice kept, following the go/cache/theine/singleflight.go precedent.
   - Process-wide `sync.Pool`s:
     - one `*gzip.Writer` pool per level (reused with `Reset`, then `Reset(io.Discard)` before pooling to drop the reference);
     - one `*bytes.Buffer` pool for output, returned to the pool after `w.w.Write` (io.Writer must not retain p);
     - one `*[]byte` pool for input chunks, replacing `string(p)`, returned after compression.
   - The API is unchanged: NewWriter, ChunkSize, Parallel, CompressionLevel, Write, Close.
2. `compression.go`: the import switches to the internal package. `go.mod` and `go.sum` drop `planetscale/pargzip`.
   `go mod tidy -diff` is clean.
3. `xtrabackupengine.go`: a `blockCopier` reuses one copy buffer (up to 32 KB) and one `io.LimitedReader` across blocks.
   It is used in `copyToStripes` (backup) and `stripeReader` (restore).
   - Before, `io.CopyN` allocated 32 KB plus a LimitedReader per 100 KB block (0.32 B/B), because `*LimitedReader` has no
     WriteTo and the compressors have no ReadFrom.
   - The comment at :516 claiming that bufio lets CopyN use WriteTo was wrong for the striped path; I fixed it.
   - This only applies when `--xtrabackup-stripes > 1` (the default is unstriped, which uses `io.Copy` with bufio WriteTo).

## 6. Tests

- New `internal/pargzip/pargzip_test.go`:
  - Byte equality against a non-pooled reference (fresh gzip.Writer per chunk behind the same bufio chunking) across 5
    levels × 4 chunk sizes × 3 write sizes.
  - A round trip with 8 concurrent writers sharing the pools, read back with both the pgzip and stdlib readers.
  - Empty input (never written, or written with an empty slice), double Close, invalid level, and propagation of an
    underlying write error.
  - A benchmark.
  - Passes, including `-race` on a subset.
- New `TestStripeCopyReusesBuffer` (xtrabackupengine_test.go): round trip through writers and readers that have no
  ReadFrom/WriteTo, and an allocation bound.
  - **It fails on main**: 520 allocs vs a bound of 25, for 256 blocks.
  - It passes with the fix.
- Full `go test ./go/vt/mysqlctl/` passes. It needs VT_MYSQL_ROOT pointing at a fake `mysqld --version` because no mysqld is
  installed. Existing compression and stripe tests pass.
- Not covered: the byte identity against upstream is not a test in the repo, because the dependency is removed. I checked it
  in the scratch bench on 3 datasets of 52-64 MB.

## 7. CRC32 on the backup-side source reader

- builtinbackupengine.go:1053 `br := newBackupReader(...)` runs CRC32-IEEE over every uncompressed source byte
  (`Read`, :925-929). Only `bw.HashString()` is stored (:1144/:1146); `br.HashString()` is used only on restore
  (:1687, :1796), where br reads the stored bytes.
- So the backup-side br CRC is dead work. Measured: `hash/crc32` IEEE runs at 15-22 GB/s on this AVX-512 box, about
  0.05 cpu-s/GB, which is **under 1%** of the 5-10 cpu-s/GB of compression. Expect a few percent at most on older
  amd64/arm64.
- The fix is trivial (a flag or constructor variant that skips hashing for the source reader), but the benefit is
  negligible. It is not included in the prototype.

## 8. Gotchas

- **Pool retention.**
  - Idle gzip writers (~1.1 MB each) stay pooled until two GC cycles pass. The number is bounded by peak concurrent chunk
    compressions: backup concurrency × Parallel.
  - Output buffers can grow to about ChunkSize (incompressible data), and input buffers are ChunkSize. When ChunkSize
    shrinks, larger pooled buffers are reused; smaller ones are replaced.
  - Peak live memory is about the same as upstream; garbage drops by about 98%.
- **Error paths.** When a chunk write fails, later chunks are discarded without returning their output buffers to the pool
  (plain GC, same as upstream). A failed compress returns its output buffer to the pool but drops the gzip.Writer.
- **Aliasing.** A pooled output buffer is reused after `w.w.Write` returns. This relies on the io.Writer contract. The
  backup writers (bufio, storage handles) comply.
- **Maintenance.** The fork makes Vitess own about 250 LOC that were an external module; upstream has been untouched since
  2020. An alternative is to upstream the fix to planetscale/pargzip and bump the module (same code).
- **Default engine.** A future switch to pgzip or zstd is still possible, but this change removes most of the reason for it.
  zstd would be a much bigger CPU/ratio win, but it is a user choice and not N-1 relevant (supported since v15).
- **Side finding, not addressed.** The builtin engine allocates a 2 MB `bufio.Writer` per file (and per chunk), even for
  16-96 KB files. For many small tables that dominates allocation. It could be capped at `min(2 MiB, dataSize+slack)`.
