# Performance investigation notes (working notes, not for merge)

This directory records a multi-round performance investigation of Vitess (base commit aa9ccf9, Go 1.27.1). **Remove it before opening any PR from this branch**; upstream changes should be cut as separate, focused PRs.

## Start here

- `ROADMAP.md`: everything ranked by payoff vs effort. It includes:
  - the classification of fixed waits (long-running vs time-critical);
  - the general roadmap (waves 1–3 plus strategic projects);
  - the VReplication roadmap (R1–R22).
- `BUGS.md`: every bug found (P0–P3), with status and source.

## By round

| Round | What | Files |
|---|---|---|
| 0 | Original question: can Go's SIMD experiment help? | `SIMD.md`, `SIMD-bench/` |
| 1 | Codebase-wide search for byte-level and allocation-level tuning | Survey: `SURVEY-round1.md` (what was checked and rejected). Investigations: `SUMMARY.md` (index and table), `F01`–`F30` `.md`/`.patch` (no F28; the xtrabackup item was folded into F07). Brief: `BRIEF.md`. |
| 2 | End-to-end profiling on a real local cluster | Baseline: `BASELINE.md`. Brief: `BRIEF2.md`. Plan and statuses: `ROUND2_PLAN.md`. Reports: `P1-point-read` (point selects), `P2-writes-tx` (writes and transactions), `P3-scatter` (cross-shard), `P4-vreplication`, `P5-operations` (failover, backup), `P6-runtime` (connections, GC, TLS), `P7-combined` (the safe changes integrated and measured end to end; `P7-combined.patch` is the integration branch). |
| 2b | Why Vitess is ~15x slower than plain MySQL on point selects | `HOP-OVERHEAD-prelim.md`, `H1-hop-rootcause` (root cause: goroutine hand-offs and vCPU wake-ups in the gRPC hop), `H2-grpc-transport` (unary vs streams vs pooled TCP; PRs #19620/#20215) |
| 3 | VReplication deep dive | Brief: `BRIEF3-vreplication.md`. Reports: `V1-copy`, `V2-apply` (incl. PR #19535), `V3-vdiff`, `V4-scale` + `V4b-scale-cont`, `V5-vstream`, `V6-static` |
| 4 | Bug validation (failing tests or repros, no fixes) | Brief: `BRIEF-VAL.md`. Reports: `VAL-A`…`VAL-D` (`.md` plus `.patch` with tests and repro scripts). Results are in the Validated column of `BUGS.md`. |

Each `<ID>.md` is a full report with benchmark output. Each `<ID>.patch` is a prototype against aa9ccf9 (`git apply perf-findings/<ID>.patch`); some include harness scripts under `perf-findings/<ID>-scripts/` or `perf-findings/harness/`. `*-raw/` directories hold raw benchmark output.

## Harness and process notes (learned the hard way)

- **Cluster harness:** `harness/cluster.sh` brings up a sharded keyspace on a configurable port base. Multi-keyspace VReplication harnesses are inside `P4-vreplication.patch`, `V1-copy.patch`, `V2-apply.patch` and `V4b-scripts/`.
- **Run as a non-root user.** The examples and mysqld refuse to run as root; use `runuser -u vt -- ...`.
- **MySQL-backed Go tests** (vreplication, vstreamer, vdiff, ...):
  1. Compile with `go test -c` as root.
  2. Run the binary as `vt`, from the package directory, with:
     - `VTROOT=<dir with bin/mysqlctl and a copy of the repo's config/>`
     - `VT_MYSQL_ROOT=/usr`
     - `VTDATAROOT=<vt-owned dir>`
     - `PATH=<vitess bin>:/usr/sbin:/usr/bin:/bin`
  3. Interrupted runs leave orphaned test mysqlds; kill them and delete their data dirs.
- **Concurrent benchmarks on one box:**
  - Serialize measurements with `flock -o <lockfile> <cmd>`. Never start or restart a cluster inside the lock: without `-o`, the daemons inherit the lock fd and deadlock everyone.
  - Reserve the harness port ranges (`echo 30000-30999,40000-40999 > /proc/sys/net/ipv4/ip_local_reserved_ports`), or other clusters' client connections can grab a listen port as their ephemeral source port.
- **Metrics:** CPU µs/query per process (`cluster.sh cpu` before/after) is far more robust than latency or QPS on a shared box; alternate A/B runs. Instruction counts (cachegrind/callgrind) are immune to load.
- **Disk:** binlogs from write-heavy runs, per-worktree Go build caches and agent binaries fill the disk quickly. Purge binlogs between runs, use `GOFLAGS=-trimpath` so worktrees share the build cache, and trim old cache entries.
- **The VM caveat:** waking a halted vCPU costs ~20 µs here, which inflates per-hop costs (H1). Re-measure transport and wake-up-related items on dedicated hardware or with halt-polling before committing to them.
