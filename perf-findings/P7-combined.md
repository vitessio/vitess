# P7-combined: all low-risk round-1 and round-2 code changes in one build

Investigator P7, BASE 40000, base commit aa9ccf9. Combined diff: `perf-findings/P7-combined.patch`. It is staged, not
committed, in the worktree `agent-a2e2c3d7f0270d818`: 196 files, +15,671/−1,712 lines, of which 91 are test files with
+11,058 lines. Binaries: `/home/vt/bin-P7`, with `/home/vt/bin` as the base. Harness and raw lines: `/home/vt/perf/P7/`,
copied to `perf-findings/P7-raw/`. The per-row table is `P7-raw/final-table.md`, aggregated from `P7-raw/ab1.txt`.

The first P7 agent built the combined branch and ran the benchmark, then its process was restarted before it wrote this
report. This report was written afterwards from its artefacts. The benchmark numbers come from that run and were not
repeated. The applied/skipped audit, provenance check, tests and fixes below were done afterwards.

## TL;DR

- **The code alone** (`p7` vs `base`) gives these results:
  - **Point selects:** +3..+9% QPS, −4..−7% vtgate CPU/query, −3..−15% tablet CPU/query. The gains are larger at 32
    threads.
  - **sysbench read_only / write_only / read_write:** within noise (about +1%).
  - **Autocommit PK update (`oltp_update_index`):** **+16% TPS, −14% latency, −20% tablet and −17% mysqld
    CPU/query.**
  - **Cross-shard join (10-row LHS):** **2x QPS, −49% latency, −46% vtgate / −53% tablet / −41% mysqld CPU/query.**
  - **10k-row scatter GROUP BY:** +10% QPS, −17% vtgate CPU/query.
  - **10k-row OLAP read:** **+28% QPS, −22% latency, −28% vtgate and tablet CPU/query.**
- **Code + config** (`p7tuned`: GOMAXPROCS=2, GOGC=400, GOMEMLIMIT=1GiB, static gRPC windows, 256 KiB stream buffers)
  gives these results:
  - **Every workload improves 9..98% in throughput. vtgate CPU/query drops 21..56% and tablet CPU/query drops
    19..57%.** p99 latency is 15..52% lower.
  - For most OLTP rows the config, mainly GOMAXPROCS, is worth more than the code: another −9..−25% CPU/query on top
    of p7. For the join and the PK update the code dominates.
- **Audit:**
  - 26 of the 29 round-1 patches are applied unchanged. F06, F09 and F10 are applied with integration adaptations.
  - P1, P3, P5 and P6 are applied in full (code parts; harness files excluded).
  - P2 and P4 are partial. P2's deferred BEGIN is skipped. P4's Online DDL re-review and vstreamer event pipeline are
    skipped.
  - **P6 is fully included:** parser `//go:noinline` getters, smartconnpool ticker only while waiting, per-query mutex
    removals, and the theine plan-cache size metric.
- **Tests:** `go build ./go/...` passes, and every touched package passes. Packages that need mysqld were run as the
  `vt` user. Three problems were found and fixed in the diff (none affects the benchmark binaries):
  1. The vtcombo flag doc was missing `--join-rhs-concurrency` (from P3).
  2. `vttablet/endtoend` TestCommit/TestAutoCommit still expected the `DeleteLimit` plan type (from P2).
  3. Five stray `vtexplain/testdata/plan_test*` failure artefacts had been staged. They are removed.
- **Provenance:** `/home/vt/bin-P7` is **byte-identical** to a fresh `-trimpath` build of the final staged diff. I
  checked all seven binaries: vtgate, vttablet, vtctld, vtctl, vtctldclient, mysqlctl, vtorc.

## What users would see

### Setup

The setup is P7's copy of the P1/P2 harness:

- 2 shards (-80, 80-), 1 primary each, 4 sysbench tables × 250k rows.
- For each config, vtgate and the vttablets are restarted in place: purge binlogs, restart, 5 s warm-up.
- 4 rounds. Each round runs base → p7 → p7tuned.
- OLTP rows run for 20 s and the scatter shapes (P3's `p3.lua`) for 15 s, all at 8 threads unless stated.
- CPU µs/query is process CPU (`/proc`) divided by queries:
  - "tablets" is both tablets summed.
  - "mysqld" is both mysqlds summed.
- The machine was shared with P6 during part of the run. The load average was 7–14, as shown in the load column.
  - The `p7tuned` rows ran at slightly lower load, partly because they burn less CPU themselves.
  - Treat QPS/latency differences under about 5–10% as noise. CPU/query ranges across the 4 rounds are shown in
    brackets and are tight (±2–3%).

The workloads (`/home/vt/perf/P7/suite.sh`):

| name | workload |
|---|---|
| ps8 / ps32 | oltp_point_select, prepared statements, 8 / 32 threads |
| text8 / text32 | oltp_point_select, text protocol |
| read_only / write_only / read_write | sysbench standard transactions (explicit BEGIN…COMMIT) |
| update_index | oltp_update_index (autocommit `UPDATE … SET k=k+1 WHERE id=?`) |
| join_x10 | `SELECT a.c, b.c FROM sbtestA a JOIN sbtestB b ON a.k = b.k WHERE a.k BETWEEN s AND s+9` (non-vindex join, 10-row LHS) |
| grp_high10k | `SELECT k, COUNT(*) … WHERE id BETWEEN s AND s+9999 GROUP BY k` (scatter, ~10k groups) |
| big10k_olap | `SELECT * … WHERE id BETWEEN s AND s+9999` with `set workload=olap` (streaming) |

### Results (mean of 4 rounds; % vs base)

| workload | config | TPS | avg ms | p95 ms | p99 ms | vtgate µs/q [range] | tablets µs/q [range] | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|
| ps8 | base | 5584 | 1.43 | 2.67 | 4.12 | 181 [179–183] | 204 [202–208] | 139 | 7.0 |
| ps8 | p7 | 5780 (+3.5%) | 1.38 (−3.3%) | 2.60 (−2.6%) | 4.09 (−0.8%) | 174 (−4.1%) [171–178] | 196 (−4.2%) [194–199] | 137 (−1.5%) | 8.3 |
| ps8 | p7tuned | 6559 (+17.5%) | 1.22 (−14.9%) | 2.12 (−20.8%) | 2.98 (−27.7%) | 131 (−27.8%) [128–134] | 154 (−24.5%) [152–157] | 129 (−7.1%) | 7.8 |
| ps32 | base | 9058 | 3.53 | 6.85 | 9.47 | 113 [111–114] | 127 [126–128] | 109 | 9.3 |
| ps32 | p7 | 9894 (+9.2%) | 3.23 (−8.3%) | 6.35 (−7.3%) | 9.07 (−4.2%) | 105 (−6.7%) [102–113] | 108 (−14.5%) [106–114] | 102 (−6.1%) | 10.3 |
| ps32 | p7tuned | 10016 (+10.6%) | 3.19 (−9.5%) | 5.80 (−15.3%) | 7.88 (−16.8%) | 88.8 (−21.1%) [87–91] | 98.6 (−22.3%) [98–100] | 102 (−6.5%) | 8.9 |
| text8 | base | 5173 | 1.54 | 2.86 | 4.27 | 216 [214–218] | 211 [209–213] | 142 | 9.8 |
| text8 | p7 | 5316 (+2.8%) | 1.50 (−2.6%) | 2.81 (−1.7%) | 4.22 (−1.3%) | 207 (−4.1%) [203–216] | 204 (−3.2%) [200–212] | 142 (±0) | 10.2 |
| text8 | p7tuned | 5955 (+15.1%) | 1.34 (−13.1%) | 2.33 (−18.6%) | 3.24 (−24.2%) | 157 (−27.1%) [152–163] | 163 (−22.8%) [158–168] | 133 (−6.0%) | 8.6 |
| text32 | base | 8324 | 3.84 | 7.36 | 10.05 | 140 [139–140] | 131 [129–133] | 112 | 12.2 |
| text32 | p7 | 8845 (+6.2%) | 3.62 (−5.9%) | 7.04 (−4.4%) | 9.87 (−1.8%) | 132 (−5.1%) [131–135] | 115 (−11.8%) [112–117] | 107 (−4.6%) | 12.1 |
| text32 | p7tuned | 9109 (+9.4%) | 3.51 (−8.6%) | 6.26 (−14.9%) | 8.36 (−16.8%) | 105 (−24.9%) [104–106] | 105 (−19.4%) [105–106] | 107 (−4.1%) | 10.0 |
| read_only | base | 261 | 30.69 | 40.02 | 46.84 | 240 [236–248] | 276 [272–285] | 212 | 12.4 |
| read_only | p7 | 263 (+1.1%) | 30.37 (−1.1%) | 39.66 (−0.9%) | 46.85 (±0) | 238 (−1.1%) [233–246] | 270 (−2.2%) [266–279] | 211 (−0.3%) | 12.0 |
| read_only | p7tuned | 291 (+11.9%) | 27.43 (−10.6%) | 35.12 (−12.3%) | 39.84 (−15.0%) | 182 (−24.3%) [181–184] | 213 (−22.6%) [210–216] | 203 (−4.2%) | 9.9 |
| write_only | base | 770 | 10.38 | 15.83 | 20.47 | 195 [191–200] | 233 [229–237] | 222 | 12.5 |
| write_only | p7 | 781 (+1.4%) | 10.24 (−1.4%) | 15.48 (−2.2%) | 19.75 (−3.5%) | 195 (±0) [192–198] | 229 (−1.5%) [225–233] | 220 (−1.1%) | 12.5 |
| write_only | p7tuned | 877 (+13.9%) | 9.12 (−12.2%) | 13.16 (−16.9%) | 16.41 (−19.8%) | 149 (−23.8%) [147–151] | 180 (−22.5%) [179–184] | 207 (−7.0%) | 10.3 |
| read_write | base | 194 | 41.28 | 54.83 | 64.21 | 243 [238–246] | 287 [284–289] | 257 | 12.7 |
| read_write | p7 | 196 (+1.2%) | 40.80 (−1.1%) | 54.38 (−0.8%) | 63.96 (−0.4%) | 243 (±0) [239–250] | 282 (−1.8%) [276–289] | 257 (±0) | 12.7 |
| read_write | p7tuned | 218 (+12.8%) | 36.61 (−11.3%) | 47.06 (−14.2%) | 54.59 (−15.0%) | 186 (−23.3%) [185–188] | 221 (−22.8%) [220–222] | 245 (−4.7%) | 9.9 |
| update_index | base | 3054 | 2.62 | 4.91 | 7.07 | 245 [239–251] | 343 [337–352] | 427 | 12.7 |
| update_index | p7 | 3537 (+15.8%) | 2.26 (−13.8%) | 4.27 (−13.0%) | 6.21 (−12.2%) | 232 (−5.4%) [229–237] | 275 (−20.0%) [272–280] | 353 (−17.3%) | 12.9 |
| update_index | p7tuned | 3957 (+29.6%) | 2.02 (−22.7%) | 3.65 (−25.7%) | 5.19 (−26.7%) | 175 (−28.4%) [171–179] | 217 (−36.7%) [212–222] | 340 (−20.4%) | 9.8 |
| join_x10 | base | 305 | 26.22 | 43.42 | 57.24 | 2573 [2504–2685] | 4175 [4010–4394] | 3717 | 12.9 |
| join_x10 | p7 | 596 (+95.3%) | 13.42 (−48.8%) | 22.10 (−49.1%) | 28.38 (−50.4%) | 1394 (−45.8%) [1358–1422] | 1975 (−52.7%) [1916–2033] | 2190 (−41.1%) | 13.6 |
| join_x10 | p7tuned | 604 (+98.0%) | 13.25 (−49.5%) | 21.14 (−51.3%) | 27.51 (−51.9%) | 1138 (−55.8%) [1116–1164] | 1810 (−56.6%) [1758–1858] | 2223 (−40.2%) | 11.3 |
| grp_high10k | base | 265 | 30.12 | 49.45 | 62.21 | 5037 [4941–5159] | 3552 [3487–3650] | 4884 | 13.3 |
| grp_high10k | p7 | 291 (+9.7%) | 27.45 (−8.9%) | 45.00 (−9.0%) | 56.87 (−8.6%) | 4181 (−17.0%) [4130–4229] | 3171 (−10.7%) [3125–3211] | 4923 (+0.8%) | 14.2 |
| grp_high10k | p7tuned | 359 (+35.1%) | 22.31 (−25.9%) | 36.58 (−26.0%) | 45.63 (−26.7%) | 2648 (−47.4%) [2579–2742] | 2098 (−40.9%) [2056–2152] | 4862 (−0.5%) | 12.2 |
| big10k_olap | base | 106 | 75.95 | 104 | 123 | 12847 [12082–13770] | 13930 [13427–14565] | 4747 | 12.6 |
| big10k_olap | p7 | 136 (+28.5%) | 58.98 (−22.3%) | 89.18 (−14.6%) | 110 (−10.1%) | 9237 (−28.1%) [8998–9534] | 10084 (−27.6%) [9929–10318] | 4616 (−2.8%) | 14.0 |
| big10k_olap | p7tuned | 186 (+75.9%) | 43.12 (−43.2%) | 65.14 (−37.6%) | 79.83 (−35.0%) | 5699 (−55.6%) [5504–5915] | 6862 (−50.7%) [6687–7103] | 4435 (−6.6%) | 11.7 |

In read_only, write_only and read_write, TPS counts sysbench transactions (16, 6 and 20 queries each). Their QPS change
is the same percentage.

### Interpretation: which change drives which gain

These attributions come from the per-finding A/Bs. P7 measured only the three whole configurations, not each change on
its own.

- **Point selects, code only (−4..−7% vtgate, −3..−15% tablets, +3..+9% QPS; larger at 32 threads):**
  - **P1** gives most of this.
    - The vttablet gRPC stream-worker pool now defaults to max(GOMAXPROCS, 64). In P1 this gave tablets −4..−6% at 8
      threads and −10..−14% at 32 threads, which matches the tablet column here (−4% / −15% prepared, −3% / −12% text).
    - The per-query micro fixes (status aggregator channel, TransactionMode viper lookup, reflection `String()`, remote
      address formatting) gave vtgate −3..−5% at 8 threads in P1.
  - **P6** adds a little. It measured the parser stack fix at −3..−5% vtgate at low load. Its mutex removals were not
    measurable on this box.
  - Round-1 F10/F11/F15/F19/F25 measured no change for point selects in P1 A/B 4.
  - mysqld CPU/query also falls 5..6% at 32 threads. This is a scheduling side effect: the same queries arrive in
    burstier batches.
- **sysbench read_only / write_only / read_write, code only: no measurable change (≈ +1%).** These transactions run in
  explicit BEGIN…COMMIT, so the P2 PK-DML change does not apply to them. Their per-query costs are dominated by the
  same per-hop overhead that only the config changes attack. P2's deferred BEGIN, the only code change aimed at them,
  is not in the build and measured small in P2 anyway.
- **update_index (+16% TPS, tablets −20%, mysqld −17%): P2's PK-pinned DML planning.**
  - An autocommit `UPDATE … WHERE id=?` used to run as `begin; update … limit 10001; commit` (3 MySQL round trips).
    Now it runs as one statement.
  - P2 measured tablets −14%, mysqld −16%, TPS +8% (8 threads). P7's larger TPS gain includes P1's tablet-side
    changes.
- **join_x10 (2x QPS, −49% latency, −46..−53% CPU in vtgate and tablets, −41% mysqld): P3's concurrent join RHS**
  (`--join-rhs-concurrency`, default 8).
  - The 10 RHS scatters of each join now run in parallel instead of one after the other.
  - P3 measured −55..−60% latency and 2.1–2.3x QPS.
  - The config adds little on top: another −10% vtgate CPU and no latency gain.
- **grp_high10k (−17% vtgate, −11% tablets, +10% QPS, code only):**
  - P3's run-merge sort gives the vtgate part. `Comparison.Sort` merges the pre-sorted shard results instead of
    running a pdqsort; P3 measured −8..−21% vtgate.
  - Round-1 F11 (ReadQueryResult allocations) and F10 (send-side row slabs) give the tablet side.
  - **With the config: −47% vtgate, −41% tablets, +35% QPS.** 10k-row results create a lot of garbage, so
    GOGC=400/GOMEMLIMIT matters much more here than for point selects. P6 measured −10..−11% from GOGC alone for
    1000-row results.
- **big10k_olap (−28% vtgate and tablets, +28% QPS, code only):**
  - P3's gRPC codec buffer pool is the main driver. Every ~33 KiB streamed chunk used to be served from, and zeroed
    in, gRPC's 1 MiB tier. P3 measured −17..−22% vtgate and −16..−27% tablets.
  - F10 (send side) and F11 add to the tablet side. P3 A/B 3 measured F11 + F10 send at −9..−17% tablet CPU.
  - **With the config: −56% vtgate, −51% tablets, +76% QPS.** The 256 KiB stream buffer gives most of the extra
    (P3 A/B 6: −27..−35% vtgate on its own). GOGC and GOMAXPROCS give the rest.
- **The config on OLTP (another −20..−25% vtgate and tablet CPU/query on top of p7 at 8 threads, −9..−20% at 32
  threads; +10..+14% TPS at 8 threads, +1..+3% at 32):**
  - GOMAXPROCS=2 is the largest part. It removes idle-P spinning and futex wake-ups. P1 measured −15% on each process
    at 8 threads; this box runs 2 mysqld + 2 vttablet + vtgate + sysbench on 4 vCPUs.
  - GOGC=400 adds −1..−10%.
  - The static gRPC windows add −2..−6%. They turn off the BDP ping.
  - p99 improves most (−15..−28%).
  - At 32 threads P1 found GOMAXPROCS=2 can cost peak QPS. Here p7tuned is still +1..+3% QPS over p7 at 32 threads
    while using 9..20% less CPU/query.
  - On a host with dedicated cores, re-measure the GOMAXPROCS effect. The general advice is "set GOMAXPROCS to the
    CPU really available", for example through a container CPU limit.

Several round-2 changes are not exercised by this suite. Their effects are in the per-finding reports:

- P4: MoveTables 8.4x faster for many small tables; SwitchTraffic write outage 2.3 s → 0.4 s; tablet picker resumes in
  1.3 s instead of 30 s.
- P5: PRS outage with semi-sync 1.16 s → 0.21 s; backup/restore −2.3/−3.3 s; VTOrc dead-primary detection.
- P6: −50..−64% vtgate stack memory and −20% RSS at 2000 connections; −80% idle vttablet CPU; correct plan-cache size
  metric.
- Round-1 vreplication patches: F05, F13, F16, F17, F18, F20, F26, F27.
- F07 backup pargzip fork: −23% backup CPU.

## What is in the combined diff

Method: a script (see "How this was checked") compares every hunk of every candidate patch with the staged worktree.
For each hunk it checks:

- whether the post-image block is present verbatim;
- otherwise, what fraction of the added lines is present, and whether the pre-image lines are still there.

The reverse check lists every added or removed line in the combined diff that does not come from any candidate patch.
Skipped parts were also checked with `git apply --check --include=…`. All of them still apply cleanly to base and to the
combined tree, so each skip was a choice, not a conflict.

### Round 1 (F01..F30; there is no F28)

| patch | status | notes |
|---|---|---|
| F01 sketch reset (bug fix) | applied | |
| F02 tokenizer scan | applied | |
| F03 SQL escape | applied | shares `sqltypes/value.go` with F20 (disjoint hunks) |
| F04 shard lookup | applied | its `rowDestinations` in `engine/insert.go` supersedes one F09 hunk (see F09) |
| F05 binlog cell format | applied | |
| F06 collation hash | applied, 1 test line adapted | `engine/distinct_test.go` calls `newProbeTable(checkCols, collations.MySQL8(), len(rows))` because P3 added a size-hint parameter |
| F07 backup pargzip fork | applied | removes `github.com/planetscale/pargzip` from go.mod/go.sum |
| F08 MergeSort | applied | |
| F09 insert per-row AST | applied except 1 hunk | the `getInsertShardedQueries` hunk (shared buffers for the ksid index values) conflicts with F04, which rewrote the same block as `rowDestinations(keyspaceIDs)` with the same allocation savings; F04's version is kept |
| F10 proto row slabs | send side applied; **receive side reworked** | `proto3ToRows` allocates one `[]Value` slab per ≤128 rows (`proto3ToRowsSlabRows`) instead of one slab for the whole result. The original F10 gotcha was that one retained row, for example in a streaming MemorySort+LIMIT, pins the whole result. New test `TestProto3ToRowsSlabIsCapped` |
| F11 ReadQueryResult | applied | |
| F12 compliantName | applied | |
| F13 charset convert | applied | |
| F14 collate fast path | applied | |
| F15 binary row writer | applied | |
| F16 vrepl applier | applied | |
| F17 vstreamer vindex filter | applied | |
| F18 JSON escaping | applied | |
| F19 stats labels | applied | |
| F20 BIT encoding | applied | |
| F21 literal format | applied | builds on F03 |
| F22 literal to bind var | applied | |
| F23 astfmt ints / buffer | applied | includes the astfmtgen generator change and regenerated `ast_format_fast.go` |
| F24 keyword lookup | applied | |
| F25 flush timer | applied | semantic change: flush after the first buffered write, not the last |
| F26 GTID set string | applied | |
| F27 throttler client | applied | |
| F29 utf8 validate/slice | applied | |
| F30 cold batch | applied | includes its bug fixes (destination sort race, Timings.Reset) |

Round 1 totals: 26 applied as-is, 3 applied with integration adaptations (F06 test, F09 hunk superseded by F04, F10
receive side reworked), 0 skipped.

### Round 2 (code parts only; harness, raw data and scripts under `perf-findings/` are excluded)

| patch | status | what is in / out |
|---|---|---|
| P1 point read | **applied in full** | stream workers flag and default, status aggregator inline, `getStatsAggregator` struct key + RWMutex, TransactionMode read only in tx, `checkPermissions` timer/remote-address, `topoproto.TabletTypeString`, lazy callinfo remote address, flag docs. Excluded by design: `P1-multiconn-prototype.patch` |
| P2 writes/tx | **partial** | In: PK-pinned DML planned as plain `Update`/`Delete` (`planbuilder/builder.go`), with its test and fixture updates (tabletserver, vtexplain goldens, `vttablet/endtoend/queries_test.go`, `test/endtoend/transaction/twopc`). Out: deferred BEGIN (`--queryserver-config-defer-begin`, `ExecuteFetchWithPrefix`, `ExecWithPrefix`, `pendingBegin` in stateful_connection/tx_pool/tx_engine, flag docs). It is opt-in (default off, so it would not change any number here), M-sized, and P2 measured it within noise. It still applies cleanly. Excluded by design: `P2-prototypes.patch` (sequence cache, parallel MULTI commit, TWOPC without semi-sync) |
| P3 scatter | **applied in full** | codec buffer pool, run-merge sort, concurrent join RHS + `--join-rhs-concurrency`, Distinct hasher reuse |
| P4 vreplication | **partial** | In: copy-phase catch-up skip (`vcopier.go`, `vreplicator.go`); WaitForPos 100 ms poll + immediate save of a skipped position (`engine.go`, `controller.go`, `vplayer.go`, `relaylog.go`); tablet picker exponential backoff; tests. Out: (a) Online DDL re-review of a not-ready migration after 5 s (`onlineddl/executor.go` + test), which is low risk per P4 and applies cleanly, but the first P7 agent left it out without recording why (it is not on the query path P7 measures); (b) vstreamer event pipeline: bounded 16-event queues in `binlog/binlog_connection.go` and `vstreamer.go`, plus the lazily re-armed heartbeat timer. P4 rated (b) low–medium risk: the target vttablet was +5–9% in the same runs and not understood, so it does not count as "low-risk". |
| P5 operations | **applied in full** | DemotePrimary keeps primary semi-sync during PRS; VTOrc poll-period fix; close pooled connections before `mysqladmin shutdown` + 100 ms socket poll |
| P6 runtime | **applied in full** | `//go:noinline` on the 169 `yySymType` getters in `sql.go` + `parse_stack_test.go`; smartconnpool expire worker only while clients wait; lock-free / read-locked per-query reads in streamlog, srvtopo watch, discovery healthcheck, `Executor.VSchema`, vttablet connpool/dbconfigs; theine `UsedCapacity` includes the main policy (plan-cache size metric). Not in the Vitess tree: the goyacc generator patch (`P6-goyacc-noinline.patch`, stored only inside `P6-runtime.patch`). It targets `github.com/vitessio/goyacc` and must land there first, or `make parser` drops the directives |

### Changes that come from no patch (integration fixes)

1. `go/vt/vtexplain/vtexplain_vtgate.go` sets `engine.JoinRHSConcurrency = 1` in `initVtgateExecutor`.
   - vtexplain prints each tablet's queries in logical-time order, which requires the join RHS to run sequentially.
   - I checked that `TestExplain/selectsharded` fails without this line.
   - It mutates a package global in the process that uses vtexplain: the `vtexplain` binary and vtadmin's VTExplain
     endpoint. Neither runs real joins, so this is acceptable for a prototype. An upstream PR should pass the setting
     through the executor/VCursor instead.
   - I also fixed its formatting (`resolver :=vte…`).
2. `go/sqltypes/proto3.go` and `proto3_slab_test.go` cap the F10 receive-side slab at 128 rows (see F10 above).
3. `go/vt/vtgate/engine/distinct_test.go` adapts F06's test to P3's `newProbeTable` signature.
4. Added in this pass: `go/flags/endtoend/vtcombo.txt` gains `--join-rhs-concurrency`. vtcombo registers the vtgate
   flags, and P3 had only updated `vtgate.txt`, so `go/flags/endtoend` TestHelpOutput/vtcombo failed.
5. Added in this pass: `go/vt/vttablet/endtoend/transaction_test.go`. TestCommit and TestAutoCommit expect
   `Queries/Histograms/Delete/Count` instead of `DeleteLimit/Count`.
   - `delete from vitess_test where intval=4` pins the integer PK and is now a plain `Delete` (a P2 consequence).
   - P2 had updated `queries_test.go` but could not run this suite.
   - The `Transactions/*` counts are unchanged: the autocommit Delete is still counted as a transaction in the stats.
6. Removed in this pass: five `go/vt/vtexplain/testdata/plan_test*/…-output.txt` files. They are failure artefacts
   that `vtexplain_test.go` writes with `os.MkdirTemp("testdata", "plan_test")`, and they had been staged by mistake.

## Test status

All runs used `GOFLAGS=-trimpath` on the final staged state.

- **`go build ./go/...`**: OK.
- **Touched packages (40, excluding `go/test/endtoend`)**: all pass except the known environmental failures below.
  - These ran as root: theine, mysql, binlog, charset, unicode, colldata, json, replication, smartconnpool, sqltypes,
    stats, streamlog, astfmtgen, callinfo, dbconfigs, discovery, key, mysqlctl, pargzip, sqlparser, srvtopo,
    topoproto, vtexplain, vtgate, engine, evalengine, metro, vtorc/inst, vtorc/logic, grpctabletconn, tabletserver,
    connpool, planbuilder, rules, throttle.
  - These need mysqld and ran as `vt` with `VT_MYSQL_ROOT=/usr`: vreplication, vstreamer, vttablet/endtoend
    (+connecttcp, connkilling, twopc) and tabletmanager.
    - The vt user cannot read root's Go 1.27.1 toolchain, so the test binaries were compiled as root
      (`go test -c`, into `/home/vt/p7test`) and run as vt.
    - VTROOT was set to a vt-owned directory with `bin/{mysqlctl,mysqlctld,vtcombo,vtctldclient}` built from the
      combined tree, and `config` symlinked to the worktree.
- **Dependents (115 more packages)**: vtgate/..., sqlparser/..., mysql/..., tabletserver/..., vtcombo, vtctl/workflow,
  stats, pools, sqltypes, onlineddl, binlog, wrangler.
  - 63 ok, 46 have no tests.
  - 6 need mysqld: vtgate/endtoend (+deletetest, update), evalengine/integration, collations/integration and
    mysql/endtoend. All 6 pass as vt, once `vtctldclient` is on PATH.
- **`go/flags/endtoend`**: passes after fix 4, with all `go/cmd/...` binaries on PATH.
- **`-race`** on engine, smartconnpool, srvtopo, sqltypes and discovery: clean.
- **`scripts/fmt`** on all changed Go files: no further changes.

Failures that also happen on base (checked in a clean base worktree):

- `go/vt/servenv`: TestGetCGroupCpuUsageMetrics, TestGetCgroupMemoryUsageMetrics and TestErrHandlingWithCgroups fail
  because the container has no cgroup metrics.
- `go/vt/vtexplain`: a pre-existing flake.
  - Cause: the tablet's background `select @@global.wait_timeout` refresh sometimes lands in the vtexplain query log.
  - Frequency: base 2/20 runs, combined 5/20. The same symptom appears in both.
- Integration bugs found: the three listed above (vtexplain join ordering, which the first P7 agent had already fixed;
  the vtcombo flag doc; the vttablet endtoend plan-type expectation). None changes vtgate or vttablet code.
- **Not run:** `go/test/endtoend`. P2's fixture edits in `test/endtoend/transaction/twopc/twopc_test.go` still need a
  CI run.

## Binary and diff provenance

- `/home/vt/bin-P7/*` were built at 06:28 UTC. Their build info shows `aa9ccf9+dirty`, go1.27.1, `-trimpath`,
  CGO_ENABLED=1.
- I rebuilt vtgate, vttablet, vtctld, vtctl, vtctldclient, mysqlctl and vtorc from the final staged diff with
  `GOFLAGS=-trimpath` into a temp dir. **All seven are byte-identical** to `/home/vt/bin-P7`: same sha256 and same
  `go tool buildid`.
  - I checked this before and after this pass's fixes, which touch only test files, a flag doc and vtexplain (not
    linked into these binaries).
  - **The benchmarked binaries therefore match `P7-combined.patch` exactly.**
- The only files the first agent changed after building were `distinct_test.go` and `vtexplain_vtgate.go`, plus the
  vtexplain artefacts. None is in the benchmarked binaries.
- `P7-combined.patch` supersedes `P7-combined-wip.patch`, which has been deleted. The two differ by:
  - the 5 artefacts removed;
  - fixes 4 and 5;
  - the vtexplain gofmt fix.
- Benchmark raw data: `/home/vt/perf/P7/ab1.txt`, identical to `perf-findings/P7-raw/ab1.txt`. It has 132 lines: 4
  rounds × 3 configs × 11 workloads.

## Known risks

Risks inherited from the individual findings (see each report):

- **P3 concurrent join RHS:**
  - Up to 8 RHS scatters are in flight per statement, and nested joins multiply that (8 × 8). This reaches tablet
    pools faster.
  - It is not applied inside transactions or reserved connections, nor to streaming joins.
  - Any tool that assumes sequential RHS execution must opt out, as vtexplain now does. `vexplain trace` is unaffected.
- **P3 codec pool:** it relies on the codec never reading past the written length. Power-of-two tiers can hold up to 2x
  the requested size.
- **P2 PK-DML:**
  - Query-stats plan types change from `UpdateLimit`/`DeleteLimit` to `Update`/`Delete` for these statements, which
    affects dashboards.
  - Query logs and 2PC redo logs lose `limit 10001`.
  - With `--queryserver-config-max-result-size=0`, a PK update now succeeds.
  - The change applies to integer PKs only.
- **P1 stream workers:** up to 64 parked goroutines per vttablet. The status aggregator becomes a per-aggregator mutex
  instead of a global channel.
- **P5 DemotePrimary semi-sync:** a behaviour change in PRS. Review it with the durability-policy owners.
- **P6:**
  - The parser directive needs the goyacc change upstream first.
  - The smartconnpool worker parking changes timer behaviour; `-race` is clean.
- **F25:** flush after the first buffered write (bounded), not after the last.
- **F10 receive side:** the slab cap bounds, but does not remove, the retention of up to 128 rows' `Value` headers by
  one retained row.
- **F09:** plan memory +85%, which plan-cache sizing does not see. Note that P6's metric fix makes the real plan-cache
  size visible.
- **F04:** overlapping key ranges may pick a different shard.
- **Release compatibility:** new flags only (`--grpc-server-num-stream-workers`, `--join-rhs-concurrency`); no protocol
  change. The flag defaults change behaviour: 64 stream workers and join concurrency 8. Per the N±1 rule they are safe
  mixed-version, but they should be called out in release notes.
- **Config (`p7tuned`):** GOMAXPROCS=2 is right for this oversubscribed 4-vCPU box, not a universal value.
  - GOMEMLIMIT=1GiB with GOGC=400 must be sized per deployment.
  - The 256 KiB stream buffer costs memory per concurrent stream and delays the first row. Keep the vtgate and
    vttablet values in sync.

## Recommended landing order (independent PRs)

These groups can land in parallel unless an arrow says otherwise. Overlaps are noted so that later PRs rebase cleanly.

1. **Correctness fixes first (small, no perf dependency):**
   - F01 (theine sketch).
   - The P6 plan-cache `UsedCapacity` metric (same file family: `go/cache/theine`).
   - The F30 bug-fix bits (destination sort race in `key/destination.go`, Timings.Reset). F04 overlaps
     `destination.go`, so land F30 first.
2. **P3 codec buffer pool** (`servenv/grpc_codec*`). Standalone, and the biggest win for streamed results.
3. **P3 concurrent join RHS**, together with the vtexplain opt-out and both flag docs (`vtgate.txt`, `vtcombo.txt`).
   Consider plumbing the setting through the executor instead of the global.
4. **P3 run-merge sort + Distinct hasher**, F08 MergeSort, then F06 → F14 (colldata 8bit/unicode overlap).
   - F06's `distinct_test.go` must follow P3's `newProbeTable(…, sizeHint)` signature if P3 lands first.
5. **P2 PK-pinned DML planning**, with the vtexplain goldens, twopc and vttablet/endtoend fixture updates (including
   this pass's `transaction_test.go` fix). It needs an endtoend CI run.
6. **P1 per-hop changes:** the stream-worker flag/default, then the micro fixes.
   - P1 shares `vtgate/executor.go` with F23 and P6.
   - P1 shares `topoproto/tablet.go` with F30.
   - P1 shares `tabletserver/query_executor.go` with P2.
7. **P6 runtime**, split into three PRs:
   - (a) the goyacc upstream change → `sql.go` `//go:noinline` + test;
   - (b) smartconnpool idle ticker;
   - (c) lock removals (streamlog, srvtopo, healthcheck, `Executor.VSchema`, connpool/dbconfigs).
8. **MySQL protocol:**
   - F11 → F15, which share `mysql/query.go`. P2's deferred BEGIN also edits it if revived.
   - F25 and F30 `writePacket`, which share `conn.go`.
   - F10 with the capped receive slab.
9. **Parser/formatter series:**
   - F02 (token.go);
   - F24 → F12 → F23, which share `ast_funcs.go`;
   - F03 → F21, which share `value.go`/`tracked_buffer.go`/`ast_format*` with F23;
   - F20 (`value.go`, disjoint from F03);
   - F22 (normalizer).
   - F23 includes the astfmtgen generator, so regenerate after F21.
10. **Routing:** F04 → F09. F09's shared-buffer hunk is dropped in favour of F04's `rowDestinations`.
11. **VReplication:**
    - F05, F13, F16, F17, F18, F26, F27, F29.
    - P4 in three PRs: copy catch-up skip; WaitForPos/skipped-position save; tablet picker backoff.
    - F17 and P4's (skipped) vstreamer pipeline both touch `vstreamer.go`.
    - P4's Online DDL re-review can land on its own; it is not in this build but applies cleanly.
12. **Operations:** P5 (three independent PRs: PRS semi-sync, VTOrc poll, mysqld shutdown) and F07 (pargzip fork;
    go.mod change).
13. **Documentation/config guidance** (no code):
    - size GOMAXPROCS to the available CPU;
    - GOGC=200..400 with GOMEMLIMIT;
    - static gRPC windows on LAN;
    - a 128–256 KiB stream buffer for OLAP-heavy deployments;
    - ECDSA certs and TLS session resumption (P6).

Deferred (not in this build): P2 deferred BEGIN (opt-in flag), P4 vstreamer event pipeline (needs the target-side +5–9%
explained), and the P2 prototypes (sequence block cache needs design).

## How this was checked (reproducible)

- **Hunk audit:** a Python script parses each `perf-findings/F*.patch` and P1..P6 patch.
  - For each hunk it checks whether the post-image block (context + added lines) is present in the worktree file, and
    reports the fraction of added lines present.
  - A reverse pass lists every added/removed line of the combined diff that no candidate patch contains.
  - Skipped parts were confirmed with `git apply --check --include=<file>` against the base and combined trees.
- **Provenance:** `go build -o <tmp>/ ./go/cmd/{vtgate,vttablet,vtctld,mysqlctl,vtorc,vtctl,vtctldclient}` with
  `GOFLAGS=-trimpath`, then `cmp` / `go tool buildid` / `sha256sum` against `/home/vt/bin-P7`.
- **Base comparison:** `git worktree add <scratch>/p7base aa9ccf9`, then the same tests.

## Housekeeping note

The P7 benchmark cluster (BASE 40000: etcd, vtctld, 2 vttablets, 2 mysqld and vtgate, still with the `p7tuned` flags)
was still running when this pass started. The driver had finished; `ab1.txt` is complete. My attempt to run
`/home/vt/perf/P7/c.sh down` was blocked by the sandbox's permission policy. Someone with permission should run it.
