# V2-apply: VReplication running phase (vplayer apply) under saturation

Investigator V2-apply, BASE 40000, base commit aa9ccf9. Patch: `findings/V2-apply.patch` (uncommitted in worktree
`agent-af624d5c02fc54282`). Binaries used (removed at the end, rebuild from the patch): `bin-V2base` (base, same build
flags), `bin-V2` (final patch), `bin-V2a` (bulk UPDATE only), `bin-PR` (PR #19535 merged onto aa9ccf9), `bin-PRV2`
(PR #19535 + the bulk UPDATE of this patch). Harness: `perf-findings/harness/V2/` in the patch (copied from
`/home/vt/perf/V2/`). Raw A/B logs: `findings/V2-apply-raw/`.

## Summary (ranked by user-visible impact)

| # | Change | Measured end-to-end effect | Size | Risk |
|---|---|---|---|---|
| 1 | **Apply multi-row UPDATE events as one `UPDATE t SET c=CASE pk WHEN .. THEN .. END .. WHERE pk IN (..) ORDER BY pk` per run of monotonic PKs** instead of one UPDATE per row (`replicator_plan.go` `applyBulkUpdateChanges`, `table_plan_builder.go` `generateBulkUpdatePlan`, `vplayer.go`) | 100-row UPDATE trx backlog drain **153-162 -> 424-482 trx/s (2.9x)**, 1000-row **15-17 -> 40-49 trx/s (2.8x)**. Target mysqld CPU **-62%**/trx, target vttablet **-70%**/trx. Source doing 300 x 100-row UPDATEs/s while the workflow runs: **lag 24 s and growing (base) -> 0 s**. Stacks with PR #19535: 4 workers **298-324 -> 634-718 trx/s**. | M | medium: value semantics through CASE (restricted to integer PK, no JSON/BIT/float/geometry columns, no expressions/aggregates) |
| 2 | **Buffer the row changes of consecutive row events of the same table and shape across events and source transactions** and apply them with the bulk INSERT / DELETE / UPDATE statements (`vplayer.go` `pendingRowChanges`) | Single-table single-row INSERT backlog (log/event-table pattern) **20.9-23.5k -> 33.7-39.4k trx/s (+65-80%)**, target mysqld **-55%**/trx (40 -> 17.8 us), target vttablet -25%. Neutral (within noise) for sysbench write_only / update_index / wide rows. | M | medium: statement shapes change (2 existing test expectations updated), error is attributed to the event that triggered the flush |
| 3 | PR #19535 (parallel applier, 4 workers) evaluated | Backlog drain: 100-row UPDATE **~160 -> 298-324 (1.9x)**, wide rows **2.5-3.7k -> 4.7-5.0k (+40-60%)**, write_only **3.9-4.7k -> 6.0-6.2k (+35%) when not CPU-starved**, update_index **12.4-16.2k -> 9.5-12.5k (-15..-25%)**. Target mysqld CPU/trx **+10..+85%** (16 source trx per commit instead of ~200). Concurrent: lag write_only 64 thr 16 s -> 9-13 s; single-table inserts 1 s -> **5 s (worse)**. Checksums matched after all runs. | (L, upstream) | see section 5 |

Negative / neutral: position updates and commits are already amortized (1 per ~200 source trx, piggybacked in the COMMIT
packet); per-transaction round trips: none; overlapping SQL generation with execution would save <5%; the default relay log
size is not the limit; PR #19535 at 1 worker == base.

## Setup

- Cluster (`harness/V2/cluster.sh`, P4's multi-keyspace harness): keyspace `src` (unsharded, tablet 100) -> keyspace `dst`
  (**one shard**, tablet 101), MoveTables `wf` of `sbtest1..4` (200k rows each), `wide1` (50k rows, 25 columns, ~2.3 KB rows:
  16 varchar(64), 4 bigint, 2 datetime, decimal, 800 B text) and `bulk1` (200k sysbench-like rows). One target shard = one
  stream = the worst case: every source transaction goes through one vplayer and one target MySQL connection.
  mysqld: `innodb_flush_log_at_trx_commit=2`, `sync_binlog=0`, 512 MB buffer pool, binlog on (row, full image).
- Workloads (sysbench on the source mysqld socket, 16 threads unless noted):
  `wo` = oltp_write_only (2 UPDATE + DELETE + INSERT per trx, 4 tables), `upd` = oltp_update_index, `ins` = oltp_insert
  (4 tables), `ins1` = oltp_insert on 1 table, `wide`/`wide2` = `wide.lua` (UPDATE 3 columns of a wide row + INSERT
  [`wide2`: DELETE + re-INSERT] a wide row), `b100`/`b1000` = `bulk.lua` `UPDATE bulk1 SET k=k+1 WHERE id BETWEEN r AND r+N-1`
  (8 threads), `d100` = DELETE a 100-row range + one 100-row INSERT.
- **Drain** (`drain.sh`): stop the workflow, generate N transactions, start it, time until the target position contains the
  source GTID. This is the applier's capacity (max sustainable source rate) with the whole box available to it. Reports CPU per
  source transaction per process, target `SHOW GLOBAL STATUS` deltas per source trx, and the busiest target mysqld thread (the
  apply connection): busy % and run-queue wait % from `/proc/<tid>/schedstat`.
- **Concurrent** (`run.sh`): workload for 30 s while the workflow runs; samples the GTID gap (source seq - target seq) and
  `VReplicationLagSecondsMax` every second, then time to catch up after the load stops.
- A/B: `ab.sh` restarts both vttablets with the binary under test (mysqld kept), 3 rounds alternating binaries for drains,
  2 rounds for concurrent runs. Every measurement ran under `flock -o bench.lock`.
- Correctness: `checksum.sh` compares row counts and `CHECKSUM TABLE` of all 6 tables after the target caught up. It
  matched after every batch (base, PR w1/w4, V2a, V2, PR+V2, concurrent runs).
- Machine: shared 4 vCPU; load average 3-18 during the runs (another investigator's clusters and test suites). The apply thread's
  run-queue wait is 3-16% in most runs and up to 31% in the worst; those runs are flagged. Throughput varies by up to +-20% run
  to run; CPU/trx and statement counts are stable.

## 1. Saturation baseline (base binaries)

### Drain capacity vs source generation rate (single target stream)

| workload | source generates (16 thr, workflow stopped) | applier drains | target stmts / source trx | target commits / source trx | tgt mysqld us/trx | tgt vttablet us/trx | src vttablet us/trx | apply thread busy |
|---|---|---|---|---|---|---|---|---|
| wo | 3.6-4.0k trx/s | 3.2-5.8k trx/s | 4.03 | 0.005 | 185-350 | 70-128 | 60-74 | 70-86% |
| upd | 7.7-10.0k | 12.4-16.2k | 1.01 | 0.002 | 65-75 | 32-36 | 28-32 | 60-80% |
| ins (4 tables) | 11.8k | 19.5k | 1.005 | 0.001 | 38 | 25 | 26 | 64% |
| ins1 (1 table) | 12.2k | 20.9-23.5k | 1.005 | 0.001 | 36-43 | 26-28 | 27 | 76-80% |
| wide / wide2 | 3.1-3.4k | 2.5-3.7k | 2.07 / 3.08 | 0.016 / 0.022 | 245-320 | 128-180 | 88-130 | 55-78% |
| **b100** (100-row UPDATE) | **1.0-1.5k trx/s (100-150k rows/s)** | **141-176 trx/s (~16k rows/s)** | 100.6 | 0.09 | 6,100-7,200 | 2,400-2,700 | 560-640 | 73-90% |
| **b1000** | **176-181 trx/s** | **15-18 trx/s** | 1005 | 0.61 | 61,000-71,000 | 23,500-27,000 | 5,000 | 89-90% |
| d100 (100-row DELETE + 100-row INSERT) | 870 | 369 | 4.85 | 0.09 | 3,220 | 830 | 660 | 80% |

- **Where it breaks: multi-row UPDATEs.** A single source statement that updates N rows becomes N UPDATE statements on the
  target. The source generates 100-row UPDATEs 6-9x faster than one stream can apply them, and 1000-row UPDATEs 10x faster.
  Multi-row DELETE and INSERT already have bulk paths (`d100`: 10.8 us of mysqld per row vs ~49 us per updated row).
- Single-row workloads (wo, upd, ins) drain at ~1-2x the rate 16 source threads generate; wide rows are ~1x. So the single
  applier connection is at its limit on this box for the common OLTP mix; any source bigger than the target's one core lags.
- The **target mysqld apply thread is the bottleneck** everywhere: busy 70-90% of the drain, plus 3-15% waiting for a CPU.
  One statement per row change costs it 35-50 us.
- **Position updates, BEGIN/COMMIT and heartbeats are already amortized**: with `VPlayerBatching` (default on, flags=7) the
  vplayer merges all transactions of a relay-log fetch (~200 wo trx, ~1000 upd trx; relay log 250 KB / 5000 items) into
  one target transaction and sends `begin; <all DML>; update _vt.vreplication set pos=...; commit` as one multi-statement
  packet. Commits per source trx: 0.001-0.02 for single-row workloads. The position update is 1 statement per commit, in
  the same packet as the COMMIT. `time_updated`/heartbeat writes only happen when idle. The throttler check is one call per
  fetch (disabled by default). b1000's 0.61 commits/trx is because one 1000-row transaction (~233 KB) fills the relay log.
- **Target mysqld profile** (`perf`, b1000 drain): `dispatch_command` 82%, of which the InnoDB row update
  (`ha_update_row`) is only 16.7%; the rest is per-statement overhead: parse 12.6%, `Sql_cmd_dml::prepare` 6.8%,
  `send_ok`/`net_flush` (one socket write per statement result) 10.4%, a binlog table-map event per statement 4.7%,
  `PT_update::make_cmd` 4.3%; the purge thread takes 11.5%.
- **Target vttablet profile** (wo drain): the apply goroutine (`applyEvent`) ~35%, of which SQL generation (`applyRowEvent`)
  ~14% and `CommitTrxQueryBatch` -> `ReadQueryResult` (a read syscall per statement result) ~19%; the VStream receive
  goroutine (gRPC + `VEvent.UnmarshalVT`) ~19%; GC ~12%. Building a relay batch's SQL takes ~5% of the wall time, so
  overlapping it with execution would save at most ~5%.
- **Source vttablet**: 28-74 us per trx per stream (vstreamer `parseEvent`, `selectgo` between the binlog reader/throttler
  goroutines), as P4 found. It is not the bottleneck for one stream.

Micro-benchmark on the target mysqld (`harness/V2/gen2.py`, `run2.sh`: 20k row updates of `bulk1`-like rows, busiest
mysqld thread CPU per row, via the mysql client, so per-statement round trips are included):

| statement shape | us/row (2 runs) |
|---|---|
| 1 UPDATE per row | 96.5, 98.5 |
| CASE, 2 rows per statement (with / without ORDER BY) | 66.5 / 68.0, 64.5 / 63.5 |
| CASE, 4 rows | 39.0, 41.5 |
| CASE, 100 rows (with / without ORDER BY) | 22.0 / 18.5, 18.5 / 20.5 |
| CASE, 20 / 500 rows (first micro-run, different load) | 16.5-23 / 20.5-22.5 |
| INSERT .. ON DUPLICATE KEY UPDATE, 100 rows | 19.5, 19.5 |

`ORDER BY pk` makes MySQL use a temporary table ("Using temporary") but costs little. The CASE cost grows with rows per
statement (linear WHEN scan per row and column), hence the 100-row cap.

### Lag under concurrent load (base)

Source load and applier share the 4 vCPUs here.

| workload (30 s) | source rate | max GTID gap | max lag | catch-up after load stops |
|---|---|---|---|---|
| wo 16 thr (r2, valid run) | 2.5k trx/s | 23k trx | 11 s | 6.6 s |
| wo 64 thr | 2.8-3.2k trx/s | 42-50k trx | 16 s | 11-12 s |
| b100 at 300 trx/s (30k rows/s) | 298-306 | 4.2-4.4k trx | **24 s, growing linearly** | **24 s** |
| ins1 64 thr | 7.8-8.0k trx/s | 3.8-4.0k | 1 s | 0.04 s |

Lag grows linearly for the whole run whenever the source exceeds the drain rate; there is no stall or plateau.

## 2. Bulk UPDATE of multi-row UPDATE events (change 1)

**Change.** When a row event has several update-shaped changes (batch mode, running phase), `applyBulkUpdateChanges`
groups consecutive changes whose PK is unchanged and strictly increasing (or strictly decreasing) into one statement:

```
update t set c1=case id when 3 then 40 when 2 then 30 end, c2=case id when 3 then ... end where id in (3, 2) order by id desc
```

- **Same semantics as the per-row UPDATEs**: every column gets the same literal the per-row UPDATE would have used (the same
  bind variables and encoder), rows that do not exist are not touched, and `ORDER BY pk [DESC]` makes MySQL update the rows in
  the source's row order, so unique-key checks see the same intermediate states. `TestPlayerBatchModeBulkUpdate` shifts a
  unique key in descending id order, which fails with a duplicate key error if applied in ascending order.
- **Eligibility** (fail closed to the per-row path): plan with a single PK column, plain column copies only (no
  expressions, aggregates, `convert_tz`, extra source PK columns, lastpk/copy phase); integer PK; no JSON field in the event;
  no BIT/GEOMETRY/FLOAT/DOUBLE set columns (a CASE result could convert differently from a literal). The field types are
  checked once in `buildExecutionPlan`. Charset and int-to-enum conversions go through the same `bindFieldVal`.
- Any other change (insert, delete, PK change, partial image, direction change, duplicate PK) ends the run and is applied as
  before; a run of one row uses the regular UPDATE. Statements are capped at 100 rows and at `max_allowed_packet`.

| drain | base (3 runs) | bulk UPDATE only (V2a) | V2 (1+2) |
|---|---|---|---|
| b100 trx/s | 153, 162, 154 | 464, 362, 449 | 445, 482, 424 |
| b100 tgt mysqld us/trx | 7,180 / 6,657 / 6,247 | 2,510 / 3,480 / 2,560 | 2,617 / 2,410 / 2,623 |
| b100 tgt vttablet us/trx | 2,683 / 2,417 / 2,647 | 817 / 870 / 797 | 823 / 747 / 737 |
| b100 target statements / trx | 100.6 | 3.9 | 2.4 |
| b1000 trx/s | 17, 15, 17 | 46, 46, 49 | 47, 48, 40 |
| b1000 tgt mysqld us/trx | 62,400 / 71,267 / 63,067 | 24,833 / 24,767 / 24,267 | 25,167 / 23,567 / 29,100 |

Concurrent, source at 300 x 100-row UPDATEs/s:

| | base r1 / r2 | V2 r1 / r2 | PR w4 r1 / r2 |
|---|---|---|---|
| max GTID gap | 4,367 / 4,215 trx | 65 / 57 trx | 2,073 / 1,549 |
| catch-up after load | 24.3 / 24.2 s | 0.04 / 0.04 s | 6.3 / 5.1 s |

With PR #19535's 4 workers plus this change (`bin-PRV2`): b100 **718, 634 trx/s** (PR alone 324, 313, 298), b1000 **66,
72** (PR alone 34, 31). The two are complementary.

## 3. Cross-event buffering of row changes (change 2)

**Change.** In batch mode, `applyRowEvent` does not apply an insert-only, delete-only (with a MultiDelete plan) or
update-only (with a bulk UPDATE plan) row event right away. It appends its changes to `vp.pending` as long as the table,
shape and binlog row-event flags stay the same. Any other event that writes (COMMIT, FIELD, DDL, OTHER, JOURNAL, SBR
statements, SAVEPOINT, HEARTBEAT, a row event of another table/shape) first flushes the buffer through the existing bulk
INSERT / bulk DELETE / bulk UPDATE paths, so statement order is unchanged. GTID/BEGIN/ROWS_QUERY do not flush, so
consecutive source transactions merged into one target transaction (the existing `hasAnotherCommit` grouping) share a
statement. A buffer of one change uses `applyChange` as before; the buffer holds at most 5000 changes.

| drain | base (3 runs) | V2a | V2 |
|---|---|---|---|
| ins1 trx/s | 20,948 / 22,007 / 23,491 | 24,967 / 24,687 / 26,297 | **38,511 / 39,439 / 33,694** |
| ins1 tgt mysqld us/trx | 42.9 / 39.7 / 36.2 | 34.7 / 35.7 / 33.6 | **17.7 / 17.7 / 17.9** |
| ins1 tgt vttablet us/trx | 28.2 / 27.9 / 25.7 | 25.1 / 25.3 / 24.5 | 20.5 / 20.2 / 20.4 |
| ins1 target statements / trx | 1.005 | 1.005 | 0.025 |
| wo trx/s | 3,223 / 4,198 / 4,421 | 4,287 / 4,626 / 4,417 | 3,783 / 4,386 / 4,597 |
| upd trx/s | 15,276 / 16,158 / 15,043 | 14,738 / 14,907 / 13,571 | 13,488 / 15,484 / 15,997 |
| wide2 trx/s | 2,969 / 2,991 / 3,187 | 3,156 / 3,007 / 3,020 | 2,730 / 2,892 / 3,144 |

For wo/upd/wide2 the tables alternate randomly, so runs are short (upd: 0.79 statements per trx instead of 1.01) and the
effect is within noise; no regression is visible in CPU/trx either. In the concurrent single-table insert run (source ~8k
trx/s) base already kept up (lag 1 s), and so did V2. The benefit is for insert- or delete-heavy single tables (event/log
tables, queues) and for transactions that write many rows with separate single-row statements (ORM loops).

## 4. Other hypotheses tested

- **Position update per transaction / piggybacking**: already one position update per relay batch, inside the COMMIT
  multi-statement (`Com_update - rows` = 0.002-0.02 per source trx). Nothing to gain.
- **Transaction batching**: already merges a whole relay-log fetch. A larger relay log would only help b1000 (0.61
  commits/trx), and its commits are a small share of 60 ms of mysqld per trx. Not pursued.
- **Pipelining SQL generation with execution** (send chunks while building the rest): the apply goroutine's SQL generation
  is ~5% of the drain wall time; the apply mysqld thread is already 70-90% busy. Not pursued.
- **Target vttablet reads one result packet per statement** (`ReadQueryResult` ~19% of target vttablet CPU): real, but it
  overlaps with mysqld execution. Changes 1 and 2 cut it with the statement count (b100 tgt vttablet -70%).
- **INSERT ... ON DUPLICATE KEY UPDATE** for bulk updates: about as cheap as CASE in the micro-benchmark, but it inserts
  missing rows and can update a different row through a secondary unique key. Rejected in favour of CASE.
- **Concurrent-mode runs right after a vttablet restart**: the stream did not start for ~25 s in several runs (the tablet
  picker's flat 30 s retry, P4 finding 4). Those `wo16` runs are excluded; `waitstream.sh` now waits for the stream.

## 5. PR #19535 "VReplication: Implement Experimental Parallel Applier" (mattlord)

Fetched as `pr-19535` (head f0c75c6300, 111 commits, +20.7k/-0.3k lines, 57 files). Merged onto aa9ccf9 in a separate
worktree. Two conflicts, resolved: `controller.go` (terminal-error classification: kept base's
`terminalVReplicationError` with the PR's `getState()/isInCopyPhase()` condition) and `onlineddl/executor_test.go` (kept base).
Enabled with `--vreplication-parallel-replication-workers=4` on both vttablets.

Drain (the base column is from the same batch):

| workload | base | PR, 1 worker | PR, 4 workers | PR 4w target mysqld us/trx vs base |
|---|---|---|---|---|
| wo | 3,927 / 4,668 / 3,909 | 3,413 / 4,381 / 4,515 | 6,034 / 3,400* / 6,222 | 267-290 vs 218-232 (+25%) |
| upd | 12,436 / 14,549 / 14,601 | 13,666 / 14,145 / 14,166 | 12,496 / 12,316 / 9,500 | 121-133 vs 69-75 (+80%) |
| wide | 2,527 / 3,436 / 3,665 | 2,257 / 3,720 / 3,573 | 4,973 / 4,819 / 4,657 | 300-326 vs 245-285 (+15%) |
| b100 | 141 / 176 / 174 | 164 / 180 / 173 | 317 / 315 / 298 | 5,700-6,200 vs 6,100-6,700 (~0) |

\* 31% run-queue wait: CPU-starved by the other investigator's load.

Concurrent (30 s): wo 64 thr max lag 9 / 13 s vs base 16 / 16 s, catch-up 4.2 s vs 11-12 s; b100 at 300/s 6 / 4 s vs 24 s;
**ins1 64 thr 5 / 5 s lag (gap 43-49k trx) vs base 1 / 1 s**.

Assessment:
- **Correctness sanity**: all tables' `CHECKSUM TABLE` and row counts matched after 3 rounds of 4-worker drains
  (60k wo, 150k upd, 40k wide, 3k b100 each) and the concurrent runs. No errors in the logs. This is a sanity check, not
  proof; the workloads have no secondary unique keys or FKs, where the PR's writeset logic is most delicate.
- **Gains are workload-dependent** and on this 4 vCPU box capped by the target mysqld: +35-60% for wo/wide, 1.9x for
  multi-row UPDATEs, and a **loss for small single-row trx (upd -15..-25%, single-table inserts lag more)**. The PR caps
  batching at `workers*4` = 16 source trx per commit (0.063 commits/trx vs 0.002 serial), so each source trx costs more
  mysqld CPU (commit + position update + worse group commit); with small trx, that eats the parallelism. An adaptive cap
  (only split when there are idle workers, or larger caps for tiny trx) would help. The PR description reports 1.4-1.7x
  on an IO-bound setup, consistent with this.
- **1 worker is identical to base** (the serial path is kept), so the risk is opt-in.
- **Risks** (from the design and diff): 20k lines in a critical path; worker connections run READ COMMITTED; 2N+2 MySQL
  connections per stream; strict in-order commit, so one slow transaction blocks all later commits (head-of-line); the
  writeset is recomputed on the target (PK, unique keys, FK parents from information_schema) and fails closed for unhashable
  keys, partial images and column mappings, which is the right direction but large; `vplayer` must stay "safely copyable"
  (a load-bearing invariant documented in the PR); the batching cap trades CPU for parallelism. The merge conflicted in
  `controller.go` against current main.
- **Combining**: the bulk UPDATE of change 1 applies inside the PR's workers unchanged (`bin-PRV2`: 2.1-2.3x over the PR
  alone for multi-row UPDATEs). Change 2's buffer lives on the vplayer and would need care with the PR's per-worker vplayer
  copies (not tried).

## Recommendations

1. **Bulk UPDATE of multi-row UPDATE events** (change 1). This is where running-phase lag actually explodes: one
   `UPDATE ... WHERE status = ...` touching thousands of rows costs the target ~50 us per row, 10x the source. 2.8-3x drain,
   -62% target mysqld CPU, no lag at 30k updated rows/s where base lagged 24 s. Behind the existing `VPlayerBatching` flag.
2. **Cross-event buffering** (change 2): +65-80% for single-table insert streams, neutral otherwise. It changes the
   statements tests and users see (combined INSERTs), so it could go behind its own experimental flag bit first.
3. **PR #19535**: worth landing as experimental. Its batching cap should adapt to transaction size (small-trx workloads
   regress by up to 25% and use up to 1.8x the target CPU). Combine with change 1.

## Tests

- New, MySQL-backed: `TestPlayerBatchModeBulkUpdate` (unique-key shift in descending order, all column types incl. datetime(3),
  decimal, varbinary, enum, NULLs, multi-byte utf8mb4 strings; checks statements, data and the `BulkQueryCount["update"]` stat).
  **Fails on main** (verified with a test binary built from the unmodified production files).
- New, pure: `TestBuildPlayerPlanBulkUpdate` (eligibility: select *, renamed columns, expressions, aggregates),
  `TestApplyBulkUpdateChanges` (ascending, descending, direction change, PK change, other shapes in order, 100-row cap,
  ineligible types), `TestVPlayerBuffersRowChangesAcrossRowEvents` (inserts of two source transactions combined, flush before
  another shape and before the commit).
- Updated expectations (consecutive single-row inserts of one target transaction are now one statement):
  `TestPlayerSavepoint`, `TestPlayerSplitTransaction`, `TestPlayerTransactions`, `TestPlayerRelayLogMaxSize`.
- The whole `go/vt/vttablet/tabletmanager/vreplication` package passes (MySQL 8.0.46, run as `vt`, both the full and the
  noblob binlog-row-image phases of its TestMain).
- `go vet` clean; `scripts/fmt` run on all changed Go files.

## Environment notes

- Shared 4 vCPU box; load average 3-18. The other investigator's test suites held the bench lock for up to ~20 minutes at a
  time.
- Disk: `wide1` and `sbtest*` grew with insert workloads (the cluster reached 3.5 GB once). `shrink.sql` (with
  `sql_log_bin=0` on both sides while the workflow is stopped, verified by checksum) and `purge-binlogs 5` after every run
  keep it at ~2.5 GB.
- Cluster down; data dir `c40000`, patched binaries, test binaries and the extra worktrees removed. Scripts and run
  summaries remain in `/home/vt/perf/V2` (17 MB).
