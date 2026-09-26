# P4-vreplication: MoveTables / Reshard / Online DDL duration, SwitchTraffic and cut-over stalls, replication lag

Investigator P4, BASE 40000, base commit aa9ccf9. The patch is `findings/P4-vreplication.patch` (uncommitted in worktree
`agent-aeba14a299ec1d673`). Final patched binaries are in `/home/vt/bin-P4`. The harness and drivers are in the patch under
`perf-findings/P4-scripts/` (copied from `/home/vt/perf/P4/`).

## Summary (ranked by user-visible impact)

| # | Change | Measured end-to-end effect | Size | Risk |
|---|---|---|---|---|
| 1 | **Skip the per-table copy-phase catchup when the last copy snapshot is younger than `--vreplication-replica-lag-tolerance`** (`vcopier.go`, `vreplicator.go`) | MoveTables of 40 tables x 5k rows: **47.1 s -> 5.6 s (8.4x)**. Every table after the first cost a fixed ~1.0 s (1 s ticker, or first source heartbeat on an idle source). | S | low |
| 2 | **WaitForPos: poll every 100 ms instead of 1 s, and ask the stream to save a skipped (filtered) position right away** (`engine.go`, `controller.go`, `vplayer.go`, `relaylog.go`) | MoveTables **SwitchTraffic write outage 2.28-2.33 s -> 0.38-0.44 s** (6 vs 6). **Online DDL cut-over write stall 1.15 / 2.25 s -> 0.34 / 0.25 s** when other tables are written. | S-M | low-medium |
| 3 | **Online DDL: re-review a running vreplication migration 5 s later while it is not ready to cut over** (`onlineddl/executor.go`) | ALTER of a 1.5M-row table (copy 25-30 s): **83.6 / 84.7 s -> 38.8 / 41.9 s**. Without it the cut-over waits for the next 1-minute tick. | S | low (a few more review queries per running migration) |
| 4 | **Tablet picker: exponential backoff 1 s -> 30 s instead of a flat 30 s** (`discovery/tablet_picker.go`) | Streams resume **1.2-1.4 s instead of 30.2-30.5 s** after their source became serving (all tablets restarted together). Also used by vtgate VStream and VDiff. | S | low (a few more picker attempts in the first 30 s) |
| 5 | Flag: `vstream-packet-size=1000000` (and dynamic sizing off) for copies | Copy of 4x500k rows: **total CPU per row -8%**, target vttablet CPU -22%, source vttablet -10%, duration -9% (noisy). Dynamic sizing never gets there on its own. | flag | low |
| 6 | vstreamer event pipeline: bounded 16-event queues between the binlog reader, throttler and parser goroutines, and a lazily re-armed heartbeat timer (`binlog_connection.go`, `vstreamer.go`) | Source vttablet CPU per replicated transaction **-12.5%** (106.6 -> 93.3 us/trx, 2 streams). Target vttablet +5-9% in the same runs (not understood, see below). | S | low-medium (memory: large events are only queued alone) |
| 7 | Round-1 patches F05+F16+F17+F26+F27 (measured, not in this patch) | Running phase: source vttablet -6%, target -3.5%. Copy: target vttablet -3%, nothing measurable on duration. | - | see round 1 |

Negative results: parallel insert workers, running-phase apply throughput (the applier already outpaces the source), the lazy
heartbeat timer alone, and larger (4 MB) packets. Details below.

## Setup

- **Cluster** (`P4-scripts/cluster.sh`): one cluster, keyspaces `src` (unsharded, tablet 100) and `dst` (2 shards -80/80-,
  tablets 101 and 102), one primary per shard, durability none, vtgate. `load` fills `src` with 4 sysbench tables x 500k rows
  (2M rows, ~450 MB) directly through the mysqld socket; `dst` only has a hash vschema.
- **Copy**: `MoveTables create --tables sbtest1..4` src -> dst, i.e. two streams, each reading every table from the source and
  filtering its key range on the source. Timed until both streams are `Running` with an empty `copy_state`
  (`P4-scripts/mt.sh`). CPU per process from `cluster.sh cpu`, reported per 1M rows copied.
- **Running phase** (`catchup.sh`): stop the workflow, run 60k `oltp_write_only` transactions directly on the source mysqld
  (8 threads), restart the workflow and time until both targets' `pos` contains the source `gtid_executed`. This drain rate is the
  maximum sustainable rate before lag grows. CPU in us per source transaction.
- **SwitchTraffic** (`switch.sh`): a prober (P5's Go prober, 4 threads x 100 writes/s through vtgate) runs while
  `MoveTables switchtraffic` / `reversetraffic` runs. Metric: the longest gap between successful writes.
- **Online DDL** (`oddl.sh`): `ALTER TABLE ... ENGINE=InnoDB` with `--ddl-strategy vitess` under the same prober on the altered
  table, plus 200 writes/s on another table (`OTHER=1`).
- **Restart recovery** (`srcrestart.sh MODE=all`): 100 trx/s on the source, restart all vttablets, time until the targets have
  caught up with the source position at the moment the restart finished.
- A/B: binaries alternate within each batch (vttablets restarted in place, mysqld kept). The machine was shared with another
  investigator's CPU-heavy benchmark: load average 4-20 during the runs. Durations are noisy; CPU per row/transaction and fixed
  waits are not.

## Baseline

### Copy phase (base binaries, 4 x 500k rows, 2 target shards)

| run | duration | rows/s | src vttablet | src mysqld | tgt vttablet (each) | tgt mysqld (each) |
|---|---|---|---|---|---|---|
| base (profiled) | 16.6 s | 120k | 2.31 s/1M rows | 1.06 | 0.73 | 4.65 |
| base r1 | 17.0 s | 118k | 2.4 | 0.9 | 0.70 | 4.7 |

- **Target mysqld dominates**: ~4.6 CPU-s per 1M rows per shard, 65% of all CPU. Each stream is serial: receive a ~250 KB packet
  (~1100 rows), one bulk INSERT, insert into `copy_state`, commit. Target vttablets are nearly idle.
- The **source vttablet** profile during copy: `rowStreamer.streamQuery` 68%, of which `Plan.shouldFilter` 30% (the hash vindex:
  `crypto/des` 11%, i.e. F17's target), `parseRow`, GC.
- The **target vttablet** profile: `TablePlan.applyBulkInsert` 40%, `appendFromRow`/`encodeBytesSQLBytes2` 23%, syscalls.
- **Timeline** (vttablet log): `Copy of sbtest1 finished 00.075` -> `Copying table sbtest2 01.080`. **Every table after the first
  started exactly 1.0 s after the previous one finished**: 3 s of the 16.6 s.
- `--defer-secondary-keys` (default on) adds 0.3-0.5 s per table for the post-copy `ALTER ... ADD KEY`.

### Running phase (drain of 60k oltp_write_only transactions)

| | drain rate | src vttablet | tgt vttablet (each) | tgt mysqld (each) |
|---|---|---|---|---|
| base pipeline (9 runs) | 6.5k-7.8k trx/s | 106 us/trx (2 streams) | 44 us/trx | 95-110 us/trx |

- The same 60k transactions take 8 sysbench threads 15-25 s to generate directly on the source mysqld (2.3k-3.9k trx/s, ~700 us
  of mysqld CPU per trx). **The applier drains about 2x faster than the source can produce**: the vplayer merges the queued
  transactions into large commits. At this scale, running-phase lag is not a bottleneck.
- **Source vttablet profile** (vstreamer, 2 streams): `runtime.selectgo` 22% cumulative, `lock2/unlock2` 10%, scheduling
  (`schedule`, `park_m`) 8%. Every binlog event crosses two unbuffered channels (reader goroutine -> throttler goroutine ->
  parser) and resets a timer. `parseEvent` itself is 32%.
- **Target vttablet profile**: `ExecuteFetchMulti`/`ReadQueryResult` of the batched statements 24% (a read syscall per statement
  result), event decoding, GC. Target mysqld (~100 us per source trx) is where the running phase spends its CPU.

## 1. Per-table catchup pause in the copy phase

**Cause** (`vcopier.catchup`). Before copying each table (except the first), `copyNext` starts a catchup vplayer and waits until
`ReplicationLagSeconds < --vreplication-replica-lag-tolerance` (1 min). The lag is `MaxInt64` until the vplayer has applied an
event with a timestamp, so the first check always fails, and the loop sleeps on a `waitRetryTime` (1 s) ticker. On an idle source
the first event is the vstreamer heartbeat after 900 ms. So each table costs ~1 s even though the stream is only as far behind as
the previous table's copy took.

**Fix.** The copy phase already bounds the lag: after `fastForward` the stream is at the GTID of a row snapshot taken when
`VStreamRows` started. `vreplicator.lastCopySnapshotTime` (in memory) records that time. If it is younger than the tolerance,
catchup returns immediately, since its exit condition already holds; the next `fastForward` replays the same events it would have.
After a restart (field unset) or a copy longer than the tolerance (e.g. `--vreplication-copy-phase-duration` expired) catchup runs
as before. `TestCatchupSkippedAfterRecentCopySnapshot` covers it.

| workload | base | patched |
|---|---|---|
| 40 tables x 5k rows (200k rows), 2 target shards | 47.16 s, 47.13 s | **5.63 s, 5.68 s** |
| 4 x 500k rows | 19.3 / 16.0 / 14.6 s | 13.4 / 17.7 / 18.5 s (noise > the 3 s saved) |

The remaining per-table cost is ~100 ms: snapshot, `fastForward`, the post-copy `ADD KEY`. For keyspaces with hundreds of tables
(typical for MoveTables of a whole keyspace, and for Reshard), this saves roughly one second per table. It does not change
atomic-copy workflows.

## 2. SwitchTraffic and Online DDL cut-over: WaitForPos

**Cause.** `SwitchTraffic` stops writes on the source, reads the source position and calls `VReplicationWaitForPos` on every
target. Two 1 s timers stack up:
1. `Engine.WaitForPos` polls `_vt.vreplication.pos` on a 1 s ticker.
2. A stream only saves its position after a transaction that touched it. When the last source transactions are empty for it (other
   tables, other shards' rows, `_vt` writes), `vplayer` keeps them as `unsavedEvent` and saves the position only after `idleTimeout`
   (1.1 s). This delay is there so that a stream whose source is its own target (Online DDL) does not spin on its own position
   updates.

In the forward MoveTables to two shards, about half the source transactions are filtered per shard. Hence `Waiting for streams to
catchup` took **2.0 s** in every base run, with writes blocked. ReverseTraffic (one unsharded target, nothing filtered) was already
fast.

**Fix.**
- `WaitForPos` polls every 100 ms (`waitForPosPollInterval`). The query is one PK lookup and only runs while someone waits.
- After an unsuccessful poll, `WaitForPos` asks the stream's controller for a position flush (`requestPosFlush`, a non-blocking
  send on a 1-slot channel). The running vplayer interrupts its relay-log `Fetch` and saves the position of its `unsavedEvent` right
  away, if it has one. The idle-timeout behavior is unchanged otherwise, so no extra writes happen when nobody waits, and the
  Online DDL self-loop is bounded by the 100 ms poll.
- `TestPlayerWaitForPosSavesSkippedPosition` fails on main (30 s timeout) and passes in ~100 ms. `TestWaitForPos` now shortens
  `waitForPosPollInterval`.

| SwitchTraffic write gap (prober, 400 writes/s) | runs | range |
|---|---|---|
| base switchtraffic | 6 | **2.28-2.33 s** (command 2.5-2.8 s) |
| patched switchtraffic | 6 | **0.38-0.44 s** (command 0.55-0.94 s) |
| base / patched reversetraffic | 6 / 6 | 0.29-0.39 s / 0.30-0.36 s |

The patched catchup takes 4-105 ms. The remaining ~0.4 s is stopping source writes (topo + RefreshState, ~50 ms), two LOCK TABLES
cycles with a fixed 100 ms sleep each (`lockTablesCycles`, `lockTablesCycleDelay`, ~235 ms), journals, routing rules.

Online DDL's cut-over also waits for a position (post-sentry, then post-lock with the table locked and queries buffered). With
writes on another table, the post-lock wait was 1.0 s / 2.0 s on base and 0.10 s patched. Client-visible write stall on the altered
table: **base 1.15 s / 2.25 s, patched 0.34 s / 0.25 s**. Without writes to other tables, both are ~0.25 s, because every
transaction then touches the stream.

## 3. Online DDL: cut-over waits for the next 1-minute tick

**Cause.** The executor reviews running migrations on its ticker: at 1, 5, 10 and 20 s after an event such as submission or
start, then every `migrationCheckInterval` (1 min). A vitess migration whose copy completes after the 20 s check is only found
ready at the next 1-minute tick. So total duration is quantized to about 10 s, 20 s, 80 s, 140 s...

**Fix.** In `reviewRunningMigrations`, a running vreplication migration that is not ready to cut over schedules one more review
after `vreplMigrationReviewInterval` (5 s) via `ticks.TriggerAfter`. The existing re-entrancy guard caps ticks at 1/s.
`TestReviewRunningMigrationsReviewsNotReadyMigrationSoon` fails on main.

| ALTER big1 (1.5M rows), prober + other-table writes | total | copy phase | ready after start | cut-over write gap |
|---|---|---|---|---|
| base r1 | 83.6 s | ~30 s | 80 s | 1.15 s |
| base r2 | 84.7 s | ~25 s | 80 s | 2.25 s |
| **patched r1** | **38.8 s** | ~30 s | 37 s | 0.34 s |
| **patched r2** | **41.9 s** | ~30 s | 40 s | 0.25 s |

The review runs a handful of queries (read `_vt.vreplication`, update progress/ETA in `_vt.schema_migrations`), now every 5 s
per running migration instead of every 60 s. A 500k-row ALTER (copy ~9.5 s) took 20.6 s on base, because it just missed the 10 s
check. Those runs overlapped with another driver (harness race, see Environment), so they are not tabulated.

## 4. Tablet picker retry delay

**Cause.** `TabletPicker.PickForStreaming` sleeps a flat 30 s (`tabletPickerRetryDelay`) after any attempt that finds no healthy
serving tablet. A stream that asks while its source vttablet is still starting waits 30 s, even if the source is serving a second
later. Examples: all tablets of a small cluster restart, a whole host restarts, or a source failover where the new primary is not
serving yet at the retry.

**Fix.** The delay starts at 1 s and doubles up to 30 s. `TestPickRetriesSoonAfterNoTablet` fails on main.

| restart all vttablets under 100 trx/s | caught up after the restart completed |
|---|---|
| base | **30.5 s, 30.2 s** (log: `sleeping for 30.000 seconds`) |
| patched | **1.2 s, 1.4 s** (`sleeping for 1.000 seconds`, found 1 s later) |

Restarting only the source vttablet was the same on both, 3.2-3.7 s. The target retries after `--vreplication-retry-delay`
(5 s), when the old source process still answers health checks.

## 5. vstream packet size in the copy phase

`--config-overrides vstream-packet-size=...,vstream-dynamic-packet-size=false`, patched binary, 3 rounds alternating, CPU in
s per 1M rows:

| packet | duration (3 runs) | src vttablet | src mysqld | tgt vttablet (avg) | tgt mysqld (avg) | total CPU |
|---|---|---|---|---|---|---|
| default (250 KB, dynamic) | 14.2 / 11.4 / 15.3 | 2.31 | 0.93 | 0.667 | 4.54 | 13.65 |
| **1 MB** | 13.3 / 10.1 / 13.8 | **2.09** | 0.89 | **0.52** | **4.28** | **12.58 (-8%)** |
| 4 MB | 13.1 / 12.9 / 13.6 | 2.03 | 0.88 | 0.48 | 4.48 | 12.83 (-6%) |

An earlier batch gave the same picture: 1 MB 9.8 / 10.1 / 13.2 s vs 11.4 / 15.0 / 14.7 s; target vttablet -28%, target mysqld -6%.
Fewer, larger batches mean fewer commits and `copy_state` rows. The dynamic packet sizer measures send throughput, not
end-to-end rows/s, and does not grow the packet enough.

**Recommendation.** Consider a 1 MB default for `--vstream-packet-size`. It is a per-stream buffer size, so memory grows by
~0.75 MB per stream. It is compatible across versions: the target sends it as a config override and the source honors it.

## 6. vstreamer event pipeline

Each binlog event is handed from the binlog reader goroutine to the throttler goroutine to the parser loop over **unbuffered**
channels. Each handoff is a goroutine switch (there was a `FIXME(alainjobart)` for this). The parser loop also reset
`hbTimer` for every event.

**Change.**
- Both channels get a 16-event buffer (`binlog.EventQueueSize`).
- `binlog.SendEvent` only queues an event larger than 64 KiB once the queue is empty. The queue holds at most 16 x 64 KiB plus one
  large event, where before it held nothing. Compressed transaction payloads can be huge, so this bound matters.
- The heartbeat timer is no longer reset per event. It is re-armed for the remaining idle time when it fires early.
- `TestSendEvent*` cover the queueing and the large-event rule.

Source vttablet CPU per replicated transaction, 2 streams, 60k-trx drain. Three batches, each alternating the binaries:

| batch | base pipeline | lazy timer only | 16-buffer (unbounded size) | final (bounded queue + lazy timer) |
|---|---|---|---|---|
| hb | 108.7, 109.7, 111.7 | 108.5, 105.0, 108.2 | 91.2, 95.0, 89.2 | - |
| q | 109.5, 102.2, 107.0 | - | - | 99.3, 100.2, 94.7 |
| q2 | 108.2, 105.0, 106.7 | - | 93.8, 96.3, 93.3 | 91.0, 93.0, 95.8 |

The final version saves 12.5% of source vttablet CPU per stream: 106.6 -> 93.3 us/trx for 2 streams in the q2 batch. That is
~6.5 us per transaction per stream, and it applies to every VStream: VReplication and vtgate CDC. In the same runs, the target
vttablets used ~5-9% more CPU (44.0 -> 48.1 us/trx). I did not find the cause; smaller relay-log batches on the target, because
events arrive more smoothly, are a guess. Net CPU is still lower, and more so for workflows with many streams per source. Drain
throughput did not change measurably. **This is the least certain item.**

## 7. Round-1 patches end to end (F05, F16, F17, F26, F27)

They apply cleanly together, and on top of this patch.

| | patched | patched + round 1 |
|---|---|---|
| drain: src vttablet us/trx (3 runs) | 106.3, 104.0, 110.5 | 97.8, 102.7, 100.3 (**-6%**) |
| drain: tgt vttablet us/trx | 43.5, 45.7, 46.0 | 42.0, 43.8, 44.5 (-3.5%, noise level) |
| copy: tgt vttablet s/1M rows | 0.66, 0.66, 0.67 | 0.66, 0.65, 0.62 (-3%) |
| copy: duration | 11.4, 15.0, 14.7 s | 14.9, 14.2, 10.6 s (no difference) |

sysbench tables have no temporal, decimal or JSON columns, so F05 and F18 have nothing to do here. F17 (hash vindex) shows up in
the source copy profile (`crypto/des` 11%). But source vttablet CPU is only 17% of copy CPU, and the target mysqld is the
bottleneck, so its end-to-end effect is small. The source vttablet CPU line was missing from that copy batch (stale pid file), so
F17's copy effect on the source is not measured.

## Negative / neutral results

- **`--vreplication-parallel-insert-workers=4`**: duration 11.1 / 15.2 / 15.4 s vs 11.4 / 15.0 / 14.7 s. Target mysqld CPU +19%
  and target vttablet +36% per row. No gain on 4 vCPUs, where the target mysqld is already the bottleneck. It might help with
  more cores per target.
- **Running phase apply rate**: the vplayer drains 6.5-8k trx/s per 2-shard target, about 2x what 8 sysbench threads generate on
  the source. Transactions are already grouped into one commit per relay-log batch, and position updates are one per batch. I
  found no per-transaction round trips worth removing. The lag risk is the target mysqld's single-threaded apply, which only shows
  on sources much bigger than this box.
- **Lazy heartbeat timer alone**: -2.5%, within noise. Kept because it is part of the final pipeline change.
- **4 MB packets**: no better than 1 MB. Target mysqld CPU went back up and duration was the same.
- **Restarting only the source vttablet**: stream recovery 3.2-3.7 s on both.
- **ReverseTraffic** (unsharded target): already ~0.3 s write gap.
- **Throttler interplay**: the throttler is disabled by default. `--heartbeat-on-demand-duration` had no effect because no
  throttler checks ran. F27 did not measurably change anything.

## Follow-ups

1. **SwitchTraffic LOCK TABLES cycles**: 2 x (LOCK + 100 ms sleep) is now ~60% of the remaining 0.4 s write outage. Worth
   checking whether one cycle, or a shorter delay, is enough.
2. **Online DDL review cadence**: 5 s is conservative. The review could also be triggered by the vreplication engine when a
   stream's copy phase completes.
3. **Target mysqld cost of copy** (~4.3-4.6 us/row) dominates copy CPU: binlogging of the copied rows, statement parsing of 1 MB
   INSERTs. Candidates: prepared or batched inserts via the binary protocol, and larger `copy_state` intervals (one row per packet
   today).
4. **Reshard to N shards** reads and filters every source row N times. A shared per-table source stream that fans out by key range
   would cut source CPU by N.
5. **vtgate took ~40 s to route to a restarted source tablet** in one Online DDL run (17k `no healthy tablet` errors before the
   migration started). That is outside VReplication (health check reconnect backoff), but user-visible.
6. **Explain the +5-9% target vttablet CPU** with the vstreamer queue change: profile relay-log batch sizes and the per-`Fetch`
   timer/goroutine in `relayLog.startFetchTimer`.
7. **VStream CDC throughput through vtgate** (task item 3) was not measured. The source-side pipeline change applies there too.

## Tests

- New, each fails on main: `TestCatchupSkippedAfterRecentCopySnapshot`, `TestPlayerWaitForPosSavesSkippedPosition`,
  `TestReviewRunningMigrationsReviewsNotReadyMigrationSoon`, `TestPickRetriesSoonAfterNoTablet`, `TestSendEventQueuesSmallEvents`,
  `TestSendEventQueuesLargeEventsAlone`.
  - I checked "fails on main" for the first four by reverting only the fix. The last two do not compile on main.
- Passing:
  - full `go/vt/vttablet/tabletmanager/vreplication` (MySQL-backed, run as the `vt` user), `go/vt/vttablet/tabletserver/vstreamer`,
    `go/vt/binlog`, `go/vt/discovery`, `go/vt/vttablet/onlineddl`;
  - `go/vt/vtgate -run TestVStream`;
  - `go vet`;
  - `scripts/fmt` on all changed files.

## Environment notes

- **Load.** `uptime` load average was 4-20 during the batches (the other investigator's point-select runs). Throughput and
  duration numbers vary by up to ±25% run to run, so rely on the fixed waits (1 s, 2 s, 30 s, 60 s ticks) and on CPU per
  row/transaction.
- **Disk.** It filled up once, because catch-up runs produce ~100 MB of binlog each and Online DDL keeps the old table (350 MB for
  big1). The harness now purges binlogs older than 120 s after runs, and `cleanup_artifacts.sh` drops `_vt_*` tables.
- **Harness race.** Two drivers once overlapped, leaving an orphaned vttablet on port 40100 and a stale pid file. That invalidated
  one batch of Online DDL runs, which were redone. The harness now also kills a vttablet by its port, and `bins.sh` prints what
  each tablet runs before every Online DDL run.
- **Shutdown.** The cluster is down and the data dir and test dirs are removed.
