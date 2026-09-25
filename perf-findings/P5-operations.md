# P5-operations: wall-clock time of PRS, ERS/VTOrc failover, and backup/restore

Investigator P5, BASE 40000, commit aa9ccf9. The patch is `findings/P5-operations.patch` (uncommitted in worktree
`agent-a46c57a7422853ad3`). Patched binaries are in `/home/vt/bin-P5`. Scripts are in `findings/P5-scripts/`: a
modified harness copy that adds `DURABILITY` and `REDO_CAPACITY`, a Go write prober, and PRS, failover and backup drivers.

## Setup and method

- **Cluster.** One unsharded keyspace, 1 primary + 2 replicas (`REPLICAS=2`) unless stated otherwise.
  - Durability `none` or `semi_sync` (keyspace `--durability-policy`).
  - vtgate with and without `--enable-buffer`.
  - VTOrc started by hand with its own sqlite file. The harness does not start VTOrc.
- **Client-visible unavailability.** `prober` sends rate-limited primary writes through vtgate: 4 threads, one
  `UPDATE sbtest1 SET k=k+1 WHERE id=?` every 10 ms per thread, about 400 writes/s. It records every error and every
  latency. The main metric is the **max gap between successful write completions**, which is the write outage a client
  sees.
- **Step breakdown.** From vtctld, vttablet, vtorc and MySQL `error.log` timestamps, and from the event stream of
  `vtctldclient Backup/RestoreFromBackup`.
- **Load.** The machine was shared with P1's CPU-heavy point-select runs. Load average ranged from 1 to 17 and is noted
  per batch.
  - Most results here are dominated by sleeps, ticks and MySQL-internal timeouts, not by CPU, so they are stable
    despite the load.
  - The one exception is backup copy throughput, which is CPU-bound and noisy.
- **Environment quirk.** The worktree guard refuses `runuser`, so clusters run as `vt` through
  `setpriv --reuid=vt --regid=vt --init-groups`; see `P5-scripts/c.sh`.

## Summary of findings (ranked by user-visible impact)

| # | Change | Kind | Measured effect | Size |
|---|---|---|---|---|
| 1 | Do not disable primary-side semi-sync in DemotePrimary for PRS (`rpc_replication.go`) | code | PRS write outage with semi_sync and 3 tablets: **1.16 s → 0.21 s** (mean of 12 vs 12, interleaved) | S |
| 2 | VTOrc: `--instance-poll-time=1s` + fix the "+1 s" poll-period bug (`instance_dao.go`, `vtorc.go`) + `--recovery-poll-duration=200ms` | flag + code | dead-primary write outage: default **~4–7 s (mean 4.9)** → base+1s poll **3.5 s** → patched+1s **2.5 s** → patched+1s+200ms **1.7 s** | S |
| 3 | Close Vitess' own pooled MySQL connections before `mysqladmin shutdown`; poll mysqld socket every 100 ms (`mysqld.go`) | code | backup **−2.3 s**, restore **−3.3 s** of fixed overhead per operation (6.4→4.1 s and 10.6→7.2 s on 850 MB) | S |
| 4 | `--compression-engine-name=zstd` for builtin backups | flag | backup CPU −52%, copy phase −52%, restore copy −35%; output 3% smaller than gzip | flag |
| 5 | F07 pooled pargzip, measured end to end | code (round 1) | backup CPU −23%, copy phase −30%, byte-for-byte same format | S–M |
| 6 | vtgate `--enable-buffer` (the harness has it off) | flag | PRS goes from 30–80 client errors in a 70–190 ms window to 0 errors with a 60–230 ms latency blip | flag |

## 1. PlannedReparentShard

### Baseline, durability `none`

| | PRS command | write gap | errors |
|---|---|---|---|
| no buffering (5 runs) | 0.26–0.39 s | 78–127 ms | 29–44 per PRS |
| `--enable-buffer`, PRS ≥ 60 s apart (4 runs) | 0.27–0.40 s | 60–230 ms | **0** |
| `--enable-buffer`, back-to-back PRS | 0.27–0.37 s | 95–225 ms | 33–80 |

**Back-to-back PRS with buffering.** `--buffer-min-time-between-failovers=1m` skips buffering for a second failover
within one minute. That run returns *"primary is not serving, there may be a reparent"* errors, and the error window can
be longer (up to 191 ms) than without buffering.

**Breakdown**, from vtctld and tablet logs:

| step | duration |
|---|---|
| lock + read tablets + GetGlobalStatusVars on all tablets (parallel) | 20–30 ms |
| SetReplicationSource + wait on the primary-elect | ~40 ms |
| DemotePrimary | ~10 ms |
| WaitForPosition | ~10 ms |
| PromoteReplica: STOP, RESET REPLICA ALL, FLUSH BINARY LOGS, super_read_only off, `syncSidecarDB` ~20 ms, state transition | 30–70 ms |
| replicas' SetReplicationSource (parallel) | — |

- vtgate sees the new primary within ~15 ms of it serving.
- The replica RPCs are already parallel, and I found no fixed sleeps on the path.
- The harness' `--health-check-interval 5s` does not matter, because tablet state changes are broadcast immediately.

### Durability `semi_sync`: a 1 second outage from one MySQL statement

With `semi_sync`, the PRS write outage was consistently **~1.1 s**.

Timestamps on the demoted primary:

```
21:39:09.646 exec SET GLOBAL rpl_semi_sync_source_enabled = 0, GLOBAL rpl_semi_sync_replica_enabled = 1
21:39:10.634 reading primary status          <- 988 ms later
```

**Root cause (MySQL).** `SET GLOBAL rpl_semi_sync_source_enabled=0` stops the semi-sync ACK receiver thread.
- That thread `poll()`s the replica sockets with a 1 s timeout and only checks for the stop request when the poll
  returns.
- DemotePrimary has already stopped all writes, so no ACK arrives to wake it. The statement therefore blocks until the
  1 s timeout expires.
- It runs while the shard has no writable primary, so it is on the critical path.

Reproduced directly with `P5-scripts/semisync_disable_timing.sh`:

| where | `SET ... source_enabled = 0` takes |
|---|---|
| primary with 2 semi-sync replicas, idle 300 ms before the SET | 700 ms (1 s − 300 ms) |
| server with no semi-sync clients | ~1 ms |

**Fix** (`go/vt/vttablet/tabletmanager/rpc_replication.go`). `demotePrimary` with `force=false` (the PRS paths) no longer
disables primary-side semi-sync. The forced path (ERS, stale primary) is unchanged.

**Why this is safe:**
- MySQL is already `super_read_only`, so no commit can wait for an ACK.
- Every non-force caller follows up in one of two ways:
  - PRS calls `SetReplicationSource` on the old primary. `setReplicationSourceLocked` → `fixSemiSync(REPLICA)`
    disables source-side semi-sync *before* `START REPLICA`.
  - A failed PRS calls `UndoDemotePrimary`, which needs semi-sync enabled anyway.
- The semi-sync monitor stays open until then. On a read-only server it only reads status.
- If a PRS dies between the two calls, the tablet is left in a *more* consistent state than before: still type
  PRIMARY in topo, with primary semi-sync on.
- **N-1/N+1.** An old vtctld with a new vttablet, or a new vtctld with an old vttablet, both run the same RPC sequence,
  so the change is compatible.

**Doc and test.** The doc comment of `DemotePrimary` is updated. `TestDemotePrimaryPrimarySideSemiSync` checks that
force=false leaves semi-sync on and force=true disables it. It fails on main for force=false.

**A/B**: semi_sync, no buffering, 6 back-to-back PRS per cluster, clusters alternated base/P5/base/P5. Load average
12–17.

| | write gap per PRS (s) | mean | PRS command |
|---|---|---|---|
| base r1 | 1.31 1.25 1.22 1.10 1.11 1.11 | | 1.45–1.64 s |
| **P5** r1 | 0.24 0.23 0.35 0.15 0.13 0.23 | | 0.42–1.43 s |
| base r2 | 1.22 1.15 1.19 1.08 1.08 1.15 | **1.16 s** | 1.26–1.55 s |
| **P5** r2 | 0.43 0.28 0.24 0.08 0.09 0.08 | **0.21 s** | 0.27–1.28 s |

- An earlier non-interleaved set gave the same picture: base 1.06–1.18 s, P5 0.12–0.30 s.
- **The PRS command itself is not always faster.** The ~1 s wait moves to the old primary's `SetReplicationSource`,
  which PRS still waits for. It no longer blocks client writes, because the other replica ACKs the new primary.
- **Two-tablet shards** (1 primary + 1 replica, semi_sync) behave the same before and after: base 1.05–1.20 s, P5
  1.05–1.23 s.
  - The old primary is the only possible ACKer, so the new primary's writes wait for it to attach, and it attaches
    ~1 s later.
  - It is not a regression. Fixing that case needs `SetReplicationSource` to enable replica-side semi-sync and start
    replication first, and disable source-side afterwards. That is riskier, so it is left as a follow-up.

**Remaining semi_sync PRS outage (~100–250 ms).** Mostly PromoteReplica:
- `FLUSH BINARY LOGS` takes 20–150 ms here, because MySQL flushes the InnoDB redo log on rotation and the harness runs
  `innodb_flush_log_at_trx_commit=2` on a shared disk.
- `syncSidecarDB` takes ~20 ms.

## 2. Dead primary: manual ERS and VTOrc

**Manual ERS is fast.** VTOrc-driven ERS takes 0.13–0.25 s from "DeadPrimary" to "finished EmergencyReparentShard":
stop replication on all replicas, pick the most advanced, promote, reparent. **Detection dominates.**

**Scenario.** `kill -9` of the primary's vttablet, mysqld_safe and mysqld, which emulates host death. `mysqld_safe`
alone would restart mysqld. semi_sync, 3 tablets, `--enable-buffer`, 400 writes/s. The kill time is randomized over
0–6 s relative to VTOrc's poll phase.

### Where the time goes (VTOrc log)

**Default `--instance-poll-time=5s`:**
1. Wait for the next poll of the tablets. **The effective period is 6 s, not 5 s.**
2. The primary's `FullStatus` does not fail. tmclient dials with `WaitForReady`, so it hangs for 15 s
   (`topo.RemoteOperationTimeout`). After 1 s, the `lastAttemptedCheckTimer` marks the check as attempted, which makes
   `LastCheckValid` false.
3. Up to 1 s passes before the next `--recovery-poll-duration` tick yields `DeadPrimary`.
4. ERS takes ~0.2 s.
5. vtgate unbuffers immediately.

The replicas' polls in the same tick already show their IO threads stopped. The replica-side `recheckPrimaryHealth` →
`DiscoverInstance(primary, force)` also hangs 15 s on WaitForReady and contributes nothing.

### Poll-period bug: "+1 s"

`last_checked` is a second-resolution sqlite `DATETIME`, and three checks were off by one:
- **`ReadOutdatedInstances`** requires `last_checked < now - poll`, which is strict.
- **`IsUpToDate`** is `seconds_since <= poll`.
- **The dedup cache in `DiscoverInstance`** expires exactly `poll` after the previous attempt, which is a few ms after
  the next 1 s tick.

**Effect.** Every instance is polled every `poll+1` s:

| `--instance-poll-time` | actual poll period |
|---|---|
| 5 s (default) | 6 s |
| 1 s (the value all `examples/` use) | 2 s |

Discovery timestamps in the logs confirm it. Fixing only the first check made things *worse* (10–11 s periods, because
the up-to-date check and the cache then skip the poll). All three have to change together.

**Fix.** `<=` in `ReadOutdatedInstances`, `<` in `IsUpToDate`, and a dedup window of `poll − min(poll/2, 500ms)`.
- The 1 s health tick still keeps the period ≥ poll.
- After the fix, the measured periods are exactly 5 s and 1 s.
- Tests: new `TestReadInstanceIsUpToDate` and a new `TestReadOutdatedInstances` case, both failing on main. The rest of
  the `vtorc/...` tests pass.

### A/B: dead primary, write outage in seconds

Trials were interleaved A/B/C/D/E across 3 rounds. Batch 2 used the same method.

| config | samples | mean |
|---|---|---|
| A: base, default 5 s | 3.85, 3.53, 4.94, 7.40 | **4.9 s** (model: 4.65) |
| B: base, `--instance-poll-time=1s` | 3.57, 3.92, 3.15, 3.18 | **3.46 s** |
| C: **patched**, 1 s | 2.33, 2.56, 2.55 | **2.48 s** |
| D: patched, default 5 s | 5.14, 5.79, 3.31 | 4.7 s (model: 4.15; too few samples) |
| E: **patched**, 1 s + `--recovery-poll-duration=200ms` | 1.60, 1.93, 1.61 | **1.71 s** |

**Model.** Outage ≈ U(0, period) + 1 s attempted-check timer + U(0, recovery tick) + ~0.2 s ERS. The measurements
match it.

**Notes on the configs:**
- **Recommended config (E): −65% vs default** (4.9 → 1.7 s).
- **Code fix alone (C vs B): −1.0 s** (3.46 → 2.48 s).
- An earlier batch with a non-random kill phase gave A ≈ B ≈ 3.9 s, because both configs landed at the same poll phase.
  That is why the kill time is randomized.

**Safety of the flags:**
- `--instance-poll-time=1s` means 5× more `FullStatus` RPCs, one per tablet per second.
- `--recovery-poll-duration=200ms` runs the sqlite analysis query 5× more often.
- Both are cheap at this size, but the cost scales with the number of tablets per VTOrc.
- Changing the defaults would need release notes. A default of 1–2 s poll and 500 ms recovery tick looks reasonable.
  All the `examples/` already use 1 s.

**Remaining fixed cost: the 1 s `lastAttemptedCheckTimer`.**
- Lowering it risks false `UnreachablePrimary` restarts on slow polls.
- **Follow-up:** poll with fail-fast (`WaitForReady(false)`) for VTOrc's `FullStatus`. A refused connection, meaning the
  process died on a live host, then marks the primary invalid in ms instead of 1 s. The tmclient API has no per-call
  option, so this needs plumbing.

## 3. Backup and restore (builtin engine)

**Disk limits the dataset.** The disk had only 1.7–3.9 GB free (13 GB go build cache, shared), so the dataset is
4 × 500k rows: 570 MB of tables + 256 MB redo (`REDO_CAPACITY=256M`) + ibdata/undo, **~850 MB** of backup input.
- Backups are taken from the replica with the default `--concurrency 4` to file storage, then restored in place with
  `RestoreFromBackup`.
- The measured times are vtctldclient wall time and the vttablet CPU delta.
- The phases come from the event stream (`P5-scripts/phases.sh`).

**Fixed overhead dominates at this size.**

Backup, base, 6.4 s total:

| phase | time |
|---|---|
| mysqld shutdown | 3.0 s |
| copy + compress | 2.3 s |
| mysqld start | ~0.5–1 s |
| replication restart / wait | ~0.5 s |

Restore, base, 10.6 s total:

| phase | time |
|---|---|
| shutdown | 4.0 s |
| copy | 1.3 s |
| start (skip-grant) + `mysql_upgrade` + shutdown + start | 5.0 s |

### 3a. mysqld shutdown waits 2 s for Vitess' own idle connection

MySQL `error.log` during a backup:

```
22:57:17.534 Received SHUTDOWN from user vt_dba
22:57:19.537 Forcing close of thread 13  user: 'vt_dba'      <- exactly 2 s later
22:57:20.751 Shutdown complete                               <- InnoDB 1.2 s
(mysqladmin polls the pid file once a second -> returns at 21.536)
```

**Cause.** MySQL gives connected clients a 2 s grace period, then force-closes them. A processlist trace during the
backup (`P5-scripts/pl_watch.sh`) shows the forced thread was an **idle, db-less `vt_dba` connection held by
`mysqlctl.Mysqld`'s own dba pool**. The tabletserver pools are already closed for BACKUP/RESTORE.

**Fix** (`go/vt/mysqlctl/mysqld.go`):
- `executeShutdown` now calls `closePooledConnections` before the shutdown hook / mysqladmin. It sets the capacity of
  the dba and app pools to 0 and back, with at most 1 s of waiting for borrowed connections. The pools stay open and
  reconnect on demand.
- `Mysqld.wait` (after Start) polls the socket every 100 ms instead of every 1 s.
- Test: `TestClosePooledConnections` (fakesqldb). There are no more "Forcing close" warnings.

**Effect.** Backup and restore were alternated with 6 configs × 3 rounds.

| | base | P5 | Δ |
|---|---|---|---|
| backup wall (s) | 6.36, 6.43, 6.48 | 4.17, 4.06, 4.13 | **−2.3 s (−36%)** |
| backup shutdown phase | 3.02 | 1.02 | −2.0 s |
| restore wall (s) | 10.69, 10.50, 10.50 | 6.70, 7.04, 7.92 | **−3.3 s (−32%)** |
| restore phases | shutdown 4.02; start/upgrade/restart 5.0 | shutdown 2.02; start/upgrade/restart 3.3–4.5 | |

CPU does not change. For large datasets this is a fixed −2 s to −3.5 s per operation, so it matters most for small
shards, tablet provisioning, and tests.

**Other small sleeps (not changed):**
- `WaitForReplicationStart` polls every 1 s, and the first check usually sees the IO thread "Connecting".
- The post-backup "wait for position to move" loop sleeps 1 s.
- mysqladmin's own 1 s pid-file poll (only fixable by sending SHUTDOWN over SQL).

### 3b. Compression engines and F07

`--compression-engine-name` is a vttablet flag, so the vttablet was restarted per config. Load average 1–10.

| config | backup CPU (s) | backup copy phase (s) | restore copy (s) | restore CPU (s) | size |
|---|---|---|---|---|---|
| base pargzip | 7.45 7.69 7.90 (**7.68**) | 2.23 2.28 2.35 | 1.48 1.28 1.31 | 4.6 | 214 MB |
| F07 pooled pargzip | 5.29 6.00 6.35 (**5.88, −23%**) | 1.50 1.65 1.83 (−30%) | 1.19 1.22 1.12 | 4.0 | 213 MB |
| pgzip | 5.80 5.64 5.89 (5.78, −25%) | 1.61 1.56 (3.09 outlier) | 1.13 1.30 1.37 | 4.3 | 213 MB |
| **zstd** | 3.71 3.73 3.68 (**3.71, −52%**) | **1.10 1.09 1.11 (−52%)** | 0.85 0.86 0.91 (−35%) | 2.8 | **207 MB** |
| lz4 | 4.51 4.63 3.86 | 1.33 1.35 (2.42) | 0.70 0.57 0.99 | 2.1 | 386 MB |

Base copy throughput is ~370 MB/s at ~2.7 cores, so the copy phase is CPU-bound on this box.

**F07.** The round-1 per-GB CPU saving (−25…−43%) is confirmed end to end, with an unchanged format.

**zstd:**
- Halves backup CPU and time and produces slightly smaller output. The restore side reads the engine from the manifest.
- Supported since v15, so it is N-1/N+1 safe.
- As a default change, it would alter the backup file format and extension, and memory behaviour. That is a
  deprecate/announce item. As a per-deployment flag, it is the biggest backup-speed lever I measured.

**Per-file parallelism (inconclusive).**
- The test used `--concurrency=1` to emulate one dominant table, and `--backup-storage-number-blocks` 2 vs 4. Machine
  load was 5–10 at the time, and results varied 3.4–6.6 s run to run, so it is inconclusive.
- By design, a single huge `.ibd` compresses with at most `number-blocks` (2) cores, and restores serially.
- `--builtinbackup-file-chunk-threshold` (opt-in, not N-1 restorable) is the existing answer.

## Negative / neutral results

- **PRS without semi-sync is already efficient.** Replica RPCs are parallel and there are no fixed sleeps. The harness
  `--health-check-interval 5s` and `--heartbeat-on-demand-duration` play no role in PRS latency.
- **The semi-sync fix does not help 2-tablet shards** (see above). It does not hurt them either.
- **The first attempt at the VTOrc poll fix was wrong.** Changing only `ReadOutdatedInstances` to `<=` made polling
  10–11 s. That is recorded here because the three places are coupled.
- **Backup `--concurrency=8` and `number-blocks=4`** gave no measurable gain on 4 vCPU under load.
- **`FLUSH BINARY LOGS` in PromoteReplica** (20–150 ms) is a harness artifact of `innodb_flush_log_at_trx_commit=2`,
  not a Vitess issue.
- **Manual ERS** takes ~0.2–0.25 s. Nothing to gain there.

## Follow-ups

1. **VTOrc fail-fast `FullStatus` polls** (WaitForReady off): removes the 1 s attempted-check delay when the tablet
   process is gone. After a replica reports its IO thread stopped, trigger the analysis immediately instead of on the
   next tick.
2. **Semi-sync PRS in 2-tablet shards:** split `fixSemiSync` in `SetReplicationSource` for an ex-primary. Enable
   replica-side + `START REPLICA` first, and disable source-side after. The ACK comes from the IO thread, so the new
   primary is unblocked ~1 s earlier. Needs care with applier commits.
3. **VTOrc defaults:** consider `--instance-poll-time` 1–2 s and `--recovery-poll-duration` 500 ms. Measure VTOrc CPU
   with hundreds of tablets first.
4. **Backup tail sleeps:** `WaitForReplicationStart` and the position loop could poll at 100 ms. Sending `SHUTDOWN` via
   SQL instead of mysqladmin would save ~0.5 s per shutdown.
5. **Larger-dataset backup runs** (≥ 5 GB) on a machine with free disk, to quantify zstd and F07 wall-clock under
   single-big-table layouts and with chunking.

## Environment notes

- `uptime` load average during the batches:

  | batch | load average |
  |---|---|
  | VTOrc | 1–10 |
  | backups | 1–10 |
  | PRS A/B | 2.6–17 |

- The disk was at 90–98% the whole time. I kept the dataset small, purged binlogs after prepare, and deleted the
  backups after each run.
- `/home/vt/bin-P5` holds the patched vttablet, vtorc and mysqlctl. The other binaries in it are base plus the patch as
  of their build time. My clusters are down.
