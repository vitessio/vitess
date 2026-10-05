# Unplanned failover audit: 3 cells, `cross_cell`, VTOrc

Audit of unplanned-failover correctness at HEAD `83eb933`. The reference deployment is one shard with a primary and two replicas, each tablet in a different cell, durability policy `cross_cell` (1 semi-sync ACK from another cell), and one VTOrc per cell. Single-cell VTOrc placement is covered at the end.

Method: code review of VTOrc, `reparentutil` (ERS/PRS), the tablet manager, semi-sync handling, vtgate healthcheck and topo locking; unit reproductions with the existing fakes; MySQL 8.0.46 experiments; and an end-to-end chaos harness (`go/test/endtoend/vtorc/chaos/`) that runs real MySQL, etcd (global plus one etcd per cell), vttablet, VTOrc and vtgate with fault injection (kills, SIGSTOP, cgroup-scoped iptables partitions).

Status legend: **E2E** reproduced on a real cluster; **UNIT** reproduced with fakes; **MYSQL** reproduced on MySQL 8.0.46; **CODE** unambiguous code path; **PLAUSIBLE** strong code evidence, not demonstrated.

## 1. What the setup actually guarantees

With one tablet per cell, `cross_cell` behaves exactly like `semi_sync`: every other REPLICA is an eligible acker, and a failover never flips a replica's semi-sync flag.

- **Acked writes survive a successful ERS, provided the ACK is still backed by a replica's relay log.** When the primary is unreachable, ERS proceeds only if *both* replicas answer (`haveRevoked`, `durability_funcs.go`), so the reached set overlaps every possible acker. It then promotes the most advanced tablet by `Retrieved ∪ Executed`, and waits for its relay log to apply. This held in every E2E and UNIT scenario (S1, S2, S5, S7, S10).
- **The ACK guarantee depends on the replica never discarding unapplied relay-log events.** Several routine operations do exactly that (section 2). This is the one confirmed data-loss path.
- **Automatic failover tolerates exactly one node loss.** Primary plus one replica down, or a replica stopped for maintenance when the primary dies, means ERS refuses (correctly) and waits.
- **Failover requires every cell-local topo to be readable.** A whole-cell outage that takes the cell's topo with it blocks failover entirely (section 3, B).
- **Writes on an isolated old primary block on semi-sync**, but only because the shipped `my.cnf` sets `rpl_semi_sync_source_timeout=1e18` and `wait_no_replica=1`. vttablet never sets or checks these.
- **Reads are not fenced.** An isolated primary keeps serving reads to vtgates that can reach it for the whole partition. After heal, `shard_sync` demotes it within about 25 ms if its watch is alive, or up to 30 s (`--shard-sync-retry-delay`).
- **Old primaries with unacked commits cannot silently rejoin**, except via the race in section 3, A.

## 2. Acked-write loss through relay-log discard (the reported incident)

A semi-sync ACK means the replica wrote the event to its relay log, not that it applied it. If the relay log is discarded before the applier executes those events, the transaction disappears from the replica's `Retrieved_Gtid_Set` and `gtid_executed`. ERS cannot see it. If the primary is gone before the replica fetches it again (about 15 ms when the primary is up), the acked write is lost and ERS reports success.

### What discards unapplied relay-log events (MYSQL, 8.0.46, Vitess settings)

| Operation | Where Vitess does it | Discards |
|---|---|---|
| mysqld start with `relay_log_recovery=1`, after any stop (graceful, `kill -9`, or Vitess's shutdown preparation) | every shipped `config/mycnf/mysql*.cnf` sets it (e.g. `mysql8026.cnf:13`); `prepareReplicaForShutdown` (`go/vt/mysqlctl/replication.go`) stops the threads but does not drain the applier | yes |
| `STOP REPLICA; CHANGE REPLICATION SOURCE TO …`, even with identical parameters | `Mysqld.SetReplicationSource` (`go/vt/mysqlctl/replication.go:1068-1089`), `setReplicationSourceCommand` (`go/mysql/flavor_mysql.go:556-586`). VTOrc's `fixReplica` always passes a non-zero heartbeat (`topology_recovery.go:1521`), so it always takes this path, even for the same source | yes |
| `RESET REPLICA` / `RESET REPLICA ALL` | self-heal paths `handleRecoverableReplicationInitError` and `setReplicationSourceRecoverable` (`rpc_replication.go:1414-1430`, `:1370-1372`); `Promote` on the new primary (safe: ERS/PRS wait first) | yes |
| `STOP REPLICA IO_THREAD` then `CHANGE REPLICATION SOURCE TO` with receiver options only, applier **running** | not used today | **no**, even when switching to a source that lacks the events (verified twice); a partial transaction at the relay-log tail is dropped and re-fetched whole, or dropped cleanly if the new source lacks it (it was never ACKed) |
| Same receiver-only CHANGE with the applier **stopped** (both threads stopped) | — | **yes** — MySQL deletes relay logs whenever both threads are stopped |
| `WAIT_FOR_EXECUTED_GTID_SET(Retrieved)` with IO stopped, then STOP + CHANGE | not used today | **no** |
| `relay_log_recovery=0` restart (graceful or `kill -9`) | not the shipped default | **no**, but a relay log cut mid-event (power loss, full disk) stops the applier permanently |

`RELAY_LOG_FILE`/`RELAY_LOG_POS` cannot be combined with auto-position (`ERROR 1776`). The exact Vitess CHANGE command is refused while the applier runs because of `SOURCE_AUTO_POSITION=1` (`ERROR 3081`); dropping that option (it is already persisted) makes the receiver-only form work.

### End-to-end reproduction (E2E)

The harness makes R1 the only acker (R2 partitioned from P) and holds R1's applier back (`SOURCE_DELAY`).

- **S11 / S11k, replica restart.** Before the restart R1 had `Retrieved=…:1-1326` and `Executed=…:1-826`. After a clean `mysqlctl` restart (or `kill -9`) it had `Retrieved=""`. P was killed. VTOrc ran ERS in 0.4-2 s, logged that the candidate "applied all of its received relay logs" after `WaitForPosition(…1-826)`, and promoted R1. All 500 tagged acked rows were missing, with no error. The dead primary still holds them as errant GTIDs.
- **S11c, VTOrc `fixReplica`.** Disabling semi-sync on R1 triggers `ReplicaSemiSyncMustBeSet` → `fixReplica` → `STOP REPLICA; CHANGE …; START REPLICA`. The CHANGE purged R1's relay log. With R1 unable to reach P's MySQL port at that moment, it could not re-fetch, P died, and 500 acked writes were lost. With P reachable (S11b), the data was re-fetched and nothing was lost.

Realistic triggers: a lagging replica is restarted during an incident (operator, OOM, Kubernetes liveness probe, rolling restart) while the primary is failing, or VTOrc repairs a replica while the primary is flapping. The exposure is largest when R2 is already down or lagging, because R1 is then the only holder of recent ACKs.

### What to do

1. **Configuration (available now):** on replicas, `relay_log_recovery=0` and `sync_relay_log=1`. Restarts then keep and apply ACKed events (MYSQL, 7 runs). `sync_relay_log=1` makes an ACK mean the event is on disk, which matters for OS crashes. The remaining risk is a torn tail after power loss or a full disk, which stops the applier. That is safely self-healable: by the time the applier stops, everything before the tear is applied, so a `RESET REPLICA` and re-fetch then loses only the torn, never-ACKed event. vttablet can do this automatically on `MY-013121`. Cost: an fsync per relay-log event on replicas, which adds to primary commit latency under semi-sync. Changing the shipped default must be staged across releases.
2. **Repoint without discarding (code):** in `setReplicationSourceLocked`, when auto-position is already configured, stop only the receiver and run `CHANGE REPLICATION SOURCE TO` with receiver options only (without `SOURCE_AUTO_POSITION`, which forces both threads to be stopped), leaving the applier running. This closes the `fixReplica` window and makes tablet-startup, ERS/PRS and `shard_sync` repoints non-destructive. MySQL documents the rule: "Relay logs are preserved if at least one of the replication applier thread and the replication I/O (receiver) thread is running." Limits:
   - It only works while the applier is running. If the applier is stopped (for example on a SQL error that `START REPLICA SQL_THREAD` cannot clear), any CHANGE discards the relay log, and `RELAY_LOG_FILE`/`RELAY_LOG_POS` cannot be combined with auto-position. In that case only proceed if the new source's `gtid_executed` contains the replica's full `Retrieved_Gtid_Set` (the existing errant-GTID check excludes the old primary's UUID, so it does not cover this); otherwise refuse and alert.
   - The applier could stop between the check and the CHANGE; re-check `Retrieved_Gtid_Set` afterwards and alert on shrinkage.
   - First-time setup (no replication configured, or after `RESET REPLICA ALL`) still needs the full command, but then there is no relay log to lose.
   - Verified on MySQL 8.0.46 only; 8.4 and 5.7 need the same test, and MariaDB has different semantics and needs its own flavor handling.
3. **Drain before unavoidable discards (code):** before the `RESET REPLICA` self-heal paths and in `prepareReplicaForShutdown`, stop the receiver and wait, bounded, for the applier to execute `Retrieved_Gtid_Set`, and fail loudly if it cannot. This makes planned replica restarts safe even with `relay_log_recovery=1`.
4. **Fail closed instead of losing silently (optional):** persist each replica's retrieved-GTID high-water mark (tablet-side at shutdown and periodically, or VTOrc-side from polls). If a replica restarts having dropped ACKed-but-unapplied GTIDs, it reports that it cannot vouch for its ACKs, and ERS treats it as an unreached acker, refusing and alerting instead of promoting.

Recommendation: 1 and 2, plus 3 for the self-heal and shutdown paths. Together they close every loss path reproduced here. After an incident caused by `relay_log_recovery`, the old relay files often remain on disk for a while, and the lost transactions may be recoverable manually with `mysqlbinlog`.

### Later findings on the restart path

- **`relay_log_recovery=ON` discards on every startup, not only after corruption** (MYSQL, source). MySQL 8.0's "Handling an Unexpected Halt of a Replica" says the setting "ignores the existing relay log files, in case they are corrupted or inconsistent" and "starts a new relay log file and fetches transactions from the source beginning at the replication SQL thread position". The corruption wording is the reason to enable it, not a condition. A clean `mysqladmin shutdown` and restart logs `[MY-010539] Recovery from source pos …` and empties `Retrieved_Gtid_Set`. Vitess's own shutdown preparation (`sync_relay_log=1`, `FLUSH RELAY LOGS`) does not change this.
- **`GTID_ONLY=1` does not help** (MYSQL, source). In the 8.0.46 source, `Relay_log_info::rli_init_info` (`sql/rpl_rli.cc` ~1784) calls `init_recovery` whenever recovery is on, and `recover_relay_log` (`sql/rpl_replica.cc:1095`) skips only the file/position bookkeeping for `GTID_ONLY` channels before it always starts a fresh relay log and clears the retrieved GTID set. With `GTID_ONLY=1`: recovery on logs `[MY-013836] Relay log recovery on channel with GTID_ONLY=1` and lost all three acked transactions; recovery off kept and applied them. The 8.0.46 replication source has no relay log sanitization, which matches the torn-tail result.
- **Turning recovery off needs an ERS change first** (E2E, S13 T0: 4 of 4 runs). With `relay_log_recovery=0` and Vitess's `skip_replica_start`, a replica that restarts with unapplied relay log comes up with its applier stopped (vttablet only starts replication at startup if it can reach the primary). If the primary is dead, ERS picks that replica as the most advanced and waits for an applier that never runs: `failed to apply relay logs: DEADLINE_EXCEEDED` every 30 s, no failover in 120 s, writes down about 150 s until the old primary came back. ERS only uses `StopReplicationAndGetStatus` in `IOTHREADONLY` mode (`reparentutil/replication.go:471`), and vttablet returns early in that mode without touching a stopped applier (`rpc_replication.go:1116-1125`). Fix: in that mode, start the applier (`START REPLICA SQL_THREAD`) when it is stopped and there are received but unapplied GTIDs. That is safe with a torn tail: the applier stops exactly after the last complete transaction.
- **VTOrc already heals a torn relay log** (E2E, S13, `relay_log_recovery=0`, `sync_relay_log=1`, fix-branch binaries). A stopped applier is `ReplicationStopped` (or, right after a mysqld restart, `ReplicaSemiSyncMustBeSet`, because the semi-sync flag is not persisted) → `fixReplica` under the shard lock → `SetReplicationSource`. Tears in the last (never acked) transaction and in about 200 already acked transactions were healed in 2–6 s with 0 acked writes lost and identical checksums, on fix-branch and main binaries alike. MySQL leaves the torn transaction and everything after it out of `Retrieved_Gtid_Set`, so the fix branch's loss check passes, and it also cannot see what was torn.
- **`sync_relay_log=1` is required** (E2E, S13 T3). When the tear cut into acked transactions, R1 was the only acker and the primary died before the repair, ERS promoted R1 in about 1 s and 201 and 202 acked writes were lost without any error; the only later signal was the returning old primary's errant GTIDs.
- **VTOrc has no backoff for applier data errors** (E2E, S13 T4). A duplicate-key error made the three VTOrcs run `fixReplica` 58–59 times a minute, each logged as successful and each re-downloading a growing backlog. A distinct analysis for relay-log corruption (`MY-013121`, `MY-010818`) and backoff with an alert for data errors are missing.

### Status of the fix

Branch `claude/preserve-relay-logs-on-repoint` implements item 2: `repointReplication` in `go/vt/vttablet/tabletmanager/relay_log.go` stops only the receiver and changes only receiver options for MySQL 8.0+ replicas with auto-position, refuses discards that would lose transactions the new source lacks (including the `RESET REPLICA` self-heal paths), and adds the `--replication-preserve-relay-logs` kill switch (default on). Chaos S11c went from 500 acked writes lost to 0; S11/S11k (restart path) still lose, as expected. Open items on that branch: the tablet-startup path still discards other servers' relay-log transactions that the new source lacks with only a warning (exercised by S12 d2), the loss check can refuse transiently because of the source's own in-flight transactions, and MySQL 8.4 is untested. The receiver-only repoint keeps a replica's `SOURCE_DELAY` and its delay semantics (MYSQL, `verify_delayed_replica.sh`).

### Rolling this out to mixed-version clusters

The changes have to work while vttablet, VTOrc and vtctld run different versions. That holds if the fix lives in vttablet, inside RPCs that already exist, and `relay_log_recovery=0` is only enabled on tablets that already run that vttablet:

| Combination | Result |
|---|---|
| Old ERS caller (VTOrc or vtctld), new vttablet with `relay_log_recovery=0` | Works: the new vttablet starts the applier inside `StopReplicationAndGetStatus(IOTHREADONLY)`, which every ERS version calls |
| New ERS caller, old vttablet with `relay_log_recovery=1` | Unchanged: the relay log is discarded at startup, so there is never a stopped applier with content to wait on |
| One new (recovery off) and one old (recovery on) replica in a shard | ERS waits on the most advanced replica, usually the new one, which applies its relay log in the stop phase |
| Old or new VTOrc repairing a torn relay log on a new vttablet | Both use `fixReplica` → `SetReplicationSource`; the new vttablet's loss check makes it safe |
| **Old vttablet with `relay_log_recovery=0`** | **Unsafe: the S13 T0 outage.** Configuration must never change before the binary |

Release N ships the applier start in the ERS stop phase, the relay-log-preserving repoint, an opt-in way to render `relay_log_recovery=0` and set `sync_relay_log=1`, and the VTOrc analysis and backoff; the shipped `my.cnf` keeps `relay_log_recovery=1`. Release N+1 changes the shipped default, once every vttablet in the supported version window has the applier start. mysqld only picks the setting up when it restarts under the new binary, so only a custom `my.cnf` can create the unsafe combination.

## 3. Other confirmed issues

### A. Old primary rejoins with errant GTIDs and `super_read_only=OFF` (E2E ×4: S3 ×2, S7d, V2)

`setReplicationSourceLocked` reads the tablet's position for the errant-GTID check at `rpc_replication.go:947`, then calls `fixSemiSync` at `:981`, which disables source-side semi-sync and releases every commit blocked on an ACK. The errant check at `:1019` compares the stale position and passes, and replication starts with errant GTIDs. The same path changes the tablet to REPLICA with `DBActionNone` (`:936-941`), so `super_read_only` stays OFF; VTOrc's later `ReplicaIsWritable` fix only sets `read_only=ON`. Trigger: ERS sends `SetReplicationSource` to the unreachable old primary with a detached 30 s context (`emergency_reparenter.go:1020`), and it lands about 1 s after the partition heals. Result: a replica silently diverged from the primary that never converges.

Fix: disable source semi-sync (or re-read the executed position after doing so) before the errant check; set `super_read_only` when converting a PRIMARY to REPLICA.

### B. Failover blocked by any unreachable cell-local topo; shard lock expires underneath (E2E: S9, S9i, S9b; UNIT)

ERS reads the old primary's tablet record from its cell topo and the tablet map from every cell, and fails on any error or partial result (`emergency_reparenter.go:271-284`, `topo/shard.go:695-709`). VTOrc calls it without a deadline, so it blocks. With per-cell etcds:

- S9, primary cell killed (tablet, VTOrc, cell etcd): no failover; writes down 155 s until the cell was restored.
- S9i, primary cell partitioned: no failover (133 s).
- S9b, primary dies while another cell's etcd is down: failover only 58 s after that etcd returned, with three ERS runs overlapping.

The overlap is caused by the etcd lock lease: the lease `KeepAlive` is bound to the acquisition context (`etcd2topo/lock.go:199-214`), which `topo/locks.go:169-170` cancels on return. The lock therefore lives only `--topo-etcd-lease-ttl` (30 s) past the last `CheckShardLocked`. A slow ERS loses it and another VTOrc takes it; the stale ERS kept issuing `StopReplicationAndGetStatus` in the middle of the valid one before failing on "lost topology lock". UNIT tests against real etcd show the lease expiring in about TTL while the holder is alive, and `CheckShardLocked` hanging while etcd is unreachable.

Fixes: bound topo reads in ERS by `RemoteOperationTimeout`; make the old primary's record optional (it is only used for cell preference); accept a partial tablet map only when revocation and acker accounting can still be proven, or fall back to VTOrc's cached tablet list; renew the lease in the background until unlock (with a hard maximum); give mutating RPCs deadlines that end before the lease can expire. Operationally, host cell-local topos on an etcd spread across zones (for example, on the global etcd).

### C. Replication-only partition delays ERS by 70 s or more (E2E: S4 ×2)

When replicas lose replication but VTOrc still reaches the primary, the analysis prioritizes `ReplicationStopped` → `fixReplica` over the shard-wide `PrimarySemiSyncBlocked` (`inst/analysis_dao.go:567-575`). Each `fixReplica` holds the shard lock for 15 s until `DEADLINE_EXCEEDED`, because the replica cannot reach the primary. Writes were blocked 73 s and 83 s; one run did not fail over within 72 s. Fix: do not defer `PrimarySemiSyncBlocked` behind replica fixes that repoint to the same primary, or bound them much more tightly.

### D. VTOrc acts on stale data after a partial topo read (UNIT, impact corrected by CODE)

After taking the shard lock VTOrc re-reads tablet records, but `refreshTabletsInKeyspaceShard` discards everything on a partial result (`vtorc/logic/tablet_discovery.go:362-367`) and the recovery continues on old data. If another VTOrc has just promoted a new primary, this VTOrc's `fixReplica` sets the new primary read-only (`topology_recovery.go:1515`), and `SetReplicationSource` changes it to REPLICA (`rpc_replication.go:936-941`) *before* the errant-GTID check refuses the repoint (the new primary always has its reparent-journal GTID). Result: a healthy new primary demoted, shard without a writable primary. Fix: abort non-ERS recoveries on a partial refresh (or store the tablets that were read), and require `shardPrimary()` to match the shard record re-read under the lock. VTOrc should also pass `ExpectedPrimaryAlias` to ERS/PRS (`topology_recovery.go:384-392`).

### E. vtgate can route back to an isolated old primary (UNIT)

`healthcheck.go:580-606` rejects a PRIMARY with an older term only while a current primary is held. When the current primary briefly goes not-serving, the slot is cleared and the highest term is forgotten, so an old primary still claiming PRIMARY is accepted. Reads are stale and writes hang. Fix: keep a per-shard maximum term that is never cleared.

### F. Smaller issues

- **ERS abort leaves IO threads stopped** (UNIT). `replicasWithStoppedIO` only restarts threads that were `IOHealthy()` (`reparentutil/replication.go:392`). Threads in `Connecting` stay stopped, so a primary that comes back has no ackers until VTOrc repairs replication.
- **`--ignore-replicas` bypasses revocation** (UNIT, 4+ tablets only). The lowered success target lets a lone primary error return before `haveRevoked` runs (`replication.go:564-601`); an ignored acker keeps ACKing. Not reachable with 3 tablets.
- **Semi-sync fallback prevented only by `my.cnf`** (CODE). vttablet sets only `*_enabled` (`mysqlctl/replication.go:1250-1271`); with MySQL's default 10 s timeout a slow pair of replicas silently turns the primary async.
- **Primary term timestamps are wall-clock** (PLAUSIBLE). `PrimaryTermStartTime = time.Now()` on the promoted host (`tm_state.go:215`) and is compared by `shard_sync`, tablet startup and vtgate. Skew larger than the gap between two failovers can pick the wrong side. Fix: set the new term to max(now, previous term + ε).
- **Replica startup repoints from the shard record without the lock**: confirmed, see G.
- **`InitPrimary` goes read-write before enabling semi-sync** (PLAUSIBLE). `rpc_replication.go:454` vs `:460`; ERS uses it when `ShardInfo.PrimaryAlias == nil`.
- **`IncapacitatedPrimary` uses a single vantage point** (CODE). It needs only VTOrc's own failed polls and a successful ping (`analysis_problem.go:214`), then runs PRS with ERS fallback (`topology_recovery.go:424-452`). A lossy link from any one VTOrc can fail over a healthy primary.
- **In-flight unacked commits become errant on the old primary** (E2E S8b; expected). With `change-tablets-with-errant-gtid-to-drained=false` the tablet stays a non-replicating REPLICA until someone rebuilds it.
- **`PreventCrossCellPromotion` makes ERS impossible** with one tablet per cell (`emergency_reparenter.go:1360`).

### G. A replica restarted during ERS repoints to the old primary and acks its writes (E2E: S12, every variant twice)

`initializeReplication` (`tm_init.go:1146-1244`) reads the shard record without a lock, checks only *executed* GTIDs against that primary, and repoints with semi-sync acking on. ERS never writes the shard record (only the new primary's `shard_sync` does, `shard_sync.go:181`). So a replica whose vttablet restarts after ERS stopped its receiver, but before promotion, reconnects to the old primary P and acks P's commits, if P is alive and reachable from that replica but not from the ERS caller (an operator's ERS from a vtctld cut off from P, VTOrc's `IncapacitatedPrimary` over a lossy link, or a flapping partition). The restart only takes this path without `--restore-from-backup`/`--restore-with-clone`; with them, a tablet with data goes through the restore path and leaves replication stopped.

| Variant | ERS | Acked writes lost | Notes |
|---|---|---|---|
| 3 tablets, the other replica is the candidate | Aborted after about 26 s (the restarted replica's repoint is refused, and it is the only acker) | 0 | P stays primary, no divergence; same with heartbeats on |
| 3 tablets, restarted replica promoted, applier keeping up | Succeeded | 0 | 4–5 unacked commits left on P |
| **3 tablets, restarted replica promoted, applier lagging** | Succeeded | **1,204 and 1,208** | Promotion's `RESET REPLICA ALL` discards what it acked but had not applied |
| **4 tablets** | Succeeded with another replica acking | **532 and 504** | P and the restarted replica diverge; VTOrc's repair of it fails in a loop |
| After promotion, stale shard record, replica caught up | – | 0 | The replica's vttablet refuses and exits (crash loop until the record is fixed) |
| After promotion, stale shard record, replica lagging | – | 0 only because clients timed out after 3 s | The executed-only check passes; the replica repoints to the deposed P, its relay log (with the new primary's writes) is purged, the new primary loses its only acker and stalls |

Fix: in `initializeReplication`, skip the repoint while a reparent holds the shard lock (non-blocking try-lock) or unless the target's own tablet record is PRIMARY with the shard record's term, leaving replication to VTOrc; and run the errant-GTID check against received GTIDs, as `SetReplicationSource` does.

### H. An isolated old primary could not be demoted after the partition healed (E2E: S3 with heartbeats, 1 run; root cause open)

With vttablet heartbeats on and no client writes, the isolated old primary's binlog held 16 unacked transactions (1 on `_vt.heartbeat`, then one `_vt.semisync_heartbeat` write per second from the semi-sync monitor; none on client tables). They were not in `gtid_executed` while blocked; they become errant GTIDs as soon as they complete (semi-sync turned off by a forced demotion or by A's race, or crash recovery). After the heal, `DemotePrimary(force)` and VTOrc's `SetReadOnly` kept hanging or timing out, the tablet stayed `read_only=OFF`, `super_read_only=OFF` with source semi-sync on, and the cluster never converged. The heartbeat writer could not cancel its own stuck write (`You are not owner of thread`, errno 1095). An older S3 run without heartbeats also left `super_read_only` off. Likely cause, unverified: setting `read_only` waits behind the commits blocked on semi-sync, and demotion never reaches the step that turns semi-sync off.

Because most production deployments run with `--heartbeat-enable`, almost every failover with an isolated (not crashed) old primary leaves it with transactions nobody else has, and A's race is hit on nearly every such failover. Mitigations beyond fixing A: stop the heartbeat writer while semi-sync is blocked (the semi-sync monitor knows), consider injecting empty transactions on the new primary for errant GTIDs that touched only `_vt.heartbeat`/`_vt.semisync_heartbeat`, and automate the rebuild or drain of old primaries with errant GTIDs.

### Checked and held up

S1 (primary mysqld killed, 1.6-2.3 s failover), S2 (hang then resume; old primary self-demotes in about 25 ms, no dual writes accepted), S5/S5b (primary crash with the acking replica hung or isolated: ERS refuses, no loss), S6/S6b (VTOrcs cut from the primary: no failover), S7/S7b/S7c (double failure and short flapping: correct refusals), S8 and S10 (VTOrc or global etcd stalled during ERS: completes safely). Unit-tested ERS cases that held: acker timing out during stop, relay-log wait timeout on the holder of the latest write, lost `PromoteReplica` response, `SetReplicationSource` failure on the only acker, half-promoted primaries.

## Prioritized fixes

Ranked by impact (lost acknowledged writes, then silent divergence, then lost availability), then by effort. Evidence: E2E, UNIT, MYSQL, CODE, PLAUSIBLE as in the legend.

| # | Issue | Evidence | Effort | Status |
|---|---|---|---|---|
| 1 | Relay log discarded when a replica is repointed (section 2) | E2E | done | Fixed on `claude/preserve-relay-logs-on-repoint`; leftovers in #16 |
| 2 | Old primary rejoins with errant GTIDs and `super_read_only=OFF` (A); hit on nearly every isolated-primary failover with heartbeats | E2E | small | open |
| 3 | Relay log discarded on replica mysqld restart, `relay_log_recovery=1` (section 2) | E2E | medium: applier start in the ERS stop phase first, then `relay_log_recovery=0` + `sync_relay_log=1`, VTOrc relay-log analysis, applier drain at shutdown; staged rollout | open |
| 4 | Replica restarted during ERS repoints to the old primary (G) | E2E | small–medium | open |
| 5 | Old primary cannot be demoted after the heal (H) | E2E, 1 run | investigate | open |
| 6 | No fallback to async is guaranteed only by `my.cnf` (F) | CODE | small | open |
| 7 | Unreachable cell-local topo blocks ERS (B) | E2E | small–medium | open |
| 8 | Shard lock lease expires under a running ERS; ERS runs overlap (B) | E2E, UNIT | medium | open |
| 9 | VTOrc acts on stale data after a partial topo read; no `ExpectedPrimaryAlias` (D) | UNIT | small | open |
| 10 | vtgate forgets the highest primary term (E) | UNIT | small | open |
| 11 | Replication-only partition delays ERS by 70–80 s (C) | E2E | small–medium | open |
| 12 | Wall-clock primary terms (F) | PLAUSIBLE | small | open |
| 13 | ERS abort leaves `Connecting` IO threads stopped (F) | UNIT | small | open |
| 14 | `IncapacitatedPrimary` uses one VTOrc's view (F) | CODE | small–medium | open |
| 15 | A delayed RDONLY/SPARE tablet can be ERS's intermediate source and stall it for the delay (`util.go:448-450` only excludes BACKUP, RESTORE, DRAINED) | CODE | small–medium | open |
| 16 | Fix-branch leftovers: startup still discards other servers' relay-log transactions; transient refusal on the source's in-flight transactions | E2E (S12 d2), CODE | small | open |
| 17 | No VTOrc analysis or backoff for relay-log corruption and applier data errors (S13 T4) | E2E | small | open |
| 18 | `InitPrimary` goes read-write before enabling semi-sync (F) | PLAUSIBLE | small | open |
| 19 | A hung mysqld blocks `StopReplicationAndGetStatus` with no timeout | PLAUSIBLE | small–medium | open |
| 20 | `--ignore-replicas` bypasses revocation, 4+ tablets (F) | UNIT | small | open |
| 21 | `PreventCrossCellPromotion` makes ERS impossible with one tablet per cell (F) | CODE | tiny | open |
| 22 | An isolated old primary serves stale reads indefinitely (section 1) | CODE | large | by design |
| 23 | Unacked in-flight commits become errant GTIDs on the old primary (F, H) | E2E | operational | expected |

## 4. Running VTOrc in only one of the three cells

### Edge cases this placement causes or worsens

1. **VTOrc in the primary's cell, and that cell fails: no failover at all** (CODE; with VTOrc in a surviving cell, V3 failed over in 2.6 s). Writes are down until someone acts. A manual ERS hits the same cell-topo block (section 3, B) if that cell's topo lived there. Per-cell VTOrcs only help if the cell topos survive the zone loss.
2. **VTOrc in a replica's cell, and that cell fails.** The lost failover mostly could not have happened anyway (ERS would reach only one replica). What is lost is upkeep: replica repair, semi-sync flags, stale-primary demotion, errant-GTID handling. Vitess no longer restarts replication by itself, so if the last acker stops replicating for a non-network reason, writes block until a human steps in.
3. **VTOrc's cell partitioned from the other two** (E2E V1): no false failover, writes continue. VTOrc cannot take the global lock from the minority, and ERS would abort anyway. If only the link to the primary's cell is cut, `UnreachablePrimaryWithBrokenReplicas` restarts replication on both replicas, including the only acker: a brief stall, at most once a minute per tablet.
4. **VTOrc co-located with the primary, primary cell cut from the replicas but not from etcd** (E2E V2). ERS force-demotes the primary and aborts; `fixPrimary` makes it writable again; at heal a queued ERS promoted a replica while the old primary was writable: a 1.6 s dual-writable window, errant GTIDs, and writes down 91 s.
5. **Single vantage point for `IncapacitatedPrimary`.** A flaky link between VTOrc's cell and the primary's cell can cause an unneeded failover (per-cell VTOrcs are worse here: any of three can trigger it).
6. **"VTOrc in the primary's cell" does not stay true.** Every failover moves the primary to another cell.

### Issues it prevents

- Stale-view `fixReplica` demoting a primary another VTOrc just promoted (section 3, D).
- Two ERS runs at once for one shard: a single VTOrc serializes its own recoveries via its local registration (`topology_recovery_dao.go:126-137`), even after the etcd lease has lapsed. S9b's three overlapping ERS runs cannot happen with one VTOrc.
- A second ERS right after the first, without `ExpectedPrimaryAlias`.
- Fewer false positives and less lock contention.

All of these are prevented only as long as no operator or vtctld reparent runs concurrently.

### Recommendation

Place the single VTOrc in a non-primary cell and move it off the primary's cell after failovers; add a standby VTOrc in the other non-primary cell (or allow cross-zone rescheduling with a persistent `--sqlite-data-file`); spread the global etcd one member per zone and keep cell-local topos off single zones; keep `--prevent-cross-cell-failover=false`; size `--instance-poll-time` for cross-cell RTT. The shard-lock lease fix in section 3, B matters as soon as there is a second VTOrc or operator reparents.

## 5. Reproducing

Everything below runs as root on a Linux host (tested on Ubuntu 24.04 with MySQL 8.0.46 and etcd v3.6.7). Branches: this audit branch `claude/vitess-failover-validation-nm4msw` (harness, scripts, report) and `claude/preserve-relay-logs-on-repoint` (the fix).

### Environment

```
E2E_SETUP=1 source doc/failover-audit/env/e2e_env.sh   # once: MySQL, etcd, Vitess binaries, user "vitess"
source doc/failover-audit/env/e2e_env.sh               # later shells
```

`e2e_env.sh` also defines `e2e_clean` (stops leftovers, removes the harness's iptables chains and per-cluster data) and `e2e_run <pkg> [go test flags]` for other e2e packages. Vitess servers refuse to run as root, so tests are compiled as root and run as `$RUN_USER` (default `vitess`) with `CAP_NET_ADMIN`/`CAP_NET_RAW`.

### Chaos harness (`go/test/endtoend/vtorc/chaos/`)

```
doc/failover-audit/env/run_chaos.sh <label> '<test regex>'
```

It cleans up, runs the scenarios, prints each `report.txt` and keeps everything in `/home/vitess/chaos-results/<label>/<scenario>/` (`report.txt`, `events.txt`, `logs/`) plus `run.log`. The cluster has 3 cells with one tablet each, `cross_cell` durability, one etcd per cell plus a global one, one VTOrc per cell and a vtgate; nodes are partitioned with per-node cgroup v2 leaves and iptables (`CHAOS_MARK`/`CHAOS_DROP` chains only). Switches:

| Variable | Effect |
|---|---|
| `BINDIR=<dir>` | Binaries the cluster runs (default `$VTROOT/bin`), e.g. a `make build` of the fix branch in another worktree |
| `RELAYLOG_SAFE=1` | Every tablet's mysqld gets `env/relaylog-safe.cnf` (`relay_log_recovery=0`, `sync_relay_log=1`); S13 sets this up itself |
| `CHAOS_VTTABLET_HEARTBEAT=1` | vttablets run with `--heartbeat-enable --heartbeat-interval 1s` |
| `CHAOS_S11_CLEAR_DELAY=1` | S11b/S11c clear R1's one-hour apply delay before the primary dies (needed with fix-branch binaries, otherwise ERS waits an hour for the kept relay log) |
| `S12_RESTORE_FROM_BACKUP=1` | S12 keeps `--restore-from-backup` on the restarted replica |
| `CHAOS_CGROUP_V2_MOUNT` | cgroup v2 mount (default `/sys/fs/cgroup/unified` if present, else `/sys/fs/cgroup`; only the hybrid layout was tested) |

| Validation | Command |
|---|---|
| Baseline failovers (S1–S10, V1–V3) | `run_chaos.sh base '^TestS([1-9]\|10)[a-z]?[A-Z]\|^TestV[1-3]'` (22 tests; or one at a time, e.g. `'^TestS3IsolatePrimary$'`) |
| Relay-log loss on restart / repoint, main (S11*) | `run_chaos.sh s11 '^TestS11'` (S11, S11k, S11c lose 500 acked writes) |
| Repoint fix, S11c 500 → 0 | worktree of `claude/preserve-relay-logs-on-repoint`, `make build` there, then `BINDIR=<worktree>/bin CHAOS_S11_CLEAR_DELAY=1 run_chaos.sh s11c-fix '^TestS11cRelayLogDiscardFixReplicaCut$'` |
| Replica restart during ERS (S12) | `run_chaos.sh s12 '^TestS12'`; heartbeats: `CHAOS_VTTABLET_HEARTBEAT=1 run_chaos.sh s3hb '^TestS3hbIsolatePrimaryNoWorkload$'` |
| Torn relay logs healed by VTOrc (S13) | `BINDIR=<fix worktree>/bin run_chaos.sh s13 '^TestS13'` |
| Restart path with `relay_log_recovery=0` (S13 T0 outage) | `BINDIR=<fix worktree>/bin RELAYLOG_SAFE=1 run_chaos.sh t0 '^TestS11kRelayLogDiscardKill9$\|^TestS11RelayLogDiscardGraceful$'` |

In the table above, `\|` is Markdown escaping: type a plain `|` in the regex.

### MySQL-only experiments (`repros/mysql/`)

```
doc/failover-audit/repros/mysql/setup.sh          # three mysqld instances and snapshots in $RELAYLOG_DIR (default ~/relaylog-work)
MODE=sqlstop N=3 bash ~/relaylog-work/e1.sh graceful                        # restart discards the relay log
MODE=sqlstop N=3 bash ~/relaylog-work/e1.sh graceful relay_log_recovery=0   # kept and applied
MODE=sqlstop N=3 bash ~/relaylog-work/verify_gtid_only.sh graceful relay_log_recovery=1
cd ~/relaylog-work && ./verify_receiver_only_change.sh V1      # V1..V6
cd ~/relaylog-work && ./verify_delayed_replica.sh
```

`ex.sh` (E3–E6), `e2torn.sh` (torn tails), `e4d.sh`/`e4e.sh` (receiver-only CHANGE options) cover the rest of the table in section 2. They use ports 45001–45003 on 127.0.0.1 and kill only the mysqld processes they started. The MySQL source used for the `GTID_ONLY` check is `apt-get source mysql-server-8.0` (needs `deb-src` entries); the relevant code is in `sql/rpl_replica.cc` and `sql/rpl_rli.cc`.

### Unit reproductions (`repros/unit/`)

Each file documents current behaviour (it passes on HEAD while the issue exists). Copy it back to the package named in the file name, dropping the `.txt` suffix, and run `go test -run <name>`. The etcd tests need an `etcd` binary on `PATH`.
