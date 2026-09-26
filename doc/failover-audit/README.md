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
| `STOP REPLICA IO_THREAD` then `CHANGE REPLICATION SOURCE TO` with receiver options only, applier running | not used today | **no**, even when switching source |
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
2. **Repoint without discarding (code):** in `setReplicationSourceLocked`, when auto-position is already configured, stop only the receiver and run `CHANGE REPLICATION SOURCE TO` with receiver options only, leaving the applier running. This closes the `fixReplica` window and makes tablet-startup, ERS/PRS and `shard_sync` repoints non-destructive.
3. **Drain before unavoidable discards (code):** before the `RESET REPLICA` self-heal paths and in `prepareReplicaForShutdown`, stop the receiver and wait, bounded, for the applier to execute `Retrieved_Gtid_Set`, and fail loudly if it cannot. This makes planned replica restarts safe even with `relay_log_recovery=1`.
4. **Fail closed instead of losing silently (optional):** persist each replica's retrieved-GTID high-water mark (tablet-side at shutdown and periodically, or VTOrc-side from polls). If a replica restarts having dropped ACKed-but-unapplied GTIDs, it reports that it cannot vouch for its ACKs, and ERS treats it as an unreached acker, refusing and alerting instead of promoting.

Recommendation: 1 and 2, plus 3 for the self-heal and shutdown paths. Together they close every loss path reproduced here. After an incident caused by `relay_log_recovery`, the old relay files often remain on disk for a while, and the lost transactions may be recoverable manually with `mysqlbinlog`.

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
- **Replica startup repoints from the shard record without the lock** (PLAUSIBLE). `initializeReplication` (`tm_init.go:1146+`) can reconnect to the old primary in the window before the new primary's `shard_sync` updates the shard record, and ACK its blocked commits.
- **`InitPrimary` goes read-write before enabling semi-sync** (PLAUSIBLE). `rpc_replication.go:454` vs `:460`; ERS uses it when `ShardInfo.PrimaryAlias == nil`.
- **`IncapacitatedPrimary` uses a single vantage point** (CODE). It needs only VTOrc's own failed polls and a successful ping (`analysis_problem.go:214`), then runs PRS with ERS fallback (`topology_recovery.go:424-452`). A lossy link from any one VTOrc can fail over a healthy primary.
- **In-flight unacked commits become errant on the old primary** (E2E S8b; expected). With `change-tablets-with-errant-gtid-to-drained=false` the tablet stays a non-replicating REPLICA until someone rebuilds it.
- **`PreventCrossCellPromotion` makes ERS impossible** with one tablet per cell (`emergency_reparenter.go:1360`).

### Checked and held up

S1 (primary mysqld killed, 1.6-2.3 s failover), S2 (hang then resume; old primary self-demotes in about 25 ms, no dual writes accepted), S5/S5b (primary crash with the acking replica hung or isolated: ERS refuses, no loss), S6/S6b (VTOrcs cut from the primary: no failover), S7/S7b/S7c (double failure and short flapping: correct refusals), S8 and S10 (VTOrc or global etcd stalled during ERS: completes safely). Unit-tested ERS cases that held: acker timing out during stop, relay-log wait timeout on the holder of the latest write, lost `PromoteReplica` response, `SetReplicationSource` failure on the only acker, half-promoted primaries.

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

- Chaos harness: `go/test/endtoend/vtorc/chaos/`. Run as root with MySQL 8.0, etcd and built binaries: `go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestS11kRelayLogDiscardKill9$' -test.v -test.timeout 20m`. It creates cgroups, drops to an unprivileged user with `CAP_NET_ADMIN`, and sets `CHAOS_E2E=1` (tests skip without it).
- Unit reproductions: `repros/unit/`. Each file documents current behaviour (it passes on HEAD while the issue exists). Copy it back to the package named in the file name, dropping the `.txt` suffix, and run `go test -run <name>`. The etcd tests need an `etcd` binary on `PATH`.
- MySQL experiments: `repros/mysql/` (scripts used for the relay-log table above; paths are specific to the audit environment).
