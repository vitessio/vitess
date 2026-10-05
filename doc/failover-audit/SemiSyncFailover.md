# Semi-sync failover with one and three VTOrcs

This report covers two lines of work against `main` (`fb653f2`):

- **Chaos runs.** The [failover audit](https://github.com/vitessio/vitess/blob/claude/vitess-failover-validation-nm4msw/doc/failover-audit/README.md)'s chaos harness, re-run on a semi-sync MySQL cluster (one ACK, AFTER_SYNC, no fallback to asynchronous replication) across three cells. One profile runs three VTOrcs, one per cell; the other runs a single VTOrc.
- **A TLA+ model** of semi-sync replication, ERS and VTOrc (`doc/design-docs/semi_sync_tla`).

The branch fixes four of the problems found, with unit tests.

## Setup

| | Deployment under test | Chaos profile |
|---|---|---|
| Tablets | 1 primary + 2 replicas, one per availability zone | 3 tablets, one per cell (`zone1..3`) |
| Durability | `semi_sync`, 1 ACK, any REPLICA | `semi_sync` |
| MySQL | `AFTER_SYNC`, timeout 1e18, `wait_no_replica=ON`, `sync_binlog=1`, `innodb_flush_log_at_trx_commit=1`, 20 parallel workers, `replica_net_timeout=8` | the same settings (`profile.go`), MySQL 8.4.6 instead of 8.0 |
| `relay_log_recovery` | Vitess default `1` (assumed not overridden) | `1` |
| VTOrc | `--allow-emergency-reparent`, `--change-tablets-with-errant-gtid-to-drained`, `--clusters-to-watch` | the same |
| VTOrc placement | either 1 instance in the first cell, or 3, one per cell | `semisync-1vtorc`: 1 VTOrc in `zone1`, primary in `zone1` (`colo`) or `zone2` (`remote`); `semisync-3vtorc`: 3 |
| Topo | assumed to survive the loss of one availability zone | the cells' topos live on the global etcd, which no fault kills |

Workload: 4 writers (an INSERT every 40ms each, 5s client timeout) and a primary reader (every 100ms), all through vtgate. P1 and P2 add a write probe: transactions and autocommit INSERTs that wait up to 60s for their outcome.

Checks:
- acknowledged writes missing on the final primary;
- two writable PRIMARY tablets;
- writes acknowledged by a deposed primary;
- convergence within 2 minutes: one primary, read-only replicas, the semi-sync settings of the policy;
- errant GTIDs. A tablet that VTOrc drained for errant GTIDs is reported, not counted as a violation: the deployment is expected to replace it.

Reproducing: `go/test/endtoend/vtorc/chaos/README.md` lists the prerequisites, how to build the binaries of `main` and of this branch, and the plan (`chaos_plan.sh main`, `probe`, `fixed`) that produced the tables below; `chaos_summary.py` builds them from the per-scenario reports in `results/`. The model's runs are reproduced with `doc/design-docs/semi_sync_tla/run.sh`.

## Summary

Each finding is backed by one or more kinds of evidence:
- **E2E:** reproduced on a real cluster with the chaos harness, on `main`.
- **MODEL:** a TLC counterexample.
- **UNIT:** reproduced with fakes.

B-numbers are new. S-numbers and section numbers refer to the failover audit.

| # | Finding | Effect | Evidence | Status |
|---|---|---|---|---|
| S11 | A semi-sync ACK means the transaction is in the replica's relay log, nothing more. A replica's mysqld restart with `relay_log_recovery=1`, the Vitess default, drops that relay log. If the primary then dies, ERS promotes a tablet without the ACKed writes and reports success. | **Acknowledged writes lost**: 500 (S11), 499 (S11k), 400 (S13-T3), and 500 again with the fixed binaries | E2E, MODEL | open: configuration (below) |
| S12 | A replica's vttablet that restarts during an ERS runs `initializeReplication`. It reads the shard record without the lock and repoints the replica to the old primary that ERS is replacing, with semi-sync ACKs enabled. | **Acknowledged writes lost**: 1,232 (S12b2) | E2E, MODEL | open |
| B1 | ERS sends `SetReplicationSource` to an old primary it could not demote, either because it was unreachable or because its `DemotePrimary` was cancelled once two tablets had answered. The RPC turns it into a writable REPLICA without killing the sessions waiting for ACKs, then disables source-side semi-sync. MySQL completes those commits: the clients that still wait get an OK for rows no replica has. The errant GTID check reads its position before that release, so the tablet then replicates with errant GTIDs and `super_read_only=0` (audit 3A). | Acknowledged write lost if a client still waits when the RPC lands. Every time: errant GTIDs, the old primary drained, `super_read_only=0`. | MODEL; E2E for the code path in S3, S4, S7d, S9i, P1 and P2. Clients were already cut off by vtgate or vttablet timeouts (`errno 1105` after about 10s, query timeout `3024` after 30s) | **fixed** |
| N1 | ERS's `IOTHREADONLY` stop leaves a receiver that is retrying the old primary running, yet counts the replica as revoked. When the old primary is reachable again, the replica ACKs its blocked commits. They are then dropped by `RESET REPLICA ALL`, or missing on the promoted tablet. | Acknowledged writes lost | UNIT, MODEL | **fixed** |
| B5 | `PromoteReplica` runs `RESET REPLICA ALL` on whatever the candidate received after ERS's relay log wait (N1, or a stale repoint). | Acknowledged writes lost | MODEL | **fixed** |
| B5' | A successful ERS leaves its `SetReplicationSource` RPCs running after it releases the lock, for up to `--wait-replicas-timeout` (30s). One that lands after a later ERS revoked the replica points it back at the primary that ERS is replacing. | Acknowledged writes lost | MODEL | open |
| B3 | `fixReplica` acts on a stale view of the primary (VTOrc's cache, audit 3D). It repoints a replica back to a primary that was just replaced and never demoted, and the replica ACKs that primary's blocked writes. | Acknowledged writes lost | MODEL | open |
| B8 | With two or more VTOrcs: an ERS's lock lease expires while its repoints are in flight (audit 3B). Another VTOrc's `fixPrimary` then makes the demoted old primary writable again, and runs its own ERS. | Acknowledged writes lost | MODEL (two VTOrcs; without the expiry the fixed model passes) | open |
| B6 | VTOrc drains a tablet with errant GTIDs as a semi-sync acker (`rpl_semi_sync_replica_enabled=ON`), and it keeps replicating. ERS neither waits for a DRAINED tablet nor promotes it, so an ACK it sends can acknowledge a write that a failover loses. | Durability hole: the new primary counted 2 semi-sync clients | E2E (S3, S4, P2) | **fixed** |
| 3C | When a primary's replicas lose replication but VTOrc still reaches it, every VTOrc runs `fixReplica` for `ReplicationStopped`. Each holds the shard lock for 15s until `DEADLINE_EXCEEDED`, and they run back to back, so the `PrimarySemiSyncBlocked` ERS cannot take the lock. | **Writes blocked 83s with no failover with 3 VTOrcs** (185 failed lock attempts); 56–103s with 1 | E2E (S4, P2) | open |
| B7 | ERS repoints the replicas to its candidate before it promotes it. If the promotion then fails, they replicate from a tablet that is not the primary and may hold commits the primary lacks. | Replicas drained (availability) | MODEL | open |
| B4 | An ERS that read the shard record before a new primary published itself demotes that new primary. VTOrc passes no `ExpectedPrimaryAlias`. | Availability | MODEL | open |
| V | Single VTOrc: when its cell is lost or partitioned, nothing fails over, whatever the primary's state. | No failover for 66–136s (until the test restored the cell) | E2E (V2, V4, S8/S9/S9i colo, S8b remote) | by design; placement |

## Chaos results on main

Each cell gives the failover time, the time without any acknowledged write, and the violations.

Harness artifacts:
- **"acked by deposed" (S7b):** zone1 was legitimately promoted again by an ERS. The harness now dates a re-promotion from the new term.
- **"did not catch up":** reported for DRAINED tablets, which by design stop replicating. The harness now skips them.
- **S11b:** its own `SOURCE_DELAY=0` repoint discarded the relay log.
- **S13:** expects `relay_log_recovery=0`.

"Dual-writable" is mostly an isolated or partitioned old primary that still reports PRIMARY. It is writable but cannot commit, because no ACK can reach it.

| Scenario | semisync-3vtorc | semisync-1vtorc-colo | semisync-1vtorc-remote |
|---|---|---|---|
| S1-kill9-primary-mysqld | failover 1.4s; down 1.45s | failover 2.6s; down 2.65s | failover 1.6s; down 1.64s |
| S1b-kill9-primary-mysqld-autorestart | failover 1.6s; down 1.68s | no failover; down 2.96s | failover 2.8s; down 2.81s |
| S2-sigstop-primary | failover 11.7s; down 12.05s; violations: dual-writable | failover 12.7s; down 15.05s; violations: dual-writable | failover 12.9s; down 15.07s; violations: dual-writable, zone2-0000000200 did not catch up to pri; old primary drained |
| S3-isolate-primary | failover 11.5s; down 11.36s; violations: dual-writable, not converged, semi-sync; old primary drained | failover 11.7s; down 11.54s; violations: dual-writable, not converged, semi-sync; old primary drained | failover 12.9s; down 12.91s; violations: dual-writable, not converged, semi-sync; old primary drained |
| S4-replication-partition | no failover; down 83.12s | failover 58.5s; down 73.11s; violations: dual-writable, semi-sync; old primary drained | failover 44.0s; down 56.14s; violations: dual-writable, semi-sync; old primary drained |
| S5-primary-crash-acker-down | down 121.73s | no failover; down 238.32s | no failover; down 238.47s |
| S5b-primary-crash-acker-isolated | down 122.41s | down 121.95s | down 121.72s |
| S6-one-vtorc-cut-from-primary | no failover; down 0.00s | no failover; down 0.00s | no failover; down 0.00s |
| S6b-all-vtorcs-cut-from-primary | no failover; down 0.00s | no failover; down 0.00s | no failover; down 0.00s |
| S7-double-failure | failover 3.4s; down 104.00s | failover 3.2s; down 101.45s | failover 3.2s; down 101.32s |
| S7b-kill-during-failover | down 101.08s; violations: acked by deposed | down 101.09s; violations: acked by deposed | down 100.40s; violations: acked by deposed |
| S7c-flapping-primary | no failover; down 33.90s | no failover; down 33.90s | no failover; down 34.81s |
| S7d-flapping-primary-long | down 47.72s; violations: not converged; old primary drained | down 45.70s; violations: not converged | down 46.89s; violations: not converged; old primary drained |
| S8-vtorc-cut-from-topo-during-ers | failover 30.4s; down 31.80s | no failover; down 133.89s | no failover; down 135.05s |
| S8b-vtorc-cut-from-topo-before | failover 2.4s; down 2.41s; violations: zone1-0000000100 did not catch up to pri; old primary drained | failover 2.6s; down 2.60s | no failover; down 130.85s |
| S9-primary-cell-outage-kill | failover 3.6s; down 3.56s | no failover; down 136.48s | failover 3.8s; down 3.80s |
| S9i-primary-cell-partition | failover 11.3s; down 11.36s; violations: dual-writable, not converged | no failover; down 132.72s | failover 11.5s; down 11.44s; violations: dual-writable, not converged |
| S10-global-topo-hang-during-ers | failover 0.4s; down 12.48s | failover 0.4s; down 12.92s | failover 0.2s; down 13.37s |
| S11-relaylog-discard-graceful-restart | failover 3.2s; down 9.17s; **LOST 513 acked**; violations: zone3-0000000300 did not catch up to pri; old primary drained |  |  |
| S11b-relaylog-fixreplica-p-reachable | no failover; down 122.21s; **LOST 764 acked**; violations: zone1-0000000100 did not catch up to pri; old primary drained |  |  |
| S11c-relaylog-fixreplica-r1-cut | no failover; down 151.91s |  |  |
| S11k-relaylog-discard-kill9 | failover 1.2s; down 4.32s; **LOST 503 acked**; violations: zone2-0000000200 did not catch up to pri; old primary drained |  |  |
| S12a-ers-replica-restart-3tablets | no failover; down 1.24s |  |  |
| S12b2-ers-promotes-restarted-replica-lagging | down 5.22s; **LOST 1232 acked**; violations: dual-writable, zone1-0000000100 did not catch up to pri; old primary drained |  |  |
| S12b-ers-promotes-restarted-replica | down 4.07s; violations: dual-writable, zone1-0000000100 did not catch up to pri; old primary drained |  |  |
| S12d-post-ers-stale-shard-record | down 4.04s; violations: dual-writable, zone1-0000000100 did not catch up to pri; old primary drained |  |  |
| S13-T1-torn-last-trx | no failover; down 0.00s; violations: HARNESS: zone1-0000000100 runs relay_log, HARNESS: zone2-0000000200 runs relay_log, HARNESS: zone3-0000000300 runs relay_log, HARNESS: tear not verified (on boundary= |  |  |
| S13-T2-torn-acked-trx | no failover; down 0.00s; violations: HARNESS: zone1-0000000100 runs relay_log, HARNESS: zone2-0000000200 runs relay_log, HARNESS: zone3-0000000300 runs relay_log, HARNESS: tear not verified (on boundary= |  |  |
| S13-T3-torn-acked-trx-primary-dies | failover 1.0s; down 34.73s; **LOST 400 acked**; violations: HARNESS: zone1-0000000100 runs relay_log, HARNESS: zone2-0000000200 runs relay_log, HARNESS: zone3-0000000300 runs relay_log, HARNESS: tear not verified (on boundary=, zone3-0000000300 did not catch up to pri; old primary drained |  |  |
| S13-T4-applier-duplicate-key | no failover; down 0.00s; violations: HARNESS: zone1-0000000100 runs relay_log, HARNESS: zone2-0000000200 runs relay_log, HARNESS: zone3-0000000300 runs relay_log |  |  |
| V1-single-vtorc-replica-cell-partitioned | no failover; down 0.00s |  |  |
| V2-single-vtorc-with-primary-cut-from-replicas | no failover; down 95.36s |  |  |
| V3-single-vtorc-kill-primary | failover 2.6s; down 2.60s |  |  |
| V4-single-vtorc-primary-cell-outage | no failover; down 66.48s |  |  |

P1 and P2 on `main` (write probe, semisync-3vtorc and semisync-1vtorc-colo, two runs each):

| Scenario | 3vtorc-1 | 3vtorc-2 | 1vtorc-1 | 1vtorc-2 |
|---|---|---|---|---|
| P1 (isolate the primary) | failover 11.7s; old primary drained | failover 12.5s; `super_read_only=0` | failover 11.7s; old primary drained | failover 12.5s |
| P2 (replication-only partition) | failover 70.7s; `super_read_only=0`; old primary still has source semi-sync ON | failover 24.6s; drained acker | failover 55.7s; drained acker | failover 71.7s; `super_read_only=0` |

No probe got an OK from a deposed primary. Every probe routed to the old primary failed first: `errno 1105` within 10s when it was isolated, and the 30s query timeout (`3024`) or `1203` when only replication was cut. The release of the blocked commits came 15–40s after they started.

### With the fixes

The fixed binaries are this branch's Vitess code, which has not changed since these runs (see `go/test/endtoend/vtorc/chaos/README.md` for how to build them). The scenarios that failed on `main` because of B1, 3A or B6 were run twice per profile:

| Scenario | fixed-3vtorc-1 | fixed-3vtorc-2 | fixed-1vtorc-1 | fixed-1vtorc-2 |
|---|---|---|---|---|
| P1-isolated-primary-acks-after-failover | failover 12.5s; down 12.43s; violations: dual-writable | failover 11.7s; down 11.71s; violations: dual-writable; old primary drained | failover 11.3s; down 11.36s; violations: dual-writable | failover 12.9s; down 12.84s; violations: dual-writable |
| P2-replication-partition-acks-on-deposed-primary | failover 102.8s; down 102.81s; violations: dual-writable; old primary drained | failover 70.6s; down 70.47s; violations: dual-writable; old primary drained | failover 70.8s; down 71.17s; violations: dual-writable; old primary drained | failover 70.8s; down 71.18s; violations: dual-writable; old primary drained |
| S3-isolate-primary | failover 11.7s; down 11.60s; violations: dual-writable | failover 11.7s; down 11.68s; violations: dual-writable; old primary drained | failover 13.1s; down 13.16s; violations: dual-writable; old primary drained | failover 11.7s; down 11.64s; violations: dual-writable |
| S7d-flapping-primary-long | down 45.40s; violations: dual-writable | down 47.41s; violations: dual-writable; old primary drained |  |  |
| S9i-primary-cell-partition | failover 11.3s; down 11.35s; violations: dual-writable | failover 11.3s; down 11.40s; violations: dual-writable |  |  |
| S11k-relaylog-discard-kill9 | failover 0.8s; down 3.77s; **LOST 500 acked**; old primary drained | failover 1.4s; down 4.40s; **LOST 500 acked**; old primary drained |  |  |

What changed:
- Every P1, P2, S3, S7d and S9i run converged.
- No old primary was left at `super_read_only=0`. The tablet now demotes itself before the repoint: it stops serving, disables semi-sync, then sets `super_read_only`.
- No DRAINED tablet ACKed, and no write was lost.

What remains:
- The "dual-writable" samples come from the isolated or partitioned old primary, which cannot commit.
- An old primary is sometimes still drained. Its unacknowledged commits, which the demotion releases, become errant GTIDs, as they do in `DemotePrimary(force)`.
- S11k still loses about 500 acknowledged writes: `relay_log_recovery=1` is a configuration problem that these fixes do not address.
- P2 still takes 70–103s to fail over. That is 3C, not fixed here.

## TLA+ model

See `doc/design-docs/semi_sync_tla/README.md` for the model, its bounds and every counterexample.

| Configuration | Bug | Invariant violated | Distinct states |
|---|---|---|---|
| `b1_srs_ack` | B1 | `NoUnbackedAck` | 35,296 |
| `n1_retrying_io` | N1 | `NoLostAck` | 129,921 |
| `s11_relay_recovery` | S11 | `NoLostAck` | 149,108 |
| `stale_fix_replica` | B3 | `NoLostAck` | 367,090 |
| `ers_stale_record` | B4 | `NoErrantServingReplica` | 3,684,055 |
| `b5_stale_repoint` | B5 | `NoLostAck` | 5,476,865 |
| `detached_repoint` | B5' | `NoLostAck` | 5,249,764 |
| `b7_abort_leaves_repoint` | B7 | `NoErrantServingReplica` | 4,558,128 |
| `s12_startup_repoint` | S12 | `NoLostAck` | 16,221 |
| `orcs2_lease_expiry` | B8 | `NoLostAck` | 46,255,026 |
| `current_fixed` | none (all fixes, one VTOrc) | passes, complete | 7,105,556 |
| `s12_fixed` | none (with vttablet restarts) | passes, complete | 345,384 |
| `orcs2_no_expiry` | none (two VTOrcs, no lease expiry) | passes, complete | 10,280,620 |

Bounds: 3 tablets, 2 transactions (1 with two VTOrcs), one crash, one network fault, 2 ERS attempts. The fixed model is the code with every fix that has a switch; four of those fixes are in this branch (see "Fixes"). Losing an acknowledged write needs only one fault in every counterexample above, plus the race.

## Fixes in this branch

All four fixes have unit tests that fail on `main` and pass with the fix. They are tablet-side, or in VTOrc with a tablet-side guard, so mixed versions are safe.

1. **B1 and 3A** (`rpc_replication.go`). `setReplicationSourceLocked` on a PRIMARY tablet now runs the locked part of `DemotePrimary(force)` first: stop serving (killing the sessions still waiting after the shutdown grace period), disable source-side semi-sync, set `super_read_only`. Then it changes the type. The errant GTID check therefore sees the released commits, and the tablet ends up `super_read_only`. Test: `TestSetReplicationSourceDemotesPrimaryBeforeSemiSync`.
2. **N1** (`rpc_replication.go`, `reparentutil/replication.go`). `StopReplicationAndGetStatus(IOTHREADONLY)` now stops any receiver that is not stopped, including one retrying with an IO error. ERS's abort cleanup restarts it. Tests: `TestStopReplicationAndGetStatusStopsRetryingIOThread`, `TestReplicaIOThreadWasRunning`.
3. **B6** (`vtorc/logic/topology_recovery.go`, `rpc_actions.go`). VTOrc asks for the durability rules of the DRAINED type, and a tablet never enables replica semi-sync as DRAINED, which also covers older VTOrcs. Tests: `TestRecoverErrantGTIDDetectedDrainsWithoutSemiSync`, `TestChangeTypeDrainedStopsSemiSyncAcks`.
4. **B5** (`mysqlctl/reparent.go`). `Promote` stops the receiver and waits for the applier to execute everything received before `RESET REPLICA ALL`. It refuses if the applier is stopped while unapplied transactions exist. Tests: `TestPromoteAppliesReceivedTransactions`, `TestPromoteRefusesUnappliedTransactionsWithStoppedApplier`.

The E2E reruns with the fixed binaries are in the table above.

## Recommendations

**Deployment configuration, now.**
- Set `relay_log_recovery=0` and `sync_relay_log=1` in the tablets' my.cnf. This closes S11, the loss path reproduced most often (500 writes per run). Cost: an fsync per relay log event on replicas, which adds to commit latency under semi-sync. Measure it.
- Consider VTOrc's `--emergency-reparent-require-primary-position`. It makes some of these losses fail closed.
- **One VTOrc:** the single VTOrc is a single point of failover. With it in the primary's cell, a cell loss or partition meant no failover in V4, S9, S9i and S8. Run a standby VTOrc in another cell (three VTOrcs, one per cell), or at least keep the VTOrc out of the primary's cell.
- **Three VTOrcs:** they contend for one shard lock. S4 and P2 show it starving the one recovery that matters (3C). The lock lease is not renewed (3B), which B8 and S8 (30s failover) depend on.

**Vitess, next.** In order of severity:
1. 3C: do not run `fixReplica` against a primary that its replicas cannot reach, or let shard-wide recoveries preempt it.
2. S12: `initializeReplication` must not repoint or start replication while a reparent holds the shard lock.
3. B5': ERS should cancel or await its `SetReplicationSource` RPCs before it releases the shard lock.
4. B3/B4: `fixReplica` and ERS should refuse while a tablet holds a newer primary term than their target. VTOrc should pass `ExpectedPrimaryAlias`.
5. 3B/B8: renew the etcd lease until unlock. `fixPrimary` should not undo a forced demotion.
6. S11 in code: repoint without discarding the relay log (receiver-only `CHANGE`), and drain the applier before unavoidable discards (audit §2).
7. B7: repoint replicas to the candidate only once it is promoted, or restore them on abort.

## What VTOrc does that it should not

This section comes from VTOrc's logs over every run of the matrix, and from the model. The busiest profile, `semisync-3vtorc`:
- 2,505 failed shard-lock attempts;
- 1,029 analyses that turned out to be `NoProblem` once re-checked under the lock;
- 256 failed and 33 successful `DeadPrimary` ERS runs;
- 104 failed `ReplicaIsWritable` repairs.

**Harmful.**
- **`fixReplica` for `ReplicationStopped`** while a partition cuts the replicas off the primary (S4, P2). It repoints each replica to the same primary it cannot reach. Each attempt holds the shard lock for 15s and then fails with `DEADLINE_EXCEEDED`: the errant GTID check's `PrimaryStatus` RPC to the primary hangs. It does this back to back on both replicas for the whole partition, so the `PrimarySemiSyncBlocked` ERS never gets the lock. Writes were blocked 83s with no failover at all in semisync-3vtorc, and 56–103s elsewhere. The repair cannot succeed while the partition lasts, and it is the reason the failover does not happen.
- **ERS's detached `SetReplicationSource` to the old primary** (B1). The old primary, which ERS failed to demote, is turned into a writable REPLICA, its blocked commits are released, and it is left with `super_read_only=0`. `StaleTopoPrimary` already does this safely: force-demote first, then repoint. ERS does not need to repoint an old primary it could not demote. Fixed tablet-side.
- **ERS cancelling its own `DemotePrimary`** of a live primary once two tablets answered. `DemotePrimary` reverts on failure, so the primary goes back to serving. It can be repointed later (B1), or re-enabled by another VTOrc (B8). Fail closed: keep it not serving.
- **ERS repoints that outlive the ERS** (B5'). They undo a later ERS's revocation.
- **`fixReplica` and `fixPrimary` on a stale view of the primary** (B3, B8). They repoint a replica to, or make writable again, a primary that was just replaced. Both should check that no tablet holds a newer primary term.
- **Repointing replicas to the ERS candidate before promoting it** (B7). If the promotion fails, the replicas are left on a non-primary.

**Insufficient.** `ReplicaIsWritable` runs a full `fixReplica`: it sets `read_only` (not `super_read_only`) and repoints. Because the analysis checks `read_only` only, old primaries were left at `super_read_only=0` until the end of S3, S7d, S9i and P2. Every failed repoint of a tablet with errant GTIDs also retried the whole repair.

**Unnecessary.**
- **`ReplicaIsWritable` against a tablet whose mysqld is down:** 104 attempts, all `UNAVAILABLE`, about one per second per VTOrc. Each takes the shard lock (0.1s), so it is cheap but noisy.
- **`UnreachablePrimary` (`restartArbitraryDirectReplica`)** when only VTOrc lost the primary and the replicas were healthy (S6, S6b). It restarted replication on a healthy semi-sync replica. It is rate-limited, but with two replicas and one ACK needed, restarting the only connected acker briefly stalls commits.
- **Three VTOrcs evaluating and locking the same shard.** 2,505 failed lock attempts in semisync-3vtorc. Most retry within a second, which is etcd load and log noise, and it amplifies 3C.

**Benign.** `ClusterHasNoPrimary` at bootstrap, and `PrimaryHasPrimary` during the setup PRS: they fired on transient states, and the re-check under the lock made them no-ops. `StaleTopoPrimary` (force-demote, then repoint) did the right thing in every run.
