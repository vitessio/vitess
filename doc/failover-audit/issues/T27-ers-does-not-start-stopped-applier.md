<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: EmergencyReparentShard never starts a candidate's stopped applier, so it cannot fail over

**Suggested labels:** Type: Bug, Component: Cluster management, Component: VTOrc
**Tracker task:** T27

## Overview of the Issue

ERS must not promote a tablet with received but unapplied transactions, so it waits for each candidate to apply its relay log (`WaitForRelayLogsToApply` in `go/vt/vtctl/reparentutil/replication.go`, a `WaitForPosition` on the relay log position). That wait is passive: it never starts the applier. `StopReplicationAndGetStatus(IOTHREADONLY)` stops only the receiver and leaves the applier as it was.

After a mysqld restart, replication does not start (`skip_replica_start`), and vttablet cannot repoint the replica while the primary is down. A candidate that restarted with received but unapplied transactions in its relay log therefore never applies them: every ERS waits out `--wait-replicas-timeout` and fails ("all candidates failed to apply relay logs within the provided waitReplicasTimeout"), and the shard stays without a primary.

With `relay_log_recovery=1`, the Vitess default, this does not show: the restart discarded the relay log, so there is nothing to wait for, and the acknowledged writes it held are lost instead (S11). It is what keeps `relay_log_recovery=0` from closing S11: with it, the relay log survives the restart and the shard cannot fail over. The same applies to a replica whose applier was stopped for any other reason (by an operator, or by `STOP REPLICA`), and `PromoteReplica` (with this branch's fix for B5) refuses a candidate whose applier is stopped with transactions to apply.

### Proposed fix

In the tablet's `StopReplicationAndGetStatus` with `IOTHREADONLY`, after stopping the receiver, start the applier if it is stopped without an error while the relay log holds unapplied transactions (`START REPLICA SQL_THREAD`), so that ERS's relay log wait can complete. The receiver stays stopped, so the replica neither fetches from nor acknowledges the old primary. An applier stopped by an error stays stopped and the wait fails as today, as does one on a tablet that is taking a backup. Only ERS sends `IOTHREADONLY`.

ERS still sees the replica's replication as stopped before the reparent (`Before`), so it does not count the replica as a semi-sync acker. When ERS repoints it, the tablet finds its applier running and restarts replication, as VTOrc's `ReplicationStopped` recovery would.

Implemented on branch `claude/practical-dijkstra-ucvu74` (`startIdleApplierLocked` in `go/vt/vttablet/tabletmanager/rpc_replication.go`, with `Mysqld.StartSQLThread`), with the unit test `TestStopReplicationAndGetStatusStartsStoppedApplier`.

### How to test the fix

- Unit: a fake MySQL whose applier is stopped (`SQLState=Stopped`, no error) with a relay log position ahead of the executed position; `StopReplicationAndGetStatus(IOTHREADONLY)` must issue `START REPLICA SQL_THREAD` and must not start the receiver.
- E2E: S11k and S13-T3b with `CHAOS_RELAY_LOG_SAFE=1` fail over without losing an acknowledged write, as S11ka (where the harness starts the applier) does.

## Reproduction Steps

1. `CHAOS_RELAY_LOG_SAFE=1 CHAOS_PROFILE=semisync-3vtorc go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestS11kRelayLogDiscardKill9` (as root). The only acker's applier is blocked while it acknowledges 500 writes; its mysqld is killed and restarted (the relay log survives with `relay_log_recovery=0`), and the primary is killed.
2. VTOrc's ERS fails every 30s and the shard has no primary for the rest of the run (150s).
3. `TestS11kaRelayLogKill9ApplierStarted` is the same with the applier started after the restart: failover in 1.2s, no acknowledged write lost.

## Binary Version

```sh
this branch (claude/practical-dijkstra-ucvu74), based on main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, with `relay_log_recovery=0` and `sync_relay_log=1` (`CHAOS_RELAY_LOG_SAFE=1`).

## Log Fragments

```sh
S11k: Recovery for DeadPrimary on ks/0: ERS - EmergencyReparent candidate zone1-0000000100 failed to apply relay logs: Code: DEADLINE_EXCEEDED
S11k: failed EmergencyReparentShard: all candidates failed to apply relay logs within the provided waitReplicasTimeout (30s)   (25 times)
S11k: after-restart zone1-0000000100: io=No sql=No Retrieved=...:1-1326 Executed=...:1-826 (500 acknowledged writes unapplied)
S11ka (applier started): failover happened=true after 1.2s; 497 tagged acked writes, 0 LOST
```

Full context: `doc/failover-audit/SemiSyncFailover.md`, "Keeping the relay log".
