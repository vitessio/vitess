<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: A replica whose vttablet restarts during ERS repoints to the old primary and ACKs its writes

**Suggested labels:** Type: Bug, Component: VTTablet, Component: Cluster management
**Tracker task:** T2

## Overview of the Issue

When a replica's vttablet starts, `initializeReplication` (`go/vt/vttablet/tabletmanager/tm_init.go`) reads the shard record without the shard lock and repoints the replica to the primary it names, with semi-sync ACKs enabled, and starts replication.

During an EmergencyReparentShard the shard record still names the old primary until the new primary's `shard_sync` rewrites it. A vttablet restart in that window (crash, OOM, rolling restart) undoes ERS's revocation: ERS stopped this replica's IO thread so the old primary could get no more ACKs, and the restarted vttablet points it back at the old primary and starts it. If the old primary is alive (ERS triggered by `PrimarySemiSyncBlocked`, a partition, or a cancelled demotion), its blocked commits get ACKed and acknowledged to clients, and ERS then promotes a different tablet that lacks them.

### Proposed fix

At startup, a vttablet must not change or start replication while a reparent may be in flight:
- If the shard is locked (a lock node exists under the shard's lock path), or any tablet record in the shard holds a newer primary term than the shard record's primary, leave replication as it is (stopped stays stopped) and let VTOrc repoint the tablet under the lock once the reparent finishes.
- Otherwise keep today's behavior.

A cheaper partial fix: when MySQL already has a replication source configured, never switch it at startup; only start replication, and only if the configured source is the shard record's primary. This does not cover the case where ERS stopped replication against the old primary, so the lock check is needed.

The model configuration `s12_fixed` (startup never repoints) passes exhaustively (345k states).

### How to test the fix

- Unit test in `tm_init_test.go`: a replica tablet starting while a fake shard lock is held (memorytopo) must not call `SetReplicationSource` or `StartReplication`.
- E2E: S12a/S12b/S12b2 should report 0 acked writes after revocation and 0 lost.

## Reproduction Steps

1. Run the chaos scenario: `CHAOS_PROFILE=semisync-3vtorc go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestS12b2ERSPromotesRestartedReplicaLaggingApplier` (as root). It makes the old primary unreachable from vtctld and VTOrc only, starts a manual ERS, and restarts R1's vttablet while ERS waits on R2.
2. The report shows "R1 repointed to P by its restarted vttablet: true (after 0.2s)" and "DURABILITY: 1232/6312 acked writes missing on new primary".
3. Model: `cd doc/design-docs/semi_sync_tla && ./run.sh s12_startup_repoint` violates `NoLostAck` in 13 steps; `./trace.py out/s12_startup_repoint.out` prints the trace.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
S12b2 report:
   R1 repointed to P by its restarted vttablet: true (after 0.2s)
   ERS took 13.4s, err=<nil>
   topo primary right after ERS: zone2-0000000200; vttablet types: zone1-0000000100=PRIMARY zone2-0000000200=PRIMARY zone3-0000000300=REPLICA
   acked after R1 restart until ERS end: 1232
   DURABILITY: 1232/6312 acked writes missing on new primary zone2-0000000200
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
