<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: UnreachablePrimary restarts replication on healthy replicas when only VTOrc lost the primary

**Suggested labels:** Type: Bug, Component: VTOrc
**Tracker task:** T14

## Overview of the Issue

`UnreachablePrimary` (VTOrc cannot reach the primary, but every replica replicates fine) runs `restartArbitraryDirectReplica`, which restarts replication on one replica. When only VTOrc's link to the primary is broken, that restarts a healthy semi-sync replica. With two replicas and one ACK required, restarting the only connected acker stalls commits for the duration of the restart.

### Proposed fix

Skip the restart when the replicas report a running IO thread, recent heartbeats, and (under semi-sync) a connected semi-sync status. Restarting is only useful when the replica's connection looks stale (no events beyond `replica_net_timeout`).

### How to test the fix

- Unit test: `restartDirectReplicas` does nothing for replicas whose status shows a healthy, recently active IO thread.

## Reproduction Steps

1. `CHAOS_PROFILE=semisync-1vtorc CHAOS_PRIMARY_CELL=zone1 chaos_matrix.sh TestS6bAllVTOrcsCutFromPrimary`: VTOrc logs "Restarting replication on direct replica zone2-0000000200" while both replicas were replicating and writes were unaffected.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
11:39:39.148 Recovery for UnreachablePrimary on ks/0: Restarting replication on direct replica zone2-0000000200
11:39:39.256 Recovery for UnreachablePrimary on ks/0: Completed restart of 1/1 direct replicas for unreachable primary cell:"zone1" uid:100. err=<nil>
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
