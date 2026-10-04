<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: Replica repairs hold the shard lock for the whole partition and starve the PrimarySemiSyncBlocked failover

**Suggested labels:** Type: Bug, Component: VTOrc
**Tracker task:** T7

## Overview of the Issue

When the replicas lose replication from the primary but VTOrc can still reach it (a partition between the primary's AZ and the others' MySQL ports, or the primary's network to replicas only), every VTOrc detects `ReplicationStopped` on each replica and runs `fixReplica`. `ReplicationStopped` is analysed before `PrimarySemiSyncBlocked` (`BeforeAnalyses` in `go/vt/vtorc/inst/analysis_problem.go`).

`fixReplica` repoints each replica to the same primary it cannot reach. Each attempt held the lock for 15s and ended with `DEADLINE_EXCEEDED`; the likely cause is that the replica's `SetReplicationSource` calls `PrimaryStatus` on the unreachable primary for its errant-GTID check (not traced). So each repair holds the shard lock for 15s and fails with `DEADLINE_EXCEEDED`, and the VTOrcs run them back to back for both replicas. The `PrimarySemiSyncBlocked` recovery, which would run ERS and restore writes, never gets the lock.

With three VTOrcs (one per cell) writes were blocked for the whole 82s partition and no failover happened; with one VTOrc, 56–103s.

### Proposed fix

- Do not run `fixReplica` toward a primary when the replica's last IO error says it cannot connect to that primary, and do not run replica repairs at all while the primary is semi-sync blocked: they cannot help, and a shard-wide recovery is needed.
- Let shard-wide recoveries (`PrimarySemiSyncBlocked`, `DeadPrimary`) preempt pending replica repairs for the same shard.
- Bound the RPCs inside `fixReplica` (`PrimaryStatus` for the errant check in particular) well below the lock hold time, e.g. 2–3s.

### How to test the fix

- Unit test in `analysis_problem_test.go` / `topology_recovery_test.go`: with the primary semi-sync blocked and replicas reporting IO errors connecting to it, the analysis yields `PrimarySemiSyncBlocked` and no replica repair runs.
- E2E: S4 fails over within the `PrimarySemiSyncBlocked` detection time (seconds) in both profiles.

## Reproduction Steps

1. `CHAOS_PROFILE=semisync-3vtorc go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestS4ReplicationPartition` (as root): partitions the primary's tablet from both replicas' tablets, keeps VTOrc and vtgate connected.
2. Report: "no failover; unavailable 83.12s". VTOrc logs: 185 `Failed to lock shard` for `PrimarySemiSyncBlocked`, and `ReplicationStopped` repairs alternating between the two replicas, each ending `with error Code: DEADLINE_EXCEEDED` 15s after it took the lock.
3. With `CHAOS_PROFILE=semisync-1vtorc`: failover after 44–58s. P2 (same fault, with a write probe): 25–103s.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
10:40:21.198 Recovery for ReplicationStopped on ks/0: Analysis: ReplicationStopped, will fix replica cell:"zone1" uid:100
10:40:36.213 Unlocking shard ks/0 for action VTOrc Recovery for ReplicationStopped on zone1-0000000100 with error Code: DEADLINE_EXCEEDED
10:40:36.250 Recovery for ReplicationStopped on ks/0: Analysis: ReplicationStopped, will fix replica cell:"zone2" uid:200
10:40:51.261 Unlocking shard ks/0 for action VTOrc Recovery for ReplicationStopped on zone2-0000000200 with error Code: DEADLINE_EXCEEDED
... (repeats until the partition heals at 10:41:33)
185 x Recovery for PrimarySemiSyncBlocked on ks/0: Failed to lock shard, aborting recovery: node already exists: lock already exists at path keyspaces/ks/shards/0
10:41:37.322 Unlocking shard ks/0 for successful action VTOrc Recovery for PrimarySemiSyncBlocked on zone3-0000000300
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
