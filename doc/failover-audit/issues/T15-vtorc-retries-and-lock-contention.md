<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: VTOrc retries repairs against tablets whose mysqld is down, and VTOrcs contend for the shard lock every second

**Suggested labels:** Type: Bug, Component: VTOrc
**Tracker task:** T15

## Overview of the Issue

Two kinds of noise from the chaos runs:
- `ReplicaIsWritable` (and other single-tablet repairs) retried about once a second per VTOrc against a tablet whose mysqld was down; every attempt took the shard lock and failed with `UNAVAILABLE` (104 failures over the three-VTOrc runs).
- With three VTOrcs, each evaluates the same shard and tries to lock it for the same problem: 2,505 failed lock attempts over the three-VTOrc runs. Besides etcd load and log noise, this amplifies T7 (whoever wins the lock may run the wrong recovery).

### Proposed fix

- Skip single-tablet repairs while the target's MySQL is unreachable, with exponential backoff per tablet and analysis.
- Before locking, check the shard's recent recovery registrations (already in the topo for audits) and back off when another VTOrc is working on the same shard; or shard the work between VTOrcs and only fail over to another VTOrc's ownership when it is unhealthy.

### How to test the fix

- Unit test: a repair whose target's last check failed with an unreachable MySQL is not attempted again before the backoff.

## Reproduction Steps

1. Run the matrix with `CHAOS_PROFILE=semisync-3vtorc` and count: `grep -c "Failed to lock shard" */logs/vtorc-*-stderr.txt`; `grep "Recovery for ReplicaIsWritable.*with error"`.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
61 x Recovery for ReplicaIsWritable on zone2-0000000200 with error Code: UNAVAILABLE
49 x Recovery for ReplicaIsWritable on zone3-0000000300 with error Code: UNAVAILABLE
2505 x Failed to lock shard, aborting recovery: node already exists: lock already exists at path keyspaces/ks/shards/0
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
