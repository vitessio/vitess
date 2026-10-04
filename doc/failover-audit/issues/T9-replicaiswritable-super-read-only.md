<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: ReplicaIsWritable sets read_only, not super_read_only, and runs a full repoint

**Suggested labels:** Type: Bug, Component: VTOrc
**Tracker task:** T9

## Overview of the Issue

The `ReplicaIsWritable` analysis maps to `fixReplica` (`go/vt/vtorc/logic/topology_recovery.go`), which calls `SetReadOnly` (sets `read_only=ON` only) and then `SetReplicationSource`. The analysis itself checks `read_only`. So a replica with `super_read_only=OFF`, typically an old primary that `SetReplicationSource` converted to REPLICA without a database action, is "fixed" with `read_only=ON` and never gets `super_read_only` back. When its repoint fails (errant GTIDs), the whole repair retries.

On the chaos runs, old primaries stayed at `super_read_only=0` until the end of S3, S7d, S9i and P2 (2 minutes after the heal). A user with `SUPER`/`CONNECTION_ADMIN` (or a misrouted dba connection) can still write to such a replica.

### Proposed fix

- Give `ReplicaIsWritable` its own recovery that sets `super_read_only=ON` on the tablet and does nothing else (no repoint), and make the analysis check `super_read_only`.
- Keep repoints in the recoveries that are about replication (`ReplicationStopped`, `NotConnectedToPrimary`, ...).

### How to test the fix

- Unit tests: analysis flags a replica with `read_only=ON, super_read_only=OFF`; the recovery calls a super-read-only setter and not `SetReplicationSource`.

## Reproduction Steps

1. `CHAOS_PROFILE=semisync-3vtorc chaos_matrix.sh TestS3IsolatePrimary` on `main`: "CONVERGENCE: not converged after 2m0s: zone1-0000000100 super_read_only=0".
2. (With the branch's fix for B1 the tablet now sets `super_read_only` itself when demoted through `SetReplicationSource`, so S3 converges; the VTOrc recovery is still wrong for any other way a replica ends up writable.)

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
CONVERGENCE: not converged after 2m0s: zone1-0000000100 super_read_only=0 err=<nil>
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
