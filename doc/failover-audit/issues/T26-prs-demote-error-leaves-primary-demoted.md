<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: PlannedReparentShard leaves the primary demoted when DemotePrimary returns an error

**Suggested labels:** Type: Bug, Component: Cluster management
**Tracker task:** T26

## Overview of the Issue

In `performGracefulPromotion` (`go/vt/vtctl/reparentutil/planned_reparenter.go`), an error from `DemotePrimary` makes PRS return at once. PRS only runs `UndoDemotePrimary` when the later wait for the primary-elect fails.

The tablet reverts a demotion that fails on its side, but not one that succeeded while its response was lost: a network fault between vtctld and the primary, a vtctld restart, or the RPC's 15s deadline running out just as the tablet finishes (the demotion includes the shutdown grace period for in-flight transactions). In those cases PRS reports the failure while the primary is not serving and `super_read_only`. The shard has no writable primary until something undoes the demotion: VTOrc's `fixPrimary` (`PrimaryIsReadOnly`), if VTOrc runs, or an operator.

### Proposed fix

When `DemotePrimary` returns an error, run `UndoDemotePrimary` on the primary with a fresh context, as the wait-timeout path does, and return both errors. `UndoDemotePrimary` is idempotent on a primary that was never demoted.

### How to test the fix

- Unit: a fake tablet manager client whose `DemotePrimary` returns an error after recording the call; PRS must call `UndoDemotePrimary` on the same tablet.
- E2E: R3 leaves the old primary serving as soon as the cut heals, without waiting for VTOrc; with VTOrc stopped, the primary must not stay demoted.

## Reproduction Steps

1. `CHAOS_PROFILE=semisync-3vtorc go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestR3PRSDemoteResponseLost` (as root). A transaction held open on the primary keeps `DemotePrimary` waiting; vtctld is then cut off from the primary and the transaction is rolled back, so the tablet completes the demotion but PRS never gets the response.
2. PRS fails when its `DemotePrimary` RPC times out (`context deadline exceeded`, or a connection timeout); the primary stays not serving and `super_read_only` until VTOrc's `fixPrimary` runs once PRS releases the lock.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
R3 (semisync-3vtorc, main): PRS returned after 10.7s: error reading from server: connection timed out
R3 (semisync-1vtorc, main): PRS returned after 15.1s: context deadline exceeded
R3: VTOrc: Recovery for PrimaryIsReadOnly on ks/0: Analysis: PrimaryIsReadOnly, will fix primary to read-write  (once PRS released the lock)
R3: unavailable 11.24s, 15.55s (main); 15.20s, 15.28s (this branch's binaries); every acknowledged write on the final primary
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
