<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: A DemotePrimary cancelled by ERS reverts the old primary to serving

**Suggested labels:** Type: Bug, Component: VTTablet, Component: Cluster management
**Tracker task:** T8

## Overview of the Issue

`stopReplicationAndBuildStatusMaps` (`go/vt/vtctl/reparentutil/replication.go`) waits for N-1 tablets and cancels the rest. When the old primary is alive, its `DemotePrimary(force)` is the slowest call: it stops the query service and waits the shutdown grace period (3s) before killing sessions. So with three tablets, the two replicas answer first and the demotion is cancelled. `DemotePrimary` uses `revertPartialFailure=true`: on the cancelled context it reverts, and the old primary serves writes again (blocked on semi-sync, since ERS stopped its replicas).

The old primary is then a writable, serving PRIMARY that ERS believes it has revoked. It can later be repointed by ERS's `SetReplicationSource` (fixed on the branch, B1), re-enabled by another VTOrc (T5), or picked up by a stale repoint (T3, T4). Each of those paths loses acknowledged writes in the model.

### Proposed fix

- Fail closed: a forced demotion (`force=true`) should not revert to serving on a cancelled context. Once a reparent decided to replace the primary, leaving it not-serving (and super_read_only) is the safe state; a later reparent or `fixPrimary` on the legitimate primary restores service.
- Alternatively, ERS waits for the primary's demotion when the primary answered at all (it is reachable), instead of cancelling it with the N-1 rule; the N-1 rule exists for an unreachable primary.

### How to test the fix

- Unit test in `rpc_replication_test.go`: `DemotePrimary(force=true)` with a context cancelled after `SetServingType(false)` must leave the query service not serving.

## Reproduction Steps

1. Model: in every B1, B5' and B8 counterexample the old primary is serving again after `ERSDemoteCancelled`.
2. E2E: P2 (fixed binaries) shows the demotion racing with the repoint: `DemotePrimary(force:true) ... error: context canceled` then a second demotion.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

TLA+ model `doc/design-docs/semi_sync_tla/SemiSyncFailover.tla`, TLC v1.8.0: 3 tablets, `semi_sync`, one or two VTOrcs. The model follows the code on `main`; the README maps each action to its Go function.

## Log Fragments

```sh
13:44:00.700 tabletmanager/rpc_replication.go:590 demoting primary force=true
13:44:18.364 TabletManager.DemotePrimary(force:true)(on zone1-0000000100) error: context canceled
13:44:24.836 TabletManager.DemotePrimary(force:true)(on zone1-0000000100) error: connection pool context already expired
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
