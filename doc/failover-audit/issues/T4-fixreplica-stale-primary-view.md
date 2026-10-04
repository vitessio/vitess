<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: VTOrc's fixReplica repoints a replica to a primary that was just replaced

**Suggested labels:** Type: Bug, Component: VTOrc
**Tracker task:** T4

## Overview of the Issue

`fixReplica` (`go/vt/vtorc/logic/topology_recovery.go`) repoints a replica to `shardPrimary()`, the PRIMARY tablet with the newest `primary_timestamp` in VTOrc's own database. That view can be stale: `refreshTabletsInKeyspaceShard` discards everything on a partial topo read and the recovery continues on old data (earlier audit item 3D), and a just-promoted primary's record may not be refreshed yet.

If the stale target is an old primary that was replaced but never demoted (it was unreachable during ERS, or its demotion was cancelled) and it still has commits waiting for ACKs, the repointed replica ACKs them and their clients get an OK; the new primary never has those writes. Otherwise the replica copies the old primary's unacknowledged commits and is drained for errant GTIDs.

### Proposed fix

- Under the lock, after the refresh, refuse to repoint to a target when any tablet record in the shard holds a newer primary term than the target, and abort (rather than continue) when the refresh was partial.
- Pass the expected primary alias through to `SetReplicationSource`'s caller checks, so the tablet can refuse a parent that is not the newest primary it knows of.

Model configuration `current_fixed` includes this check and passes.

### How to test the fix

- Unit test in `topology_recovery_test.go`: VTOrc's DB holds two PRIMARY tablets (old and new term), refresh returns partial results; `fixReplica` must not call `SetReplicationSource` with the old primary.

## Reproduction Steps

1. `cd doc/design-docs/semi_sync_tla && ./run.sh stale_fix_replica` violates `NoLostAck` in 14 steps.
2. Trace: t1 isolated with a commit waiting; ERS promotes t2; before VTOrc's view includes t2, `fixReplica` repoints t3 to t1; t3 ACKs t1's commit.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

TLA+ model `doc/design-docs/semi_sync_tla/SemiSyncFailover.tla`, TLC v1.8.0: 3 tablets, `semi_sync`, one or two VTOrcs. The model follows the code on `main`; the README maps each action to its Go function.

## Log Fragments

```sh
n/a (model counterexample; see the reproduction steps)
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
