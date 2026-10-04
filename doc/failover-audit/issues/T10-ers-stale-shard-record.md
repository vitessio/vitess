<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: ERS on a stale shard record demotes the primary that was just promoted

**Suggested labels:** Type: Bug, Component: VTOrc, Component: Cluster management
**Tracker task:** T10

## Overview of the Issue

A new primary publishes itself in the shard record asynchronously (its `shard_sync`). An ERS that locks the shard and reads the shard record before that, for example a second VTOrc acting on `DeadPrimary` for the old primary, treats the new primary as a tablet to revoke and calls `DemotePrimary(force)` on it. It may then promote the old primary (its unacknowledged commits make it "most advanced") or fail on a split-brain check. VTOrc does not pass `ExpectedPrimaryAlias` to ERS (`EmergencyReparentOptions` in `topology_recovery.go`).

No acknowledged write is lost in the model, but the healthy new primary is demoted and replicas can end up with errant GTIDs.

### Proposed fix

- VTOrc passes the primary it analysed as `ExpectedPrimaryAlias`.
- ERS, after reading the shard record and the tablet map, refuses while any tablet holds a newer primary term than the shard record's primary (the record is stale; retry after `shard_sync`).

### How to test the fix

- Unit test in `emergency_reparenter_test.go`: tablet map with a PRIMARY whose term is newer than the shard record's → ERS returns FAILED_PRECONDITION without demoting anything.

## Reproduction Steps

1. `cd doc/design-docs/semi_sync_tla && ./run.sh ers_stale_record` violates `NoErrantServingReplica`.

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
