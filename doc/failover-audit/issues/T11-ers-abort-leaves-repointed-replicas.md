<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: An ERS that fails after repointing replicas leaves them on an unpromoted candidate

**Suggested labels:** Type: Bug, Component: Cluster management
**Tracker task:** T11

## Overview of the Issue

ERS repoints every replica to its chosen candidate (`reparentReplicas`, before `PromoteReplica`), waits for an acker quorum, then promotes. If the promotion then fails (lock lost, candidate unreachable, PromoteReplica error), the replicas stay pointed at a tablet that is not the primary. When the candidate was an old primary with unacknowledged commits, the replicas copy them and become errant relative to the real primary, and are drained.

### Proposed fix

- On a failed promotion, repoint the replicas back to their previous source (or stop their replication), or
- Promote first (read-only, no ACKs needed yet), then repoint, then make the primary writable once the acker quorum is connected.

### How to test the fix

- Unit test in `emergency_reparenter_test.go`: `PromoteReplica` fails after the replicas were repointed; assert they are restored or stopped.

## Reproduction Steps

1. `cd doc/design-docs/semi_sync_tla && ./run.sh b7_abort_leaves_repoint` violates `NoErrantServingReplica`.

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
