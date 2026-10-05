<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: PromoteReplica discards transactions the candidate received after ERS's relay log wait

**Suggested labels:** Type: Bug, Component: VTTablet, Component: Cluster management
**Tracker task:** T20  
**Status:** fixed on the branch, PR to open

## Overview of the Issue

`Mysqld.Promote` (`go/vt/mysqlctl/reparent.go`) runs `STOP REPLICA; RESET REPLICA ALL`. ERS waited for the candidate to apply its relay log earlier, but the candidate's receiver can have received more since: a receiver ERS did not stop (T18), or one a stale repoint restarted (T3). Those events may have been ACKed and acknowledged to clients; `RESET REPLICA ALL` drops them.

### Proposed fix

Fixed on branch `claude/practical-dijkstra-ucvu74`; PR not opened yet. This draft documents the bug for the PR.

The fix: before the reset, `Promote` stops the receiver and waits for the applier to execute everything received (`WAIT_FOR_EXECUTED_GTID_SET` on the relay log position); it refuses with FAILED_PRECONDITION when unapplied transactions exist and the applier is stopped. GTID flavors only.

### How to test the fix

- `TestPromoteAppliesReceivedTransactions`, `TestPromoteRefusesUnappliedTransactionsWithStoppedApplier`; `TestPromote` still passes.

## Reproduction Steps

1. Model: `./run.sh b5_stale_repoint` violates `NoLostAck` (a stale repoint restarts the candidate's receiver, which ACKs a write; the promotion's reset drops it).

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
