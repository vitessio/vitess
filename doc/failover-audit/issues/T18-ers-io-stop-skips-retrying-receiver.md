<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: ERS's IOTHREADONLY stop leaves a retrying receiver running, which can ACK the old primary

**Suggested labels:** Type: Bug, Component: VTTablet, Component: Cluster management
**Tracker task:** T18  
**Status:** fixed on the branch, PR to open

## Overview of the Issue

`StopReplicationAndGetStatus(IOTHREADONLY)` returns without stopping anything when `!rs.IOHealthy()`, and an IO thread in `Connecting` with a `LastIOError` is not "healthy". That is exactly the state of every replica while the old primary is unreachable. ERS nevertheless counts the replica as reached and revoked (`haveRevoked`). The receiver keeps retrying with semi-sync ACKs enabled; when the old primary becomes reachable during the ERS, the replica fetches and ACKs its blocked commits, which are then lost: `RESET REPLICA ALL` drops them if this replica is promoted, or the promoted replica lacks them.

### Proposed fix

Fixed on branch `claude/practical-dijkstra-ucvu74`; PR not opened yet. This draft documents the bug for the PR.

The fix: stop the receiver unless it is already stopped (`IOState != Stopped`), and make ERS's abort cleanup restart such a receiver (`replicaIOThreadWasRunning`).

### How to test the fix

- `TestStopReplicationAndGetStatusStopsRetryingIOThread`, `TestReplicaIOThreadWasRunning` (updated case).

## Reproduction Steps

1. Unit reproduction: a fake MySQL reporting `IOState=Connecting` with an IO error; `StopReplicationAndGetStatus(IOTHREADONLY)` issues no `STOP REPLICA IO_THREAD`.
2. Model: `./run.sh n1_retrying_io` violates `NoLostAck`.

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
