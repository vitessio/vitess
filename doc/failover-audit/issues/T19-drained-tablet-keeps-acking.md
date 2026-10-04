<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: A tablet drained for errant GTIDs keeps sending semi-sync ACKs

**Suggested labels:** Type: Bug, Component: VTOrc, Component: VTTablet
**Tracker task:** T19  
**Status:** fixed on the branch, PR to open

## Overview of the Issue

VTOrc's `recoverErrantGTIDDetected` changes the tablet to DRAINED with `policy.IsReplicaSemiSync(durability, primary, analyzedTablet)`, evaluated for the tablet's current type, REPLICA, so true. The tablet's `fixSemiSync` then sets `rpl_semi_sync_replica_enabled=1`, and the tablet keeps replicating. ERS neither waits for a DRAINED tablet nor promotes it, and revocation does not count it as an acker, so an ACK it sends can acknowledge a write that the next failover loses.

### Proposed fix

Fixed on branch `claude/practical-dijkstra-ucvu74` (commit `1a8de59`); PR not opened yet. This draft documents the bug for the PR.

The fix: VTOrc evaluates the durability rules for the DRAINED type (as vtctld's `ChangeTabletType` already does), and a tablet never enables replica-side semi-sync as DRAINED, which also covers VTOrcs one version back.

### How to test the fix

- `TestRecoverErrantGTIDDetectedDrainsWithoutSemiSync`, `TestChangeTypeDrainedStopsSemiSyncAcks`.
- E2E with the fixed binaries: the drained tablet sets `rpl_semi_sync_replica_enabled = 0`; no SEMISYNC violation.

## Reproduction Steps

1. `CHAOS_PROFILE=semisync-3vtorc chaos_matrix.sh TestS3IsolatePrimary` (or S4, P2) on `main`: the drained old primary replicates from the new primary with `rpl_semi_sync_replica_enabled=ON`, and the new primary reports 2 semi-sync clients.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
S3 on main, zone1 vttablet:
10:37:07.593 Changing Tablet Type: DRAINED for cell:"zone1" uid:100
10:37:07.613 exec SET GLOBAL rpl_semi_sync_source_enabled = 0, GLOBAL rpl_semi_sync_replica_enabled = 1
TabletManager.ChangeType(tablet_type:DRAINED semiSync:true)
P2 on main (semisync-1vtorc), invariant checks:
P2 on main (semisync-1vtorc), invariant checks:
SEMISYNC: DRAINED zone1-0000000100 replicates from port 21515 with rpl_semi_sync_replica_enabled=ON: it ACKs the primary's commits
SEMISYNC: primary zone3-0000000300 has 2 semi-sync clients, want 1
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
