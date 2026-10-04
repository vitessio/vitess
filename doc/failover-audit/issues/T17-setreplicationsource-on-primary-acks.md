<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: SetReplicationSource on a PRIMARY tablet acknowledges commits that no replica has

**Suggested labels:** Type: Bug, Component: VTTablet, Component: Cluster management
**Tracker task:** T17  
**Status:** fixed on the branch, PR to open

## Overview of the Issue

ERS sends `SetReplicationSource` to every tablet, including the old primary when it could not demote it (unreachable, or the demotion was cancelled; T8). If the old primary is reachable when the RPC lands, `setReplicationSourceLocked` (`go/vt/vttablet/tabletmanager/rpc_replication.go`):
1. changes the type to REPLICA with `DBActionNone`: MySQL stays writable, and the PRIMARY→REPLICA transition does not kill the sessions waiting on semi-sync;
2. reads the position for the errant-GTID check;
3. calls `fixSemiSync(REPLICA)`, which disables source-side semi-sync. MySQL completes every commit waiting for an ACK, and clients still waiting get an OK for rows no replica has;
4. runs the errant check with the position from step 2, which does not include those commits, so replication starts with errant GTIDs.

Result: possibly acknowledged writes lost, and every time an old primary with errant GTIDs, `super_read_only=OFF`, later drained.

### Proposed fix

Fixed on branch `claude/practical-dijkstra-ucvu74` (commit `1a8de59`); PR not opened yet. This draft documents the bug for the PR.

The fix: on a PRIMARY tablet, `setReplicationSourceLocked` first runs the locked part of `DemotePrimary(force)`: stop serving (killing the sessions still waiting after the shutdown grace period), disable source-side semi-sync, set `super_read_only`. Only then does it change the type. The errant check now sees the released commits, and the tablet ends up `super_read_only`.

### How to test the fix

- `TestSetReplicationSourceDemotesPrimaryBeforeSemiSync` (fails on `main`, passes with the fix).
- E2E with the fixed binaries: P1, P2, S3, S7d, S9i converge in every run (2–4 runs per scenario); old primaries are `super_read_only`; 0 lost.

## Reproduction Steps

1. `CHAOS_PROFILE=semisync-3vtorc chaos_matrix.sh TestS3IsolatePrimary` (or P1, P2, S7d, S9i) on `main`: the old primary is converted to REPLICA while PRIMARY Serving, source semi-sync is disabled about 10s later, it replicates with errant GTIDs and is drained with `super_read_only=0`.
2. P1/P2 add a write probe that waits 60s for commits: on `main` no probe got an OK in our runs because vtgate (about 10s) or the 30s query timeout cut the clients first; the release came 15–40s after the commits started. A client with a longer timeout, or a faster repoint, gets the OK.
3. Model: `./run.sh b1_srs_ack` violates `NoUnbackedAck` in 4 steps.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
10:36:55.764 SetReplicationSource: parent: cell:"zone2" uid:200 force: false semiSync: true
10:36:55.769 Changing Tablet Type: REPLICA for cell:"zone1" uid:100
10:36:55.773 TabletServer transition: PRIMARY: Serving -> REPLICA: Serving
10:37:05.791 exec SET GLOBAL rpl_semi_sync_source_enabled = 0, GLOBAL rpl_semi_sync_replica_enabled = 1
10:37:05.801 exec CHANGE REPLICATION SOURCE TO
10:37:05.971 SetReplicationSource(... force_start_replication:true ...) error: Errant GTID detected
CONVERGENCE: not converged after 2m0s: zone1-0000000100 super_read_only=0
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
