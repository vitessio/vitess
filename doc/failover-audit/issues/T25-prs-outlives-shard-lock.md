<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: PlannedReparentShard outlives its shard lock and leaves the primary demoted

**Suggested labels:** Type: Bug, Component: Cluster management, Component: Topology
**Tracker task:** T25

## Overview of the Issue

`topo.Lock` cancels the etcd lease's `KeepAlive` as soon as the lock is acquired (`go/vt/topo/locks.go` passes a context that it cancels on return, and `etcd2topo/lock.go` ties the `KeepAlive` to it). The shard lock therefore lives `--topo-etcd-lease-ttl` (30s) past the last `CheckShardLocked`, whose `KeepAliveOnce` is the only renewal. PlannedReparentShard renews it at its phase boundaries, but each phase can wait up to `--wait-replicas-timeout`, plus `DemotePrimary`'s 15s for the demotion phase. With a primary-elect that lags, the lease expires mid-PRS:

1. **During the catch-up** (before the demotion): PRS's next check finds the lease gone and PRS fails ("lost topology lock, aborting: node doesn't exist: lease") without having changed anything. So `--wait-replicas-timeout` above about 30s has no effect: the lease, not the flag, bounds each phase.
2. **After the demotion**, while PRS waits for the primary-elect to reach the demoted position: the shard has no serving primary. Once the lease is gone, VTOrc gets the lock, sees `PrimaryIsReadOnly` on the demoted primary, and undoes the demotion (`UndoDemotePrimary`) while PRS is still running. PRS's check after the wait then fails, and PRS returns without `UndoDemotePrimary`: it only undoes the demotion when the wait itself fails. Without VTOrc, the primary would stay demoted.
3. **During `reparentTablets`** (the journal write after `PromoteReplica`, which has no lock check): in the TLA+ model, VTOrc then runs ERS because the new primary's commits wait for an ACK (`PrimarySemiSyncBlocked`). PRS's repoint of the old primary is still in flight, lands on ERS's new primary, and turns it into a replica of PRS's primary-elect. The two reparents split the shard between two primaries (finding B9).

### Proposed fix

- Keep the lease alive in the background from lock to unlock (the fix proposed for T5), so that a reparent holds the lock for as long as it runs.
- Until then, in `performGracefulPromotion` / `reparentShardLocked`: when the lock check after the demotion fails, run `UndoDemotePrimary` before returning, as the wait-timeout path does.
- Reject a `--wait-replicas-timeout` that cannot fit in the lease, or document the cap.

### How to test the fix

- E2E: R2 must either complete the PRS or leave the old primary serving at once, with no 30s outage; R2b must honour `--wait-replicas-timeout 60s`.
- Model: `prs_lease_expiry` passes with the lease kept alive (`MaxExpire=0`), as `prs_fixed` does.

## Reproduction Steps

1. `CHAOS_PROFILE=semisync-3vtorc go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestR2PRSOutlivesShardLock` (as root). A transaction held open on the primary keeps `DemotePrimary` in its shutdown grace period while the primary-elect gets `SOURCE_DELAY=35`; the transaction then commits, so the primary-elect needs about 35s to reach the demoted position. PRS runs with `--wait-replicas-timeout 60s`.
2. `TestR2bPRSCatchupOutlivesShardLock`: the primary-elect applies 35s behind before PRS starts.
3. Model: `cd doc/design-docs/semi_sync_tla && ./run.sh prs_lease_expiry` violates `NoLostAck` (38,090 states).

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
R2 (semisync-3vtorc, main): PRS returned after 36.5s: lost topology lock, aborting: node doesn't exist: lease
R2: VTOrc: Recovery for PrimaryIsReadOnly on ks/0: Analysis: PrimaryIsReadOnly, will fix primary to read-write  (about 31s after the demotion, while PRS still waited)
R2: unavailable 30.80s (semisync-3vtorc), 30.92s (semisync-1vtorc); every acknowledged write on the final primary
R2b: PRS returned after 36.4s with --wait-replicas-timeout 60s: lost topology lock, aborting
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
