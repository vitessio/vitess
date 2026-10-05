<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: Shard lock lease expires under a running ERS, and fixPrimary undoes its demotion

**Suggested labels:** Type: Bug, Component: Cluster management, Component: VTOrc, Component: Topology
**Tracker task:** T5

## Overview of the Issue

Two problems combine when there is more than one VTOrc (for example, one per cell):

1. The etcd lock lease is renewed by a `KeepAlive` bound to the acquisition context (`go/vt/topo/etcd2topo/lock.go`), which `topo/locks.go` cancels when the lock call returns. The lock therefore lives only about `--topo-etcd-lease-ttl` (30s) past the last `CheckShardLocked`, while its holder keeps going. Chaos scenario S8 shows the other effect: when the VTOrc running ERS is cut from topo, failover takes 30s.
2. Once the lease is gone, another VTOrc's `fixPrimary` (`PrimaryIsReadOnly`) sees the shard record's primary read-only, which is exactly the state the first ERS's forced demotion left it in, and makes it writable again (`UndoDemotePrimary`). It then runs its own ERS.

The first ERS's repoint RPCs (T3) then point a replica at that re-enabled old primary, which ACKs its writes, and the second ERS promotes a tablet that lacks them.

### Proposed fix

- Keep the lease alive in the background from lock to unlock, independent of the acquisition context, with a hard maximum lifetime, and make `CheckShardLocked` fail fast when the keepalive has lapsed.
- Give mutating RPCs made under the lock deadlines that end before the lease can expire.
- In `fixPrimary`, do not run `UndoDemotePrimary` on a primary that another tablet has superseded (newer term), and do not undo a forced demotion in general: a primary that a reparent force-demoted should only become writable again through a reparent.

### How to test the fix

- Unit test against real etcd (the earlier audit's `etcd2topo__lock_audit_test.go.txt`): hold a lock for 2× the TTL with a live holder; the lock must still be held and a second `LockShard` must block.
- E2E: S8 fails over within the normal ERS time.

## Reproduction Steps

1. `cd doc/design-docs/semi_sync_tla && ./run.sh orcs2_lease_expiry` violates `NoLostAck` (21 steps; 46M states, about 40 min with 2 workers).
2. The same model without lease expiry (`./run.sh orcs2_no_expiry`) passes exhaustively (10.3M states).
3. E2E, the lease part: `CHAOS_PROFILE=semisync-3vtorc chaos_matrix.sh TestS8VTOrcCutFromTopoDuringERS` fails over only after 30.4s. The same lease cuts PlannedReparentShard short (T25).
4. The `fixPrimary` part without a lease expiry: `./run.sh prs_stale_fix_primary` violates `NoLostAck` (finding B10). A PRS that fails after `PromoteReplica` leaves the old primary demoted, still PRIMARY with the older term, and `fixPrimary` makes it writable again. In chaos run R5, VTOrc's re-check under the lock, which refreshes the tablet records, rejected that stale `PrimaryIsReadOnly` after a successful PRS, so in practice the window is the time before the new primary's tablet record is published.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

TLA+ model `doc/design-docs/semi_sync_tla/SemiSyncFailover.tla`, TLC v1.8.0: 3 tablets, `semi_sync`, one or two VTOrcs. The model follows the code on `main`; the README maps each action to its Go function.

E2E for the lease: Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
S8 (semisync-3vtorc): failover happened=true after 30.4s; unavailable 31.80s
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
