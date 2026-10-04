<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: vtgate's healthcheck forgets the highest primary term and can route to an old primary

**Suggested labels:** Type: Bug, Component: VTGate
**Tracker task:** T16

## Overview of the Issue

`healthcheck.go` rejects a PRIMARY with an older term only while it holds a current primary. When the current primary briefly goes not-serving, the slot is cleared and the highest term seen is forgotten, so an old primary that still reports PRIMARY (isolated, then healed) can be accepted again: reads are stale and writes block on semi-sync. (Earlier audit item 3E.)

### Proposed fix

Keep a per-shard maximum primary term that is never cleared, and reject any PRIMARY health update with a lower term. (Longer term: compare a monotonic term number instead of `PrimaryTermStartTime`, which is wall-clock and sensitive to clock skew.)

### How to test the fix

- Unit test in `go/vt/discovery/healthcheck_test.go`: new primary (term 2) goes not-serving, then the old primary (term 1) reports PRIMARY serving: it must not become the primary target.

## Reproduction Steps

1. Unit reproduction from the earlier audit (branch `claude/vitess-failover-validation-nm4msw`, `doc/failover-audit/repros/unit/`).
2. E2E hint: in S7/S7b of the Group Replication audit, vtgate routed primary reads to an old primary whose vttablet still reported PRIMARY.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
n/a (model counterexample; see the reproduction steps)
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
