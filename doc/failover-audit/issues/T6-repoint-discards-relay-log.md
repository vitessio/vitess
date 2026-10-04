<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: Repointing a replica discards relay log events it already ACKed

**Suggested labels:** Type: Bug, Component: VTTablet
**Tracker task:** T6

## Overview of the Issue

`Mysqld.SetReplicationSource` runs `STOP REPLICA` then `CHANGE REPLICATION SOURCE TO ...` whenever the source changes or a heartbeat interval is passed (`go/vt/mysqlctl/replication.go`, `setReplicationSourceCommand` in `go/mysql/flavor_mysql.go`). With both threads stopped, MySQL deletes the relay log, including events the replica already ACKed but has not applied. The `RESET REPLICA` self-heal paths (`setReplicationSourceRecoverable`, `handleRecoverableReplicationInitError` in `rpc_replication.go`) and `prepareReplicaForShutdown` (which stops the threads without draining the applier) discard them too.

If the primary is still up, the replica re-fetches the events in milliseconds. If the primary is gone or unreachable at that moment, the ACKed writes exist only on the primary and are lost in the next failover. (#21251 already made VTOrc's same-source repairs use `STOP`/`START` instead of `CHANGE`.)

### Proposed fix

From the earlier audit, verified on MySQL 8.0.46:
- When auto-position is already configured, repoint with a receiver-only change: `STOP REPLICA IO_THREAD`, then `CHANGE REPLICATION SOURCE TO SOURCE_HOST=..., SOURCE_PORT=...` without `SOURCE_AUTO_POSITION` (it is persisted, and including it forces both threads stopped), leaving the applier running. MySQL keeps the relay log while either thread runs.
- If the applier is stopped (e.g. a SQL error), only proceed when the new source's `gtid_executed` contains the replica's whole `Retrieved_Gtid_Set`; otherwise refuse and alert.
- Before `RESET REPLICA` in the self-heal paths, and in `prepareReplicaForShutdown`, stop the receiver and wait (bounded) for the applier to execute `Retrieved_Gtid_Set`; fail loudly if it cannot.
- Verify the receiver-only form on MySQL 8.4 (and decide the MariaDB story separately).

### How to test the fix

- Unit tests in `mysqlctl/replication_test.go` with fakesqldb: the command sequence for a source change with auto-position configured and the applier running must not include `STOP REPLICA` (both threads) or `SOURCE_AUTO_POSITION`.
- E2E on MySQL 8.0 and 8.4: S11b with a receiver-only repoint loses 0 writes.

## Reproduction Steps

1. Earlier audit, MySQL 8.0.46: `doc/failover-audit/repros/mysql/` on branch `claude/vitess-failover-validation-nm4msw` shows which commands discard the relay log.
2. Chaos S11b on this branch reproduces it by accident: the scenario's own `CHANGE REPLICATION SOURCE TO SOURCE_DELAY=0` with both threads stopped shrank R1's received set from `:1-1586` to `:1-822`, and 764 acknowledged writes were lost after the next ERS.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

Chaos harness `go/test/endtoend/vtorc/chaos` (branch `claude/practical-dijkstra-ucvu74`) on Ubuntu 24.04 (linux/amd64), MySQL 8.4.6, etcd 3.5.17.
Profile `semisync-3vtorc`: durability `semi_sync`, 3 tablets in 3 cells, 3 VTOrcs with `--allow-emergency-reparent --change-tablets-with-errant-gtid-to-drained`, AFTER_SYNC semi-sync with a 1e18 timeout and `wait_no_replica=ON`, `sync_binlog=1`, `relay_log_recovery=1`. Profile `semisync-1vtorc`: the same with one VTOrc.

## Log Fragments

```sh
S11b, R1 (zone2) before and after the CHANGE:
WaitForPosition: MySQL56/330cd3c9-bff2-11f1-9e06-02fc00000001:1-1586   (ERS waits, times out: applier delayed)
WaitForPosition: MySQL56/330cd3c9-bff2-11f1-9e06-02fc00000001:1-822    (after the CHANGE)
DURABILITY: 764/4280 acked writes missing on new primary zone2-0000000200
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
