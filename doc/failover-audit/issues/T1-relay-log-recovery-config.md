<!-- Draft for the deployment's mysqld configuration, not for vitessio/vitess. Not filed. -->
# Stop replicas discarding ACKed transactions on restart: set relay_log_recovery=0 and sync_relay_log=1

**Tracker task:** T1
**Owner area:** deployment: mysqld configuration of the tablets

## Problem

A semi-sync ACK only means the transaction reached the replica's relay log, not that it was applied. Vitess ships `relay_log_recovery = 1` (`config/mycnf/mysql8026.cnf`, `mysql84.cnf`), and a MySQL cluster that does not override it keeps that behavior. With it, any mysqld restart of a replica (crash, OOM kill, rolling restart, liveness probe) drops the received but unapplied relay log. If that replica was the only one holding a recent ACKed write and the primary then dies, ERS promotes a tablet without those writes and reports success.

This is the loss path we reproduced most often, and the Vitess code fixes in the branch do not change it.

## Evidence

- E2E, profile `semisync-3vtorc`: S11 (graceful restart of the only acker) lost 500 acknowledged writes, S11k (`kill -9`) 499 and 500 (fixed binaries), S13-T3 400. ERS reported success each time.
- TLA+ model: configuration `s11_relay_recovery` violates `NoLostAck`.
- MySQL experiments from the earlier failover audit (`doc/failover-audit/repros/mysql/` on branch `claude/vitess-failover-validation-nm4msw`): `relay_log_recovery=0` keeps and applies ACKed events after `kill -9` (7 runs).

## Proposed change

`relay_log_recovery=0` keeps received events across restarts; `sync_relay_log=1` makes the ACK mean "on disk" (it matters for host crashes and power loss). The remaining risk is a relay log torn by a host crash: the applier then stops at the tear, after applying everything before it, and a `RESET REPLICA` re-fetches only the torn, never-ACKed event (chaos scenario S13 covers this; vttablet could self-heal on `MY-013121`).

If the latency cost is too high, the code-side alternative is T6 (no discard on repoint) plus draining the applier before shutdown, which covers planned restarts but not crashes.

## Steps

1. Confirm what the deployment actually sets on the tablets' mysqld: `relay_log_recovery`, `sync_relay_log`, `relay_log_info_repository` (`SHOW GLOBAL VARIABLES` on a running replica).
2. Set `relay_log_recovery = 0` and `sync_relay_log = 1` in the my.cnf of the tablets' mysqld (tablets only; vtbackup does not need it).
3. Measure commit latency on a semi-sync primary before and after (the ACK now waits for an fsync of the relay log on the acking replica). Sizes: a busy shard on a large instance and a small one.
4. Re-run the chaos scenarios S11, S11k and S13-T3 with the new settings; expect 0 lost writes.
5. Roll out gradually, to a subset of clusters first.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
