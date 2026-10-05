<!-- Draft for the deployment's mysqld configuration, not for vitessio/vitess. Not filed. -->
# Stop replicas discarding ACKed transactions on restart: set relay_log_recovery=0 and sync_relay_log=1 (after T27)

**Tracker task:** T1
**Owner area:** deployment: mysqld configuration of the tablets
**Depends on:** T27 (ERS never starts a stopped applier)

## Problem

A semi-sync ACK only means the transaction reached the replica's relay log, not that it was applied. Vitess ships `relay_log_recovery = 1` (`config/mycnf/mysql8026.cnf`, `mysql84.cnf`), and a MySQL cluster that does not override it keeps that behavior. With it, any mysqld restart of a replica (crash, OOM kill, rolling restart, liveness probe) drops the received but unapplied relay log. If that replica was the only one holding a recent ACKed write and the primary then dies, ERS promotes a tablet without those writes and reports success.

This is the loss path we reproduced most often, and the Vitess code fixes for T17–T20 do not change it.

## Evidence

With `relay_log_recovery=1` (profile `semisync-3vtorc`):
- E2E: S11 (graceful restart of the only acker) lost 500 acknowledged writes, S11k (`kill -9`) 499 and 500. ERS reported success each time.
- TLA+ model: configuration `s11_relay_recovery` violates `NoLostAck`.

With `relay_log_recovery=0` and `sync_relay_log=1` (`CHAOS_RELAY_LOG_SAFE=1`, this branch's binaries, `results/fixed-relaylog-safe-semisync-3vtorc`):
- S11: failover in 2.8s, 0 of 500 lost. S11ka (`kill -9`, the acker's applier started after its restart): failover in 1.2s, 0 of 497 lost.
- S11k and S13-T3b (nothing starts the acker's applier): **no failover** for the whole run. ERS waits for the relay log to be applied but never starts the applier, which stays stopped after the restart (`skip_replica_start`). This is T27, and it is why the settings must wait for its fix.
- A torn relay log is harmless: S13-T1/T1b/T2/T2b tear the newest relay log inside an event after a `kill -9`. MySQL 8.4's applier does not stop on the torn tail, and the receiver fetches the cut transactions again; VTOrc's repair restarts replication 2–6s after it may act, and nothing is lost.
- S13-T3 tears 201 complete, acknowledged transactions and then kills the primary: 198 acknowledged writes lost. That tear is what a host crash can do with `sync_relay_log` above 1; with `sync_relay_log=1`, a host crash can only cut the event being written, which the replica has not acknowledged yet.

## Proposed change

Once T27 is fixed, set `relay_log_recovery=0` (keep received events across restarts) and `sync_relay_log=1` (the ACK means "on disk", which matters for host crashes and power loss; a disk that does not lose acknowledged writes still loses the page cache in a host crash). These settings do not prevent a torn relay log; they make it harmless.

Without the T27 fix, `relay_log_recovery=0` trades lost writes for a shard that cannot fail over after its acker restarted, unless something starts the replica's applier after the restart.

If the latency cost is too high, the code-side alternative is T6 (no discard on repoint) plus draining the applier before shutdown, which covers planned restarts but not crashes.

## Steps

1. Fix T27.
2. Confirm what the deployment actually sets on the tablets' mysqld: `relay_log_recovery`, `sync_relay_log`, `relay_log_info_repository` (`SHOW GLOBAL VARIABLES` on a running replica).
3. Set `relay_log_recovery = 0` and `sync_relay_log = 1` in the my.cnf of the tablets' mysqld (tablets only; vtbackup does not need it).
4. Measure commit latency on a semi-sync primary before and after (the ACK now waits for an fsync of the relay log on the acking replica). Sizes: a busy shard on a large instance and a small one.
5. Re-run the chaos scenarios S11, S11k and S13 with `CHAOS_RELAY_LOG_SAFE=1` and the T27 fix; expect a failover and 0 lost writes in all but S13-T3 (which simulates `sync_relay_log` above 1) and S13-T4 (a data error).
6. Roll out gradually, to a subset of clusters first.

Full context: `doc/failover-audit/SemiSyncFailover.md`, "Keeping the relay log".
