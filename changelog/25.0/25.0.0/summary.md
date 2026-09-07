# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
        - [VTOrc failover of an unreachable primary `vttablet` via replica quorum](#vtorc-quorum-unreachable-primary)
    - **[Breaking Changes](#breaking-changes)**
        - [`--watch-replication-stream` flag removed](#vttablet-watch-replication-stream-removed)
        - [VRLog feature removed](#vttablet-vrlog-removed)
        - [Snapshot Topology feature removed](#vtorc-snapshot-topology-removed)
        - [VTOrc `--cell` flag is now required](#vtorc-cell-required)
        - [`BackupHandle` interface gains `Wait()` method](#backup-handle-wait-method)
        - [VTOrc: `--cells-to-watch` removed in favor of `--cells-no-recovery`](#vtorc-cells-no-recovery)
    - **[Deprecations](#deprecations)**
        - [CLI Flags](#deprecated-cli-flags)
        - [Legacy streaming-path plan types in query rules](#deprecated-selectstream-rule-plan)
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
        - [`vdiff show --no-samples` strips the per-table row-sample report](#vreplication-vdiff-no-samples)
        - [Preserve Materialize target data on cancel by default](#vreplication-materialize-cancel-data-protection)
        - [Online DDL migrations are no longer failed by recoverable vreplication errors](#onlineddl-vrepl-auto-resume)
    - **[VTGate](#minor-changes-vtgate)**
        - [Ingress bytes in query LogStats](#vtgate-logstats-ingress-bytes)
        - [New controls for cross-keyspace reads](#vtgate-cross-keyspace-reads)
        - [Streaming errors no longer surface as connection loss](#vtgate-streamexecute-real-errors)
        - [Temporary-table connections are kept alive with a heartbeat](#vtgate-temp-table-heartbeat)
        - [Temp-table idle timeout gives gRPC API sessions MySQL-equivalent temp-table lifetime](#vttablet-temp-table-idle-timeout)
        - [SHA256-hashed passwords in the static gRPC auth plugin](#vtgate-grpc-static-auth-sha256)
        - [PREPARE statements no longer report the prepared statement's tables](#vtgate-prepare-tables-used)
        - [Preparing a statement no longer starts an implicit transaction](#vtgate-prepare-no-implicit-tx)
        - [Stricter validation of SQL-level PREPARE statements](#vtgate-prepare-stricter-validation)
        - [Stricter PROXY protocol v1 header validation](#vtgate-proxy-protocol-v1-strictness)
        - [MySQL-faithful validation and rejection of unsupported `sql_mode` values](#vtgate-sql-mode-rejection)
        - [New `VEXPLAIN MYSQLPLAN` statement](#vtgate-vexplain-mysqlplan)
    - **[Reparent](#minor-changes-reparent)**
        - [`EmergencyReparentShard` no longer waits on replicas that cannot win the election](#ers-lagging-relay-log-wait)
        - [`EmergencyReparentShard` can explicitly recover from split brain](#ers-allow-split-brain-promotion)
        - [Reparent candidate ordering now respects partially ordered GTID histories](#reparent-gtid-candidate-ordering)
    - **[VTTablet](#minor-changes-vttablet)**
        - [VTTablet rejects unsupported `sql_mode` values](#vttablet-reject-unsupported-sql-modes)
        - [Consolidator Reject on Waiter Cap](#vttablet-consolidator-reject-on-cap)
        - [Query timeouts no longer kill reserved connections outside transactions](#vttablet-reserved-conn-kill-query)
        - [Query timeout for state-changing statements on the streaming path](#vttablet-stream-query-timeout)
        - [Query rules now apply to queries on the streaming path](#vttablet-rules-apply-to-streaming)
        - [New `--demote-primary-lock-wait-timeout` flag](#vttablet-demote-primary-lock-wait-timeout)
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)
        - [Replicas are placed in a crash-safe state before shutdown](#vttablet-replica-crash-safe-shutdown)
        - [Skip MySQL version check when restoring from a mysql-shell backup](#vttablet-mysql-shell-restore-skip-version-check)
        - [ApplySchema session variables](#vttablet-applyschema-session-variables)
    - **[VTCtld](#minor-changes-vtctld)**
        - [MySQL version-aware reparent candidate election](#vtctld-version-aware-reparent)
    - **[Backup/Restore](#minor-changes-backup)**
        - [Chunked backup/restore for the builtinbackupengine](#backup-chunked-builtin)
        - [Slow clean mysqld shutdowns no longer fail backups](#backup-mysqld-shutdown-timeout)
    - **[General](#minor-changes-general)**
        - [Build version metadata now sourced from VCS stamping](#build-info-from-vcs)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

#### <a id="vtorc-quorum-unreachable-primary"/>VTOrc failover of an unreachable primary `vttablet` via replica quorum</a>

VTOrc can now run an Emergency Reparent Shard (ERS) when a `PRIMARY` tablet's `vttablet` process is unreachable while its MySQL keeps running — a case the existing replication-based detection misses, because the replicas keep replicating from the still-running MySQL.

To avoid acting on VTOrc's own connectivity problems (for example, a network partition between VTOrc and the primary), the failover requires a quorum: a configurable fraction of the shard's `REPLICA`/`RDONLY` tablets must also report the primary's `vttablet` unreachable, in addition to VTOrc's own failed check. The liveness signal is gathered over the existing `Ping` and `FullStatus` RPCs — there is no new protocol or service.

The feature is opt-in and disabled by default:

- On `vttablet`, set `--track-shard-tablet-health` so the tablet periodically pings its shard's current primary and reports the primary's `vttablet` liveness in `FullStatus`.
- On VTOrc, set `--emergency-reparent-on-primary-tablet-unreachable` to act on the quorum. The strictness is tunable via `--shard-tablet-health-quorum-fraction` (default `1.0`, i.e. unanimous), `--shard-tablet-health-quorum-min-observers`, `--shard-tablet-health-failure-threshold`, and `--shard-tablet-health-freshness`.

The quorum decision is observable: VTOrc logs why it did or did not fail over an unreachable primary, records the per-observer vote tally in the recovery audit message, and exposes the live per-shard quorum state — the primary, the verdict, and each observer's vote — at the read-only `/api/shard-tablet-health-quorum` endpoint.

Detection and decision latency is bounded by the tunables: an observer considers the primary down after `--shard-tablet-health-failure-threshold` consecutive failed pings at `--shard-tablet-health-interval` (defaults: 3 × 1s), its report reaches VTOrc with the next `FullStatus` poll of that observer, and the report counts toward quorum only while younger than `--shard-tablet-health-freshness` (default `15s`, measured from the observer's underlying ping). VTOrc therefore acts on evidence that is at most one freshness window old — and immediately before reparenting it re-polls the observers and the primary itself and re-evaluates the analysis under the shard lock, so a primary whose `vttablet` recovered in the meantime aborts the failover. Note that `--shard-tablet-health-quorum-fraction` values below `1.0` make detection more tolerant of partial agreement, but give up the default (unanimous) guarantee that a single fresh "up" report vetoes the failover.

For the quorum to add protection beyond VTOrc's own view, the shard's observers should sit in failure domains independent of VTOrc and of one another: if VTOrc and a majority of observers share a network segment that is partitioned from a still-serving primary, the failover can still fire on a live primary. The strict-majority gate requires a genuine majority of the shard's `REPLICA`/`RDONLY` tablets, counted from topology — so unreported (e.g. down) replicas still count toward the denominator, and for shards with two or more eligible observers no single flaky report can drive a reparent. A shard whose only eligible observer is a single `REPLICA`/`RDONLY` therefore rests on that one observer plus VTOrc's own failed check; raise `--shard-tablet-health-quorum-min-observers` if you want quorum ERS to require more than one observer (at the cost of not failing such single-observer shards over automatically).

A graceful `vttablet` shutdown records a shutdown marker in the topology server so that intentionally stopped primaries (e.g. during rolling restarts) are not failed over by the quorum. That marker write is best-effort: if it fails — which requires the topology server to be unavailable at shutdown time — the graceful shutdown is indistinguishable from a crash and may be failed over. In that situation VTOrc's own topology access is typically degraded as well, which further gates any recovery.

Note that in this scenario the old primary's MySQL keeps running, and because its `vttablet` is the unreachable component, it cannot be demoted until that `vttablet` comes back and discovers the shard has a new primary. As with any emergency reparent away from an unreachable primary, a semi-sync durability policy (e.g. `semi_sync`) is what prevents the old primary from acknowledging new writes in the meantime; with `none` durability, anything writing directly to the old MySQL (bypassing `vtgate`) could cause a split brain.

See [#19918](https://github.com/vitessio/vitess/issues/19918).

### <a id="breaking-changes"/>Breaking Changes</a>

#### <a id="vttablet-watch-replication-stream-removed"/>`--watch-replication-stream` flag removed</a>

The deprecated `--watch-replication-stream` VTTablet flag has been removed.

**Migration**: remove `--watch-replication-stream` from VTTablet startup arguments.

**Impact**: VTTablet will fail to start if `--watch-replication-stream` is still passed.

See [#20048](https://github.com/vitessio/vitess/pull/20048) for the removal and [#19204](https://github.com/vitessio/vitess/pull/19204) for the original deprecation.

#### <a id="vttablet-vrlog-removed"/>VRLog feature removed</a>

The VRLog feature — a streaming log of VReplication events served at VTTablet's `/debug/vrlog` HTTP endpoint, [disabled by default since v22](../../22.0/22.0.0/changelog.md) — has been removed. The `--vreplication-enable-http-log` flag that enabled it is now a deprecated no-op and will be removed in v26.

**Migration**: remove `--vreplication-enable-http-log` from VTTablet startup arguments.

**Impact**: The `/debug/vrlog` endpoint no longer exists. Passing `--vreplication-enable-http-log` logs a deprecation warning and has no effect.

#### <a id="vtorc-snapshot-topology-removed"/>Snapshot Topology feature removed</a>

VTOrc's Snapshot Topology feature, [deprecated in v24](../../24.0/24.0.0/summary.md#vtorc-snapshot-topology-deprecation), has been removed. This includes the `--snapshot-topology-interval` flag and the `database_instance_topology_history` table.

**Migration**: remove `--snapshot-topology-interval` from VTOrc startup arguments.

**Impact**: VTOrc will fail to start if `--snapshot-topology-interval` is still passed.

See [#20048](https://github.com/vitessio/vitess/pull/20048) for the removal and [#19070](https://github.com/vitessio/vitess/pull/19070) for the original deprecation.

#### <a id="vtorc-cell-required"/>VTOrc `--cell` flag is now required</a>

The `--cell` VTOrc flag, [introduced in v24](../../24.0/24.0.0/summary.md#vtorc-cell-flag), is now required.

**Migration**: ensure `--cell` is set on every VTOrc deployment.

**Impact**: VTOrc will fail to start with a `FAILED_PRECONDITION` error if `--cell` is empty.

See [#20048](https://github.com/vitessio/vitess/pull/20048) for the removal and [#19047](https://github.com/vitessio/vitess/pull/19047) for the original `--cell` flag introduction.

#### <a id="backup-handle-wait-method"/>`BackupHandle` interface gains `Wait()` method</a>

The `backupstorage.BackupHandle` interface now requires a `Wait()` method. This method blocks until all pending asynchronous `AddFile` operations complete without finalizing the backup. It is idempotent and safe to call multiple times.

**Impact**: Any out-of-tree or custom `BackupHandle` implementation will fail to compile until a `Wait()` method is added. For synchronous backends, a no-op implementation is sufficient:

```go
func (bh *MyBackupHandle) Wait() {}
```

See [#20167](https://github.com/vitessio/vitess/pull/20167) for details.

#### <a id="vtorc-cells-no-recovery"/>VTOrc: `--cells-to-watch` removed in favor of `--cells-no-recovery`</a>

The `--cells-to-watch` flag has been removed. It restricted vtorc's tablet discovery to a fixed set of cells, which created a serious failure mode for any keyspace that spanned cells: if the primary lived in a cell *not* in `--cells-to-watch`, vtorc filtered the primary out of discovery, concluded the keyspace had no primary, and triggered an `EmergencyReparentShard` against a replica in a watched cell. The other cell's vtorc then saw its primary demoted and ran its own ERS — the two vtorcs ping-ponged ERS operations until the keyspace was destroyed. The flag only "worked" under true cell isolation (each cell hosting an independent primary), a configuration with no practical purpose.

The replacement, `--cells-no-recovery`, is a deny-list for *recovery actions only*; vtorc's discovery still spans all cells, so it always sees the real topology. When a problem is detected, vtorc skips the actionable recovery if the *analyzed* (failed) tablet is in a listed cell, recording a `CellNoRecovery` reason under the existing `SkippedRecoveries` stat. For `ClusterHasNoPrimary` (no primary exists in the shard), recovery is suppressed only when every cell that has tablets in the shard appears in the deny-list; a partial deny-list lets the initial PlannedReparentShard (PRS) proceed. Detection still happens for tablets in listed cells (so operators retain visibility), and non-actionable recoveries (pure detection paths) are unaffected. The cells passed to `--cells-no-recovery` are validated against the topology's known cells at startup; an unknown cell name causes vtorc to exit. For per-tablet recoveries, the filter gates on the analyzed tablet's cell: it does not, on its own, prevent a replica in a no-recovery cell from being chosen as a promotion candidate during an `EmergencyReparentShard` triggered by a failure in another cell (use `--prevent-cross-cell-failover` for that).

**Schema change:** this PR changes `recovery_detection.detection_id` from a plain `INTEGER PRIMARY KEY` to `INTEGER PRIMARY KEY AUTOINCREMENT`, adds a `UNIQUE (alias, analysis)` index, and changes the detection write to an upsert (`INSERT … ON CONFLICT(alias, analysis) DO UPDATE SET detection_timestamp = now()`). VTOrc drops and recreates its SQLite database on startup, so no migration is needed. The behavioral effect is that repeated detections of the same ongoing failure on the same tablet upsert a single row, refreshing `detection_timestamp` on each poll, rather than accumulating one row per poll cycle; the `detection_id` is stable for the duration of that incident. When a recovery successfully promotes a new primary (ERS/PRS), the triggering `recovery_detection` row is deleted, creating a clean incident boundary: the next recurrence of the same failure inserts a fresh row with a new `detection_id`. Failed recovery attempts and non-primary-promotion recoveries (`fixReplica`, `fixPrimary`, etc.) leave the row intact so retries within the same incident share the same `detection_id`; those rows are cleaned up by expiry-based history pruning. Suppressed recoveries (e.g. cell gate, quorum gate) follow the same expiry path. This change applies to all vtorc deployments, not only those using `--cells-no-recovery`.

**Migration:** drop `--cells-to-watch` from your vtorc invocation. If you previously used it for true cell-isolated deployments, the new flag is not a like-for-like replacement (vtorc will now discover and watch all cells); discuss your scenario in the linked issue if the new flag does not cover your needs. If you are upgrading from v24.0.0 specifically and have `--cells-to-watch` in your vtorc flags, note that this flag was already removed in v24.0.1; replace it with `--cells-no-recovery` before upgrading.

See [#20021](https://github.com/vitessio/vitess/issues/20021) for details.

### <a id="deprecations"/>Deprecations</a>

#### <a id="deprecated-cli-flags"/>CLI Flags</a>

The VTGate flag `--legacy-replication-lag-algorithm` is now deprecated and is a no-op. VTGate always uses the simpler replication lag algorithm based on low lag, high lag and the minimum number of tablets. A detailed explanation of the algorithm [is available in this code comment](https://github.com/vitessio/vitess/blob/main/go/vt/discovery/replicationlag.go).

The flag will be removed entirely in v26. This deprecation is tracked in https://github.com/vitessio/vitess/issues/18914.

**Impact**: Remove any usage of the `--legacy-replication-lag-algorithm` flag from VTGate startup scripts or configuration.

The VTTablet flag `--vreplication-enable-http-log` is now deprecated and is a no-op, as the [VRLog feature it enabled has been removed](#vttablet-vrlog-removed). The flag will be removed entirely in v26.

**Impact**: Remove any usage of the `--vreplication-enable-http-log` flag from VTTablet startup scripts or configuration.

#### <a id="deprecated-selectstream-rule-plan"/>Legacy streaming-path plan types in query rules</a>

The `SelectStream` query plan type no longer exists: statements served over the streaming path now produce the same plan types as buffered execution (`Select`, `Show`, `SelectLockFunc`, ...), so query rules keyed on those concrete plan names now apply to both execution paths.

For backward compatibility, rules keep matching queries on the streaming path by their pre-v25 plan types:

- Rules files using `SelectStream` in a `Plans` condition keep loading and match only queries on the streaming path, for the statement shapes the streaming planner used to label `SelectStream` (`Select`, `SelectImpossible`, `SelectLockFunc`, `Nextval`, `Show`, `ShowMigrations`, `OtherRead`). VTTablet logs a deprecation warning when such a rule is loaded.
- `ANALYZE` statements on the streaming path, which used to carry the `OtherRead` plan type and now plan as `Select`, keep matching rules keyed on `OtherRead` (and do not match `SelectStream` rules, as before). Because `OtherRead` remains a valid plan name, this cannot be detected when the rules file is loaded; VTTablet logs a deprecation warning when a rule matches a streamed `ANALYZE` only through this compatibility behavior.

Both compatibility behaviors will be removed in v26, along with the `SelectStream` plan name.

**Impact**: Update query rules that use `SelectStream` to the concrete plan names listed above, and re-key `OtherRead` rules meant to gate streamed `ANALYZE` on the `Select` plan or a `Query` pattern. Note that rules keyed on concrete plan names match on both execution paths, not only streamed queries.

## <a id="minor-changes"/>Minor Changes</a>

#### <a id="vreplication-reverse-workflow-data-protection"/>Default data protection for `_reverse` workflow cancel/complete</a>

When calling `cancel` or `complete` on an auto-generated `_reverse` workflow without explicitly providing `--keep-data=false`, the system now defaults to keeping data and returns a warning. This prevents accidental deletion of production tables on the original source side, where the `_reverse` workflow's target is actually your production keyspace.

**Behavior change:**

| Workflow type | `--keep-data` flag | Effective `keep_data` | Warning emitted |
|--------------|-------------------|----------------------|-----------------|
| Normal       | omitted           | `false`              | No              |
| `_reverse`   | omitted           | `true`               | **Yes** |
| `_reverse`   | `--keep-data=false` | `false`            | No              |

The `--keep-data` flag help text has been updated to note this default explicitly. This change applies to MoveTables, Reshard, and other VReplication workflow types that use the shared cancel/complete paths.

See [#19906](https://github.com/vitessio/vitess/pull/19906) for details.

#### <a id="vreplication-vdiff-no-samples"/>`vdiff show --no-samples` strips the per-table row-sample report</a>

`vtctldclient vdiff ... show` now accepts a `--no-samples` flag. When set, the per-table diff report has its row-sample arrays (`MismatchedRowsSample`, `ExtraRowsSourceSample`, `ExtraRowsTargetSample`) stripped on the tablet, while the scalar counters and all other summary fields are preserved. This avoids exceeding gRPC message limits when `vdiff show` aggregates large blob/JSON row samples across every target shard. It is exposed as `no_samples` on the `VDiffShowRequest` (vtctld) and `VDiffReportOptions` (tablet) protobuf messages, and is opt-in and backward compatible.

`vdiff create --wait` also uses `no_samples` for its internal progress polls. Text output is unchanged; with `--format json`, the per-interval progress output no longer includes the row samples (they remain available via `vdiff show --verbose` once the diff completes).

See [#20870](https://github.com/vitessio/vitess/pull/20870) for details.

#### <a id="vreplication-materialize-cancel-data-protection"/>Preserve Materialize target data on cancel by default</a>

`vtctldclient Materialize cancel` now preserves the materialized target tables and their data. To remove the target tables when canceling the workflow, explicitly pass `--keep-data=false`.

Previously `Materialize cancel` exposed no `--keep-data` flag and always omitted `keep_data` from the `WorkflowDelete` request. The server resolves an omitted `keep_data` to `false`, so canceling a Materialize workflow always dropped the target tables with no way to opt out. `Materialize cancel` now has its own command that always sends `keep_data` explicitly.

This is a client-side fix. The server and the generic `vtctldclient Workflow delete` command are unchanged, so operators must upgrade `vtctldclient` to pick it up; an older client canceling a Materialize workflow against a newer server still drops the target tables.

See [#20711](https://github.com/vitessio/vitess/issues/20711) for details.

#### <a id="onlineddl-vrepl-auto-resume"/>Online DDL migrations are no longer failed by recoverable vreplication errors</a>

Online DDL now creates its vreplication streams with a per-workflow configuration override that pins `--vreplication-max-time-to-retry-on-error` to 0 (retry forever). A recoverable error therefore keeps the stream retrying instead of exhausting the retry window and failing the migration, regardless of the tablet-wide flag value. Genuinely unrecoverable errors (e.g. a duplicate-key error on the shadow table) still fail the migration immediately, and the existing 180-minute stale-migration policy remains the overall limit on a migration that makes no progress. A stream created before this change (an in-flight migration across a rolling upgrade) that stops on a retries-exhausted error is repaired on the fly: the executor restarts it with the retry-forever override installed, preserving the migration's copy progress.
As part of this change, vreplication terminal errors are now classified in their error message as either unrecoverable (retrying cannot fix them) or retries-exhausted (the retry window expired on an otherwise recoverable error), making it clear to operators why a stream stopped.

See [#20926](https://github.com/vitessio/vitess/issues/20926) for details.

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-logstats-ingress-bytes"/>Ingress bytes in query LogStats</a>

VTGate query `LogStats` now include an `IngressBytes` field that records the approximate number of inbound request bytes attributed to each query.

For MySQL-protocol connections, this is the number of bytes read from the client packets for the command that produced the query, including any prepared-statement long-data chunks folded in when `COM_STMT_EXECUTE` consumes them. For gRPC connections, it is approximated from the serialized size of the protobuf request. When a single command carries multiple statements, the bytes are distributed across them by query length.

`IngressBytes` is available through the `LogStats` struct for telemetry and monitoring integrations. It is not written to VTGate's query log output and defaults to zero for callers that do not set it.

See [#20358](https://github.com/vitessio/vitess/pull/20358) for details.

#### <a id="vtgate-cross-keyspace-reads"/>New controls for cross-keyspace reads</a>

VTGate now supports preventing cross-keyspace reads (joins and UNIONs), preventing queries that would combine data from different keyspaces. This can be configured at two levels:

**VTGate flag** (applies to all queries):

```
--prevent-cross-keyspace-reads
```

**Per-keyspace VSchema setting** (applies to specific keyspaces):

```bash
vtctldclient ApplyVSchema --vschema='{"prevent_cross_keyspace_reads": true}' my_keyspace
```

When enabled, the planner will reject queries that require joining or combining (via UNION) tables from different keyspaces. This can be overridden on a per-query basis using the `ALLOW_CROSS_KEYSPACE_READS` comment directive:

```sql
/*vt+ ALLOW_CROSS_KEYSPACE_READS */ SELECT * FROM ks1.t1 JOIN ks2.t2 ON t1.id = t2.id;
```

The VTGate flag prevents cross-keyspace reads globally, regardless of per-keyspace VSchema settings.

#### <a id="vtgate-streamexecute-real-errors"/>Streaming errors no longer surface as connection loss</a>

Streaming queries (under `SET workload = 'OLAP'`, multi-statement batches, and prepared-statement execution) previously returned `ERROR 2013 (HY000): Lost connection to MySQL server during query` and tore down the underlying TCP connection whenever the streaming handler returned an error *after* the first row or field packet had been emitted. VTGate now writes a proper ERR packet in place of the result-set terminator, so the real error code and message reach the client and the connection remains usable for subsequent queries.

This affects all three streaming code paths in `go/mysql`: `COM_QUERY` (text protocol), multi-statement `COM_QUERY`, and `COM_STMT_EXECUTE` (binary protocol).

**Impact**: Application error-handling and retry logic that branched on `2013 / Lost connection` will now see the real error code — for example, `errno 1317 / context canceled` after a `KILL QUERY` against a streaming session, or planner errors such as `specifying two different database in the query is not supported`.

#### <a id="vtgate-temp-table-heartbeat"/>Temporary-table connections are kept alive with a heartbeat</a>

A session that creates an explicit `CREATE TEMPORARY TABLE` pins a reserved connection on the tablet. Previously that connection was reclaimed by the tablet's idle timeout (`--queryserver-config-transaction-timeout`, default 30s), silently dropping the temporary table out from under an idle session. VTGate now sends a low-frequency background keepalive on those reserved connections, controlled by the new `--temp-table-heartbeat-time` flag (default 10s). The keepalive refreshes only the tablet's own reserved-connection timers — nothing is sent to mysqld, so mysqld's `wait_timeout` keeps counting real session traffic and reclaims idle connections exactly as MySQL would: a session idle past `wait_timeout` loses its connection, and with it its temporary tables, just like on a direct MySQL connection. All of a tablet's reserved connections are refreshed with batched touch RPCs — each touch refreshes up to 1024 reservations, so a tablet holding more is refreshed in a few concurrent touches. Batching amortizes the RPC overhead to one touch per 1024 reservations per tablet; VTGate's registry and the tablet-side timer work still scale with the number of reservations and participating tablets. The keepalive also detects a connection that mysqld has already closed (again without sending anything) and releases it, so dead connections do not linger in the tablet's pools. Keepalives run concurrently with client commands: a keepalive that finds the reserved connection busy counts it as alive, and a client command that collides with a keepalive's brief tablet-side timer refresh waits it out (a matter of microseconds, with no query executed). Keepalives stop for reserved connections that no longer exist, and for one whose tablet has changed type (for example `REPLICA` to `RDONLY`) so the session can no longer reach it: the tablet validates the keepalive's target just as it would a query, and vtgate drops a registration the tablet rejects as a wrong tablet, so the orphaned reservation is reclaimed at the tablet's idle timeout rather than kept open. **Upgrade tablets before vtgate.** The keepalive always carries reserved id 0 (the ids to refresh travel in a separate list), so a tablet that predates this feature runs the fallback query on a throwaway pooled connection rather than a reserved one — it can never kill a reserved connection or its temporary tables. Such a tablet's reserved connections are simply not kept alive until it is upgraded, falling back to the tablet timeout as before this feature. Reserved connections on shards with an open transaction are not kept alive: the tablet intentionally does not reset its transaction timer for in-transaction activity, so transactions — with or without temporary tables — remain subject to the transaction timeout as always. Within the same session, reserved connections on other shards keep their keepalives, and a shard's keepalives resume once its transaction commits.

`--temp-table-heartbeat-time` **must be set far enough below the tablets' effective reserved-connection timeout for the workload** — `--queryserver-config-transaction-timeout`, or `--queryserver-config-olap-transaction-timeout` for OLAP sessions — **to leave room for one keepalive round-trip**, or the connection can still be reclaimed between heartbeats. The tablet refreshes a connection's timer only when a keepalive reaches it, so the worst-case gap is the interval plus one round-trip (bounded by the per-tablet beat budget, which can reach three-quarters of the interval at short intervals); as a rule of thumb keep the interval under half of that timeout. A session whose own shorter transaction timeout (`SET @@transaction_timeout`) is at or below that worst-case gap is not protected by the keepalive — its temporary tables are then subject to that shorter timeout, by design. Note the scope: the tablet applies the session value to a reserved connection when the connection is reserved, and again whenever a transaction begins on it; setting a shorter timeout after the connection was reserved does not shorten the existing reservation's timer until a transaction next begins on that connection. If VTGate is lost (crash or restart), the heartbeats stop and the tablet reclaims the connection at its normal timeout, so nothing leaks.

The keepalive applies to connections using the MySQL protocol. Sessions used via the VTGate gRPC API travel with each request and live client-side between calls, so VTGate cannot observe whether the client is still alive between calls and they do not receive keepalives. Their temporary tables are instead covered by the tablet-side temp-table idle timeout described in the next section.

Relatedly, `CREATE TEMPORARY TABLE` issued with an explicit tablet-type or shard target (e.g. `USE \`ks:-80\``) now runs on a reserved connection and registers keepalives like any other temporary-table create. Previously it passed through to an ordinary pooled connection, where the temporary table could outlive the query and leak into whichever session used that connection next. Because a temporary table lives on one reserved connection, temporary-table DDL (`CREATE`/`DROP`) now requires its destination to resolve to exactly one shard. A multi-shard destination is rejected — whether from an explicit keyrange target spanning several shards or from an untargeted statement on a sharded keyspace, which resolves to all shards; both previously fanned the statement out to arbitrary pooled connections on every shard.

A tablet that is not serving (not serving state, unhealthy replication, stalled demotion, shutting down) rejects keepalives just as it rejects queries, so its reserved connections age out at the tablet's timeout exactly as they did before this feature.

`COM_RESET_CONNECTION` now resets vtgate's session the way MySQL resets its own: it releases the reserved connections and rebuilds the session as a fresh default — system and user variables, `LAST_INSERT_ID`, autocommit, and the temp-table and reserved-connection state all return to their just-connected values, and only the default database is preserved. Previously the recorded `SET` values survived a reset and were silently re-applied on the next query; applications behind connection poolers that reset connections between checkouts (Connector/J, ProxySQL) will now observe MySQL-standard behavior.

See [#20320](https://github.com/vitessio/vitess/issues/20320) for details.

#### <a id="vttablet-temp-table-idle-timeout"/>Temp-table idle timeout gives gRPC API sessions MySQL-equivalent temp-table lifetime</a>

The heartbeat described above covers MySQL-protocol sessions, whose keepalives are anchored to the client's wire connection to VTGate. Sessions used via the VTGate gRPC API have no such anchor — the session travels with each request — so their temporary tables were still reclaimed at the tablet's reserved-connection timeout (`--queryserver-config-transaction-timeout`, default 30s) when the session idled.

VTTablet now applies a separate idle timeout to reserved connections that hold temporary tables and are **not** covered by the VTGate keepalive, controlled by the new `--queryserver-config-temp-table-idle-timeout` flag:

- `-1` (default, "auto"): the tablet mirrors its own mysqld's `@@global.wait_timeout`, read via the dba pool when the query service opens and refreshed on the periodic schema reload, so a runtime `SET GLOBAL wait_timeout` converges without a restart. Out of the box, a gRPC API session's temporary tables live exactly as long as they would on a direct mysqld connection — mysqld reclaims any connection idle past `wait_timeout` regardless, so `wait_timeout` is the ceiling it already enforces.
- `0`: disabled — temp-table reserved connections are reclaimed at the transaction timeout, exactly as before this feature. This is the kill switch.
- `> 0`: an explicit idle timeout. Keep it **at or below mysqld's `wait_timeout`** (mysqld reclaims first otherwise, making extra headroom meaningless) and **at or above the transaction timeout** (the flag replaces the transaction timeout for these connections, so a smaller value reclaims them *sooner* than before; the tablet logs a startup warning in that case).

Every query on the connection refreshes the idle clock, just as every query resets mysqld's `wait_timeout` clock. Activity on the *session* counts too — queries no matter where they route, statement prepares, and any other client protocol command, including the ones VTGate answers locally (`COM_PING`, `COM_SET_OPTION`, prepared-statement bookkeeping), each of which restarts the idle `wait_timeout` wait on a direct MySQL connection: VTGate fans session activity out to the session's idle temp-table reserved connections by running a trivial statement on each (rate-limited to once per `--temp-table-heartbeat-time` per connection, fire-and-forget), resetting both the tablet's idle timer and mysqld's `wait_timeout` clock. An active session therefore keeps its temporary tables even when its queries route to other shards, exactly as on a direct MySQL connection — where session activity and connection activity are the same thing — while a truly idle session still ages out at `wait_timeout` as MySQL intends. Connections with an open transaction always keep the transaction timeout — bounded transaction lifetime is intended semantics — and connections kept alive by the VTGate heartbeat keep the short timer, so they are still reclaimed quickly when their heartbeats stop.

**This default is a behavior change on tablet upgrade**: an abandoned gRPC temp-table session (e.g. a crashed client) now lingers up to `wait_timeout` (8 hours by mysqld default) instead of the transaction timeout — the same window mysqld itself grants a vanished client. These connections occupy the stateful pool capped by `--queryserver-config-transaction-cap` (default **20**), so a burst of abandoned temp-table sessions could pin most of that pool for the full window. Size the cap and the flag together for your workload, or set the flag to `0` to restore the previous behavior.

Two new metrics make the feature observable: the `TempTableUnmanagedConnections` gauge counts connections currently holding temporary tables without keepalive coverage, and the `TempTableIdleTimeoutKills` counter counts connections the tablet reclaimed because this idle timeout elapsed.

#### <a id="vtgate-grpc-static-auth-sha256"/>SHA256-hashed passwords in the static gRPC auth plugin</a>

The static gRPC authentication plugin (`--grpc-auth-static-password-file`) now accepts SHA256-hashed passwords in addition to plaintext ones. Each entry in the credentials file gains an optional `CachingSha2Password` field holding the hex-encoded `SHA256(SHA256(password))`, with an optional leading `*`. This is the same format the MySQL protocol's static auth server uses for its own `CachingSha2Password` field, so a single stored credential can authenticate a user on both the MySQL and gRPC endpoints, and existing `caching_sha2_password`-style hashes can be copied over verbatim.

When an entry sets `CachingSha2Password`, it takes precedence over the plaintext `Password` field. A single credentials file may mix plaintext and hashed entries:

```json
[
  {"Username": "user1", "Password": "plaintext_password"},
  {"Username": "user2", "CachingSha2Password": "*49bbd275dd4bfb1170ced93e839a8ec1d5b86eab6acb0842502130a31702390d"}
]
```

The hash is validated and hex-decoded once when the plugin loads. An entry whose `CachingSha2Password` is not valid hex, or does not decode to a 32-byte SHA256 digest, causes the plugin to fail to initialize. No new plugin or flag is introduced.

See [#19250](https://github.com/vitessio/vitess/pull/19250) for details.

#### <a id="vtgate-prepare-tables-used"/>PREPARE statements no longer report the prepared statement's tables</a>

Plans for SQL-level `PREPARE` statements no longer record the tables of the statement text being prepared. `PREPARE` only plans the statement text and registers it in the session; it does not access any tables. As a result, VTGate query logs no longer list those tables in `TablesUsed` for `PREPARE` statements, and the `QueryExecutionsByTable` metric no longer counts a `PREPARE` as an execution against them. `EXECUTE` is unchanged and still reports the tables of the statement it runs.

See [#20562](https://github.com/vitessio/vitess/pull/20562) for details.

#### <a id="vtgate-prepare-no-implicit-tx"/>Preparing a statement no longer starts an implicit transaction</a>

With autocommit disabled, preparing a statement no longer opens an implicit transaction. This applies both to preparing over the MySQL binary protocol (`COM_STMT_PREPARE`) and to the SQL-level `PREPARE` and `DEALLOCATE PREPARE` statements, which previously started an implicit transaction like any other statement.

This matches MySQL's behavior: preparing a statement doesn't access table data, so the transaction only starts when the prepared statement is executed. `EXECUTE` and `COM_STMT_EXECUTE` still start an implicit transaction as before.

See [#20538](https://github.com/vitessio/vitess/pull/20538) and [#20562](https://github.com/vitessio/vitess/pull/20562) for details.

#### <a id="vtgate-prepare-stricter-validation"/>Stricter validation of SQL-level PREPARE statements</a>

SQL-level `PREPARE` and binary-protocol `COM_STMT_PREPARE` now reject statement text that itself manages prepared statements (`PREPARE`, `EXECUTE`, `DEALLOCATE PREPARE`) with MySQL's `ER_UNSUPPORTED_PS` error (1295). Previous versions accepted most of these and performed the nested statement's session changes while planning the outer one; MySQL rejects them all at PREPARE time.

Additionally, `PREPARE ... FROM ?` is now a syntax error, matching MySQL: the grammar accidentally accepted a positional parameter as the statement text, but no value could ever reach it and the statement always failed. This also affects programs that parse SQL using the `go/vt/sqlparser` package directly.

See [#20562](https://github.com/vitessio/vitess/pull/20562) for details.

#### <a id="vtgate-proxy-protocol-v1-strictness"/>Stricter PROXY protocol v1 header validation</a>

On listeners with `--proxy-protocol` enabled, malformed PROXY protocol v1 headers that earlier versions tolerated are now rejected, and the connection is closed before the MySQL handshake. This also comes from the go-proxyproto upgrade, which brought the v1 parser in line with the PROXY protocol specification. The newly rejected forms are:

- `TCP6` headers whose address fields contain plain IPv4 addresses. The specification requires addresses in IPv6 format on a `TCP6` line; the nginx OSS stream module is known to emit the IPv4 form when it proxies between address families (for example, an IPv6 client reaching an IPv4 upstream).
- Port fields with leading zeros (`01234`) or a sign (`+80`).
- Header lines with extra fields after the destination port.
- IPv6 addresses carrying a zone identifier (`fe80::1%eth0`).

Specification-conformant v1 headers, as emitted by HAProxy, AWS load balancers, and common nginx configurations, are unaffected.

**Impact**: Deployments whose proxy emits one of the forms above — most notably the nginx stream module proxying between IPv6 clients and IPv4 upstreams — will have those connections rejected before the MySQL handshake. Configure the proxy to emit specification-conformant headers (for nginx, listen on a matching address family or on a v4-mapped socket so addresses are rendered in IPv6 form).

See [#20733](https://github.com/vitessio/vitess/pull/20733) for details.

#### <a id="vtgate-sql-mode-rejection"/>MySQL-faithful validation and rejection of unsupported `sql_mode` values</a>

VTGate already rejected `SET sql_mode = ...` statements that enable a mode the Vitess parser does not support (`ANSI_QUOTES`, `NO_BACKSLASH_ESCAPES`, `PIPES_AS_CONCAT`, `REAL_AS_FLOAT`). The check compared mode names textually and could be bypassed. VTGate now implements MySQL 8.x's `sql_mode` assignment semantics, verified against MySQL 8.0.46. It validates every assignment against them:

- Setting an unsupported mode is rejected even when the underlying MySQL already runs with that mode, that is, when the `SET` would not change the value. Such statements previously succeeded, even though VTGate does not parse queries under these modes. The combination mode `ANSI` is rejected as well, because it enables `ANSI_QUOTES`, `PIPES_AS_CONCAT`, and `REAL_AS_FLOAT`.
- `IGNORE_SPACE` and `HIGH_NOT_PRECEDENCE` are now also rejected. They are the two remaining modes that change how SQL text is interpreted. The Vitess parser does not honor them, so queries would be parsed differently at the VTGate than the session's `sql_mode` promises.
- Unknown mode names and invalid numeric values fail with MySQL's own errors: `ER_WRONG_VALUE_FOR_VAR` (1231), and `ER_UNSUPPORTED_SQL_MODE` (3899) for the bits of modes removed in MySQL 8.0.
- Numeric values decode against MySQL's `sql_mode` bitmask. For example, `SET sql_mode = 1048576` reports that `NO_BACKSLASH_ESCAPES` is unsupported. Valid numeric values are accepted.
- Constant values are validated at planning time, with no shard round trip. This includes constant expressions such as `CONCAT` over literals, and it applies also when `--enable-system-settings` is disabled. Non-constant expressions are validated at execution time, once their value is known.

**Impact**: Clients that issue `SET sql_mode` with an unsupported mode now receive an error, also when the `SET` is a no-op that matches the backend's existing `sql_mode`. Clients that set mode names the backend MySQL would itself reject receive an error as well. Such sessions were already unreliable, because VTGate parses queries without honoring these modes.

#### <a id="vtgate-vexplain-mysqlplan"/>New `VEXPLAIN MYSQLPLAN` statement</a>

A new `VEXPLAIN MYSQLPLAN <query>` statement runs MySQL's `EXPLAIN FORMAT=JSON` against the shards a `SELECT` would target, **without executing the query itself**. It resolves each `Route`'s target shards from its vindex at resolution time and issues `EXPLAIN` against every resolved shard, attaching the per-shard MySQL plan to the VTGate plan tree keyed by shard, so per-shard plan and cost differences are visible.

Unlike `VEXPLAIN ALL`, which executes the query to discover the shard-level queries before explaining them, `VEXPLAIN MYSQLPLAN` never runs the wrapped query.

This no-execution guarantee is specifically about the wrapped query. The `EXPLAIN FORMAT=JSON` statement itself is still executed by MySQL on each shard, and — exactly like a plain `EXPLAIN` — MySQL may evaluate parts of it as a side effect: [`EXPLAIN` can execute a stored function](https://dev.mysql.com/doc/refman/8.4/en/derived-tables.html) reached through a view or derived table that MySQL [materializes during optimization](https://dev.mysql.com/doc/refman/8.4/en/derived-table-optimization.html), and such a function can modify data. `VEXPLAIN MYSQLPLAN` rejects the query shapes it can detect that carry this risk (derived tables, subqueries, CTEs, sequence and advisory-lock functions, and views known to the schema tracker), but with view tracking disabled (`--enable-views=false`) it cannot tell an untracked view from a base table, so `EXPLAIN` against such a view can still trigger these side effects. This matches what issuing `EXPLAIN` directly does — except that a plain `EXPLAIN` lands on a single arbitrary shard whereas `VEXPLAIN MYSQLPLAN` runs it against every resolved shard, and `VEXPLAIN ALL` is more exposed still, since it executes the wrapped query before explaining it.

Only `SELECT` statements whose target shards can be resolved from a vindex without reading cluster data are supported. DML (`INSERT`/`UPDATE`/`DELETE`), and any query whose shard set depends on data — cross-shard joins, subqueries, recursive CTEs, and lookup vindexes — are rejected with an error suggesting `VEXPLAIN ALL` instead. Derived tables, views, and common table expressions are likewise unsupported: `EXPLAIN FORMAT=JSON` can materialize a derived table during optimization (running any stored function inside it once per shard), which would break the promise never to run the wrapped query.

For queries eligible for deferred plan optimization (where equal bind variable values let the plan collapse to a single shard at execution time), `VEXPLAIN MYSQLPLAN` explains the general (baseline) plan rather than the value-specific optimized one, so it reports the full shard footprint the query can target regardless of the bind variable values supplied.

For each `Route` in the plan, the per-shard `EXPLAIN` queries are run concurrently across that `Route`'s shards, reusing the same scatter fan-out a real query would use; plans with multiple `Route` nodes (for example, a `UNION`) explain each `Route` in turn. If the `EXPLAIN` against any targeted shard fails (for example, an unreachable shard), the whole `VEXPLAIN MYSQLPLAN` command fails with that error rather than returning a partial result — matching the default all-or-nothing behavior of a scatter query.

Because each per-shard `EXPLAIN` runs on a separate connection, a `VEXPLAIN MYSQLPLAN` issued inside an open transaction reflects the pre-transaction state of each shard rather than any uncommitted changes made in that transaction — the same limitation as `VEXPLAIN ALL`.

Like a plain `EXPLAIN`, the per-shard `EXPLAIN FORMAT=JSON` queries `VEXPLAIN MYSQLPLAN` issues are not subject to table ACL checks on the explained tables, so `VEXPLAIN MYSQLPLAN` can return per-shard plan metadata (index names, row estimates, filtered percentages) for tables the caller could not otherwise read. For the same reason — the tablet plans an `EXPLAIN` without the explained table's identity — query denylist rules that are conditioned on a table name are not enforced against these per-shard `EXPLAIN` queries either; denylist rules conditioned on the query pattern still apply if their pattern matches the `explain format = json ...` query text. Unlike a plain `EXPLAIN`, which reaches a single arbitrary shard, `VEXPLAIN MYSQLPLAN` extends this to every resolved shard of every keyspace in the plan. Deployments that rely on table ACLs or table-scoped query denylist rules to restrict read access should restrict access to `VEXPLAIN MYSQLPLAN` accordingly.

### <a id="minor-changes-reparent"/>Reparent</a>

#### <a id="ers-lagging-relay-log-wait"/>`EmergencyReparentShard` no longer waits on replicas that cannot win the election</a>

`EmergencyReparentShard` (including VTOrc-triggered failovers) used to wait for every candidate to finish applying its relay logs before electing a new primary, and to fail the whole reparent if any candidate could not do so within `--wait-replicas-timeout`. A single lagging or stuck replica — a busy `RDONLY`, a replica freshly restored from a backup, a stopped SQL thread — could fail an emergency failover that it could never have won anyway.

For shards using GTID-based replication, ERS now waits only on the candidates at the most-advanced *received* relay log position, and one of them finishing to apply is sufficient to elect a winner. Candidates that are behind on received relay logs, or that fail to apply them, are excluded from the election but are still repointed under the new primary afterwards. Shards not using GTID-based replication (e.g. FilePos) keep the previous wait-for-all behavior.

This mirrors a tradeoff `orchestrator` made before Vitess: it never gated dead-primary promotion on all replicas draining their relay logs — its relay-log gates (`DelayMasterPromotionIfSQLThreadNotUpToDate`, `FailMasterPromotionIfSQLThreadNotUpToDate`) were candidate-scoped and opt-in, and `PostponeReplicaRecoveryOnLagMinutes` explicitly deferred lagging replicas until after the election. ERS remains more conservative: the promoted candidate must always have fully applied everything it received.

**Impact**: emergency failovers now succeed in shard states where they previously timed out. A reparent can succeed while some replicas are still catching up; they are repointed and continue replicating under the new primary. No flags were added or changed.

See [#18529](https://github.com/vitessio/vitess/issues/18529).

#### <a id="ers-allow-split-brain-promotion"/>`EmergencyReparentShard` can explicitly recover from split brain</a>

`EmergencyReparentShard` now identifies divergent leading MySQL GTID histories before waiting for relay logs or filtering errant GTIDs. The default path proceeds only when existing errant-GTID detection proves exactly one upfront leader remains; otherwise ERS fails with the tablet alias and position of every leader. An operator can proceed by passing `--new-primary <tablet-alias> --allow-split-brain-promotion`; the requested primary must be one of those upfront undominated candidates. The override requires `--new-primary`. ERS promotes exactly that tablet and preserves its complete history. It bypasses nothing else: the chosen primary must still apply its relay logs, and the `MustNot` promotion rule, cross-cell restrictions, the semi-sync forward-progress check, and the shard lock re-checks all still apply. VTOrc never opts in automatically.

**Impact**: allowing a split-brain promotion discards divergent transactions that exist only on losing branches, and tablets containing those branches may need to be rebuilt from the new primary. Each override whose promotion completes increments `EmergencyReparentSplitBrainOverrides{Keyspace,Shard}` so operators can alert on and audit use of the escape hatch; an override that aborts before promotion discards nothing and is not counted.

See [#20199](https://github.com/vitessio/vitess/issues/20199).

#### <a id="reparent-gtid-candidate-ordering"/>Reparent candidate ordering now respects partially ordered GTID histories</a>

GTID containment is pairwise, so a candidate set can mix comparable and divergent histories: candidate A at `p:1-100,a:1-10` is strictly ahead of B at `p:1-100,a:1-5`, while C at `p:1-100,c:1-3` is incomparable with both. The reparent sorter that both `EmergencyReparentShard` and `PlannedReparentShard` use compared such candidates non-transitively, so ordering could depend on map iteration or RPC completion order, and `PlannedReparentShard` could select B even though A was known to be more advanced.

Candidates are now ordered by GTID dominance before the existing promotion-rule, buffer-pool, and tablet-alias tiebreakers, so a dominated candidate can never rank ahead of its dominator regardless of input order. `EmergencyReparentShard` still rejects incomparable candidates as split brain, and `PlannedReparentShard` still chooses among incomparable maximal candidates. Positions that contain each other without being equal (possible with MariaDB GTIDs, where containment ignores the origin server) are now also rejected by `EmergencyReparentShard` as split brain, wherever the pair sits among the candidates; previously a leading pair failed with an internal sorting error, while a pair behind a more advanced candidate was not detected at all.

See [#20579](https://github.com/vitessio/vitess/issues/20579).

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-reject-unsupported-sql-modes"/>VTTablet rejects unsupported `sql_mode` values</a>

VTTablet now applies the same `sql_mode` validation as VTGate (see [the VTGate section](#vtgate-sql-mode-rejection)) at its own entry points and returns the identical errors. The entry points are connection settings (the settings pool and reserved connections) and session-scope `SET sql_mode` statements. This check concerns clients that bypass VTGate's validation: older VTGates in a mixed-version cluster, and clients that talk to the query service directly. `SET_VAR` optimizer hints are not judged: a hint applies to the hinted statement's execution only and cannot change how that statement's own text is lexed, so VTTablet forwards it verbatim and MySQL warns about and ignores an invalid hint value, exactly as it does for a client that sends the hint to it directly.

Constant values are judged before execution. Connection settings must parse as SET statements that carry constant `sql_mode` values. Settings are applied with no verification afterwards, so a value that cannot be judged upfront, such as a `CONCAT` expression, is rejected. This holds on both the settings-pool and reserved-connection paths.

A non-constant expression in a `SET` statement executed on a dedicated connection is verified after execution instead. The applied `@@sql_mode` is read back. The assignment is undone by restoring the previous mode when that value fails validation, does not decode as MySQL 8.x modes, or cannot be read back at all, so the failed `SET` does not apply, just as in MySQL. The connection is closed if the restore itself fails. Such a `SET` must assign `sql_mode` alone: MySQL applies none of a failed `SET`'s assignments, and a multi-assignment `SET` whose `sql_mode` can only be judged afterwards would already have applied its other assignments by then, so it is rejected upfront.

The `ApplySchema` `--session-variable` option validates `sql_mode` values the same way. Validation runs when the DDL strategy is parsed, and again on the tablet applying the variables.

The server's own configuration is covered as well. MySQL lexes every statement under the session `sql_mode` in effect *before* the statement; a `SET_VAR` hint cannot influence the parsing of its own statement. Vitess-formatted SQL should therefore always be lexed under the same rules it was serialized with, regardless of how the backend happens to be configured. Every MySQL connection Vitess creates now strips the lexer modes from the session `sql_mode` it inherits from the server's global value. This happens at the single connector choke point all components dial through: query serving, schema tracking and apply, heartbeats, replication management, Online DDL, vreplication and VDiff, VStream snapshots, and init scripts alike. All runtime modes (strict modes, zero-date handling, and so on) are preserved. The settings-pool reset restores this neutralized value rather than `default`, for the same reason.

Two consequences are deliberate. Connections that serve `ExecuteFetchAsDba`-style admin RPCs also start neutralized: operator-supplied statements run with the server's lexer modes stripped, and a multi-statement batch can still set the session `sql_mode` explicitly. Connections to *external* MySQL servers (vreplication sources, point-in-time recovery) are neutralized too, because Vitess sends them the same Vitess-formatted SQL. Statements that an operator's `sql_mode`-sensitive tooling sends outside Vitess are unaffected: the neutralization is session-scoped and never touches the global value.

**Impact**: During a rolling upgrade, VTTablets are typically upgraded before VTGates. A session that set a now-rejected `sql_mode`, such as `ANSI`, through a not-yet-upgraded VTGate will start receiving errors from upgraded VTTablets. Such sessions were already unreliable, because VTGate parses queries without honoring these modes. Queries now always run with the server's lexer modes stripped from the session, so Vitess-formatted SQL parses consistently regardless of the backend's global configuration.

#### <a id="vttablet-consolidator-reject-on-cap"/>Consolidator Reject on Waiter Cap</a>

A new `--consolidator-reject-on-cap` flag (default `false`) has been added to VTTablet. When enabled alongside a non-zero `--consolidator-query-waiter-cap`, queries that would join a consolidated result but exceed the **global** consolidator waiter cap are rejected with a `RESOURCE_EXHAUSTED` error instead of silently falling back to independent MySQL execution.

**Important:** The cap is enforced against the consolidator's global `totalWaiterCount` across all queries, not a per-query waiter count. This means a duplicate for query B can be rejected because query A has already consumed most of the global waiter budget. This provides backpressure when the consolidator as a whole is saturated, rather than when any single query has too many waiters.

See [#19836](https://github.com/vitessio/vitess/pull/19836) for details.

#### <a id="vttablet-reserved-conn-kill-query"/>Query timeouts no longer kill reserved connections outside transactions</a>

Previously, when a query executing on a reserved connection (one holding temporary tables or session settings) exceeded its timeout, the tablet killed the entire MySQL connection, destroying the session state along with it. Now, when the reserved connection is not inside a transaction and the statement is a read or DML, only the query is killed (`KILL QUERY`): the client receives a query-interrupted error, and the connection — with its temporary tables and settings — survives. This applies to both regular and streaming (OLAP) execution. This matches how timeouts already behaved for regular (non-reserved) queries and mirrors MySQL's own statement-timeout semantics. Statements whose interruption could leave session state the session never recorded — `SET`, lock functions such as `GET_LOCK`, DDL, and admin statements — still kill the whole connection, as does any timeout inside a transaction, because a partially-executed transaction cannot be safely continued. If a `KILL QUERY` does not actually unblock the statement (for example a thread wedged in storage I/O), the tablet escalates to killing the connection after one kill timeout (5s) rather than leaving the connection stuck in use; this escalation applies to timeouts on pooled connections as well, where previously only the query was killed — so a statement whose rollback outlasts the kill timeout now also costs its pooled connection. Stored-procedure calls (`CALL`) outside a transaction are also an exception: because a procedure can start a transaction that Vitess does not track, any `CALL` error — including a timeout — closes the reserved connection, so its temporary tables and settings do not survive in that case. Inside a Vitess-tracked transaction a failed buffered `CALL` returns the error and leaves the connection and transaction usable, matching MySQL; on the streaming path any `CALL` error still closes the connection, whose interrupted result stream cannot be safely reused. The mutating advisory-lock functions (`release_lock`, `release_all_locks`) are now classified like `get_lock` on the tablet for timeout handling: on a reserved connection, a timeout kills the connection rather than keeping it with lock state the session never recorded. Unlike `get_lock`, they do not require a reserved connection — when the session holds no locks they continue to execute on a pooled connection, where releasing is a safe no-op. The classification now also looks at the whole statement rather than only the select list, and covers DML as well as SELECT: a `get_lock` in a predicate (e.g. `select id from t where get_lock('x', 0) = 1`, or the same predicate in an `UPDATE`/`DELETE`/`INSERT ... SELECT`) previously planned as an ordinary statement and acquired the lock on an arbitrary pooled connection, leaking it to that connection's next borrower; such statements now require a reserved connection and are rejected without one, and DML containing any mutating lock function likewise kills the connection on a timeout rather than keeping it.

See [#20429](https://github.com/vitessio/vitess/pull/20429) for details.

#### <a id="vttablet-stream-query-timeout"/>Query timeout for state-changing statements on the streaming path</a>

Streaming reads (`StreamExecute` outside a transaction) remain exempt from the tablet query timeout so OLAP results can stream indefinitely. State-changing statements served over the streaming path — DML, DDL, `FLUSH`, sequence allocation, migration commands, and similar — are bounded by the same query timeout that buffered execution applies. In v24 and earlier, these statements were rejected on the streaming path entirely; v25 introduces support for them, bounded by the standard query timeout from the start. Only streaming reads retain the unbounded exemption, unchanged from previous releases.

See [#20499](https://github.com/vitessio/vitess/pull/20499) for details.

#### <a id="vttablet-rules-apply-to-streaming"/>Query rules now apply to queries on the streaming path</a>

Before v25, queries served over the streaming path (`workload=olap` connections and the `StreamExecute` API) carried the internal `SelectStream` plan type, so query rules keyed on concrete plan types such as `Select`, `Insert`, or `Show` matched only buffered execution. In v25 these queries produce the same plan types as buffered execution, so a query rule keyed on a concrete plan type now applies to both execution paths.

**Impact**: Review existing query rules. A rule written for buffered queries — including one enforcing a `FAIL` or `BUFFER` policy — now also affects the same statements arriving over `workload=olap`/`StreamExecute` connections. See the [`SelectStream` deprecation note](#deprecated-selectstream-rule-plan) for the backward-compatibility behavior of rules keyed on the old streaming plan types.

See [#20499](https://github.com/vitessio/vitess/pull/20499) for details.

#### <a id="vttablet-demote-primary-lock-wait-timeout"/>New `--demote-primary-lock-wait-timeout` flag</a>

A new VTTablet flag, `--demote-primary-lock-wait-timeout` (default `0`, disabled), bounds how long enabling `super_read_only` waits for metadata locks during a primary demotion. Long-running queries hold metadata locks that block `SET GLOBAL super_read_only`, which can stall a `PlannedReparentShard` or `EmergencyReparentShard` behind them. With the flag set, the demotion applies a session `lock_wait_timeout` (rounded up to whole seconds) so the statement fails fast with a lock-wait-timeout error instead of waiting indefinitely.

When disabled (the default), demotion behavior is unchanged and the wait is unbounded.

See [#20285](https://github.com/vitessio/vitess/pull/20285) for details.

#### <a id="vttablet-schema-max-table-count"/>Schema engine table-count limit is now configurable</a>

Previously the schema engine had a hardcoded cap of 10,000 tables: a vttablet whose underlying MySQL had more than 10,000 tables would fail to load its schema and could not serve queries. This made recovery from `EmergencyReparentShard` impossible without dropping tables directly on MySQL.

Two changes:

1. The schema engine no longer enforces a row cap on its reload queries. A vttablet with any number of tables will load successfully.
2. A new flag, `--queryserver-config-schema-max-table-count` (default `10000`), governs new schema object creation for tables and views. `CREATE TABLE` and `CREATE VIEW` statements that would push the engine's tracked schema-object count above this limit are rejected at vttablet with a clear error before they reach MySQL. The flag is dynamic: changes are observed without restart.

Tablets that already have more tracked schema objects than the configured limit will reload fine — only new creations are gated. Operators who need to support more tables and views should increase the flag and ensure both vttablet and mysqld have enough memory to comfortably hold the larger schema.

See [#19978](https://github.com/vitessio/vitess/issues/19978) for details.

#### <a id="vttablet-replica-crash-safe-shutdown"/>Replicas are placed in a crash-safe state before shutdown</a>

When VTTablet gracefully shuts down a `REPLICA`/`RDONLY` MySQL, it now proactively puts the server into a crash-safe state first, so that an interrupted shutdown or a host crash during shutdown cannot leave the replica with unsynced writes that are lost or re-applied on restart.

Just before handing off to the shutdown hook, VTTablet, on replicas only:

- restores full commit durability by setting `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1` (these are commonly relaxed together to let a replica catch up faster, and may still be relaxed when a shutdown begins),
- sets `sync_relay_log=1`, then flushes the engine, binary, and relay logs so the InnoDB redo, binary-log, and relay-log tails already written under the relaxed settings become durable (the settings alone only govern commits from that point on), and
- stops the replication receiver (I/O) and applier (SQL) threads so the multi-threaded applier queue drains to a gap-free, position-consistent point.

The whole preparation is best effort: if any step fails, or the (bounded) preparation times out, the error is logged and shutdown proceeds regardless, so making a replica crash-safe never blocks or fails the shutdown itself. If the shutdown itself then fails while mysqld is still running — for example a failing `mysqld_shutdown` hook — the previous replication and durability state is restored (best effort), so a failed shutdown does not leave a live replica with replication stopped.

A failed shutdown that leaves such a restoration pending can delay process exit past `--shutdown-wait-time` while the exit waits for the restoration to finish — by up to roughly 10 minutes in the worst case — and environments that enforce a hard exit deadline (e.g. Kubernetes' termination grace period) will SIGKILL and discard the restoration, leaving the safely fenced replica to external recovery such as VTOrc. Shutdown attempts are also now serialized across processes sharing a mysqld instance via a lock file next to the pid file: while one process's shutdown attempt (or its pending restoration) is in flight, another process's attempt waits within its own timeout and then fails, rather than proceed unserialized.

**Impact**: On a graceful replica shutdown that completes the preparation, `innodb_flush_log_at_trx_commit`, `sync_binlog`, and `sync_relay_log` are set to `1` and both replication threads are stopped, regardless of their prior runtime values; if the preparation cannot complete, it is skipped and logged. The preparation keys off `SHOW REPLICA STATUS`, so it applies to any mysqld with a replication source configured — which excludes a normally promoted `PRIMARY`, but includes a `PRIMARY` that keeps a replication channel configured (e.g. one replicating from an external source with `--disable_active_reparents`), whose replication is stopped by the preparation like any replica's.

See [#20599](https://github.com/vitessio/vitess/pull/20599) for details.

#### <a id="vttablet-mysql-shell-restore-skip-version-check"/>Skip MySQL version check when restoring from a mysql-shell backup</a>

A new `--mysql-shell-restore-skip-version-check` flag (default `false`) has been added to VTTablet and VTBackup. When enabled, the MySQL version compatibility check that normally gates restores is skipped, but only for backups taken with the `mysqlshell` engine. Backups taken with other engines still go through the usual version check regardless of this flag.

Because mysql-shell performs a logical restore, its backups are not tied to the on-disk data dictionary format the way physical backups are, so restoring across otherwise-incompatible MySQL versions can be safe. This flag lets operators opt into that behavior.

**Impact**: With this flag set, VTTablet may select and restore a `mysqlshell` backup whose MySQL version would otherwise be rejected as incompatible. Leave it unset to preserve the existing behavior.

#### <a id="vttablet-applyschema-session-variables"/>ApplySchema session variables</a>

`ApplySchema` now accepts repeatable `--session-variable name=value` DDL
strategy options. The assignments use MySQL `SESSION` scope and are applied in
the order supplied.

A `sql_mode` assignment gets the same MySQL-faithful validation as a `SET sql_mode`
statement on a VTGate session (see [the VTGate section](#vtgate-sql-mode-rejection)).
This includes the rejection of modes that change how SQL text is interpreted, because
the statements executed under these variables are Vitess-formatted SQL. Validation runs
when the DDL strategy is parsed, and again on the tablet applying the variables.

For the `direct` strategy, the variables apply to the dedicated DBA connection
that executes the requested schema statements. For Online DDL, they apply to
the dedicated connections used for:

- scheduler-executed direct DDL;
- VReplication shadow-table creation, alteration, and `AUTO_INCREMENT`
  adjustment;
- declarative comparison-table DDL; and
- online view artifact creation and its view swap.

The variables do not apply to the pooled connections used during a
VReplication cutover. In particular, they do not affect sentry-table DDL or the
final `RENAME TABLE` that swaps the original and shadow tables.

Each affected connection's previous values are restored afterward. Invalid,
duplicate, or denied variable names and failed assignments stop the operation
before schema DDL executes on that connection. `sql_log_bin`,
`foreign_key_checks`, and `gtid_next` are denied.

**Compatibility note:** `--session-variable` requires vtctld and vttablet at
v25 or newer. On a mixed-version cluster, an upgraded caller can send the new
`session_variables` RPC field (or Online DDL options) to an older tablet that
does not understand them. The tablet may still run the DDL while skipping the
requested session state, so the option can appear to succeed without effect.
Upgrade vtctld, vtctldclient, vtgate and vttablet before executing a schema
change with `--session-variable`.

See [#20654](https://github.com/vitessio/vitess/pull/20654) for details.

### <a id="minor-changes-vtctld"/>VTCtld</a>

#### <a id="vtctld-version-aware-reparent"/>MySQL version-aware reparent candidate election</a>

`PlannedReparentShard` (PRS) and `EmergencyReparentShard` (ERS) now consider the MySQL server version when electing a new primary. During rolling MySQL upgrades, this prevents promoting a newer-version tablet that would break replication for replicas still on the older version.

Versions are compared by major.minor. The patch component is normally ignored (patch releases are bugfix-only and do not affect replication compatibility), with one exception: within the MySQL 8.0 series, feature additions before 8.0.34 (the first bugfix-only 8.0 patch — e.g. binary log transaction compression added in 8.0.20) can make a newer patch incompatible as a source to an older-patch replica. So when both candidates are in the 8.0 series and the lower patch is below 8.0.34, the patch is compared too.

**Sort order:**
- PRS (graceful and initialization paths): promotion rules > MySQL version > replication position > buffer pool > alias
- PRS (no-clear-primary path): replication position > promotion rules > MySQL version > buffer pool > alias
- ERS: replication position > promotion rules > MySQL version > buffer pool > alias

PRS prefers version over position on most paths because it catches the elected tablet up to a known position before promotion (so position is not a data-safety concern), and on the initialization path because no tablet has ever replicated (nothing to lose). On the no-clear-primary path, however, PRS promotes the elected tablet *without* catching it up, and demoted replicas can hold received-but-unapplied relay-log transactions — so there PRS keeps position first (matching ERS), using version only to break ties among equally-advanced candidates. When this causes a lower (more broadly compatible) version to be passed over because a higher-version candidate is more advanced, PRS logs a warning; run PRS again once the shard has a healthy primary to move to the preferred version if desired.

    **Behavior change:** PRS during a rolling MySQL upgrade will now prefer lower-version candidates on the graceful and initialization paths regardless of how far behind they are in replication position — the ordering compares version before position, so there is no bound on the position gap (see the no-clear-primary caveat above). The elected tablet will catch up to the old primary's demotion position before completing the reparent, which may increase reparent latency. Operators should ensure `--wait-replicas-timeout` is generous enough to accommodate this catch-up time.

**Behavior change (PRS ordering):** independent of any version difference, PRS on the graceful and initialization paths now orders candidates by promotion rule before replication position (previously position came first). This can change which tablet PRS elects even in a cluster with a single MySQL version — for example, a `PREFERRED` candidate that is slightly behind is now chosen over a same-position candidate with a neutral promotion rule. As above, those paths either catch the elected tablet up to the old primary's position before completing or promote a never-replicated tablet, so this does not risk data loss. On the no-clear-primary path PRS keeps replication position first (as it promotes without catch-up), and ERS is likewise unchanged with position first.

**Cross-cell limitation:** PRS will still promote a higher-version tablet if no lower-version candidate exists in the same cell as the current primary. The cell boundary is enforced before version comparison. Operators who want version preference to override cell locality can use `--allow-cross-cell-promotion`.

Tablets that do not report a version (e.g. running an older Vitess build) are treated as "unknown version" and sorted last. When every candidate is unknown, the version comparison is a no-op and election falls through to the previous position/promotion ordering. But during a rolling upgrade of Vitess itself — where some `vttablet`s already report a version and others do not yet — this biases elections toward the already-upgraded tablets: the known-version candidates remain comparable to each other while the not-yet-upgraded ones sort last. How strong that bias is depends on the path. On the graceful and initialization paths (version-first ordering), a known-version candidate is preferred over an unknown-version one regardless of replication position — the elect is caught up (graceful) or has never replicated (init), so position is not a data-safety concern there. On the no-clear-primary path and in ERS (position-first ordering), the version bias only breaks ties among equally-advanced candidates and never overrides a more-advanced tablet. In neither case does this promote a replication-incompatible primary.

**Former-primary tie-breaking:** as part of this change, an ERS candidate that was the former primary and is equally advanced (same replication position) as a replica now participates in the promotion-rule and version tie-breakers. Previously such a candidate was treated as slightly behind and could lose the election to an equally-advanced replica, because its executed position was left uninitialized.

**Flavor compatibility:** version comparison is only applied when all candidates belong to the same flavor family. MySQL and Percona Server share a version lineage and are compared against each other; MariaDB is a separate lineage, so a shard mixing MariaDB with MySQL/Percona disables version-aware election and falls back to the previous position/promotion ordering (with a warning logged).

**ERS is version-aware only for MySQL-GTID shards; PRS applies to all single-family shards.** This is a deliberate asymmetry between the two operations:

- **PRS** applies version-aware election whenever all candidates share one flavor family (see above), including MariaDB-only shards and shards using file-position (non-GTID) replication. On the graceful path PRS catches the elected tablet up to the old primary's exact position before completing, and on the initialization path no tablet has ever replicated, so replication position is not a data-safety concern on those paths and preferring a compatible version is safe. On the no-clear-primary path PRS promotes without catch-up, so it orders by position first there (version only breaks ties among equally-advanced candidates) — see the sort-order note above.
- **ERS** applies version-aware election only when the shard uses MySQL GTID-based replication (`MySQL56` GTID sets — i.e. MySQL or Percona Server). ERS is **not** version-aware, and retains the previous position/promotion ordering with no version tiebreak, when:
  - the shard uses file-position (non-GTID) replication, or
  - the shard uses MariaDB (whose GTID sets are not `MySQL56`-based and take the same non-GTID code path).

  ERS prioritizes certainty that it picked the most-advanced candidate to minimize data loss, and only the MySQL-GTID path reconciles equally-advanced candidates to a common applied position after the relay-log wait — which is what lets the version tiebreak fire without misordering candidates by position. On the other paths ERS compares candidates on their executed positions and leaves version out of the decision.

**Upgrade ordering for non-`REPLICA` tablets:** version-aware election only considers `REPLICA`-type candidates, but a completed reparent repoints every non-`RESTORE` tablet (including `RDONLY`) to the new primary. Because the election prefers the lowest-version replica, all other replicas are at least as new as the winner and replicate safely; a non-`REPLICA` tablet on an *older* MySQL version, however, could be made to replicate from a newer-version primary, which is not guaranteed safe. To avoid this during a rolling MySQL upgrade, upgrade non-`REPLICA` tablets (e.g. `RDONLY`) **before** `REPLICA` tablets, so that no older non-`REPLICA` tablet ever needs to replicate from a newer elected primary. This is operational guidance — PRS does not enforce the ordering or fail when it is not followed.

The same caveat applies to a `REPLICA` that is *excluded from election* — because it is taking a backup, reports an unknown replication status, or exceeds `--tolerable-replication-lag`. Such a replica does not contribute to the version floor the election computes, yet it is still repointed to the new primary afterward, so an older excluded replica could end up replicating from a newer elected primary. During a rolling MySQL upgrade, avoid running PRS while an older-version replica is in one of those excluded states. As above, PRS does not currently enforce this or fail when it is not followed; basing the compatibility floor on the full repointed set (and a force flag to override) is planned as a follow-up.

**Behavior change (file-position PRS without an explicit primary):** when PRS elects the new primary itself (no `--new-primary` given) on the initialization or no-clear-primary paths, it now confirms the elected tablet's replication position contains every other tablet's before promoting — those paths promote directly, without first catching the elect up to a source. On file-position (non-GTID) shards this containment cannot be established: each tablet's binlog coordinates are local to that tablet and not comparable across tablets. PRS now **fails closed** in that case rather than risk promoting a tablet that is missing another's transactions. Operators of file-position shards should pass an explicit `--new-primary` to PRS on these paths. GTID-based shards (MySQL/Percona/MariaDB GTID) and freshly-initialized shards with empty positions are unaffected. A shard that *mixes* GTID-based and file-position tablets (for example, mid-migration between the two) also **fails closed** on these paths, and here even an explicit `--new-primary` does not override the rejection: a GTID elect's transactions cannot be compared against a file-position peer's, so there is no safe way to prove the elect is not discarding the peer's history.

**Behavior change (self-elected initial promotion now reads and checks every tablet's position):** on the initialization path (a shard that has never had a primary), PRS now issues a `PrimaryPosition` read to every non-`RESTORE` tablet and requires the elect's position to contain all of them before calling `InitPrimary` (the containment check above). This closes a data-loss window — a tablet seeded from a backup or external source can hold transactions the version-preferred elect lacks, which a blind `InitPrimary` would discard. Two consequences follow: initialization now also **fails if any tablet's position read fails** (PRS already required every tablet to be reachable, but it did not previously read positions on this path), and on file-position (non-GTID) shards the elect's containment cannot be established across tablets, so PRS **fails closed** unless an explicit `--new-primary` is given. Passing `--new-primary` does not skip the GTID dominance check — an explicitly-named primary must still contain every reachable tablet's position. On MariaDB shards, which report an executed GTID position but no separate relay-log position, the containment check compares each peer's executed position (the only cross-tablet-comparable position MariaDB exposes), so a version-preferred elect that is behind a MariaDB peer is correctly rejected.

See [#20211](https://github.com/vitessio/vitess/pull/20211) for details.

### <a id="minor-changes-backup"/>Backup/Restore</a>

#### <a id="backup-chunked-builtin"/>Chunked backup/restore for the `builtinbackupengine`</a>

The builtin backup engine now supports splitting large files into chunks for parallel backup and restore. This significantly improves restore throughput for keyspaces dominated by a small number of large InnoDB files, as individual chunks can be restored concurrently via parallel writes.

Two new flags control chunking behavior:

- `--builtinbackup-file-chunk-threshold` (default `0`, chunking disabled): files larger than this size in bytes are split into chunks during backup.
- `--builtinbackup-file-chunk-size` (default `1073741824` / 1 GiB): the target size in bytes for each chunk.

**Compatibility note:** Backups created with chunking enabled are **not restorable by older Vitess versions** that do not understand the `Chunks` field in the backup MANIFEST. Non-chunked backups (the default) remain fully compatible with older versions.

See [#20167](https://github.com/vitessio/vitess/pull/20167) for details.

#### <a id="backup-mysqld-shutdown-timeout"/>Slow clean mysqld shutdowns no longer fail backups</a>

The builtin backup engine's shutdown deadline (`--builtinbackup-mysqld-timeout`) is now raised to the backup request's mysqld shutdown timeout (e.g. vtbackup's `--mysql-shutdown-timeout`) plus a 30 second grace period whenever that is larger, so the two settings can no longer silently conflict. The same grace period now pads the shutdown contexts of `mysqlctl`, `mysqlctld` and `vtbackup`, which moves `mysqlctld`'s derived `--onterm-timeout` default from `5m10s` to `5m30s`.

In addition, when `mysqladmin` gives up waiting for mysqld to stop, the shutdown is no longer failed immediately: the `SHUTDOWN` command has already been delivered at that point, so Vitess keeps waiting on the pid/socket files until the caller's deadline expires (or for a 30 second grace period, when the caller has no deadline). Slow-but-clean shutdowns, such as upgrade-safe backups running with `innodb_fast_shutdown=0` on large databases, previously failed with `Aborted waiting on pid file` even though mysqld was stopping normally.

### <a id="minor-changes-general"/>General</a>

#### <a id="build-info-from-vcs"/>Build version metadata now sourced from VCS stamping</a>

The build timestamp is no longer injected via linker flags. Because it changed on every `make build`, it forced every binary to be re-linked even when nothing else changed. Build metadata is now read from the VCS information the Go toolchain stamps into the binary (`runtime/debug.ReadBuildInfo`), which makes the linker flags stable across rebuilds and lets the build cache hit.

User-visible consequences:

- The `build_time` reported by `--version`, exposed via `/debug/vars` (`BuildTimestamp`), and set as a tablet tag (`build_time`) now defaults to the **commit time** of the built revision rather than the wall-clock time of the build.
- Binaries built from a dirty working tree report their Git revision with a `-dirty` suffix.

The `BUILD_GIT_REV`, `BUILD_GIT_BRANCH`, and `BUILD_TIME` environment-variable overrides still work for builds without VCS metadata (e.g. from a release tarball). When `BUILD_TIME` is set, it takes precedence over the commit time.
