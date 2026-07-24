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
    - **[Deprecations](#deprecations)**
        - [CLI Flags](#deprecated-cli-flags)
        - [Legacy streaming-path plan types in query rules](#deprecated-selectstream-rule-plan)
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
    - **[VTGate](#minor-changes-vtgate)**
        - [Ingress bytes in query LogStats](#vtgate-logstats-ingress-bytes)
        - [New controls for cross-keyspace reads](#vtgate-cross-keyspace-reads)
        - [Streaming errors no longer surface as connection loss](#vtgate-streamexecute-real-errors)
        - [SHA256-hashed passwords in the static gRPC auth plugin](#vtgate-grpc-static-auth-sha256)
        - [PREPARE statements no longer report the prepared statement's tables](#vtgate-prepare-tables-used)
        - [Preparing a statement no longer starts an implicit transaction](#vtgate-prepare-no-implicit-tx)
        - [Stricter validation of SQL-level PREPARE statements](#vtgate-prepare-stricter-validation)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Consolidator Reject on Waiter Cap](#vttablet-consolidator-reject-on-cap)
        - [Query timeout for state-changing statements on the streaming path](#vttablet-stream-query-timeout)
        - [Query rules now apply to queries on the streaming path](#vttablet-rules-apply-to-streaming)
        - [New `--demote-primary-lock-wait-timeout` flag](#vttablet-demote-primary-lock-wait-timeout)
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)
        - [Replicas are placed in a crash-safe state before shutdown](#vttablet-replica-crash-safe-shutdown)
        - [Skip MySQL version check when restoring from a mysql-shell backup](#vttablet-mysql-shell-restore-skip-version-check)
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

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-consolidator-reject-on-cap"/>Consolidator Reject on Waiter Cap</a>

A new `--consolidator-reject-on-cap` flag (default `false`) has been added to VTTablet. When enabled alongside a non-zero `--consolidator-query-waiter-cap`, queries that would join a consolidated result but exceed the **global** consolidator waiter cap are rejected with a `RESOURCE_EXHAUSTED` error instead of silently falling back to independent MySQL execution.

**Important:** The cap is enforced against the consolidator's global `totalWaiterCount` across all queries, not a per-query waiter count. This means a duplicate for query B can be rejected because query A has already consumed most of the global waiter budget. This provides backpressure when the consolidator as a whole is saturated, rather than when any single query has too many waiters.

See [#19836](https://github.com/vitessio/vitess/pull/19836) for details.

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

**Impact**: On a graceful replica shutdown that completes the preparation, `innodb_flush_log_at_trx_commit`, `sync_binlog`, and `sync_relay_log` are set to `1` and both replication threads are stopped, regardless of their prior runtime values; if the preparation cannot complete, it is skipped and logged. This does not affect `PRIMARY` tablets.

See [#20599](https://github.com/vitessio/vitess/pull/20599) for details.

#### <a id="vttablet-mysql-shell-restore-skip-version-check"/>Skip MySQL version check when restoring from a mysql-shell backup</a>

A new `--mysql-shell-restore-skip-version-check` flag (default `false`) has been added to VTTablet and VTBackup. When enabled, the MySQL version compatibility check that normally gates restores is skipped, but only for backups taken with the `mysqlshell` engine. Backups taken with other engines still go through the usual version check regardless of this flag.

Because mysql-shell performs a logical restore, its backups are not tied to the on-disk data dictionary format the way physical backups are, so restoring across otherwise-incompatible MySQL versions can be safe. This flag lets operators opt into that behavior.

**Impact**: With this flag set, VTTablet may select and restore a `mysqlshell` backup whose MySQL version would otherwise be rejected as incompatible. Leave it unset to preserve the existing behavior.

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
