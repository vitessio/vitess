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
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
    - **[VTGate](#minor-changes-vtgate)**
        - [Ingress bytes in query LogStats](#vtgate-logstats-ingress-bytes)
        - [New controls for cross-keyspace reads](#vtgate-cross-keyspace-reads)
        - [Streaming errors no longer surface as connection loss](#vtgate-streamexecute-real-errors)
        - [SHA256-hashed passwords in the static gRPC auth plugin](#vtgate-grpc-static-auth-sha256)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Consolidator Reject on Waiter Cap](#vttablet-consolidator-reject-on-cap)
        - [New `--demote-primary-lock-wait-timeout` flag](#vttablet-demote-primary-lock-wait-timeout)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)
        - [Skip MySQL version check when restoring from a mysql-shell backup](#vttablet-mysql-shell-restore-skip-version-check)
    - **[Backup/Restore](#minor-changes-backup)**
        - [Chunked backup/restore for the builtinbackupengine](#backup-chunked-builtin)
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

**Migration:** drop `--cells-to-watch` from your vtorc invocation. If you previously used it for true cell-isolated deployments, the new flag is not a like-for-like replacement (vtorc will now discover and watch all cells); discuss your scenario in the linked issue if the new flag does not cover your needs.

See [#20021](https://github.com/vitessio/vitess/issues/20021) for details.

### <a id="deprecations"/>Deprecations</a>

#### <a id="deprecated-cli-flags"/>CLI Flags</a>

The VTGate flag `--legacy-replication-lag-algorithm` is now deprecated and is a no-op. VTGate always uses the simpler replication lag algorithm based on low lag, high lag and the minimum number of tablets. A detailed explanation of the algorithm [is available in this code comment](https://github.com/vitessio/vitess/blob/main/go/vt/discovery/replicationlag.go).

The flag will be removed entirely in v26. This deprecation is tracked in https://github.com/vitessio/vitess/issues/18914.

**Impact**: Remove any usage of the `--legacy-replication-lag-algorithm` flag from VTGate startup scripts or configuration.

The VTTablet flag `--vreplication-enable-http-log` is now deprecated and is a no-op, as the [VRLog feature it enabled has been removed](#vttablet-vrlog-removed). The flag will be removed entirely in v26.

**Impact**: Remove any usage of the `--vreplication-enable-http-log` flag from VTTablet startup scripts or configuration.

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

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-consolidator-reject-on-cap"/>Consolidator Reject on Waiter Cap</a>

A new `--consolidator-reject-on-cap` flag (default `false`) has been added to VTTablet. When enabled alongside a non-zero `--consolidator-query-waiter-cap`, queries that would join a consolidated result but exceed the **global** consolidator waiter cap are rejected with a `RESOURCE_EXHAUSTED` error instead of silently falling back to independent MySQL execution.

**Important:** The cap is enforced against the consolidator's global `totalWaiterCount` across all queries, not a per-query waiter count. This means a duplicate for query B can be rejected because query A has already consumed most of the global waiter budget. This provides backpressure when the consolidator as a whole is saturated, rather than when any single query has too many waiters.

See [#19836](https://github.com/vitessio/vitess/pull/19836) for details.

#### <a id="vttablet-demote-primary-lock-wait-timeout"/>New `--demote-primary-lock-wait-timeout` flag</a>

A new VTTablet flag, `--demote-primary-lock-wait-timeout` (default `0`, disabled), bounds how long enabling `super_read_only` waits for metadata locks during a primary demotion. Long-running queries hold metadata locks that block `SET GLOBAL super_read_only`, which can stall a `PlannedReparentShard` or `EmergencyReparentShard` behind them. With the flag set, the demotion applies a session `lock_wait_timeout` (rounded up to whole seconds) so the statement fails fast with a lock-wait-timeout error instead of waiting indefinitely.

When disabled (the default), demotion behavior is unchanged and the wait is unbounded.

See [#20285](https://github.com/vitessio/vitess/pull/20285) for details.

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-schema-max-table-count"/>Schema engine table-count limit is now configurable</a>

Previously the schema engine had a hardcoded cap of 10,000 tables: a vttablet whose underlying MySQL had more than 10,000 tables would fail to load its schema and could not serve queries. This made recovery from `EmergencyReparentShard` impossible without dropping tables directly on MySQL.

Two changes:

1. The schema engine no longer enforces a row cap on its reload queries. A vttablet with any number of tables will load successfully.
2. A new flag, `--queryserver-config-schema-max-table-count` (default `10000`), governs new schema object creation for tables and views. `CREATE TABLE` and `CREATE VIEW` statements that would push the engine's tracked schema-object count above this limit are rejected at vttablet with a clear error before they reach MySQL. The flag is dynamic: changes are observed without restart.

Tablets that already have more tracked schema objects than the configured limit will reload fine — only new creations are gated. Operators who need to support more tables and views should increase the flag and ensure both vttablet and mysqld have enough memory to comfortably hold the larger schema.

See [#19978](https://github.com/vitessio/vitess/issues/19978) for details.

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

### <a id="minor-changes-general"/>General</a>

#### <a id="build-info-from-vcs"/>Build version metadata now sourced from VCS stamping</a>

The build timestamp is no longer injected via linker flags. Because it changed on every `make build`, it forced every binary to be re-linked even when nothing else changed. Build metadata is now read from the VCS information the Go toolchain stamps into the binary (`runtime/debug.ReadBuildInfo`), which makes the linker flags stable across rebuilds and lets the build cache hit.

User-visible consequences:

- The `build_time` reported by `--version`, exposed via `/debug/vars` (`BuildTimestamp`), and set as a tablet tag (`build_time`) now defaults to the **commit time** of the built revision rather than the wall-clock time of the build.
- Binaries built from a dirty working tree report their Git revision with a `-dirty` suffix.

The `BUILD_GIT_REV`, `BUILD_GIT_BRANCH`, and `BUILD_TIME` environment-variable overrides still work for builds without VCS metadata (e.g. from a release tarball). When `BUILD_TIME` is set, it takes precedence over the commit time.
