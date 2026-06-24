# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
        - [Experimental parallel VReplication applier](#vreplication-parallel-applier)
    - **[Breaking Changes](#breaking-changes)**
        - [`--watch-replication-stream` flag removed](#vttablet-watch-replication-stream-removed)
        - [Snapshot Topology feature removed](#vtorc-snapshot-topology-removed)
        - [VTOrc `--cell` flag is now required](#vtorc-cell-required)
    - **[Deprecations](#deprecations)**
        - [CLI Flags](#deprecated-cli-flags)
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
        - [Unknown VStream event types are now hard errors in the applier](#vreplication-unknown-event-error)
        - [Workflow config overrides sent to source tablets are now allowlisted](#vreplication-source-overrides-allowlist)
    - **[VTGate](#minor-changes-vtgate)**
        - [New controls for cross-keyspace reads](#vtgate-cross-keyspace-reads)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Consolidator Reject on Waiter Cap](#vttablet-consolidator-reject-on-cap)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)
    - **[General](#minor-changes-general)**
        - [Build version metadata now sourced from VCS stamping](#build-info-from-vcs)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

#### <a id="vreplication-parallel-applier"/>Experimental parallel VReplication applier</a>

> [!WARNING]
> This feature is experimental.

VReplication can now apply binlog events using multiple concurrent MySQL connections instead of a single serial connection. Set `--vreplication-parallel-replication-workers=N` (default `1` = serial, maximum `64`) on `vttablet`, or the `vreplication-parallel-replication-workers` per-workflow config override, to dispatch non-conflicting transactions to `N` worker goroutines during the replication (running) phase. Conflicts are detected with target-side writeset hashing (primary key, unique key, and foreign key values — similar to MySQL's own `WRITESET` dependency tracking), so it works regardless of the source's `binlog_transaction_dependency_tracking` setting. Commits remain strictly ordered, so the workflow position, lag metrics, and `WaitForPos` semantics are unchanged. Transactions the conflict detector cannot reason about (DDL, statement-based events, partial row images, prefix/expression unique indexes, and similar) fall back to serial application.

Note that each worker holds two MySQL connections, so a workflow with `N` workers uses `2N+2` target-side connections.

### <a id="breaking-changes"/>Breaking Changes</a>

#### <a id="vttablet-watch-replication-stream-removed"/>`--watch-replication-stream` flag removed</a>

The deprecated `--watch-replication-stream` VTTablet flag has been removed.

**Migration**: remove `--watch-replication-stream` from VTTablet startup arguments.

**Impact**: VTTablet will fail to start if `--watch-replication-stream` is still passed.

See [#20048](https://github.com/vitessio/vitess/pull/20048) for the removal and [#19204](https://github.com/vitessio/vitess/pull/19204) for the original deprecation.

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

### <a id="deprecations"/>Deprecations</a>

#### <a id="deprecated-cli-flags"/>CLI Flags</a>

The VTGate flag `--legacy-replication-lag-algorithm` is now deprecated and is a no-op. VTGate always uses the simpler replication lag algorithm based on low lag, high lag and the minimum number of tablets. A detailed explanation of the algorithm [is available in this code comment](https://github.com/vitessio/vitess/blob/main/go/vt/discovery/replicationlag.go).

The flag will be removed entirely in v26. This deprecation is tracked in https://github.com/vitessio/vitess/issues/18914.

**Impact**: Remove any usage of the `--legacy-replication-lag-algorithm` flag from VTGate startup scripts or configuration.

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

#### <a id="vreplication-unknown-event-error"/>Unknown VStream event types are now hard errors in the applier</a>

The VReplication applier previously ignored VStream event types it did not recognize. It now fails the workflow with an error for unknown event types (and unknown `on-ddl` actions), failing closed instead of silently skipping events. All event types produced by supported Vitess versions are handled; this only affects streams from sources emitting event types unknown to the target's version.

#### <a id="vreplication-source-overrides-allowlist"/>Workflow config overrides sent to source tablets are now allowlisted</a>

When a workflow has per-workflow config overrides, the target now sends only the source-relevant subset (packet size, timeouts, experimental flags, and similar) to the source tablet's VStreamer instead of the full override map. This keeps newer target-only override keys from failing workflows whose source tablets run an older version that rejects unknown keys.

See [#19906](https://github.com/vitessio/vitess/pull/19906) for details.

### <a id="minor-changes-vtgate"/>VTGate</a>

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

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-consolidator-reject-on-cap"/>Consolidator Reject on Waiter Cap</a>

A new `--consolidator-reject-on-cap` flag (default `false`) has been added to VTTablet. When enabled alongside a non-zero `--consolidator-query-waiter-cap`, queries that would join a consolidated result but exceed the **global** consolidator waiter cap are rejected with a `RESOURCE_EXHAUSTED` error instead of silently falling back to independent MySQL execution.

**Important:** The cap is enforced against the consolidator's global `totalWaiterCount` across all queries, not a per-query waiter count. This means a duplicate for query B can be rejected because query A has already consumed most of the global waiter budget. This provides backpressure when the consolidator as a whole is saturated, rather than when any single query has too many waiters.

See [#19836](https://github.com/vitessio/vitess/pull/19836) for details.

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-schema-max-table-count"/>Schema engine table-count limit is now configurable</a>

Previously the schema engine had a hardcoded cap of 10,000 tables: a vttablet whose underlying MySQL had more than 10,000 tables would fail to load its schema and could not serve queries. This made recovery from `EmergencyReparentShard` impossible without dropping tables directly on MySQL.

Two changes:

1. The schema engine no longer enforces a row cap on its reload queries. A vttablet with any number of tables will load successfully.
2. A new flag, `--queryserver-config-schema-max-table-count` (default `10000`), governs new schema object creation for tables and views. `CREATE TABLE` and `CREATE VIEW` statements that would push the engine's tracked schema-object count above this limit are rejected at vttablet with a clear error before they reach MySQL. The flag is dynamic: changes are observed without restart.

Tablets that already have more tracked schema objects than the configured limit will reload fine — only new creations are gated. Operators who need to support more tables and views should increase the flag and ensure both vttablet and mysqld have enough memory to comfortably hold the larger schema.

See [#19978](https://github.com/vitessio/vitess/issues/19978) for details.

### <a id="minor-changes-general"/>General</a>

#### <a id="build-info-from-vcs"/>Build version metadata now sourced from VCS stamping</a>

The build timestamp is no longer injected via linker flags. Because it changed on every `make build`, it forced every binary to be re-linked even when nothing else changed. Build metadata is now read from the VCS information the Go toolchain stamps into the binary (`runtime/debug.ReadBuildInfo`), which makes the linker flags stable across rebuilds and lets the build cache hit.

User-visible consequences:

- The `build_time` reported by `--version`, exposed via `/debug/vars` (`BuildTimestamp`), and set as a tablet tag (`build_time`) now defaults to the **commit time** of the built revision rather than the wall-clock time of the build.
- Binaries built from a dirty working tree report their Git revision with a `-dirty` suffix.

The `BUILD_GIT_REV`, `BUILD_GIT_BRANCH`, and `BUILD_TIME` environment-variable overrides still work for builds without VCS metadata (e.g. from a release tarball). When `BUILD_TIME` is set, it takes precedence over the commit time.