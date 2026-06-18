# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
    - **[Breaking Changes](#breaking-changes)**
        - [`--watch-replication-stream` flag removed](#vttablet-watch-replication-stream-removed)
        - [Snapshot Topology feature removed](#vtorc-snapshot-topology-removed)
        - [VTOrc `--cell` flag is now required](#vtorc-cell-required)
    - **[Deprecations](#deprecations)**
        - [CLI Flags](#deprecated-cli-flags)
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
    - **[VTGate](#minor-changes-vtgate)**
        - [New controls for cross-keyspace reads](#vtgate-cross-keyspace-reads)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Consolidator Reject on Waiter Cap](#vttablet-consolidator-reject-on-cap)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)
- **[Bug Fixes](#bug-fixes)**
    - **[VTTablet](#bug-fixes-vttablet)**
        - [Connection pool livelock under timeout storms](#smartconnpool-expired-waiter-livelock-fix)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

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

## <a id="bug-fixes"/>Bug Fixes</a>

### <a id="bug-fixes-vttablet"/>VTTablet</a>

#### <a id="smartconnpool-expired-waiter-livelock-fix"/>Connection pool livelock under timeout storms</a>

VTTablet connection pools no longer livelock when many queued requests time out at once. Previously, a connection returned to the pool could be handed to a waiting request whose context had already expired. That request failed immediately and recycled the connection to the next expired waiter, so a sustained timeout storm could drive useful throughput to zero, leaving the tablet unable to serve queries until it was restarted.

When handing off a returned connection, the pool now skips and evicts waiters whose context has already expired. If every waiting request has expired, the connection goes back into the pool for new requests to use, and requests whose context expired fail with the usual timeout error.

No configuration change is required.

See [#20308](https://github.com/vitessio/vitess/pull/20308) for details.