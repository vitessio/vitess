# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
    - **[Breaking Changes](#breaking-changes)**
- **[Minor Changes](#minor-changes)**
    - **[VTGate](#minor-changes-vtgate)**
        - [Vindex Routing Information in LogStats](#vtgate-vindex-routing-logstats)
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
    - **[VTGate](#minor-changes-vtgate)**
        - [New controls for cross-keyspace reads](#vtgate-cross-keyspace-reads)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

### <a id="breaking-changes"/>Breaking Changes</a>

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-vindex-routing-logstats"/>Vindex Routing Information in LogStats</a>

VTGate now tracks which vindexes are used for shard routing in the `LogStats` struct. The new `RoutingIndexesUsed` field contains a list of `{keyspace, vindex_name, table}` tuples representing the vindexes consulted when routing each query.

This information helps debug sharded query performance by revealing which vindexes VTGate used for shard targeting. If a query unexpectedly scatters across all shards, an empty `RoutingIndexesUsed` indicates no vindex was available for routing.

**Behavior by statement type:**

- **SELECT, UPDATE, DELETE**: Records the vindex used in the WHERE clause for route targeting
- **INSERT**: Records the primary vindex (the first column vindex) that determines shard placement. Secondary vindexes populated as a side effect are not included.

**Note:** This data is available programmatically via the `LogStats` struct for telemetry and monitoring integrations. It is not currently included in VTGate's query log output.

See [#19913](https://github.com/vitessio/vitess/pull/19913) for details.

### <a id="minor-changes-vreplication"/>VReplication</a>

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

#### <a id="vttablet-schema-max-table-count"/>Schema engine table-count limit is now configurable</a>

Previously the schema engine had a hardcoded cap of 10,000 tables: a vttablet whose underlying MySQL had more than 10,000 tables would fail to load its schema and could not serve queries. This made recovery from `EmergencyReparentShard` impossible without dropping tables directly on MySQL.

Two changes:

1. The schema engine no longer enforces a row cap on its reload queries. A vttablet with any number of tables will load successfully.
2. A new flag, `--queryserver-config-schema-max-table-count` (default `10000`), governs new schema object creation for tables and views. `CREATE TABLE` and `CREATE VIEW` statements that would push the engine's tracked schema-object count above this limit are rejected at vttablet with a clear error before they reach MySQL. The flag is dynamic: changes are observed without restart.

Tablets that already have more tracked schema objects than the configured limit will reload fine — only new creations are gated. Operators who need to support more tables and views should increase the flag and ensure both vttablet and mysqld have enough memory to comfortably hold the larger schema.

See [#19978](https://github.com/vitessio/vitess/issues/19978) for details.
