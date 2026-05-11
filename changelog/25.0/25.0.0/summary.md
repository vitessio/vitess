# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
        - [QueryThrottler TABLET_THROTTLER Strategy](#querythrottler-tablet-throttler-strategy)
    - **[Breaking Changes](#breaking-changes)**
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
    - **[VTGate](#minor-changes-vtgate)**
        - [New controls for cross-keyspace reads](#vtgate-cross-keyspace-reads)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

#### <a id="querythrottler-tablet-throttler-strategy"/>QueryThrottler TABLET_THROTTLER Strategy</a>

The Query Throttler now supports a `TABLET_THROTTLER` strategy that integrates with the Tablet Throttler to make throttling decisions based on replication lag and custom metrics ([PR #19919](https://github.com/vitessio/vitess/pull/19919)). This builds on the experimental Query Throttler framework introduced in v23.0.0.

**New vtctldclient Command: `UpdateQueryThrottlerConfig`**

A new command enables dynamic configuration of query throttling per keyspace:

```bash
# Update configuration with inline JSON
vtctldclient UpdateQueryThrottlerConfig --config '{"enabled":true,"strategy":"TABLET_THROTTLER","dry_run":false}' my_keyspace

# Update configuration from a file
vtctldclient UpdateQueryThrottlerConfig --config-file /path/to/config.json my_keyspace
```

**New VTTablet Flag**

- `--tablet-throttler-cache-update-interval` - How frequently to refresh throttle check results for the tablet throttler strategy (default: 10s)

**Configuration Schema**

The `TABLET_THROTTLER` strategy uses a hierarchical rule structure:

```json
{
  "enabled": true,
  "strategy": "TABLET_THROTTLER",
  "dry_run": false,
  "tablet_strategy_config": {
    "tablet_rules": {
      "PRIMARY": {
        "statement_rules": {
          "SELECT": {
            "metric_rules": {
              "lag": {
                "thresholds": [
                  {"above": 5.0, "throttle": 50}
                ]
              }
            }
          }
        }
      }
    }
  }
}
```

**Hierarchy**:
1. **Tablet type** (PRIMARY, REPLICA, etc.)
2. **Statement type** (SELECT, INSERT, UPDATE, DELETE, etc.)
3. **Metric** (lag, custom metrics)
4. **Thresholds** (metric value and throttle percentage)

**How It Works**

- Evaluates throttle rules based on tablet type, SQL statement type, and configured metric thresholds
- Uses a caching layer that refreshes in the background at the configured interval
- Supports priority-based throttling: priority 0 (highest) is never throttled, priority 100 (lowest) is always evaluated
- DDL statements (ALTER MIGRATION, REVERT MIGRATION) are never throttled
- Configuration changes propagate to all tablets via topology server watches

**New Metrics**

The strategy exposes detailed observability metrics:

- `TABLET_THROTTLERCacheMisses` / `CacheHits` - Cache performance
- `TABLET_THROTTLERDecisionCount` - Throttling decisions by tablet_type, stmt_type, path (fast/full), outcome (allowed/throttled), reason
- `TABLET_THROTTLERFastDecisionLatencyMicroseconds` - Fast-path evaluation latency
- `TABLET_THROTTLERFullDecisionLatencyMicroseconds` - Full evaluation latency
- `TABLET_THROTTLERCacheLoadLatencyMilliseconds` - Background cache refresh latency

### <a id="breaking-changes"/>Breaking Changes</a>

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

#### <a id="vttablet-schema-max-table-count"/>Schema engine table-count limit is now configurable</a>

Previously the schema engine had a hardcoded cap of 10,000 tables: a vttablet whose underlying MySQL had more than 10,000 tables would fail to load its schema and could not serve queries. This made recovery from `EmergencyReparentShard` impossible without dropping tables directly on MySQL.

Two changes:

1. The schema engine no longer enforces a row cap on its reload queries. A vttablet with any number of tables will load successfully.
2. A new flag, `--queryserver-config-schema-max-table-count` (default `10000`), governs new schema object creation for tables and views. `CREATE TABLE` and `CREATE VIEW` statements that would push the engine's tracked schema-object count above this limit are rejected at vttablet with a clear error before they reach MySQL. The flag is dynamic: changes are observed without restart.

Tablets that already have more tracked schema objects than the configured limit will reload fine — only new creations are gated. Operators who need to support more tables and views should increase the flag and ensure both vttablet and mysqld have enough memory to comfortably hold the larger schema.

See [#19978](https://github.com/vitessio/vitess/issues/19978) for details.
