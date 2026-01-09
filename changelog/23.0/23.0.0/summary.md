# Release of Vitess v23.0.0
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[Breaking Changes](#breaking-changes)**
    - **[Flag Naming Convention Migration](#flag-naming-convention-migration)**
    - **[New default versions](#new-default-versions)**
        - [Upgrade to MySQL 8.4](#upgrade-to-mysql-8-4)
    - **[New Support](#new-support)**
        - [Multi-Query Execution](#multi-query-execution)
        - [Transaction Timeout Session Variable](#transaction-timeout-session-variable)
        - [Experimental: Query Throttler](#query-throttler)
        - [Multiple Lookup Vindexes Support](#multiple-lookup-vindexes-support)
        - [Reference Tables in Materialize Workflows](#reference-tables-in-materialize-workflows)
        - [Online DDL Shard-Specific Completion](#online-ddl-shard-specific-completion)
        - [WITH RECURSIVE CTEs](#with-recursive-ctes)
        - [CREATE TABLE ... SELECT Support](#create-table-select-support)
    - **[Deprecations](#deprecations)**
        - [Metrics](#deprecated-metrics)
        - [CLI Flags](#deprecated-cli-flags)
    - **[Deletions](#deletions)**
        - [Metrics](#deleted-metrics)
    - **[New Metrics](#new-metrics)**
        - [VTGate](#new-vtgate-metrics)
        - [VTTablet](#new-vttablet-metrics)
        - [VTOrc](#new-vtorc-metrics)
- **[Minor Changes](#minor-changes)**
    - **[New CLI Flags](#new-cli-flags)**
        - [VReplication/Materialize](#new-vreplication-flags)
        - [Observability](#new-observability-flags)
        - [VTOrc](#new-vtorc-flags)
        - [Backup/Restore](#new-backup-flags)
        - [CLI Tools](#new-cli-tool-flags)
        - [VTAdmin](#new-vtadmin-flags)
        - [VTGate](#new-vtgate-flags-section)
    - **[Modified Metrics](#modified-metrics)**
    - **[Parser/SQL Enhancements](#parser-sql-enhancements)**
    - **[Query Planning Improvements](#query-planning-improvements)**
    - **[Topology](#minor-changes-topo)**
        - [`--consul-auth-static-file` requires 1 or more credentials](#consul-auth-static-file-check-creds)
    - **[VTOrc](#minor-changes-vtorc)**
        - [Aggregated Discovery Metrics HTTP API removed](#aggregated-discovery-metrics-api-removed)
        - [Dynamic control of `EmergencyReparentShard`-based recoveries](#vtorc-dynamic-ers-disabled)
        - [Recovery stats to include keyspace/shard](#recoveries-stats-keyspace-shard)
        - [`/api/replication-analysis` HTTP API deprecation](#replication-analysis-api-deprecation)
    - **[VTTablet](#minor-changes-vttablet)**
        - [API Changes](#api-changes)
        - [CLI Flags](#flags-vttablet)
        - [Managed MySQL configuration defaults to caching-sha2-password](#mysql-caching-sha2-password)
        - [MySQL timezone environment propagation](#mysql-timezone-env)
        - [gRPC `tabletmanager` client error changes](#grpctmclient-err-changes)
    - **[Docker](#docker)**

## <a id="major-changes"/>Major Changes</a>

### <a id="breaking-changes"/>Breaking Changes</a>

#### <a id="deleted-metrics-breaking"/>Deleted VTGate Metrics</a>

Four deprecated VTGate metrics have been completely removed in v23.0.0. These metrics were deprecated in v22.0.0:

| Metric Name               | Component | Deprecated In |
|---------------------------|-----------|---------------|
| `QueriesProcessed`        | vtgate    | v22.0.0       |
| `QueriesRouted`           | vtgate    | v22.0.0       |
| `QueriesProcessedByTable` | vtgate    | v22.0.0       |
| `QueriesRoutedByTable`    | vtgate    | v22.0.0       |

**Impact**: Any monitoring dashboards or alerting systems using these metrics must be updated to use the replacement metrics introduced in v22.0.0:
- Use `QueryExecutions` instead of `QueriesProcessed`
- Use `QueryRoutes` instead of `QueriesRouted`
- Use `QueryExecutionsByTable` instead of `QueriesProcessedByTable` and `QueriesRoutedByTable`

See the [v22.0.0 release notes](https://github.com/vitessio/vitess/blob/main/changelog/22.0/22.0.0/release_notes.md#new-vtgate-metrics) for details on the new metrics.

#### <a id="executefetchasdba-multistatement"/>ExecuteFetchAsDba No Longer Accepts Multi-Statement SQL</a>

The `ExecuteFetchAsDba` RPC method in TabletManager now explicitly rejects SQL queries containing multiple statements (as of [PR #18183](https://github.com/vitessio/vitess/pull/18183)).

**Impact**: Code or automation that previously passed multiple semicolon-separated SQL statements to `ExecuteFetchAsDba` will now receive an error. Each SQL statement must be sent in a separate RPC call.

**Migration**: Split multi-statement SQL into individual RPC calls:

```go
// Before (no longer works):
ExecuteFetchAsDba("CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT);")

// After (required in v23+):
ExecuteFetchAsDba("CREATE TABLE t1 (id INT);")
ExecuteFetchAsDba("CREATE TABLE t2 (id INT);")
```

#### <a id="grpc-error-codes"/>gRPC TabletManager Error Code Changes</a>

The `vttablet` gRPC `tabletmanager` client now returns errors wrapped by the internal `go/vt/vterrors` package ([PR #18565](https://github.com/vitessio/vitess/pull/18565)).

**Impact**: External automation relying on google-gRPC error codes must be updated to use `vterrors.Code(err)` to inspect error codes, which returns `vtrpcpb.Code`s defined in [proto/vtrpc.proto](https://github.com/vitessio/vitess/blob/main/proto/vtrpc.proto#L60).

**Migration**:
```go
// Before:
if status.Code(err) == codes.NotFound { ... }

// After:
if vterrors.Code(err) == vtrpcpb.Code_NOT_FOUND { ... }
```

#### <a id="gtid-api-changes"/>GTID API Signature Changes</a>

Several GTID-related API signatures changed in [PR #18196](https://github.com/vitessio/vitess/pull/18196) as part of GTID performance optimizations:

**Changed**: `BinlogEvent.GTID()` method signature
**Impact**: Code directly using the GTID parsing APIs may need updates. Most users are unaffected as these are internal APIs.

#### <a id="generateshardranges-api"/>GenerateShardRanges API Signature Change</a>

The `key.GenerateShardRanges()` function signature changed in [PR #18633](https://github.com/vitessio/vitess/pull/18633) to add a new `hexChars int` parameter controlling the hex width of generated shard names.

**Impact**: Code calling `GenerateShardRanges()` directly must be updated to pass the new parameter.

The corresponding vtctldclient command gained a new `--chars` flag to control this behavior.

---

### <a id="flag-naming-convention-migration"/>Flag Naming Convention Migration</a>

Vitess v23.0.0 includes a major standardization of CLI flag naming conventions across all binaries. **989 flags** have been migrated from underscore notation (`flag_name`) to dash notation (`flag-name`) in [PR #18280](https://github.com/vitessio/vitess/pull/18280) and related PRs.

#### Backward Compatibility

- **v23.0.0 and v24.0.0**: Both underscore and dash formats are supported. Underscore format is **deprecated** but functional.
- **v25.0.0**: Underscore format will be **removed**. Only dash format will be accepted.

#### Automatic Normalization

Flag normalization happens automatically at the `pflag` level ([PR #18642](https://github.com/vitessio/vitess/pull/18642)), so both formats are accepted without requiring code changes in v23/v24.

#### Example Flag Renames

Common flags affected (full list of 989 flags available in PR #18280):

**Backup flags**:
- `--azblob_backup_account_name` → `--azblob-backup-account-name`
- `--s3_backup_storage_bucket` → `--s3-backup-storage-bucket`
- `--xtrabackup_root_path` → `--xtrabackup-root-path`

**Replication flags**:
- `--heartbeat_enable` → `--heartbeat-enable`
- `--replication_connect_retry` → `--replication-connect-retry`

**gRPC flags** ([PR #18009](https://github.com/vitessio/vitess/pull/18009)):
- All gRPC-related flags standardized (30+ flags)

#### Action Required

Users should update configuration files, scripts, and automation to use dash-based flag names before upgrading to v25.0.0. The migration is backward compatible in v23 and v24, allowing gradual updates.

---

### <a id="new-default-versions"/>New default versions</a>

#### <a id="upgrade-to-mysql-8-4"/>Upgrade to MySQL 8.4</a>

The default major MySQL version used by our `vitess/lite:latest` image is going from `8.0.40` to `8.4.6`.
This change was merged in [#18569](https://github.com/vitessio/vitess/pull/18569).

VTGate also advertises MySQL version `8.4.6` by default instead of `8.0.40`. If that is not what you are running, you can set the `mysql_server_version` flag to advertise the desired version.

>  ⚠️ Upgrading to this release with vitess-operator:
>
> If you are using the `vitess-operator`, considering that we are bumping the MySQL version from `8.0.40` to `8.4.6`, you will have to manually upgrade:
>
> 1. Add `innodb_fast_shutdown=0` to your extra cnf in your YAML file.
> 2. Apply this file.
> 3. Wait for all the pods to be healthy.
> 4. Then change your YAML file to use the new Docker Images (`vitess/lite:v23.0.0`).
> 5. Remove `innodb_fast_shutdown=0` from your extra cnf in your YAML file.
> 6. Apply this file.
>
> This is only needed once when going from the latest `8.0.x` to `8.4.x`. Once you're on `8.4.x`, it is possible to upgrade and downgrade between `8.4.x` versions without needing to run `innodb_fast_shutdown=0`.

---

### <a id="new-support"/>New Support</a>

#### <a id="multi-query-execution"/>Multi-Query Execution</a>

Vitess v23.0.0 introduces native support for executing multiple queries in a single RPC call through new `ExecuteMulti` and `StreamExecuteMulti` APIs ([PR #18059](https://github.com/vitessio/vitess/pull/18059)).

This feature provides more efficient batch query execution without requiring manual query splitting or multiple round trips.

**Usage Example**:
```go
queries := []string{
    "SELECT * FROM users WHERE id = 1",
    "SELECT * FROM orders WHERE user_id = 1",
    "SELECT * FROM payments WHERE user_id = 1",
}
results, err := vtgateConn.ExecuteMulti(ctx, queries)
```

**Configuration**: Enable with the `--mysql-server-multi-query-protocol` flag on VTGate.

#### <a id="transaction-timeout-session-variable"/>Transaction Timeout Session Variable</a>

A new `transaction_timeout` session variable has been added ([PR #18560](https://github.com/vitessio/vitess/pull/18560)), allowing per-session control over transaction timeout duration.

**Usage**:
```sql
-- Set transaction timeout to 30 seconds for this session
SET transaction_timeout = 30;

-- Begin a transaction that will automatically rollback if not committed within 30s
BEGIN;
-- ... perform operations ...
COMMIT;
```

This provides more granular timeout control compared to global server settings, useful for:
- Long-running batch operations that need extended timeouts
- Interactive sessions that should fail fast
- Different timeout requirements per application workload

#### <a id="query-throttler"/>Experimental: Query Throttler</a>

Vitess v23.0.0 introduces a new, experimental Query Throttler framework for rate-limiting incoming queries ([RFC issue #18412](https://github.com/vitessio/vitess/issues/18412), [PR #18449](https://github.com/vitessio/vitess/pull/18449), [PR #18657](https://github.com/vitessio/vitess/pull/18657)). Work on this new throttler is ongoing with the potential for breaking changes in the future.

Feedback on this experimental feature is appreciated in GitHub issues or the `#feat-handling-overload` channel of the [Vitess Community Slack](https://vitess.io/slack).

**Features**:
- File-based configuration for throttling rules
- Dry-run mode for testing throttling without enforcement
- Dynamic rule reloading

**Configuration**:
- `--query-throttler-config-refresh-interval` - How often to reload throttler configuration

**Dry-run Mode**: Test throttling rules without actually blocking queries, useful for validating configuration before enforcement.

#### <a id="multiple-lookup-vindexes-support"/>Multiple Lookup Vindexes Support</a>

Creating multiple lookup vindexes in a single workflow is now supported through the `--params-file` flag ([PR #17566](https://github.com/vitessio/vitess/pull/17566)).

**Usage**:
```bash
# Create multiple lookup vindexes from JSON configuration
vtctldclient LookupVindexCreate \
  --workflow my_lookup_workflow \
  --params-file /path/to/params.json \
  commerce
```

**params.json example**:
```json
{
  "vindexes": [
    {
      "name": "user_email_lookup",
      "type": "consistent_lookup_unique",
      "table_owner": "users",
      "table_owner_columns": ["email"]
    },
    {
      "name": "user_name_lookup",
      "type": "consistent_lookup",
      "table_owner": "users",
      "table_owner_columns": ["name"]
    }
  ]
}
```

This significantly improves workflow efficiency when setting up multiple vindexes, reducing the number of separate operations required.

#### <a id="reference-tables-in-materialize-workflows"/>Reference Tables in Materialize Workflows</a>

Reference tables can now be added to existing materialize workflows using the new `Materialize ... update` sub-command ([PR #17804](https://github.com/vitessio/vitess/pull/17804)).

**Usage**:
```bash
# Add reference tables to an existing workflow
vtctldclient Materialize --workflow my_workflow update \
  --add-reference-tables ref_table1,ref_table2 \
  --target-keyspace my_keyspace
```

**Use Case**: Incrementally add reference tables to running materialize workflows without recreating the entire workflow, improving operational flexibility.

#### <a id="online-ddl-shard-specific-completion"/>Online DDL Shard-Specific Completion</a>

Online DDL migrations can now be completed on a per-shard basis using the new `COMPLETE VITESS_SHARDS` syntax ([PR #18331](https://github.com/vitessio/vitess/pull/18331)).

**Usage**:
```sql
-- Complete migration on specific shards only
ALTER VITESS_MIGRATION '9e8a9249_3976_11ed_9442_0a43f95f28a3'
  COMPLETE VITESS_SHARDS '-80,80-';

-- Complete migration on all remaining shards
ALTER VITESS_MIGRATION '9e8a9249_3976_11ed_9442_0a43f95f28a3'
  COMPLETE;
```

**Benefits**:
- Gradual rollout of schema changes across shards
- Ability to validate changes on subset of shards before full rollout
- Better control over migration timing and impact

#### <a id="with-recursive-ctes"/>WITH RECURSIVE CTEs</a>

Vitess now supports `WITH RECURSIVE` common table expressions ([PR #18590](https://github.com/vitessio/vitess/pull/18590)), enabling recursive queries for hierarchical data.

**Example**:
```sql
-- Find all employees in a management hierarchy
WITH RECURSIVE employee_hierarchy AS (
    SELECT id, name, manager_id, 1 as level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    SELECT e.id, e.name, e.manager_id, eh.level + 1
    FROM employees e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.id
)
SELECT * FROM employee_hierarchy ORDER BY level, name;
```

This is a major SQL compatibility enhancement for applications with hierarchical or graph-like data structures.

#### <a id="create-table-select-support"/>CREATE TABLE ... SELECT Support</a>

The SQL parser now supports `CREATE TABLE ... SELECT` statements ([PR #18443](https://github.com/vitessio/vitess/pull/18443)), improving MySQL compatibility.

**Example**:
```sql
-- Create a table from a query result
CREATE TABLE recent_orders
SELECT * FROM orders
WHERE order_date > DATE_SUB(NOW(), INTERVAL 30 DAY);
```

---

### <a id="deprecations"/>Deprecations</a>

#### <a id="deprecated-metrics"/>Metrics</a>

| Component |        Metric Name        | Notes                                  |                     Deprecation PR                      |
|:---------:|:-------------------------:|:--------------------------------------:|:-------------------------------------------------------:|
| `vtorc`   | `DiscoverInstanceTimings` | Replaced by `DiscoveryInstanceTimings` | [#18406](https://github.com/vitessio/vitess/pull/18406) |

#### <a id="deprecated-cli-flags"/>CLI Flags</a>

As part of the [Flag Naming Convention Migration](#flag-naming-convention-migration), **989 CLI flags** across all Vitess binaries have been deprecated in their underscore format. The dash format should be used going forward.

**Deprecation Timeline**:
- **v23.0.0 and v24.0.0**: Underscore format deprecated but functional
- **v25.0.0**: Underscore format will be removed

**Action Required**: Migrate to dash-based flag names before v25.0.0. See [Flag Naming Convention Migration](#flag-naming-convention-migration) for details.

---

### <a id="deletions"/>Deletions</a>

#### <a id="deleted-metrics"/>Metrics</a>

| Component |        Metric Name        | Was Deprecated In |                     Deprecation PR                      |
|:---------:|:-------------------------:|:-----------------:|:-------------------------------------------------------:|
| `vtgate`  |    `QueriesProcessed`     |     `v22.0.0`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  |      `QueriesRouted`      |     `v22.0.0`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  | `QueriesProcessedByTable` |     `v22.0.0`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  |  `QueriesRoutedByTable`   |     `v22.0.0`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |

See [Breaking Changes](#deleted-metrics-breaking) for migration guidance.

---

### <a id="new-metrics"/>New Metrics

#### <a id="new-vtgate-metrics"/>VTGate

|          Name                    |   Dimensions    |                                     Description                                     |                           PR                            |
|:--------------------------------:|:---------------:|:-----------------------------------------------------------------------------------:|:-------------------------------------------------------:|
| `TransactionsProcessed`          | `Shard`, `Type` | Counts transactions processed at VTGate by shard distribution and transaction type. | [#18171](https://github.com/vitessio/vitess/pull/18171) |
| `OptimizedQueryExecutions`       | N/A             | Counter tracking queries that used deferred optimization execution path.            | [#18067](https://github.com/vitessio/vitess/pull/18067) |

**Transaction Types in `TransactionsProcessed`**:
- `Single` - Single-shard transactions
- `Multi` - Multi-shard transactions
- `TwoPC` - Two-phase commit transactions

**Use Case**: The `TransactionsProcessed` metric helps identify transaction patterns and shard distribution, useful for:
- Monitoring transaction types across your deployment
- Identifying queries that could be optimized to single-shard
- Tracking two-phase commit usage and potential bottlenecks

#### <a id="new-vttablet-metrics"/>VTTablet

|          Name                    |   Dimensions                       |                                     Description                                      |                           PR                            |
|:--------------------------------:|:----------------------------------:|:------------------------------------------------------------------------------------:|:-------------------------------------------------------:|
| `OnlineDDLStaleMigrationMinutes` | N/A                                | Minutes since the oldest pending/ready migration was submitted.                      | [#18417](https://github.com/vitessio/vitess/pull/18417) |
| `vttablet_tablet_server_state`   | `type`, `keyspace`, `shard`        | Enhanced with additional dimensions for better observability.                        | [#18451](https://github.com/vitessio/vitess/pull/18451) |

**Use Case for `OnlineDDLStaleMigrationMinutes`**: Alert on stalled Online DDL migrations that haven't progressed for an extended period, helping identify migrations that may need intervention.

#### <a id="new-vtorc-metrics"/>VTOrc

|          Name                         |   Dimensions                                  |                   Description                        |                           PR                            |
|:-------------------------------------:|:---------------------------------------------:|:----------------------------------------------------:|:-------------------------------------------------------:|
| `SkippedRecoveries`                   | `RecoveryName`, `Keyspace`, `Shard`, `Reason` | Count of skipped recoveries with reason tracking.    | [#18644](https://github.com/vitessio/vitess/pull/18644) |
| `EmergencyReparentShardDisabled`      | `Keyspace`, `Shard`                           | Gauge indicating if ERS is disabled per keyspace/shard. | [#17985](https://github.com/vitessio/vitess/pull/17985) |

**Use Case for `EmergencyReparentShardDisabled`**: Create alerts to ensure EmergencyReparentShard-based recoveries are not disabled for an undesired period, maintaining high availability posture.

**`SkippedRecoveries` Reasons**: The `Reason` dimension tracks why recoveries were skipped (e.g., "ERSDisabled", "ReplicationLagHigh", "NotEnoughReplicas"), providing actionable insights for operational troubleshooting.

---

## <a id="minor-changes"/>Minor Changes</a>

### <a id="new-cli-flags"/>New CLI Flags</a>

#### <a id="new-vreplication-flags"/>VReplication/Materialize</a>

| Flag | Component | Description | PR |
|:-----|:----------|:------------|:---|
| `--params-file` | vtctldclient | JSON file containing lookup vindex parameters for creating multiple lookup vindexes in a single workflow. Mutually exclusive with `--type`, `--table-owner`, and `--table-owner-columns`. | [#17566](https://github.com/vitessio/vitess/pull/17566) |
| `--add-reference-tables` | vtctldclient | Comma-separated list of reference tables to add to an existing materialize workflow using the `update` sub-command. | [#17804](https://github.com/vitessio/vitess/pull/17804) |

#### <a id="new-observability-flags"/>Observability</a>

| Flag | Component | Description | PR |
|:-----|:----------|:------------|:---|
| `--skip-user-metrics` | vttablet | If enabled, replaces the username label in user-based metrics with "UserLabelDisabled" to prevent metric cardinality explosion in environments with many unique users. | [#18085](https://github.com/vitessio/vitess/pull/18085) |
| `--querylog-emit-on-any-condition-met` | vtgate, vttablet, vtcombo | Changes query log emission to emit when ANY logging condition is met (row-threshold, time-threshold, filter-tag, or error) rather than requiring ALL conditions. Default: false. | [#18546](https://github.com/vitessio/vitess/pull/18546) |
| `--querylog-time-threshold` | vtgate, vttablet | Duration threshold for query logging. Queries exceeding this duration will be logged. Works with `--querylog-emit-on-any-condition-met`. | [#18520](https://github.com/vitessio/vitess/pull/18520) |
| `--grpc-enable-orca-metrics` | vtgate, vttablet | Enable ORCA (Open Request Cost Aggregation) backend metrics reporting via gRPC for load balancing decisions. Default: false. | [#18282](https://github.com/vitessio/vitess/pull/18282) |
| `--datadog-trace-debug-mode` | All components | Makes Datadog trace debug mode configurable instead of always-on. Default: false. | [#18347](https://github.com/vitessio/vitess/pull/18347) |

#### <a id="new-vtorc-flags"/>VTOrc</a>

| Flag | Component | Description | PR |
|:-----|:----------|:------------|:---|
| `--allow-recovery` | vtorc | Boolean flag to disable all VTOrc recovery operations from startup. When false, VTOrc runs in monitoring-only mode. Default: true. | [#18005](https://github.com/vitessio/vitess/pull/18005) |

**Use Case**: The `--allow-recovery=false` flag is useful for:
- Testing VTOrc capacity/discovery performance ahead of a rollout, for example: migrating to VTOrc from another solution.
- Maintenance windows where automatic failovers should be prevented
- Debugging VTOrc behavior without triggering recoveries
- Running VTOrc in observation-only mode

#### <a id="new-backup-flags"/>Backup/Restore</a>

| Flag | Component | Description | PR |
|:-----|:----------|:------------|:---|
| `--xtrabackup-should-drain` | vttablet | Makes the ShouldDrainForBackup behavior configurable for xtrabackup engine. When true, tablet drains traffic before backup. Default: true. | [#18431](https://github.com/vitessio/vitess/pull/18431) |

#### <a id="new-cli-tool-flags"/>CLI Tools</a>

| Flag | Component | Description | PR |
|:-----|:----------|:------------|:---|
| `--chars` | vtctldclient | Specifies the hex width (number of hex characters) to use when generating shard ranges with `GenerateShardRanges` command. Allows fine-grained control over shard naming. | [#18633](https://github.com/vitessio/vitess/pull/18633) |

**Example**:
```bash
# Generate shard ranges with 4 hex characters
vtctldclient GenerateShardRanges --shards 16 --chars 4
# Output: -1000,1000-2000,2000-3000,...,f000-
```

#### <a id="new-vtadmin-flags"/>VTAdmin</a>

Five new TLS-related flags were added for secure vtctld connections ([PR #18556](https://github.com/vitessio/vitess/pull/18556)):

| Flag | Description |
|:-----|:------------|
| `--vtctld-grpc-ca` | CA certificate file for vtctld gRPC TLS |
| `--vtctld-grpc-cert` | Client certificate file for vtctld gRPC TLS |
| `--vtctld-grpc-key` | Client key file for vtctld gRPC TLS |
| `--vtctld-grpc-server-name` | Server name for vtctld gRPC TLS validation |
| `--vtctld-grpc-crl` | Certificate revocation list for vtctld gRPC TLS |

These flags enable mTLS (mutual TLS) authentication between VTAdmin and vtctld for enhanced security.

#### <a id="new-vtgate-flags-section"/>VTGate</a>

| Flag | Component | Description | PR |
|:-----|:----------|:------------|:---|
| `--vtgate-grpc-fail-fast` | vtgate | Enable gRPC fail-fast mode for faster error responses when backends are unavailable. Default: false. | [#18551](https://github.com/vitessio/vitess/pull/18551) |

---

### <a id="modified-metrics"/>Modified Metrics</a>

#### VTOrc Recovery Metrics Enhanced with Keyspace/Shard Labels

The following VTOrc recovery metrics now include `Keyspace` and `Shard` labels in addition to the existing `RecoveryType` label ([PR #18304](https://github.com/vitessio/vitess/pull/18304)):

1. `FailedRecoveries`
2. `PendingRecoveries`
3. `RecoveriesCount`
4. `SuccessfulRecoveries`

**Impact**: Monitoring queries and dashboards using these metrics may need updates to account for the additional label dimensions.

**Benefits**:
- More granular observability into which keyspaces/shards are experiencing recovery issues
- Ability to alert on recovery patterns per keyspace/shard
- Better troubleshooting of cluster-specific issues

**Example PromQL Query**:
```promql
# Before (v22): Recovery count by type only
sum(rate(RecoveriesCount[5m])) by (RecoveryType)

# After (v23): Recovery count by type, keyspace, and shard
sum(rate(RecoveriesCount[5m])) by (RecoveryType, Keyspace, Shard)
```

#### VTGate QueryExecutionsByTable Behavior Change

The `QueryExecutionsByTable` metric now only counts successful query executions ([PR #18584](https://github.com/vitessio/vitess/pull/18584)). Previously, it counted all query attempts regardless of success/failure.

**Impact**:
- Metric values may decrease if your workload had significant query failures
- More accurate representation of successful query volume
- Failed queries are tracked separately via error metrics

---

### <a id="parser-sql-enhancements"/>Parser/SQL Enhancements</a>

Vitess v23.0.0 includes significant SQL parser improvements for better MySQL compatibility:

#### New SQL Syntax Support

| Feature | Description | PR |
|:--------|:------------|:---|
| `CREATE TABLE ... SELECT` | Full support for creating tables from SELECT query results | [#18443](https://github.com/vitessio/vitess/pull/18443) |
| `WITH RECURSIVE` | Recursive common table expressions for hierarchical queries | [#18590](https://github.com/vitessio/vitess/pull/18590) |
| `SET NAMES binary` | Support for binary character set specification | [#18582](https://github.com/vitessio/vitess/pull/18582) |
| `ALTER VITESS_MIGRATION ... POSTPONE COMPLETE` | Syntax for postponing Online DDL migration completion | [#18118](https://github.com/vitessio/vitess/pull/18118) |
| `VALUE` keyword in INSERT/REPLACE | Support for `VALUE` (singular) in addition to `VALUES` | [#18116](https://github.com/vitessio/vitess/pull/18116) |

#### CREATE PROCEDURE Improvements

Enhanced `CREATE PROCEDURE` statement parsing ([PR #18142](https://github.com/vitessio/vitess/pull/18142), [PR #18279](https://github.com/vitessio/vitess/pull/18279)):
- Better handling of `DEFINER` clauses with various formats
- Differentiation between `BEGIN...END` blocks and `START TRANSACTION` statements
- Support for `SET` statements within procedure bodies
- Improved handling of semicolons within procedure definitions

#### Operator Precedence Fixes

- Fixed `MEMBER OF` operator precedence with `AND` ([PR #18237](https://github.com/vitessio/vitess/pull/18237))
- Ensures correct query evaluation when combining JSON operations with boolean logic

---

### <a id="query-planning-improvements"/>Query Planning Improvements</a>

#### Window Functions in Single-Shard Queries

Window functions can now be pushed down to single-shard queries ([PR #18103](https://github.com/vitessio/vitess/pull/18103)), improving performance for analytics workloads.

**Before v23**: Window functions were always executed at VTGate level, even for single-shard queries.

**After v23**: Window functions are pushed down when the query targets a single shard, reducing data transfer and improving performance.

**Example**:
```sql
-- This query now executes entirely on the target shard
SELECT
    user_id,
    order_date,
    amount,
    SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as running_total
FROM orders
WHERE user_id = 12345;  -- Single-shard query
```

#### UNION Query Merging Improvements

UNION query optimization has been significantly enhanced ([PR #18289](https://github.com/vitessio/vitess/pull/18289), [PR #18393](https://github.com/vitessio/vitess/pull/18393)):

**Extended UNION Merging**: UNION queries with `Equal` and `IN` opcodes can now be merged more aggressively, generating simpler SQL.

**Derived Table Elimination**: Unnecessary derived table wrapping is avoided for UNION queries, producing cleaner and more efficient SQL.

**Example**:
```sql
-- Query:
SELECT * FROM t1 WHERE id = 1
UNION
SELECT * FROM t1 WHERE id = 2;

-- Before v23: Wrapped in derived table
SELECT * FROM (
    SELECT * FROM t1 WHERE id = 1
    UNION
    SELECT * FROM t1 WHERE id = 2
) AS dt;

-- After v23: Direct UNION (simpler, more efficient)
SELECT * FROM t1 WHERE id = 1
UNION
SELECT * FROM t1 WHERE id = 2;
```

#### Multi-Shard Read-Only Transactions in SINGLE Mode

Read-only transactions can now span multiple shards when using `SINGLE` transaction mode ([PR #18173](https://github.com/vitessio/vitess/pull/18173)).

**Before v23**: SINGLE mode restricted all transactions to single shards, even read-only ones.

**After v23**: Read-only transactions can access multiple shards in SINGLE mode, improving flexibility without sacrificing consistency guarantees.

**Impact**: Applications using SINGLE transaction mode can now perform multi-shard read queries within transactions without needing to upgrade to MULTI or TWOPC modes.

#### Deferred Optimization for Prepared Statements

Prepared statements now support deferred optimization ([PR #18126](https://github.com/vitessio/vitess/pull/18126)), allowing preparation to succeed even when plan generation requires runtime values.

**Benefits**:
- More prepared statements succeed at preparation time
- Better support for queries with parameter-dependent optimization
- Reduced application errors from failed PREPARE statements

**Behavior**: When a prepared statement cannot be fully optimized at preparation time, optimization is deferred to execution time when bind variable values are available.

#### Query Buffering for INSTANT DDL

Query buffering has been implemented for `INSTANT` DDL operations ([PR #17945](https://github.com/vitessio/vitess/pull/17945)), reducing query failures during schema changes.

**Features**:
- Automatic buffering of queries during INSTANT DDL execution
- Forced termination of blocking transactions
- Transparent to applications

**Impact**: Applications experience fewer query errors during schema changes, improving availability during DDL operations.

---

### <a id="minor-changes-topo"/>Topology</a>

#### <a id="consul-auth-static-file-check-creds"/>`--consul-auth-static-file` requires 1 or more credentials</a>

The `--consul-auth-static-file` flag used in several components now requires that 1 or more credentials can be loaded from the provided json file ([PR #18152](https://github.com/vitessio/vitess/pull/18152)).

**Impact**: Configurations with empty or invalid credential files will now fail at startup rather than silently continuing with no authentication.

---

### <a id="minor-changes-vtorc"/>VTOrc</a>

#### <a id="aggregated-discovery-metrics-api-removed"/>Aggregated Discovery Metrics HTTP API removed</a>

VTOrc's undocumented `/api/aggregated-discovery-metrics` HTTP API endpoint was removed ([PR #18672](https://github.com/vitessio/vitess/pull/18672)). The list of documented VTOrc APIs can be found [here](https://vitess.io/docs/current/reference/vtorc/ui_api_metrics/#apis).

We recommend using the standard VTOrc metrics to gather the same metrics. If you find that a metric is missing in standard metrics, please open an issue or PR to address this.

#### <a id="vtorc-dynamic-ers-disabled"/>Dynamic control of `EmergencyReparentShard`-based recoveries</a>

**Note: disabling `EmergencyReparentShard`-based recoveries introduces availability risks; please use with extreme caution! If you rely on this functionality often, for example in automation, this may be signs of an anti-pattern. If so, please open an issue to discuss supporting your use case natively in VTOrc.**

The new `vtctldclient` RPC `SetVtorcEmergencyReparent` was introduced ([PR #17985](https://github.com/vitessio/vitess/pull/17985)) to allow VTOrc recoveries involving `EmergencyReparentShard` actions to be disabled on a per-keyspace and/or per-shard basis. Previous to this version, disabling EmergencyReparentShard-based recoveries was only possible globally/per-VTOrc-instance. VTOrc will now consider this keyspace/shard-level setting that is refreshed from the topo on each recovery. The disabled state is determined by first checking if the keyspace, and then the shard state. Removing a keyspace-level override does not remove per-shard overrides.

To provide observability of keyspaces/shards with EmergencyReparentShard-based VTOrc recoveries disabled, the `EmergencyReparentShardDisabled` metric was added. This metric label can be used to create alerting to ensure EmergencyReparentShard-based recoveries are not disabled for an undesired period of time.

**Example**:
```bash
# Disable ERS recoveries for a keyspace
vtctldclient SetVtorcEmergencyReparent --keyspace commerce --enabled=false

# Disable ERS recoveries for a specific shard
vtctldclient SetVtorcEmergencyReparent --keyspace commerce --shard 80- --enabled=false

# Re-enable ERS recoveries
vtctldclient SetVtorcEmergencyReparent --keyspace commerce --enabled=true
```

#### <a id="recoveries-stats-keyspace-shard"/>Recovery stats to include keyspace/shard</a>

The following recovery-related stats now include labels for keyspaces and shards ([PR #18304](https://github.com/vitessio/vitess/pull/18304)):
1. `FailedRecoveries`
2. `PendingRecoveries`
3. `RecoveriesCount`
4. `SuccessfulRecoveries`

Previous to this release, only the recovery "type" was included in labels. See [Modified Metrics](#modified-metrics) for more details.

#### <a id="replication-analysis-api-deprecation"/>`/api/replication-analysis` HTTP API deprecation</a>

The `/api/replication-analysis` HTTP API endpoint is now deprecated and is replaced with `/api/detection-analysis` ([PR #18615](https://github.com/vitessio/vitess/pull/18615)), which currently returns the same response format.

**Timeline**: The `/api/replication-analysis` endpoint will be removed in a future version. Users should migrate to `/api/detection-analysis`.

---

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="api-changes"/>API Changes</a>

- **Added** `RestartReplication` method to `TabletManagerClient` interface ([PR #18628](https://github.com/vitessio/vitess/pull/18628)). This new RPC allows stopping and restarting MySQL replication with semi-sync configuration in a single call, providing a convenient alternative to separate `StopReplication` and `StartReplication` calls.

- **Added** `GetMaxValueForSequences` and `UpdateSequenceTables` gRPC RPCs ([PR #18172](https://github.com/vitessio/vitess/pull/18172)) for VReplication sequence management during `SwitchWrites` operations.

#### <a id="flags-vttablet"/>CLI Flags</a>

- `--skip-user-metrics` flag if enabled, replaces the username label with "UserLabelDisabled" to prevent metric explosion in environments with many unique users ([PR #18085](https://github.com/vitessio/vitess/pull/18085)).

See [New CLI Flags](#new-cli-flags) for complete list of new flags.

#### <a id="mysql-caching-sha2-password"/>Managed MySQL configuration defaults to caching-sha2-password</a>

The default authentication plugin for MySQL 8.0.26 and later is now `caching_sha2_password` instead of `mysql_native_password` ([PR #18010](https://github.com/vitessio/vitess/pull/18010)). This change is made because `mysql_native_password` is deprecated and removed in future MySQL versions. `mysql_native_password` is still enabled for backwards compatibility.

This change specifically affects the replication user. If you have a user configured with an explicit password, it is recommended to make sure to upgrade this user after upgrading to v23 with a statement like the following:

```sql
ALTER USER 'vt_repl'@'%' IDENTIFIED WITH caching_sha2_password BY 'your-existing-password';
```

In future Vitess versions, the `mysql_native_password` authentication plugin will be disabled for managed MySQL instances.

#### <a id="mysql-timezone-env"/>MySQL timezone environment propagation</a>

Fixed a bug where environment variables like `TZ` were not propagated from mysqlctl to the mysqld process ([PR #18561](https://github.com/vitessio/vitess/pull/18561)).
As a result, timezone settings from the environment were previously ignored. Now mysqld correctly inherits environment variables.

⚠️ **Deployment Impact**: Deployments that relied on the old behavior and explicitly set a non-UTC timezone may see changes in how DATETIME values are interpreted. To preserve compatibility, set `TZ=UTC` explicitly in MySQL pods.

#### <a id="grpctmclient-err-changes"/>gRPC `tabletmanager` client error changes</a>

The `vttablet` gRPC `tabletmanager` client now returns errors wrapped by the internal `go/vt/vterrors` package ([PR #18565](https://github.com/vitessio/vitess/pull/18565)). External automation relying on google-gRPC error codes should now use `vterrors.Code(err)` to inspect the code of an error, which returns `vtrpcpb.Code`s defined in [the `proto/vtrpc.proto` protobuf](https://github.com/vitessio/vitess/blob/main/proto/vtrpc.proto#L60).

See [Breaking Changes](#grpc-error-codes) for migration guidance.

---

### <a id="docker"/>Docker</a>

[Bullseye went EOL 1 year ago](https://www.debian.org/releases/), so starting from v23, we will no longer build or publish images based on debian:bullseye ([PR #18609](https://github.com/vitessio/vitess/pull/18609)).

Builds will continue for Debian Bookworm, and add the recently released Debian Trixie. v23 explicitly does not change the default Debian tag to Trixie.
