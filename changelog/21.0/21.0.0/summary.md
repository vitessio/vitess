## Summary

### Table of Contents

- **[Known Issues](#known-issues)**
    - **[Backup reports itself as successful despite failures](#backup-reports-as-successful)**
- **[Major Changes](#major-changes)**
    - **[Deprecations and Deletions](#deprecations-and-deletions)**
        - [Deprecated VTTablet Flags](#vttablet-flags)
        - [Deletion of deprecated metrics](#metric-deletion)
        - [Deprecated Metrics](#deprecations-metrics)
    - **[RPC Changes](#rpc-changes)**
    - **[Traffic Mirroring](#traffic-mirroring)**
    - **[Atomic Distributed Transaction Support](#atomic-transaction)**
    - **[New VTGate Shutdown Behavior](#new-vtgate-shutdown-behavior)**
    - **[Tablet Throttler: Multi-Metric support](#tablet-throttler)**
    - **[Allow Cross Cell Promotion in PRS](#allow-cross-cell)**
    - **[Support for recursive CTEs](#recursive-cte)**
    - **[VTGate Tablet Balancer](#tablet-balancer)**
    - **[Query Timeout Override](#query-timeout)**
    - **[New Backup Engine](#new-backup-engine)**
    - **[Dynamic VReplication Configuration](#dynamic-vreplication-configuration)**
    - **[Reference Table Materialization](#reference-table-materialization)**
    - **[New VEXPLAIN Modes: TRACE and KEYS](#new-vexplain-modes)**
    - **[Automatically Replace MySQL auto_increment Clauses with Vitess Sequences](#auto-replace-mysql-autoinc-with-seq)**
    - **[Experimental MySQL 8.4 support](#experimental-mysql-84)**
    - **[Current Errant GTIDs Count Metric](#errant-gtid-metric)**
    - **[vtctldclient ChangeTabletTags](#vtctldclient-changetablettags)**
    - **[Support for specifying expected primary in reparents](#reparents-expectedprimary)**

## <a id="known-issues"/>Known Issues</a>

### <a id="backup-reports-as-successful"/>Backup reports itself as successful despite failures</a>

In this release, we have identified an issue where a backup may succeed even if one of the underlying files fails to be backed up.
The underlying errors are ignored and the backup action reports success.
This issue exists only with the `builtin` backup engine, and it can occur only when the engine has already started backing up all files.
Please refer to https://github.com/vitessio/vitess/issues/17063 for more details.

## <a id="major-changes"/>Major Changes</a>

### <a id="deprecations-and-deletions"/>Deprecations and Deletions</a>

#### <a id="vttablet-flags"/>Deprecated VTTablet Flags</a>

- `queryserver-enable-settings-pool` flag, added in `v15`, has been on by default since `v17`.
  It is now deprecated and will be removed in a future release.

#### <a id="metric-deletion"/>Deletion of deprecated metrics</a>

The following VTOrc metrics were deprecated in `v20`. They have now been deleted.

|                 Metric Name                  |
|:--------------------------------------------:|
|           `analysis.change.write`            |        
|                `audit.write`                 |     
|            `discoveries.attempt`             |          
|              `discoveries.fail`              |        
| `discoveries.instance_poll_seconds_exceeded` | 
|          `discoveries.queue_length`          |       
|          `discoveries.recent_count`          |        
|               `instance.read`                |            
|           `instance.read_topology`           |       
|         `emergency_reparent_counts`          |       
|          `planned_reparent_counts`           |      
|      `reparent_shard_operation_timings`      |  

#### <a id="deprecations-metrics"/>Deprecated Metrics</a>

The following metrics are now deprecated and will be deleted in a future release, please use their replacements.

| Component  |      Metric Name      |           Replaced By           |
|------------|:---------------------:|:-------------------------------:|
| `vttablet` |  `QueryCacheLength`   |  `QueryEnginePlanCacheLength`   |
| `vttablet` |   `QueryCacheSize`    |   `QueryEnginePlanCacheSize`    |
| `vttablet` | `QueryCacheCapacity`  | `QueryEnginePlanCacheCapacity`  |
| `vttablet` | `QueryCacheEvictions` | `QueryEnginePlanCacheEvictions` |
| `vttablet` |   `QueryCacheHits`    |   `QueryEnginePlanCacheHits`    |
| `vttablet` |  `QueryCacheMisses`   |  `QueryEnginePlanCacheMisses`   |

### <a id="rpc-changes"/>RPC Changes</a>

These are the RPC changes made in this release - 
1. `ReadReparentJournalInfo` RPC has been added in TabletManagerClient interface, that is going to be used in EmergencyReparentShard for better errant GTID detection.
2. `PrimaryStatus` RPC in TabletManagerClient interface has been updated to also return the server UUID of the primary. This is going to be used in the vttablets so that they can do their own errant GTID detection in `SetReplicationSource`.

### <a id="traffic-mirroring"/>Traffic Mirroring</a>

Traffic mirroring is intended to help reduce some of the uncertainty inherent to `MoveTables SwitchTraffic`. When
traffic mirroring is enabled, VTGate will mirror a percentage of traffic from one keyspace to another.

Mirror rules may be enabled through `vtctldclient` with `MoveTables MirrorTraffic`. For example:

```bash
$ vtctldclient --server :15999 MoveTables --target-keyspace customer --workflow commerce2customer MirrorTraffic --percent 5.0
```

Mirror rules can be inspected with `GetMirrorRules`.

### <a id="atomic-transaction"/>Atomic Distributed Transaction Support</a>

We have introduced atomic distributed transactions as an experimental feature.
Users can now run multi-shard transactions with stronger guarantees. 
Vitess now provides two modes of transactional guarantees for multi-shard transactions: Best Effort and Atomic. 
These can be selected based on the user’s requirements and the trade-offs they are willing to make.

Follow the documentation to enable [Atomic Distributed Transaction](https://vitess.io/docs/21.0/reference/features/distributed-transaction/)

For more details on the implementation and trade-offs, please refer to the [RFC](https://github.com/vitessio/vitess/issues/16245)

### <a id="new-vtgate-shutdown-behavior"/>New VTGate Shutdown Behavior</a>

We added a new option to VTGate to disallow new connections while VTGate is shutting down,
while allowing existing connections to finish their work until they manually disconnect or until
the `--onterm_timeout` is reached, without getting a `Server shutdown in progress` error.

This new behavior can be enabled by specifying the new `--mysql-server-drain-onterm` flag to VTGate.

You can find more information about this option in the [RFC](https://github.com/vitessio/vitess/issues/15971).

### <a id="tablet-throttler"/>Tablet Throttler: Multi-Metric support</a>

Up until `v20`, the tablet throttler would only monitor and use a single metric. That would be replication lag, by
default, or could be the result of a custom query. In this release, we introduce a major redesign so that the throttler 
monitors and uses multiple metrics at the same time, including the above two.

The default behavior now is to monitor all metrics, but only use `lag` (if the custom query is undefined) or the `custom` 
metric (if the custom query is defined). This is backwards-compatible with `v20`. A `v20` `PRIMARY` is compatible with
a `v21` `REPLICA`, and a `v21` `PRIMARY` is compatible with a `v20` `REPLICA`.

However, it is now possible to assign any combination of one or more metrics for a given app. The throttler
would then accept or reject the app's requests based on the health of _all_ assigned metrics. We have provided a pre-defined
list of metrics:

- `lag`: replication lag based on heartbeat injection.
- `threads_running`: concurrent active threads on the MySQL server.
- `loadavg`: per core load average measured on the tablet instance/pod.
- `custom`: the result of a custom query executed on the MySQL server.

Each metric has a default threshold which can be overridden by the `UpdateThrottlerConfig` command.

The throttler also supports the catch-all `"all"` app name, and it is thus possible to assign metrics to **all** apps.
Explicit app to metric assignments will override the catch-all configuration.

Metrics are assigned a default _scope_, which could be `self` (isolated to the tablet) or `shard` (max, aka **worst**
value among shard tablets). It is further possible to require a different scope for each metric.

### <a id="allow-cross-cell"/>Allow Cross Cell Promotion in PRS</a>

Up until now if the users wanted to promote a replica in a different cell from the current primary
using `PlannedReparentShard`, they had to specify the new primary with the `--new-primary` flag.

We have now added a new flag `--allow-cross-cell-promotion` that lets `PlannedReparentShard` choose a primary in a
different cell even if no new primary is provided explicitly.

### <a id="recursive-cte"/>Experimental support for recursive CTEs</a>

We have added experimental support for recursive CTEs in Vitess. We are marking it as experimental because it is not yet
fully tested and may have some limitations. We are looking for feedback from the community to improve this feature.

### <a id="tablet-balancer"/>VTGate Tablet Balancer</a>

When a VTGate routes a query and has multiple available tablets for a given shard / tablet type (e.g. REPLICA), the
current default behavior routes the query with local cell affinity and round robin policy. The VTGate Tablet Balancer
provides an alternate mechanism that routes queries to maintain an even distribution of query load to each tablet, while
preferentially routing to tablets in the same cell as the VTGate.

The tablet balancer is enabled by a new flag `--enable-balancer` and configured by `--balancer-vtgate-cells`
and `--balancer-keyspaces`.

See the [RFC ](https://github.com/vitessio/vitess/issues/12241) for more details on the design and configuration of this feature.

### <a id="query-timeout"/>Query Timeout Override</a>

VTGate sends an authoritative query timeout to VTTablet when the `QUERY_TIMEOUT_MS` comment directive,
`query_timeout` session system variable, or `query-timeout` flag is set.
The order of precedence is: comment directive > session variable > VTGate flag.
VTTablet overrides its default query timeout with the value received from VTGate.
All timeouts are specified in milliseconds.

When a query is executed inside a transaction, there is an additional nuance. The actual timeout used will be the smaller 
of the transaction timeout and the query timeout.

A query can also be set to have no timeout by using the `QUERY_TIMEOUT_MS` comment directive with a value of `0`.

Example usage:
`select /*vt+ QUERY_TIMEOUT_MS=30 */ col from tbl`

### <a id="new-backup-engine"/>New Backup Engine (EXPERIMENTAL)</a>

We are introducing a new backup engine for logical backups in order to support use cases that require something other 
than physical backups. This feature is experimental and is based on [MySQL Shell](https://dev.mysql.com/doc/mysql-shell/8.0/en/).

The new engine is enabled by using `--backup_engine_implementation=mysqlshell`. There are other options that are required, 
so please read the [documentation](https://vitess.io/docs/21.0/user-guides/operating-vitess/backup-and-restore/creating-a-backup/) to learn which options are required and how to configure them.

### <a id="dynamic-vreplication-configuration"/>Dynamic VReplication Configuration</a>

Previously, many of the configuration options for VReplication Workflows had to be provided using VTTablet flags. This 
meant that any change to VReplication configuration required restarting VTTablets. We now allow these to be overridden 
while creating a workflow or dynamically after the workflow is already in progress.

### <a id="reference-table-materialization"/>Reference Table Materialization</a>

There is a new option in [`Materialize` workflows](https://vitess.io/docs/reference/vreplication/materialize/) to keep a synced copy of [reference or lookup tables](https://vitess.io/docs/reference/vreplication/reference_tables/) 
(countries, states, zip codes, etc) from an unsharded keyspace, which holds the source of truth for the reference 
table, to all shards in a sharded keyspace.

### <a id="new-vexplain-modes"/>New VEXPLAIN Modes: TRACE and KEYS</a>

#### VEXPLAIN TRACE

The new `TRACE` mode for `VEXPLAIN` provides a detailed execution trace of queries, showing how they're processed through various 
operators and interactions with tablets. This mode is particularly useful for:

- Identifying performance bottlenecks
- Understanding query execution patterns
- Optimizing complex queries
- Debugging unexpected query behavior

`TRACE` mode runs the query and logs all interactions, returning a JSON representation of the query execution plan with additional 
statistics like number of calls, average rows processed, and number of shards queried.

#### VEXPLAIN KEYS

The `KEYS` mode for `VEXPLAIN` offers a concise summary of query structure, highlighting columns used in joins, filters, and 
grouping operations. This information is crucial for:

- Identifying potential sharding key candidates
- Optimizing query performance
- Analyzing query patterns to inform database design decisions

`KEYS` mode analyzes the query structure without executing it, providing JSON output that includes grouping columns, join columns, 
filter columns (potential candidates for indexes, primary keys, or sharding keys), and the statement type.

These new `VEXPLAIN` modes enhance Vitess's query analysis capabilities, allowing for more informed decisions about sharding 
strategies and query optimization.

### <a id="auto-replace-mysql-autoinc-with-seq"/>Automatically Replace MySQL auto_increment Clauses with Vitess Sequences</a>

In https://github.com/vitessio/vitess/pull/16860 we added support for replacing MySQL `auto_increment` clauses with [Vitess Sequences](https://vitess.io/docs/reference/features/vitess-sequences/), performing all of the setup and initialization
work automatically during the [`MoveTables`](https://vitess.io/docs/reference/vreplication/movetables/) workflow. As part of that work we have deprecated the
[`--remove-sharded-auto-increment` boolean flag](https://vitess.io/docs/20.0/reference/programs/vtctldclient/vtctldclient_movetables/vtctldclient_movetables_create/) and you should begin using the new
[`--sharded-auto-increment-handling` flag](https://vitess.io/docs/21.0/reference/programs/vtctldclient/vtctldclient_movetables/vtctldclient_movetables_create/) instead. Please see the new
[`MoveTables` Auto Increment Handling](https://vitess.io/docs/21.0/reference/vreplication/movetables/#auto-increment-handling) documentation for additional details.

### <a id="experimental-mysql-84"/>Experimental MySQL 8.4 support

We have added experimental support for MySQL 8.4. It passes the Vitess test suite, but it is otherwise not yet tested. We are looking for feedback from the community to improve this to move support out of the experimental phase in a future release.

### <a id="errant-gtid-metric"/>Current Errant GTIDs Count Metric
A new metric called `CurrentErrantGTIDCount` has been added to the `VTOrc` component. 
This metric shows the current count of the errant GTIDs in the tablets.

### <a id="vtctldclient-changetablettags"/>`vtctldclient ChangeTabletTags` command

The `vtctldclient` command `ChangeTabletTags` was added to allow the tags of a tablet to be changed dynamically.

### <a id="reparents-expectedprimary"/>Support specifying expected primary in reparents

The `EmergencyReparentShard` and `PlannedReparentShard` commands and RPCs now support specifying a primary we expect to still be the current primary in order for a reparent operation to be processed. This allows reparents to be conditional on a specific state being true.
