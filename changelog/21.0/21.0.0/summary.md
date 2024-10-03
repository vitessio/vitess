## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[Deprecations and Deletions](#deprecations-and-deletions)**
        - [Deletion of deprecated metrics](#metric-deletion)
        - [VTTablet Flags](#vttablet-flags)
        - [Metrics](#deprecations-metrics)
    - **[Traffic Mirroring](#traffic-mirroring)**
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
    - **[Errant GTID Detection on Vttablets](#errant-gtid-vttablet)**
    - **[vtctldclient ChangeTabletTags](#vtctldclient-changetablettags)**

## <a id="major-changes"/>Major Changes

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

#### <a id="metric-deletion"/>Deletion of deprecated metrics

The following metrics that were deprecated in the previous release, have now been deleted.

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

#### <a id="vttablet-flags"/>VTTablet Flags

- `queryserver-enable-settings-pool` flag, added in v15, has been on by default since v17.
  It is now deprecated and will be removed in a future release.

#### <a id="deprecations-metrics"/>Metrics

The following metrics are now deprecated, if provided please use their replacement.

| Component  |      Metric Name      |           Replaced By           |
|------------|:---------------------:|:-------------------------------:|
| `vttablet` |  `QueryCacheLength`   |  `QueryEnginePlanCacheLength`   |
| `vttablet` |   `QueryCacheSize`    |   `QueryEnginePlanCacheSize`    |
| `vttablet` | `QueryCacheCapacity`  | `QueryEnginePlanCacheCapacity`  |
| `vttablet` | `QueryCacheEvictions` | `QueryEnginePlanCacheEvictions` |
| `vttablet` |   `QueryCacheHits`    |   `QueryEnginePlanCacheHits`    |
| `vttablet` |  `QueryCacheMisses`   |  `QueryEnginePlanCacheMisses`   |

### <a id="traffic-mirroring"/>Traffic Mirroring

Traffic mirroring is intended to help reduce some of the uncertainty inherent to `MoveTables SwitchTraffic`. When
traffic mirroring is enabled, VTGate will mirror a percentage of traffic from one keyspace to another.

Mirror rules may be enabled through `vtctldclient` with `MoveTables MirrorTraffic`. For example:

```bash
$ vtctldclient --server :15999 MoveTables --target-keyspace customer --workflow commerce2customer MirrorTraffic --percent 5.0
```

Mirror rules can be inspected with `GetMirrorRules`.

### <a id="new-vtgate-shutdown-behavior"/>New VTGate Shutdown Behavior

We added a new option to affect the VTGate shutdown process in v21 by using a connection drain timeout rather than the
older activity drain timeout.
The goal of this new behavior, connection draining option, is to disallow new connections when VTGate is shutting down,
but continue allowing existing connections to finish their work until they manually disconnect or until
the `--onterm_timeout` timeout is reached,
without getting a `Server shutdown in progress` error.

This new behavior can be enabled by specifying the new `--mysql-server-drain-onterm` flag to VTGate.

See more information about this change by [reading its RFC](https://github.com/vitessio/vitess/issues/15971).

### <a id="tablet-throttler"/>Tablet Throttler: Multi-Metric support

Up till `v20`, the tablet throttler would only monitor and use a single metric. That would be replication lag, by
default, or could be the result of a custom query. `v21` introduces a major redesign where the throttler monitors and
uses multiple metrics at the same time, including the above two.

Backwards compatible with `v20`, the default behavior in `v21` is to monitor all metrics, but only use `lag` (if the
cutsom query is undefined) or the `cutsom` metric (if the custom query is defined). A `v20` `PRIMARY` is compatible with
a `v21` `REPLICA`, and a `v21` `PRIMARY` is compatible with a `v20` `REPLICA`.

However, with `v21` it is possible to assign any combination of metrics (one or more) for a given app. The throttler
would then accept or reject the app's requests based on the health of _all_ assigned metrics. `v21` comes with a preset
list metrics, expected to be expanded:

- `lag`: replication lag based on heartbeat injection.
- `threads_running`: concurrent active threads on the MySQL server.
- `loadavg`: per core load average measured on the tablet instance/pod.
- `custom`: the result of a custom query executed on the MySQL server.

Each metric has a factory threshold which can be overridden by the `UpdateThrottlerConfig` command.

The throttler also supports the catch-all `"all"` app name, and it is thus possible to assign metrics to _all_ apps.
Explicit app to metric assignments will override the catch-all configuration.

Metrics are assigned a default _scope_, which could be `self` (isolated to the tablet) or `shard` (max, aka _worst_
value among shard tablets). It is further possible to require a different scope for each metric.

### <a id="allow-cross-cell"/>Allow Cross Cell Promotion in PRS

Up until now if the users wanted to promote a replica in a different cell than the current primary
using `PlannedReparentShard`, they had to specify the new primary with the `--new-primary` flag.

We have now added a new flag `--allow-cross-cell-promotion` that lets `PlannedReparentShard` choose a primary in a
different cell even if no new primary is provided explicitly.

### <a id="recursive-cte"/>Experimental support for recursive CTEs

We have added experimental support for recursive CTEs in Vitess. We are marking it as experimental because it is not yet
fully tested and may have some limitations. We are looking for feedback from the community to improve this feature.

### <a id="tablet-balancer"/>VTGate Tablet Balancer

When a VTGate routes a query and has multiple available tablets for a given shard / tablet type (e.g. REPLICA), the
current default behavior routes the query with local cell affinity and round robin policy. The VTGate Tablet Balancer
provides an alternate mechanism that routes queries to maintain an even distribution of query load to each tablet, while
preferentially routing to tablets in the same cell as the VTGate.

The tablet balancer is enabled by a new flag `--enable-balancer` and configured by `--balancer-vtgate-cells`
and `--balancer-keyspaces`.

See [RFC for details](https://github.com/vitessio/vitess/issues/12241).

### <a id="query-timeout"/>Query Timeout Override

VTGate sends an authoritative query timeout to VTTablet when the `QUERY_TIMEOUT_MS` comment directive,
`query_timeout` session system variable, or `query-timeout` flag is set.
The order of precedence is: `QUERY_TIMEOUT_MS` > `query_timeout` > `query-timeout`.
VTTablet overrides its default query timeout with the value received from VTGate.
All timeouts are specified in milliseconds.

When a query is executed inside a transaction, this behavior does not apply; instead,
the smaller of the transaction timeout or the query timeout from VTGate is used.

A query can also be set to have no timeout by using the `QUERY_TIMEOUT_MS` comment directive with a value of `0`.

Example usage:
`select /*vt+ QUERY_TIMEOUT_MS=30 */ col from tbl`

### <a id="new-backup-engine"/>New Backup Engine (EXPERIMENTAL)

We are introducing a backup engine supporting logical backups starting on v21 to support use cases that require something else besides physical backups. This is experimental and is based on the 
[MySQL Shell](https://dev.mysql.com/doc/mysql-shell/8.0/en/).

The new engine is enabled by using `--backup_engine_implementation=mysqlshell`. There are other options that are required, so [check the docs](https://vitess.io/docs/21.0/user-guides/operating-vitess/backup-and-restore/creating-a-backup/) on which options are required and how to use it.

### <a id="dynamic-vreplication-configuration"/>Dynamic VReplication Configuration

Currently many of the configuration options for VReplication Workflows are vttablet flags. This means that any change
requires restarts of vttablets. We now allow these to be overridden while creating a workflow or dynamically once
the workflow is in progress. See https://github.com/vitessio/vitess/pull/16583 for details.

### <a id="reference-table-materialization"/>Reference Table Materialization

There is a new option in [`Materialize` workflows](https://vitess.io/docs/reference/vreplication/materialize/) to keep 
a synced copy of [reference or lookup tables](https://vitess.io/docs/reference/vreplication/reference_tables/) 
(countries, states, zip_codes, etc) from an unsharded keyspace, which holds the source of truth for the reference 
table, to all shards in a sharded keyspace.
### <a id="new-vexplain-modes"/>New VEXPLAIN Modes: TRACE and KEYS

#### VEXPLAIN TRACE

The new TRACE mode for VEXPLAIN provides a detailed execution trace of queries, showing how they're processed through various operators and interactions with tablets. This mode is particularly useful for:

- Identifying performance bottlenecks
- Understanding query execution patterns
- Optimizing complex queries
- Debugging unexpected query behavior

TRACE mode runs the query and logs all interactions, returning a JSON representation of the query execution plan with additional statistics like number of calls, average rows processed, and number of shards queried.

#### VEXPLAIN KEYS

The KEYS mode for VEXPLAIN offers a concise summary of query structure, highlighting columns used in joins, filters, and grouping operations. This information is crucial for:

- Identifying potential sharding key candidates
- Optimizing query performance
- Analyzing query patterns to inform database design decisions

KEYS mode analyzes the query structure without executing it, providing JSON output that includes grouping columns, join columns, filter columns (potential candidates for indexes, primary keys, or sharding keys), and the statement type.

These new VEXPLAIN modes enhance Vitess's query analysis capabilities, allowing for more informed decisions about sharding strategies and query optimization.

### <a id="errant-gtid-vttablet"/>Errant GTID Detection on Vttablets

Vttablets now run an errant GTID detection logic before they join the replication stream. So, if a replica has an errant GTID, it will
not start replicating from the primary. It will fail the call the set its replication source because of the errant GTID. This prevents us 
from running into situations from which recovery is very hard.

For users running with the vitess operator on kubernetes, this change means that the replicas with errant GTIDs will have broken replication and will report as unready. The users will need to manually clean up these errant replica tablets.

### <a id="vtctldclient-changetablettags"/>`vtctldclient ChangeTabletTags` command and RPCs

The `vtctldclient` command `ChangeTabletTags` was added to allow the tags of a tablet to be changed dynamically.

This command allows one or many tablet tags to be defined using key=value format. The provided tags are merged with existing tags by default. The optional flag `--replace` causes the existing tags to be replaced with the provided tags. To support this, the VTCtld RPC `ChangeTabletTags` and the VTTablet RPC `ChangeTags` were added.

Previous to this release the only way to define tablet tags was the `--init_tags` flag of `vttablet`, which requires a restart for a change to take effect.

Example:
```bash
$ vtctldclient --server :15999 ChangeTabletTags --replace zone1-100 hello=world
- []
+ [hello: "world"]
$ vtctldclient --server :15999 GetTablet zone1-100 | jq .tags
{
  "hello": "world"
}
```
