
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

Traffic mirroring is intended to help reduce some of the uncertainty inherent to `MoveTables SwitchTraffic`. When traffic mirroring is enabled, VTGate will mirror a percentage of traffic from one keyspace to another.

Mirror rules may be enabled through `vtctldclient` with `MoveTables MirrorTraffic`. For example:

```bash
$ vtctldclient --server :15999 MoveTables --target-keyspace customer --workflow commerce2customer MirrorTraffic --percent 5.0
```

Mirror rules can be inspected with `GetMirrorRules`.

### <a id="new-vtgate-shutdown-behavior"/>New VTGate Shutdown Behavior

We added a new option to affect the VTGate shutdown process in v21 by using a connection drain timeout rather than the older activity drain timeout.
The goal of this new behavior, connection draining option, is to disallow new connections when VTGate is shutting down,
but continue allowing existing connections to finish their work until they manually disconnect or until the `--onterm_timeout` timeout is reached,
without getting a `Server shutdown in progress` error.

This new behavior can be enabled by specifying the new `--mysql-server-drain-onterm` flag to VTGate.

See more information about this change by [reading its RFC](https://github.com/vitessio/vitess/issues/15971).

### <a id="tablet-throttler"/>Tablet Throttler: Multi-Metric support

Up till `v20`, the tablet throttler would only monitor and use a single metric. That would be replication lag, by default, or could be the result of a custom query. `v21` introduces a major redesign where the throttler monitors and uses multiple metrics at the same time, including the above two.

Backwards compatible with `v20`, the default behavior in `v21` is to monitor all metrics, but only use `lag` (if the cutsom query is undefined) or the `cutsom` metric (if the custom query is defined). A `v20` `PRIMARY` is compatible with a `v21` `REPLICA`, and a `v21` `PRIMARY` is compatible with a `v20` `REPLICA`.

However, with `v21` it is possible to assign any combination of metrics (one or more) for a given app. The throttler would then accept or reject the app's requests based on the health of _all_ assigned metrics. `v21` comes with a preset list metrics, expected to be expanded:

- `lag`: replication lag based on heartbeat injection.
- `threads_running`: concurrent active threads on the MySQL server.
- `loadavg`: per core load average measured on the tablet instance/pod.
- `custom`: the result of a custom query executed on the MySQL server.

Each metric has a factory threshold which can be overridden by the `UpdateThrottlerConfig` command.

The throttler also supports the catch-all `"all"` app name, and it is thus possible to assign metrics to _all_ apps. Explicit app to metric assignments will override the catch-all configuration.

Metrics are assigned a default _scope_, which could be `self` (isolated to the tablet) or `shard` (max, aka _worst_ value among shard tablets). It is further possible to require a different scope for each metric.

### <a id="allow-cross-cell"/>Allow Cross Cell Promotion in PRS
Up until now if the users wanted to promote a replica in a different cell than the current primary using `PlannedReparentShard`, they had to specify the new primary with the `--new-primary` flag.

We have now added a new flag `--allow-cross-cell-promotion` that lets `PlannedReparentShard` choose a primary in a different cell even if no new primary is provided explicitly.

### <a id="recursive-cte"/>Experimental support for recursive CTEs
We have added experimental support for recursive CTEs in Vitess. We are marking it as experimental because it is not yet fully tested and may have some limitations. We are looking for feedback from the community to improve this feature.