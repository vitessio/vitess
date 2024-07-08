
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deletion of deprecated metrics](#metric-deletion)
    - [VTTablet Flags](#vttablet-flags)
  - **[Traffic Mirroring](#traffic-mirroring)**
  - **[New VTGate Shutdown Behavior](#new-vtgate-shutdown-behavior)**

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