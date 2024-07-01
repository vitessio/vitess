
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deletion of deprecated metrics](#metric-deletion)
    - [VTTablet Flags](#vttablet-flags)
  - **[Breaking changes](#breaking-changes)**
  - **[Traffic Mirroring](#traffic-mirroring)**

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
