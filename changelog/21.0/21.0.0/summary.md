
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deletions](#deletions)** 
    - [Deletion of deprecated metrics](#metric-deletion)
  - **[Breaking changes](#breaking-changes)**
  - **[Traffic Mirroring](#traffic-mirroring)**

## <a id="major-changes"/>Major Changes

### <a id="deletions"/>Deletion

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
		

### <a id="traffic-mirroring"/>Traffic Mirroring

Traffic mirroring is intended to help reduce some of the uncertainty inherent to `MoveTables SwitchTraffic`. When traffic mirroring is enabled, VTGate will mirror a percentage of traffic from one keyspace to another.

Mirror rules may be enabled through `vtctldclient` in two ways:

  * With `ApplyMirrorRules`.
  * With `MoveTables MirrorTraffic`, which uses `ApplyMirrorRules` under the hood.

Example with `ApplyMirrorRules`:

```bash
$ vtctldclient --server :15999 ApplyMirrorRules --rules "$(cat <<EOF
{
  "rules": [
    {
      "from_table": "commerce.corders",
      "to_table": "customer.corders",
      "percent": 5.0
    }
  ]
}
EOF
)"
```

Example with `MoveTables MirrorTraffic`:

```bash
$ vtctldclient --server :15999 MoveTables --target-keyspace customer --workflow commerce2customer MirrorTraffic --percent 5.0
```

Mirror rules can be inspected with `GetMirrorRules`.
