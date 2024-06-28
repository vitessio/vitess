
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations](#deprecations)**
    - [Metrics](#deprecations-metrics) 
  - **[Deletions](#deletions)** 
    - [Deletion of deprecated metrics](#metric-deletion)
  - **[Breaking changes](#breaking-changes)**

## <a id="major-changes"/>Major Changes

### <a id="deprecations"/>Deprecations

#### <a id="deprecations-metrics"/>Metrics

The following metrics are now deprecated, if provided please use their replacement.

| Component  |      Metric Name      |           Replaced By           |
|------------|:---------------------:|:-------------------------------:|
| `vttablet` |  `QueryCacheLength`   |  `TabletQueryPlanCacheLength`   |
| `vttablet` |   `QueryCacheSize`    |   `TabletQueryPlanCacheSize`    |
| `vttablet` | `QueryCacheCapacity`  | `TabletQueryPlanCacheCapacity`  |
| `vttablet` | `QueryCacheEvictions` | `TabletQueryPlanCacheEvictions` |
| `vttablet` |   `QueryCacheHits`    |   `TabletQueryPlanCacheHits`    |
| `vttablet` |  `QueryCacheMisses`   |  `TabletQueryPlanCacheMisses`   |


### <a id="deletions"/>Deletions

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
		


