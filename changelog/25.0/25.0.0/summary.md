# Release of Vitess v25.0.0
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
        - [QueryThrottler TABLET_THROTTLER Strategy](#querythrottler-tablet-throttler-strategy)

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

