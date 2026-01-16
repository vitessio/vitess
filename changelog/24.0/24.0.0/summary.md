# Release of Vitess v24.0.0
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
        - [Window function pushdown for sharded keyspaces](#window-function-pushdown)
- **[Minor Changes](#minor-changes)**
    - **[Logging](#minor-changes-logging)**
        - [Structured Logging](#structured-logging)
    - **[VTGate](#minor-changes-vtgate)**
        - [New default for `--legacy-replication-lag-algorithm` flag](#vtgate-new-default-legacy-replication-lag-algorithm)
        - [New "session" mode for `--vtgate-balancer-mode` flag](#vtgate-session-balancer-mode)
    - **[Query Serving](#minor-changes-query-serving)**
        - [JSON_EXTRACT now supports dynamic path arguments](#query-serving-json-extract-dynamic-args)
    - **[VTTablet](#minor-changes-vttablet)**
        - [New Experimental flag `--init-tablet-type-lookup`](#vttablet-init-tablet-type-lookup)
        - [QueryThrottler Observability Metrics](#vttablet-querythrottler-metrics)
        - [New `in_order_completion_pending_count` field in OnlineDDL outputs](#vttablet-onlineddl-in-order-completion-count)
        - [Tablet Shutdown Tracking and Connection Validation](#vttablet-tablet-shutdown-validation)
    - **[VTOrc](#minor-changes-vtorc)**
        - [Deprecated VTOrc Metric Removed](#vtorc-deprecated-metric-removed)
        - [Improved VTOrc Discovery Logging](#vtorc-improved-discovery-logging)
        - [New `--cell` Flag](#vtorc-cell-flag)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

#### <a id="window-function-pushdown"/>Window function pushdown for sharded keyspaces</a>

This release introduces an optimization that allows window functions to be pushed down to individual shards when they are partitioned by a column that matches a unique vindex.

Previously, all window function queries required single-shard routing, which limited their applicability on sharded tables. With this change, queries where the `PARTITION BY` clause aligns with a unique vindex can now be pushed down and executed on each shard.

For examples and more details, see the [documentation](https://vitess.io/docs/24.0/reference/compatibility/mysql-compatibility/#window-functions).

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-logging"/>Logging</a>

#### <a id="structured-logging"/>Structured Logging</a>

Opt-in structured JSON logging has been added. By default, Vitess will continue to use unstructured logging through `glog`. To opt-in to structured logging, use these new flags:

- `--structured-logging`: Enables structured logging.
- `--structured-logging-level`: Minimum log level: trace, debug, info, warn, error, fatal, panic, disabled (default: info)
- `--structured-logging-pretty`: Enable pretty, human-readable output (default: false)
- `--structured-logging-file`: Log to a file instead of stdout

The existing `--log-rotate-max-size` flag controls the max size of the log file before rotation. Otherwise, the new structured logging flags are mutually exclusive with the old `glog` flags.

Example JSON output:

```console
$ vttablet --structured-logging

{"level":"info","caller":"/Users/mhamza/dev/vitess/go/vt/servenv/servenv_unix.go:57","time":"2026-01-06T09:22:36-05:00","message":"Version: 24.0.0-SNAPSHOT (Git revision  branch '') built on  by @ using go1.25.5 darwin/arm64"}
{"level":"fatal","caller":"/Users/mhamza/dev/vitess/go/vt/topo/server.go:257","time":"2026-01-06T09:22:36-05:00","message":"topo-global-server-address must be configured"}
```

Example pretty output:

```console
$ vttablet --structured-logging --structured-logging-pretty

2026-01-06T09:23:13-05:00 INF go/vt/servenv/servenv_unix.go:57 > Version: 24.0.0-SNAPSHOT (Git revision  branch '') built on  by @ using go1.25.5 darwin/arm64
2026-01-06T09:23:13-05:00 FTL go/vt/topo/server.go:257 > topo-global-server-address must be configured
```

In v25, structured logging will become the default and `glog` and its flags will be deprecated. In v26, `glog` will be removed.

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-new-default-legacy-replication-lag-algorithm"/>New default for `--legacy-replication-lag-algorithm` flag</a>

The VTGate flag `--legacy-replication-lag-algorithm` now defaults to `false`, disabling the legacy approach to handling replication lag by default.

Instead, a simpler algorithm purely based on low lag, high lag and minimum number of tablets is used, which has proven to be more stable in many production environments. A detailed explanation of the two approaches [is explained in this code comment](https://github.com/vitessio/vitess/blob/main/go/vt/discovery/replicationlag.go#L125-L149).

In v25 this flag will become deprecated and in the following release it will be removed. In the meantime, the legacy behaviour can be used by setting `--legacy-replication-lag-algorithm=true`. This deprecation is tracked in https://github.com/vitessio/vitess/issues/18914.

#### <a id="vtgate-session-balancer-mode"/>New "session" mode for `--vtgate-balancer-mode` flag</a>

The VTGate flag `--vtgate-balancer-mode` now supports a new "session" mode in addition to the existing "cell", "prefer-cell", and "random" modes. Session mode routes each session consistently to the same tablet for the session's duration.

To enable session mode, set the flag when starting VTGate:

```
--vtgate-balancer-mode=session
```

### <a id="minor-changes-query-serving"/>Query Serving</a>

#### <a id="query-serving-json-extract-dynamic-args"/>JSON_EXTRACT now supports dynamic path arguments</a>

The `JSON_EXTRACT` function now supports dynamic path arguments like bind variables or results from other function calls. Previously, `JSON_EXTRACT` only worked with static string literals for path arguments.

Null handling now matches MySQL behavior. The function returns NULL when either the document or path argument is NULL.

Static path arguments are still optimized, even when mixed with dynamic arguments, so existing queries won't see any performance regression.

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-init-tablet-type-lookup"/>New Experimental flag `--init-tablet-type-lookup`</a>

The new experimental flag `--init-tablet-type-lookup` for VTTablet allows tablets to automatically restore their previous tablet type on restart by looking up the existing topology record, rather than always using the static `--init-tablet-type` value.

When enabled, the tablet uses its alias to look up the tablet type from the existing topology record on restart. This allows tablets to maintain their changed roles (e.g., RDONLY/DRAINED) across restarts without manual reconfiguration. If disabled or if no topology record exists, the standard `--init-tablet-type` value will be used instead.

**Note**: Vitess Operator–managed deployments generally do not keep tablet records in the topo between restarts, so this feature will not take effect in those environments.

#### <a id="vttablet-querythrottler-metrics"/>QueryThrottler Observability Metrics</a>

VTTablet now exposes new metrics to track QueryThrottler behavior.

Four new metrics have been added:

- **QueryThrottlerRequests**: Total number of requests evaluated by the query throttler
- **QueryThrottlerThrottled**: Number of requests that were throttled
- **QueryThrottlerTotalLatencyNs**: Total time each request takes in query throttling, including evaluation, metric checks, and other overhead (nanoseconds)
- **QueryThrottlerEvaluateLatencyNs**: Time taken to make the throttling decision (nanoseconds)

All metrics include labels for `Strategy`, `Workload`, and `Priority`. The `QueryThrottlerThrottled` metric has additional labels for `MetricName`, `MetricValue`, and `DryRun` to identify which metric triggered the throttling and whether it occurred in dry-run mode.

These metrics help monitor throttling patterns, identify which workloads are throttled, measure performance overhead, and validate behavior in dry-run mode before configuration changes.

#### <a id="vttablet-onlineddl-in-order-completion-count"/>New `in_order_completion_pending_count` field in OnlineDDL outputs</a>

OnlineDDL migration outputs now include a new `in_order_completion_pending_count` field. When using the `--in-order-completion` flag, this field shows how many migrations must complete before the current migration. The field is visible in `SHOW vitess_migrations` queries and `vtctldclient OnlineDDL <db> show` outputs.

This provides better visibility into migration queue dependencies, making it easier to understand why a migration might be postponed. The count is automatically updated during the scheduler loop and cleared when migrations complete, fail, or are cancelled.

#### <a id="vttablet-tablet-shutdown-validation"/>Tablet Shutdown Tracking and Connection Validation</a>

Vitess now tracks when tablets cleanly shut down and validates tablet records before attempting connections, reducing unnecessary connection attempts and log noise.

**New Field**: A new `tablet_shutdown_time` field has been added to the Tablet protobuf. This field is set to the current timestamp when a tablet cleanly shuts down and is cleared (set to `nil`) when the tablet starts. This allows other Vitess components to detect when a tablet is intentionally offline.

**Connection Validation**: When a tablet record has `tablet_shutdown_time` set, Vitess components will skip connection attempts and return an error indicating the tablet is shutdown. VTOrc will now skip polling tablets that have `tablet_shutdown_time` set. For tablets that shutdown uncleanly (crashed, killed, etc.), the field remains `nil` and the pre-v24 behavior is preserved (connection attempt with error logging).

**Note**: This is a best-effort mechanism. Tablets that are killed or crash may not have the opportunity to set this field, in which case components will continue to attempt connections as they did in v23 and earlier.

### <a id="minor-changes-vtorc"/>VTOrc</a>

#### <a id="vtorc-deprecated-metric-removed"/>Deprecated VTOrc Metric Removed</a>

The `discoverInstanceTimings` metric has been removed from VTOrc in v24.0.0. This metric was deprecated in v23.

**Migration**: Use `discoveryInstanceTimings` instead, which provides the same timing information for instance discovery actions (Backend, Instance, Other).

**Impact**: Monitoring dashboards or alerting systems using `discoverInstanceTimings` must be updated to use `discoveryInstanceTimings`.

#### <a id="vtorc-improved-discovery-logging"/>Improved VTOrc Discovery Logging</a>

VTOrc's `DiscoverInstance` function now includes the tablet alias in all log messages and uses the correct log level when errors occur. Previously, error messages did not indicate which tablet failed discovery, and errors were logged at INFO level instead of ERROR level.

This improvement makes it easier to identify and debug issues with specific tablets when discovery operations fail.

#### <a id="vtorc-cell-flag"/>New `--cell` Flag</a>

VTOrc now supports a `--cell` flag that specifies which Vitess cell the VTOrc process is running in. The flag is optional in v24 but will be required in v25+, similar to VTGate's `--cell` flag.

When provided, VTOrc validates that the cell exists in the topology service on startup. Without the flag, VTOrc logs a warning about the v25+ flag requirement.

This enables future cross-cell problem validation, where VTOrc will be able to ask another cell to validate detected problems before taking recovery actions. The flag is currently validated but not yet used in VTOrc recovery logic.

**Note**: If you're running VTOrc in a multi-cell deployment, start using the `--cell` flag now to prepare for the v25 requirement.
