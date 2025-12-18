# Release of Vitess v24.0.0
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
        - [Window function pushdown for sharded keyspaces](#window-function-pushdown)
- **[Minor Changes](#minor-changes)**
    - **[VTGate](#minor-changes-vtgate)**
        - [New default for `--legacy-replication-lag-algorithm` flag](#vtgate-new-default-legacy-replication-lag-algorithm)
    - **[VTTablet](#minor-changes-vttablet)**
        - [New Experimental flag `--init-tablet-type-lookup`](#vttablet-init-tablet-type-lookup)
        - [Tablet Shutdown Tracking and Connection Validation](#vttablet-tablet-shutdown-validation)
    - **[VTOrc](#minor-changes-vtorc)**
        - [Deprecated VTOrc Metric Removed](#vtorc-deprecated-metric-removed)
        - [Improved VTOrc Discovery Logging](#vtorc-improved-discovery-logging)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

#### <a id="window-function-pushdown"/>Window function pushdown for sharded keyspaces</a>

This release introduces an optimization that allows window functions to be pushed down to individual shards when they are partitioned by a column that matches a unique vindex.

Previously, all window function queries required single-shard routing, which limited their applicability on sharded tables. With this change, queries where the `PARTITION BY` clause aligns with a unique vindex can now be pushed down and executed on each shard.

For examples and more details, see the [documentation](https://vitess.io/docs/24.0/reference/compatibility/mysql-compatibility/#window-functions).

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-new-default-legacy-replication-lag-algorithm"/>New default for `--legacy-replication-lag-algorithm` flag</a>

The VTGate flag `--legacy-replication-lag-algorithm` now defaults to `false`, disabling the legacy approach to handling replication lag by default.

Instead, a simpler algorithm purely based on low lag, high lag and minimum number of tablets is used, which has proven to be more stable in many production environments. A detailed explanation of the two approaches [is explained in this code comment](https://github.com/vitessio/vitess/blob/main/go/vt/discovery/replicationlag.go#L125-L149).

In v25 this flag will become deprecated and in the following release it will be removed. In the meantime, the legacy behaviour can be used by setting `--legacy-replication-lag-algorithm=true`. This deprecation is tracked in https://github.com/vitessio/vitess/issues/18914.

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-init-tablet-type-lookup"/>New Experimental flag `--init-tablet-type-lookup`</a>

The new experimental flag `--init-tablet-type-lookup` for VTTablet allows tablets to automatically restore their previous tablet type on restart by looking up the existing topology record, rather than always using the static `--init-tablet-type` value.

When enabled, the tablet uses its alias to look up the tablet type from the existing topology record on restart. This allows tablets to maintain their changed roles (e.g., RDONLY/DRAINED) across restarts without manual reconfiguration. If disabled or if no topology record exists, the standard `--init-tablet-type` value will be used instead.

**Note**: Vitess Operator–managed deployments generally do not keep tablet records in the topo between restarts, so this feature will not take effect in those environments.

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
