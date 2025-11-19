# Release of Vitess v24.0.0
## Summary

### Table of Contents

- **[Minor Changes](#minor-changes)**
    - **[VTGate](#minor-changes-vtgate)**
        - [New default for `--legacy-replication-lag-algorithm` flag](#vtgate-new-default-legacy-replication-lag-algorithm)

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-new-default-legacy-replication-lag-algorithm"/>Deprecation of `--legacy-replication-lag-algorithm` flag</a>

The VTGate flag `--legacy-replication-lag-algorithm` now defaults to `false`, disabling the legacy approach to handling replication lag.

Instead, a simpler algorithm purely based on low lag, high lag and minimum number of tablets is used, which has proven to be more stable in many production environments. A detailed explanation of the two approaches [is explained in this code comment](https://github.com/vitessio/vitess/blob/main/go/vt/discovery/replicationlag.go#L125-L149).

In v25 this flag will become deprecated and in the following release it will be removed. This deprecation is tracked in https://github.com/vitessio/vitess/issues/18914.
