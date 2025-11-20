# Release of Vitess v24.0.0
## Summary

### Table of Contents

- **[Minor Changes](#minor-changes)**
    - **[VTAdmin](#minor.change-vtadmin)**
        - [Updated to node v25.2.1](#vtadmin-updated-node)
    - **[VTGate](#minor-changes-vtgate)**
        - [New default for `--legacy-replication-lag-algorithm` flag](#vtgate-new-default-legacy-replication-lag-algorithm)

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vtadmin"/>VTAdmin</a>

#### <a id="vtadmin-updated-node"/>Updated to node v25.2.1</a>

Building `vtadmin-web` now requires node >= v25.2.1 (LTS). Breaking changes from v22 to v25 can be found at https://nodejs.org/en/blog/release/v25.2.1 -- with no known issues that apply to VTAdmin.
Full details on the node v25.2.1 release can be found at https://nodejs.org/en/blog/release/v25.2.1.

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-new-default-legacy-replication-lag-algorithm"/>New default for `--legacy-replication-lag-algorithm` flag</a>

The VTGate flag `--legacy-replication-lag-algorithm` now defaults to `false`, disabling the legacy approach to handling replication lag by default.

Instead, a simpler algorithm purely based on low lag, high lag and minimum number of tablets is used, which has proven to be more stable in many production environments. A detailed explanation of the two approaches [is explained in this code comment](https://github.com/vitessio/vitess/blob/main/go/vt/discovery/replicationlag.go#L125-L149).

In v25 this flag will become deprecated and in the following release it will be removed. In the meantime, the legacy behaviour can be used by setting `--legacy-replication-lag-algorithm=true`. This deprecation is tracked in https://github.com/vitessio/vitess/issues/18914.
