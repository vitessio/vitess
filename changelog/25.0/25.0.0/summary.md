# Release of Vitess v25.0.0
## Summary

### Table of Contents

- **[Deprecations and Removals](#deprecations-and-removals)**
    - **[VTGate](#deprecations-vtgate)**
        - [Deprecated `--legacy-replication-lag-algorithm` flag](#vtgate-deprecated-legacy-replication-lag-algorithm)

## <a id="deprecations-and-removals"/>Deprecations and Removals</a>

### <a id="deprecations-vtgate"/>VTGate</a>

#### <a id="vtgate-deprecated-legacy-replication-lag-algorithm"/>Deprecated `--legacy-replication-lag-algorithm` flag</a>

The VTGate flag `--legacy-replication-lag-algorithm` is now deprecated and is a no-op. VTGate always uses the simpler replication lag algorithm based on low lag, high lag and the minimum number of tablets. A detailed explanation of the algorithm [is available in this code comment](https://github.com/vitessio/vitess/blob/main/go/vt/discovery/replicationlag.go).

The flag will be removed entirely in v26. This deprecation is tracked in https://github.com/vitessio/vitess/issues/18914.

**Impact**: Remove any usage of the `--legacy-replication-lag-algorithm` flag from VTGate startup scripts or configuration.
