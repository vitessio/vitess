# Release of Vitess v25.0.0
## Summary

### Table of Contents

- **[Minor Changes](#minor-changes)**
    - **[VTGate](#minor-changes-vtgate)**
        - [Vindex Routing Information in LogStats](#vtgate-vindex-routing-logstats)

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-vindex-routing-logstats"/>Vindex Routing Information in LogStats</a>

VTGate now tracks which vindexes are used for shard routing in the `LogStats` struct. The new `RoutingIndexesUsed` field contains a list of `{keyspace, vindex_name, table}` tuples representing the vindexes consulted when routing each query.

This information helps debug sharded query performance by revealing which vindexes VTGate used for shard targeting. If a query unexpectedly scatters across all shards, an empty `RoutingIndexesUsed` indicates no vindex was available for routing.

**Behavior by statement type:**

- **SELECT, UPDATE, DELETE**: Records the vindex used in the WHERE clause for route targeting
- **INSERT**: Records the primary vindex (the first column vindex) that determines shard placement. Secondary vindexes populated as a side effect are not included.

**Note:** This data is available programmatically via the `LogStats` struct for telemetry and monitoring integrations. It is not currently included in VTGate's query log output.

See [#19913](https://github.com/vitessio/vitess/pull/19913) for details.
