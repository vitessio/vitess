## Summary

- [New command line flags and behavior](#new-command-line-flags-and-behavior)

- **[VReplication](#vreplication)**
  - [VStream Copy Resume](#vstream-copy-resume)

## Known Issues

## Major Changes

### <a id="vreplication"/>VReplication

#### <a id="vstream-copy-resume"/>VStream Copy Resume

In [PR #11103](https://github.com/vitessio/vitess/pull/11103) we introduced the ability to resume a `VTGate` [`VStream` copy operation](https://vitess.io/docs/design-docs/vreplication/vstream/vscopy/). This is useful when a [`VStream` copy operation](https://vitess.io/docs/design-docs/vreplication/vstream/vscopy/) is interrupted due to e.g. a network failure or a server restart. The `VStream` copy operation can be resumed by specifying each table's last seen primary key value in the `VStream` request. Please see the [`VStream` docs](https://vitess.io/docs/16.0/reference/vreplication/vstream/) for more details.

### Tablet throttler

The tablet throttler can now be configured dynamically. Configuration is now found in the topo service, and applies to all tablets in all shards and cells of a given keyspace. For backwards compatibility `v16` still supports `vttablet`-based command line flags for throttler ocnfiguration.

It is possible to enable/disable, to change throttling threshold as well as the throttler query.

See https://github.com/vitessio/vitess/pull/11604

### Breaking Changes

#### Orchestrator Integration Deletion

Orchestrator integration in `vttablet` was deprecated in the previous release and is deleted in this release.
Consider using `VTOrc` instead of `Orchestrator`.

### New command line flags and behavior

#### VTGate: Support query timeout --query-timeout
`--query-timeout` allows you to specify a timeout for queries. This timeout is applied to all queries.
It can be overridden by setting the `query_timeout` session variable. 
Setting it as command line directive with `QUERY_TIMEOUT_MS` will override other values.

#### VTTablet: VReplication parallel insert workers --vreplication-parallel-insert-workers
`--vreplication-parallel-insert-workers=[integer]` enables parallel bulk inserts during the copy phase
of VReplication (disabled by default). When set to a value greater than 1 the bulk inserts — each
executed as a single transaction from the vstream packet contents — may happen in-parallel and
out-of-order, but the commit of those transactions are still serialized in order.

Other aspects of the VReplication copy-phase logic are preserved:
  1. All statements executed when processing a vstream packet occur within a single MySQL transaction.
  2. Writes to `_vt.copy_state` always follow their corresponding inserts from within the vstream packet.
  3. The final `commit` for the vstream packet always follows the corresponding write to `_vt.copy_state`.
  4. The vstream packets are committed in the order seen in the stream. So for any PK1 and PK2, the write to `_vt.copy_state` and  `commit` steps (steps 2 and 3 above) for PK1 will both precede the `_vt.copy_state` write and commit steps of PK2.

 Other phases, catchup, fast-forward, and replicating/"running", are unchanged.

### vttablet --throttler-config-via-topo

The flag `--throttler-config-via-topo` switches throttler configuration from `vttablet`-flags to the topo service. This flag is `false` by default, for backwards compatibility. It will default to `true` in future versions.

### vtctldclient UpdateThrottlerConfig

Tablet throttler configuration is now supported in `topo`. Updating the throttler configuration is done via `vtctldclient UpdateThrottlerConfig` and applies to all tablet in all cells for a given keyspace.

Examples:
```shell
# disable throttler; all throttler checks will return with "200 OK"
$ vtctldclient UpdateThrottlerConfig --disable commerce

# enable throttler; checks are responded with appropriate status per current metrics
$ vtctldclient UpdateThrottlerConfig --enable commerce

# Both enable and set threshold in same command. Since no query is indicated, we assume the default check for replication lag
$ vtctldclient UpdateThrottlerConfig --enable --threshold 5.0 commerce

# Change threshold. Does not affect enabled/disabled state of the throttler
$ vtctldclient UpdateThrottlerConfig --threshold 1.5 commerce

# Use a custom query
$ vtctldclient UpdateThrottlerConfig --custom_query "show global status like 'threads_running'" --check_as_check_self --threshold 50 commerce

# Restore default query and threshold
$ vtctldclient UpdateThrottlerConfig --custom_query "" --check_as_check_shard --threshold 1.5 commerce
```

See https://github.com/vitessio/vitess/pull/11604

### Important bug fixes

#### Corrupted results for non-full-group-by queries with JOINs

An issue in versions `<= v14.0.3` and `<= v15.0.0` that generated corrupted results for non-full-group-by queries with a JOIN
is now fixed. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11625), and its fix [here](https://github.com/vitessio/vitess/pull/11633).

### Deprecations

The V3 planner is deprecated as of the V16 release, and will be removed in the V17 release of Vitess.
