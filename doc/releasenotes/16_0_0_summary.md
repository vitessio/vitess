## Summary

- [New command line flags and behavior](#new-command-line-flags-and-behavior)

## Known Issues

## Major Changes

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

### Important bug fixes

#### Corrupted results for non-full-group-by queries with JOINs

An issue in versions `<= v14.0.3` and `<= v15.0.0` that generated corrupted results for non-full-group-by queries with a JOIN
is now fixed. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11625), and its fix [here](https://github.com/vitessio/vitess/pull/11633).

### Deprecations

The V3 planner is deprecated as of the V16 release, and will be removed in the V17 release of Vitess.
