## Summary

- [New command line flags and behavior](#new-command-line-flags-and-behavior)

## Known Issues

## Major Changes

### New command line flags and behavior

#### VTGate: Support query timeout --query-timeout
`--query-timeout` allows you to specify a timeout for queries. This timeout is applied to all queries.
It can be overridden by setting the `query_timeout` session variable. 
Setting it as command line directive with `QUERY_TIMEOUT_MS` will override other values.

#### VTTablet: VReplication parallel insert workers --vreplication-parallel-insert-workers
`--vreplication-parallel-insert-workers=[integer]` enables parallel bulk inserts during the copy phase of VReplication. When it is not set (default), VReplication will perform copy-phase inserts one-at-a-time. When set, inserts may happen in-parallel and out-or-order.

Other aspects of the VReplication copy-phase logic are preserved:

  1. Inserts and updates occur within a MySQL transaction.
  2. Updates to `_vt.copy_state` always follow their corresponding insert.
  3. `commit` always follows the corresponding update to `_vt.copy_state`.
  4. For any PK1 and PK2, the update and `commit` steps for PK1 will both precede the update and commit steps of PK2

Other phases (catchup, fast-forward, "running") are not changed by this PR.
