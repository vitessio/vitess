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

### Incremental backup and point in time recovery

In [PR #11097](https://github.com/vitessio/vitess/pull/11097) we introduced native incremental backup and point in time recovery:

- It is possible to take an incremental backup, starting with last known (full or incremental) backup, and up to either a specified (GTID) position, or current ("auto") position.
- The backup is done by copying binary logs. The binary logs are rotated as needed.
- It is then possible to restore a backup up to a given point in time (GTID position). This involves finding a restore path consisting of a full backup and zero or more incremental backups, applied up to the given point in time.
- A server restored to a point in time remains in `DRAINED` tablet type, and does not join the replication stream (thus, "frozen" in time).
- It is possible to take incremental backups from different tablets. It is OK to have overlaps in incremental backup contents. The restore process chooses a valid path, and is valid as long as there are no gaps in the backed up binary log content.

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

#### VTTablet: --queryserver-config-pool-conn-max-lifetime
`--queryserver-config-pool-conn-max-lifetime=[integer]` allows you to set a timeout on each connection in the query server connection pool. It chooses a random value between its value and twice its value, and when a connection has lived longer than the chosen value, it'll be removed from the pool the next time it's returned to the pool.

#### vttablet --throttler-config-via-topo

The flag `--throttler-config-via-topo` switches throttler configuration from `vttablet`-flags to the topo service. This flag is `false` by default, for backwards compatibility. It will default to `true` in future versions.

#### vtctldclient UpdateThrottlerConfig

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

#### vtctldclient Backup --incremental_from_pos

The `Backup` command now supports `--incremental_from_pos` flag, which can receive a valid position or the value `auto`. For example:

```shell
$ vtctlclient -- Backup --incremental_from_pos "MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615" zone1-0000000102
$ vtctlclient -- Backup --incremental_from_pos "auto" zone1-0000000102
```

When the value is `auto`, the position is evaluated as the last successful backup's `Position`. The idea with incremental backups is to create a contiguous (overlaps allowed) sequence of backups that store all changes from last full backup.

The incremental backup copies binary log files. It does not take MySQL down nor places any locks. It does not interrupt traffic on the MySQL server. The incremental backup copies comlete binlog files. It initially rotates binary logs, then copies anything from the requested position and up to the last completed binary log.

The backup thus does not necessarily start _exactly_ at the requested position. It starts with the first binary log that has newer entries than requested position. It is OK if the binary logs include transactions prior to the equested position. The restore process will discard any duplicates.

Normally, you can expect the backups to be precisely contiguous. Consider an `auto` value: due to the nature of log rotation and the fact we copy complete binlog files, the next incremental backup will start with the first binay log not covered by the previous backup, which in itself copied the one previous binlog file in full. Again, it is completely valid to enter any good position.

The incremental backup fails if it is unable to attain binary logs from given position (ie binary logs have been purged).

The manifest of an incremental backup has a non-empty `FromPosition` value, and a `Incremental = true` value.

#### vtctldclient RestoreFromBackup  --restore_to_pos

- `--restore_to_pos`: request to restore the server up to the given position (inclusive) and not one step further.
- `--dry_run`:        when `true`, calculate the restore process, if possible, evaluate a path, but exit without actually making any changes to the server.

Examples:

```
$ vtctlclient -- RestoreFromBackup  --restore_to_pos  "MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-220" zone1-0000000102
```

The restore process seeks a restore _path_: a sequence of backups (handles/manifests) consisting of one full backup followed by zero or more incremental backups, that can bring the server up to the requested position, inclusive.

The command fails if it cannot evaluate a restore path. Possible reasons:

- there's gaps in the incremental backups
- existing backups don't reach as far as requested position
- all full backups exceed requested position (so there's no way to get into an ealier position)

The command outputs the restore path.

There may be multiple restore paths, the command prefers a path with the least number of backups. This has nothing to say about the amount and size of binary logs involved.

The `RestoreFromBackup  --restore_to_pos` ends with:

- the restored server in intentionally broken replication setup
- tablet type is `DRAINED`

### Important bug fixes

#### Corrupted results for non-full-group-by queries with JOINs

An issue in versions `<= v14.0.3` and `<= v15.0.0` that generated corrupted results for non-full-group-by queries with a JOIN
is now fixed. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11625), and its fix [here](https://github.com/vitessio/vitess/pull/11633).

### Deprecations and Removals

The V3 planner is deprecated as of the V16 release, and will be removed in the V17 release of Vitess.

The [VReplication v1 commands](https://vitess.io/docs/15.0/reference/vreplication/v1/) — which were deprecated in Vitess 11.0 — have been removed. You will need to use the [VReplication v2 commands](https://vitess.io/docs/16.0/reference/vreplication/v2/) instead.

### MySQL Compatibility

#### Transaction Isolation Level
Support added for `set [session] transaction isolation level <transaction_characteristic>`
```sql
transaction_characteristic: {
    ISOLATION LEVEL level
  | access_mode
}

level: {
     REPEATABLE READ
   | READ COMMITTED
   | READ UNCOMMITTED
   | SERIALIZABLE
}
```
This will set the transaction isolation level for the current session. 
This will be applied to any shard where the session will open a transaction.

#### Transaction Access Mode
Support added for `start transaction` with transaction characteristic.
```sql
START TRANSACTION
    [transaction_characteristic [, transaction_characteristic] ...]

transaction_characteristic: {
    WITH CONSISTENT SNAPSHOT
  | READ WRITE
  | READ ONLY
}
```
This will allow users to start a transaction with these characteristics.

#### Support for views

Vitess now supports views in sharded keyspace. Views are not created on the underlying database but are logically stored
in vschema.
Any query using view will get re-rewritten as derived table during query planning.
VSchema Example

```json
{
  "sharded": true,
  "vindexes": {},
  "tables": {},
  "views": {
    "view1": "select * from t1",
    "view2": "select * from t2",
  }
}
```