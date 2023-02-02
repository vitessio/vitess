## Summary

- [New command line flags and behavior](#new-command-line-flags-and-behavior)

- **[VReplication](#vreplication)**
  - [VStream Copy Resume](#vstream-copy-resume)
  - [VDiff2 GA](#vdiff2-ga)

## Known Issues

## Major Changes

### <a id="vreplication"/>VReplication

#### <a id="vstream-copy-resume"/>VStream Copy Resume

In [PR #11103](https://github.com/vitessio/vitess/pull/11103) we introduced the ability to resume a `VTGate` [`VStream` copy operation](https://vitess.io/docs/design-docs/vreplication/vstream/vscopy/). This is useful when a [`VStream` copy operation](https://vitess.io/docs/design-docs/vreplication/vstream/vscopy/) is interrupted due to e.g. a network failure or a server restart. The `VStream` copy operation can be resumed by specifying each table's last seen primary key value in the `VStream` request. Please see the [`VStream` docs](https://vitess.io/docs/16.0/reference/vreplication/vstream/) for more details.

#### <a id="vdiff2-ga"/>VDiff2 GA

We are marking [VDiff v2](https://vitess.io/docs/16.0/reference/vreplication/vdiff2/) as Generally Available or production-ready in v16. We now recommend that you use v2 rather than v1 going forward. V1 will be deprecated and eventually removed in future releases.
If you wish to use v1 for any reason, you will now need to specify the `--v1` flag.

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

### Replication manager removal and VTOrc becomes mandatory
VTOrc is now a **required** component of Vitess starting from v16. If the users want VTOrc to manage replication, then they must run VTOrc.
Replication manager is removed from vttablets since the responsibility of fixing replication lies entirely with VTOrc now.
The flag `disable-replication-manager` is deprecated and will be removed in a later release.

### Breaking Changes

#### Default MySQL version on Docker

The default major MySQL version used by our `vitess/lite:latest` image is going from `5.7` to `8.0`. Additionally, the default patch version of the `vitess/lite:mysql80` image goes from `8.0.23` to `8.0.31`.

#### vtctld UI Removal
In v13, the vtctld UI was deprecated. As of this release, the `web/vtctld2` directory is deleted and the UI will no longer be included in any Vitess images going forward. All build scripts and the Makefile have been updated to reflect this change.

However, the vtctld HTTP API will remain at `{$vtctld_web_port}/api`.

#### vtctld Flag Deprecation & Deletions
With the removal of the vtctld UI, the following vtctld flags have been deprecated:
- `--vtctld_show_topology_crud`: This was a flag that controlled the display of CRUD topology actions in the vtctld UI. The UI is removed, so this flag is no longer necessary.

The following deprecated flags have also been removed:
- `--enable_realtime_stats`
- `--enable_vtctld_ui`
- `--web_dir`
- `--web_dir2`
- `--workflow_manager_init`
- `--workflow_manager_use_election`
- `--workflow_manager_disable`

#### Orchestrator Integration Deletion

Orchestrator integration in `vttablet` was deprecated in the previous release and is deleted in this release.
Consider using `VTOrc` instead of `Orchestrator`.

#### mysqlctl Flags

The [`mysqlctl` command-line client](https://vitess.io/docs/16.0/reference/programs/mysqlctl/) had some leftover (ignored) server flags after the [v15 pflag work](https://github.com/vitessio/enhancements/blob/main/veps/vep-4.md). Those unused flags have now been removed. If you are using any of the following flags with `mysqlctl` in your scripts or other tooling, they will need to be removed prior to upgrading to v16:
  `--port --grpc_auth_static_client_creds --grpc_compression --grpc_initial_conn_window_size --grpc_initial_window_size --grpc_keepalive_time --grpc_keepalive_timeout`

#### Query Serving Errors

In this release, we are introducing a new way to report errors from Vitess through the query interface.
Errors will now have an error code for each error, which will make it easy to search for more information on the issue.
For instance, the following error:

```
aggregate functions take a single argument 'count(user_id, name)'
```

Will be transformed into:

```
VT03001: aggregate functions take a single argument 'count(user_id, name)'
```

The error code `VT03001` can then be used to search or ask for help and report problems.

If you have code searching for error strings from Vitess, this is a breaking change.
Many error strings have been tweaked.
If your application is searching for specific errors, you might need to update your code.

#### Logstats Table and Keyspace removed

Information about which tables are used is now reported by the field TablesUsed added in v15, that is a string array, listing all tables and which keyspace they are in.
The Table/Keyspace fields were deprecated in v15 and are now removed in the v16 release of Vitess.

#### Removed Stats

The stat `QueryRowCounts` is removed in v16. `QueryRowsAffected` and `QueryRowsReturned` can be used instead to gather the same information.

#### Deprecated Stats

The stats `QueriesProcessed` and `QueriesRouted` are deprecated in v16. The same information can be inferred from the stats `QueriesProcessedByTable` and `QueriesRoutedByTable` respectively. These stats will be removed in the next release.

#### Removed flag

The following flag is removed in v16:
- `enable_semi_sync`

#### <a id="lock-timeout-introduction"/> `lock-timeout` and `remote_operation_timeout` Changes

Earlier, the shard and keyspace locks used to be capped by the `remote_operation_timeout`. This is no longer the case and instead a new flag called `lock-timeout` is introduced. 
For backward compatibility, if `lock-timeout` is unspecified and `remote_operation_timeout` flag is provided, then its value will also be used for `lock-timeout` as well.
The default value for `remote_operation_timeout` has also changed from 30 seconds to 15 seconds. The default for the new flag `lock-timeout` is 45 seconds.

During upgrades, if the users want to preserve the same behaviour as previous releases, then they should provide the `remote_operation_timeout` flag explicitly before upgrading.
After the upgrade, they should then alter their configuration to also specify `lock-timeout` explicitly.

#### Normalized labels in the Prometheus Exporter

The Prometheus metrics exporter now properly normalizes _all_ label names into their `snake_case` form, as it is idiomatic for Prometheus metrics. Previously, Vitess instances were emitting inconsistent labels for their metrics, with some of them being `CamelCase` and others being `snake_case`.

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

```shell
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

#### New `vexplain` command
A new `vexplain` command has been introduced with the following syntax -
```
VEXPLAIN [ALL|QUERIES|PLAN] explainable_stmt
```

This command will help the users look at the plan that vtgate comes up with for the given query (`PLAN` type), see all the queries that are executed on all the MySQL instances (`QUERIES` type), 
and see the vtgate plan along with the MySQL explain output for the executed queries (`ALL` type).

The formats `VTEXPLAIN` and `VITESS` for `EXPLAIN` queries are deprecated, and these newly introduced commands should be used instead.

### Important bug fixes

#### Corrupted results for non-full-group-by queries with JOINs

An issue in versions `<= v14.0.3` and `<= v15.0.0` that generated corrupted results for non-full-group-by queries with a JOIN
is now fixed. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11625), and its fix [here](https://github.com/vitessio/vitess/pull/11633).

### Deprecations and Removals

- The V3 planner is deprecated as of the v16 release, and will be removed in the v17 release of Vitess.

- The [VReplication v1 commands](https://vitess.io/docs/15.0/reference/vreplication/v1/) — which were deprecated in Vitess 11.0 — have been removed. You will need to use the [VReplication v2 commands](https://vitess.io/docs/16.0/reference/vreplication/v2/) instead.

- The `vtctlclient VExec` command was removed, having been deprecated since v12.

- The `vtctlclient VReplicationExec` command has now been deprecated and will be removed in a future release. Please see [#12070](https://github.com/vitessio/vitess/pull/12070) for additional details.

- `vtctlclient OnlineDDL ... [complete|retry|cancel|cancel-all]` returns empty result on success instead of number of shard affected.

- VTTablet flag `--backup_storage_hook` has been removed, use one of the builtin compression algorithms or `--external-compressor` and `--external-decompressor` instead.

- vtbackup flag `--backup_storage_hook` has been removed, use one of the builtin compression algorithms or `--external-compressor` and `--external-decompressor` instead.

- The VTTablet flag `--init_populate_metadata` has been deprecated, since we have deleted the `local_metadata` and `shard_metadata` sidecar database tables.

- The dead legacy Workflow Manager related code was removed in [#12085](https://github.com/vitessio/vitess/pull/12085). This included the following `vtctl` client commands: `WorkflowAction`, `WorkflowCreate`, `WorkflowWait`, `WorkflowStart`, `WorkflowStop`, `WorkflowTree`, `WorkflowDelete`.

- VTAdmin's `VTExplain` endpoint has been deprecated. Users can use the new `vexplain` query format instead. The endpoint will be deleted in a future release.


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
Any query using a view will get re-written as a derived table during query planning.
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

### VTOrc

#### Flag Deprecations

The flag `lock-shard-timeout` has been deprecated. Please use the newly introduced `lock-timeout` instead. More detail [here](#lock-timeout-introduction).

### VTTestServer

#### Performance Improvement

Creating a database with vttestserver was taking ~45 seconds. This can be problematic in test environments where testcases do a lot of `create` and `drop` database.
In an effort to minimize the database creation time, we have changed the value of `tablet_refresh_interval` to 10s while instantiating vtcombo during vttestserver initialization. We have also made this configurable so that it can be reduced further if desired.
For any production cluster the default value of this flag is still [1 minute](https://vitess.io/docs/15.0/reference/programs/vtgate/). Reducing this value might put more stress on Topo Server (since we now read from Topo server more often) but for testing purposes 
this shouldn't be a concern.

## Minor changes

### Backup compression benchmarks

Compression benchmarks have been added to the `mysqlctl` package.

The benchmarks fetch and compress a ~6 GiB tar file containing 3 InnoDB files using different built-in and external compressors.

Here are sample results from a 2020-era Mac M1 with 16 GiB of memory:

```sh
$ go test -bench=BenchmarkCompress ./go/vt/mysqlctl -run=NONE -timeout=12h -benchtime=1x -v
goos: darwin
goarch: arm64
pkg: vitess.io/vitess/go/vt/mysqlctl
BenchmarkCompressLz4Builtin
    compression_benchmark_test.go:310: downloading data from https://www.dropbox.com/s/raw/smmgifsooy5qytd/enwiki-20080103-pages-articles.ibd.tar.zst
    BenchmarkCompressLz4Builtin-8                  1        11737493087 ns/op        577.98 MB/s             2.554 compression-ratio
    BenchmarkCompressPargzipBuiltin
    BenchmarkCompressPargzipBuiltin-8              1        31083784040 ns/op        218.25 MB/s             2.943 compression-ratio
    BenchmarkCompressPgzipBuiltin
    BenchmarkCompressPgzipBuiltin-8                1        13325299680 ns/op        509.11 MB/s             2.910 compression-ratio
    BenchmarkCompressZstdBuiltin
    BenchmarkCompressZstdBuiltin-8                 1        18683863911 ns/op        363.09 MB/s             3.150 compression-ratio
    BenchmarkCompressZstdExternal
    BenchmarkCompressZstdExternal-8                1        10795487675 ns/op        628.41 MB/s             3.093 compression-ratio
    BenchmarkCompressZstdExternalFast4
    BenchmarkCompressZstdExternalFast4-8           1        7139319009 ns/op         950.23 MB/s             2.323 compression-ratio
    BenchmarkCompressZstdExternalT0
    BenchmarkCompressZstdExternalT0-8              1        4393860434 ns/op        1543.97 MB/s             3.093 compression-ratio
    BenchmarkCompressZstdExternalT4
    BenchmarkCompressZstdExternalT4-8              1        4389559744 ns/op        1545.49 MB/s             3.093 compression-ratio
    PASS
    cleaning up "/var/folders/96/k7gzd7q10zdb749vr02q7sjh0000gn/T/ee7d47b45ef09786c54fa2d7354d2a68.dat"
```

## Refactor

### VTTablet sidecar schema maintenance refactor

This is an internal refactor and should not change the behavior of Vitess as seen by users. 

Developers will see a difference though: v16 changes the way we maintain vttablet's sidecar database schema (also referred to as the `_vt`
database). Instead of using the `WithDDL` package, introduced in #6348, we use a declarative approach. Users will now have to update
the desired schema in the `go/vt/sidecardb/schema` directory.

The desired schema is specified, one per table. A new module `sidecardb`, compares this to the existing schema and
performs the required `create` or `alter` to reach it. This is done whenever a primary vttablet starts up.

The sidecar tables `local_metadata` and `shard_metadata` are no longer in use and all references to them are removed as
part of this refactor. There were used previously for Orchestrator support, which has been superseded by `vtorc`.
