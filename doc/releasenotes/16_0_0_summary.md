## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
    - [VTGate Advertised MySQL Version](#advertised-mysql-version)
    - [Default MySQL version on Docker](#default-mysql-version)
    - [⚠️ Upgrading to this release with vitess-operator](#upgrading-to-this-release-with-vitess-operator)
    - [Flag Deletions and Deprecations](#flag-deletions-and-deprecations)
      - [VTCtld](#vtctld-flag-deletions-deprecations)
      - [MySQLCtl](#mysqlctl-flag-deletions-deprecations)
      - [VTTablet](#vttablet-flag-deletions-deprecations)
      - [VTBackup](#vtbackup-flag-deletions-deprecations)
      - [VTOrc](#vtorc-flag-deletions-deprecations)
    - [`lock-timeout` and `remote_operation_timeout` Changes](#lock-timeout-introduction)
    - [Orchestrator Integration Deletion](#orc-integration-removal)
    - [vtctld UI Removal](#vtcltd-ui-removal)
    - [Query Serving Errors](#qs-errors)
    - [Logstats Table and Keyspace removed](#logstats-table-keyspace)
    - [Removed Stats](#removed-stats)
    - [Deprecated Stats](#deprecated-stats)
    - [Normalized labels in the Prometheus Exporter](#normalized-lables)
  - **[Replication manager removal and VTOrc becomes mandatory](#repl-manager-removal)**
  - **[VReplication](#vreplication)**
    - [VStream Copy Resume](#vstream-copy-resume)
    - [VDiff2 GA](#vdiff2-ga)
  - **[Tablet throttler](#tablet-throttler)**
  - **[Incremental backup and point in time recovery](#inc-backup)**
  - **[New command line flags and behavior](#new-flag)**
    - [VTGate: Support query timeout --query-timeout](#vtgate-query-timeout)
    - [VTTablet: VReplication parallel insert workers --vreplication-parallel-insert-workers](#vrepl-parallel-workers)
    - [VTTablet: --queryserver-config-pool-conn-max-lifetime](#queryserver-lifetime)
    - [vttablet --throttler-config-via-topo](#vttablet-throttler-config)
    - [vtctldclient UpdateThrottlerConfig](#vtctldclient-update-throttler)
    - [vtctldclient Backup --incremental_from_pos](#vtctldclient-backup)
    - [vtctldclient RestoreFromBackup  --restore_to_pos](#vtctldclient-restore-from-backup)
    - [New `vexplain` command](#new-vexplain-command)
  - **[Important bug fixes](#important-bug-fixes)**
    - [Corrupted results for non-full-group-by queries with JOINs](#corrupted-results)
  - **[Deprecations and Removals](#deprecations-removals)**
  - **[MySQL Compatibility](#mysql-compatibility)**
    - [Transaction Isolation Level](#transaction-isolation-level)
    - [Transaction Access Mode](#transaction-access-mode)
    - [Support for views](#support-views)
  - **[VTTestServer](#vttestserver)**
    - [Performance Improvement](#perf-improvement)
- **[Minor Changes](#minor-changes)**
  - **[Backup compression benchmarks](#backup-comp-benchmarks)**
- **[Refactor](#refactor)**
  - **[VTTablet sidecar schema maintenance refactor](#vttablet-sidecar-schema)**

## <a id="major-changes"/>Major Changes

### <a id="breaking-changes"/>Breaking Changes

#### <a id="advertised-mysql-version"/>VTGate Advertised MySQL Version

Since [Pull Request #11989](https://github.com/vitessio/vitess/pull/11989), VTGate advertises MySQL version 8.0.30. This is a breaking change for clients that rely on the VTGate advertised MySQL version and still use MySQL 5.7.
The users can set the `mysql_server_version` flag to advertise the correct version.

#### <a id="default-mysql-version"/>Default MySQL version on Docker

The default major MySQL version used by our `vitess/lite:latest` image is going from `5.7` to `8.0`. Additionally, the patch version of MySQL80 has been upgraded from `8.0.23` to `8.0.30`.
This change was brought by [Pull Request #12252](https://github.com/vitessio/vitess/pull/12252).

#### <a id="upgrading-to-this-release-with-vitess-operator"/>⚠️Upgrading to this release with vitess-operator

If you are using the vitess-operator and want to remain on MySQL 5.7, **you are required** to use the `vitess/lite:v16.0.0-mysql57` Docker Image, otherwise the `vitess/lite:v16.0.0` image will be on MySQL 80.

However, if you are running MySQL 8.0 on the vitess-operator, with for instance `vitess/lite:v15.0.2-mysql80`, considering that we are bumping the patch version of MySQL 80 from `8.0.23` to `8.0.30`, you will have to manually upgrade:

1. Add `innodb_fast_shutdown=0` to your extra cnf in your YAML file.
2. Apply this file.
3. Wait for all the pods to be healthy.
4. Then change your YAML file to use the new Docker Images (`vitess/lite:v16.0.0`, defaults to mysql80).
5. Remove `innodb_fast_shutdown=0` from your extra cnf in your YAML file.
6. Apply this file.

#### <a id="flag-deletions-and-deprecations"\>Flag Deletions and Deprecations

##### <a id="vtctld-flag-deletions-deprecations"/>VTCtld
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

##### <a id="mysqlctl-flag-deletions-deprecations"/>MySQLCtld

The [`mysqlctl` command-line client](https://vitess.io/docs/16.0/reference/programs/mysqlctl/) had some leftover (ignored) server flags after the [v15 pflag work](https://github.com/vitessio/enhancements/blob/main/veps/vep-4.md). Those unused flags have now been removed. If you are using any of the following flags with `mysqlctl` in your scripts or other tooling, they will need to be removed prior to upgrading to v16:
`--port --grpc_auth_static_client_creds --grpc_compression --grpc_initial_conn_window_size --grpc_initial_window_size --grpc_keepalive_time --grpc_keepalive_timeout`

##### <a id="vttablet-flag-deletions-deprecations"/>VTTablet

The following flags were removed in v16:
- `--enable_semi_sync`
- `--backup_storage_hook`, use one of the builtin compression algorithms or `--external-compressor` and `--external-decompressor` instead.
- `--init_populate_metadata`, since we have deleted the `local_metadata` and `shard_metadata` sidecar database tables.

The flag `--disable-replication-manager` is deprecated and will be removed in a future release.

##### <a id="vtbackup-flag-deletions-deprecations"/>VTBackup

The VTBackup flag `--backup_storage_hook` has been removed, use one of the builtin compression algorithms or `--external-compressor` and `--external-decompressor` instead.


##### <a id="vtorc-flag-deletions-deprecations"/>VTOrc

The flag `--lock-shard-timeout` has been deprecated. Please use the newly introduced `--lock-timeout` flag instead. More detail [here](#lock-timeout-introduction).

#### <a id="lock-timeout-introduction"/>`lock-timeout` and `remote_operation_timeout` Changes

Before the changes made in [Pull Request #11881](https://github.com/vitessio/vitess/pull/11881), the shard and keyspace locks used to be capped by the `remote_operation_timeout`. This is no longer the case and instead a new flag called `lock-timeout` is introduced.
For backward compatibility, if `lock-timeout` is unspecified and `remote_operation_timeout` flag is provided, then its value will also be used for `lock-timeout`.
The default value for `remote_operation_timeout` has also changed from 30 seconds to 15 seconds. The default for the new flag `lock-timeout` is 45 seconds.

During upgrades, if the users want to preserve the same behaviour as previous releases, then they should provide the `remote_operation_timeout` flag explicitly before upgrading.
After the upgrade, they should then alter their configuration to also specify `lock-timeout` explicitly.

#### <a id="orc-integration-removal"/>Orchestrator Integration Deletion

Orchestrator integration in `vttablet` was deprecated in the previous release and is deleted in this release.
`VTOrc` should be deployed instead. You can read more on [how VTOrc is designed](https://vitess.io/docs/16.0/reference/vtorc/) and on [how to run VTOrc in production](https://vitess.io/docs/16.0/user-guides/configuration-basic/vtorc/).

#### <a id="vtcltd-ui-removal"/>vtctld web UI Removal
In v13, the vtctld UI was deprecated. As of this release, the `web/vtctld2` directory is deleted and the UI will no longer be included in any Vitess images going forward. All build scripts and the Makefile have been updated to reflect this change, which was done in [Pull Request #11851](https://github.com/vitessio/vitess/pull/11851)

However, the vtctld HTTP API will remain at `{$vtctld_web_port}/api`.

#### <a id="qs-errors"/>Query Serving Errors

In [Pull Request #10738](https://github.com/vitessio/vitess/pull/10738) we are introducing a new way to report errors from Vitess through the query interface.
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

#### <a id="logstats-table-keyspace"/>Logstats Table and Keyspace removed

Information about which tables are used is now reported by the field TablesUsed added in v15, that is a string array, listing all tables and which keyspace they are in.
The Table/Keyspace fields were deprecated in v15 and are now removed in the v16 release, more information can be found on [Pull Request #12083](https://github.com/vitessio/vitess/pull/12083).

#### <a id="removed-stats"/>Removed Stats

The stat `QueryRowCounts` is removed in v16 as part of [Pull Request #12083](https://github.com/vitessio/vitess/pull/12083). `QueryRowsAffected` and `QueryRowsReturned` can be used instead to gather the same information.

#### <a id="deprecated-stats"/>Deprecated Stats

The stats `QueriesProcessed` and `QueriesRouted` are deprecated in v16 as part of [Pull Request #12083](https://github.com/vitessio/vitess/pull/12083). The same information can be inferred from the stats `QueriesProcessedByTable` and `QueriesRoutedByTable` respectively. These stats will be removed in the next release.

#### <a id="normalized-lables"/>Normalized labels in the Prometheus Exporter

The Prometheus metrics exporter now properly normalizes _all_ label names into their `snake_case` form, as it is idiomatic for Prometheus metrics. Previously, Vitess instances were emitting inconsistent labels for their metrics, with some of them being `CamelCase` and others being `snake_case`.
More information about this change can be found on [Pull Request #12057](https://github.com/vitessio/vitess/pull/12057).

For example, `vtgate_topology_watcher_errors{Operation="GetTablet"} 0` will become `vtgate_topology_watcher_errors{operation="GetTablet"} 0`

Some more of these changes are listed here -

| Previous metric                                             | New Metric                                                  |
|-------------------------------------------------------------|-------------------------------------------------------------|
| vtgate_topology_watcher_operations{Operation="AddTablet"}   | vtgate_topology_watcher_operations{operation="AddTablet"}   |
| vtgate_queries_processed{Plan="Reference"}                  | vtgate_queries_processed{plan="Reference"}                  |
| vtgate_queries_routed{Plan="Reference"}                     | vtgate_queries_routed{plan="Reference"}                     |
| vttablet_table_allocated_size{Table="corder"}               | vttablet_table_allocated_size{table="corder"}               |
| vttablet_table_file_size{Table="corder"}                    | vttablet_table_file_size{table="corder"}                    |
| vttablet_topology_watcher_errors{Operation="GetTablet"}     | vttablet_topology_watcher_errors{operation="GetTablet"}     |
| vttablet_topology_watcher_operations{Operation="AddTablet"} | vttablet_topology_watcher_operations{operation="AddTablet"} |

### <a id="repl-manager-removal"/>Replication manager removal and VTOrc becomes mandatory
VTOrc is now a **required** component of Vitess starting from v16. If the users want Vitess to manage replication, then they must run VTOrc.
Replication manager is removed from vttablets since the responsibility of fixing replication lies entirely with VTOrc now.
The flag `disable-replication-manager` is deprecated and will be removed in a future release.

### <a id="vreplication"/>VReplication

#### <a id="vstream-copy-resume"/>VStream Copy Resume

In [Pull Request #11103](https://github.com/vitessio/vitess/pull/11103) we introduced the ability to resume a `VTGate` [`VStream` copy operation](https://vitess.io/docs/16.0/reference/vreplication/vstream/). This is useful when a [`VStream` copy operation](https://vitess.io/docs/16.0/reference/vreplication/vstream/) is interrupted due to e.g. a network failure or a server restart. The `VStream` copy operation can be resumed by specifying each table's last seen primary key value in the `VStream` request. Please see the [`VStream` docs](https://vitess.io/docs/16.0/reference/vreplication/vstream/) for more details.

#### <a id="vdiff2-ga"/>VDiff2 GA

We are marking [VDiff v2](https://vitess.io/docs/16.0/reference/vreplication/vdiff2/) as production-ready in v16. We now recommend that you use v2 rather than v1 going forward. V1 will be deprecated and eventually removed in future releases.
If you wish to use v1 for any reason, you will now need to specify the `--v1` flag.

### <a id="tablet-throttler"/>Tablet throttler

The tablet throttler can now be configured dynamically. Configuration is now found in the topo service, and applies to all tablets in all shards and cells of a given keyspace.
It is possible to enable or disable throttling, and to change the throttling threshold as well as the throttler query.

Please note that this feature is considered experimental in this release. For backwards compatibility `v16` still supports `vttablet`-based command line flags for throttler configuration.

More information can be found on [Pull Request #11604](https://github.com/vitessio/vitess/pull/11604).

### <a id="inc-backup"/>Incremental backup and point in time recovery

In [Pull Request #11097](https://github.com/vitessio/vitess/pull/11097) we introduced native incremental backup and point in time recovery:

- It is possible to take an incremental backup, starting with last known (full or incremental) backup, and up to either a specified (GTID) position, or current ("auto") position.
- The backup is done by copying binary logs. The binary logs are rotated as needed.
- It is then possible to restore a backup up to a given point in time (GTID position). This involves finding a restore path consisting of a full backup and zero or more incremental backups, applied up to the given point in time.
- A server restored to a point in time remains in `DRAINED` tablet type, and does not join the replication stream (thus, "frozen" in time).
- It is possible to take incremental backups from different tablets. It is OK to have overlaps in incremental backup contents. The restore process chooses a valid path, and is valid as long as there are no gaps in the backed up binary log content.

### <a id="new-flag"/>New command line flags and behavior

#### <a id="vtgate-query-timeout"/>VTGate: Support query timeout --query-timeout

`--query-timeout` allows you to specify a timeout for queries. This timeout is applied to all queries.
It can be overridden by setting the `query_timeout` session variable.
Setting it as query comment directive with `QUERY_TIMEOUT_MS` will override other values.

#### <a id="vrepl-parallel-workers"/>VTTablet: VReplication parallel insert workers --vreplication-parallel-insert-workers

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

#### <a id="queryserver-lifetime"/>VTTablet: --queryserver-config-pool-conn-max-lifetime

`--queryserver-config-pool-conn-max-lifetime=[integer]` allows you to set a timeout on each connection in the query server connection pool. It chooses a random value between its value and twice its value, and when a connection has lived longer than the chosen value, it'll be removed from the pool the next time it's returned to the pool.

#### <a id="vttablet-throttler-config"/>vttablet --throttler-config-via-topo

The flag `--throttler-config-via-topo` switches throttler configuration from `vttablet`-flags to the topo service. This flag is `false` by default, for backwards compatibility. It will default to `true` in future versions.

#### <a id="vtctldclient-update-throttler"/>vtctldclient UpdateThrottlerConfig

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

#### <a id="vtctldclient-backup"/>vtctldclient Backup --incremental_from_pos

The `Backup` command now supports `--incremental_from_pos` flag, which can receive a valid position or the value `auto`. For example:

```shell
$ vtctlclient -- Backup --incremental_from_pos "MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615" zone1-0000000102
$ vtctlclient -- Backup --incremental_from_pos "auto" zone1-0000000102
```

When the value is `auto`, the position is evaluated as the last successful backup's `Position`. The idea with incremental backups is to create a contiguous (overlaps allowed) sequence of backups that store all changes from last full backup.

The incremental backup copies binary log files. It does not take MySQL down nor places any locks. It does not interrupt traffic on the MySQL server. The incremental backup copies complete binlog files. It initially rotates binary logs, then copies anything from the requested position and up to the last completed binary log.

The backup thus does not necessarily start _exactly_ at the requested position. It starts with the first binary log that has newer entries than requested position. It is OK if the binary logs include transactions prior to the requested position. The restore process will discard any duplicates.

Normally, you can expect the backups to be precisely contiguous. Consider an `auto` value: due to the nature of log rotation and the fact we copy complete binlog files, the next incremental backup will start with the first binay log not covered by the previous backup, which in itself copied the one previous binlog file in full. Again, it is completely valid to enter any good position.

The incremental backup fails if it is unable to attain binary logs from given position (ie binary logs have been purged).

The manifest of an incremental backup has a non-empty `FromPosition` value, and a `Incremental = true` value.

#### <a id="vtctldclient-restore-from-backup"/>vtctldclient RestoreFromBackup  --restore_to_pos

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

#### <a id="new-vexplain-command"/>New `vexplain` command
A new `vexplain` command has been introduced with the following syntax:
```
VEXPLAIN [ALL|QUERIES|PLAN] explainable_stmt
```

This command will help users look at the plan that vtgate comes up with for the given query (`PLAN` type), see all the queries that are executed on all the MySQL instances (`QUERIES` type), and see the vtgate plan along with the MySQL explain output for the executed queries (`ALL` type).

The formats `VTEXPLAIN` and `VITESS` for `EXPLAIN` queries are deprecated, and these newly introduced commands should be used instead.

### <a id="important-bug-fixes"/>Important bug fixes

#### <a id="corrupted-results"/>Corrupted results for non-full-group-by queries with JOINs

An issue in versions `<= v14.0.3` and `<= v15.0.0` that generated corrupted results for non-full-group-by queries with a JOIN
is now fixed. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11625), and its fix [here](https://github.com/vitessio/vitess/pull/11633).

### <a id="deprecations-removals"/>Deprecations and Removals

- The V3 planner is deprecated as of the v16 release, and will be removed in the v17 release of Vitess.

- The [VReplication v1 commands](https://vitess.io/docs/15.0/reference/vreplication/v1/) — which were deprecated in Vitess 11.0 — have been removed. You will need to use the [VReplication v2 commands](https://vitess.io/docs/16.0/reference/vreplication/v2/) instead.

- The `vtctlclient VExec` command was removed, having been deprecated since v12.

- The `vtctlclient VReplicationExec` command has now been deprecated and will be removed in a future release. Please see [#12070](https://github.com/vitessio/vitess/pull/12070) for additional details.

- `vtctlclient OnlineDDL ... [complete|retry|cancel|cancel-all]` returns empty result on success instead of number of shard affected.

- The dead legacy Workflow Manager related code was removed in [#12085](https://github.com/vitessio/vitess/pull/12085). This included the following `vtctl` client commands: `WorkflowAction`, `WorkflowCreate`, `WorkflowWait`, `WorkflowStart`, `WorkflowStop`, `WorkflowTree`, `WorkflowDelete`.

- VTAdmin's `VTExplain` endpoint has been deprecated. Users can use the new `vexplain` query format instead. The endpoint will be deleted in a future release.

### <a id="mysql-compatibility"/>MySQL Compatibility

#### <a id="transaction-isolation-level"/>Transaction Isolation Level

In [Pull Request #11704](https://github.com/vitessio/vitess/pull/11704) we are adding support for `set [session] transaction isolation level <transaction_characteristic>`

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

#### <a id="transaction-access-mode"/>Transaction Access Mode

In [Pull Request #11704](https://github.com/vitessio/vitess/pull/11704) we are adding support for `start transaction` with transaction characteristic.

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

#### <a id="support-views"/>Support For Views

Views sharded support is released as an experimental feature in `v16.0.0`.
Views are not enabled by default in your Vitess cluster, but they can be turned on using the `--enable-views` flag on VTGate, and `--queryserver-enable-views` flag on VTTablet.

To read more on how views are implemented you can read the [Views Support RFC](https://github.com/vitessio/vitess/issues/11559).
And if you want to learn more on how to use views and its current limitations, you can read the [Views Documentation](https://vitess.io/docs/16.0/reference/compatibility/mysql-compatibility/#views).

### <a id="vttestserver"/>VTTestServer

#### <a id="perf-improvement"/>Performance Improvement

Creating a database with vttestserver was taking ~45 seconds. This can be problematic in test environments where testcases do a lot of `create` and `drop` database.
In an effort to minimize the database creation time, in [Pull Request #11918](https://github.com/vitessio/vitess/pull/11918) we have changed the value of `tablet_refresh_interval` to 10s while instantiating vtcombo during vttestserver initialization. We have also made this configurable so that it can be reduced further if desired.
For any production cluster the default value of this flag is still [1 minute](https://vitess.io/docs/16.0/reference/programs/vtgate/). Reducing this value might put more stress on Topo Server (since we now read from Topo server more often) but for testing purposes
this shouldn't be a concern.

## <a id="minor-changes"/>Minor changes

### <a id="backup-comp-benchmarks"/>Backup Compression Benchmarks

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

## <a id="refactor"/>Refactor

### <a id="vttablet-sidecar-schema"/>VTTablet Sidecar Schema Maintenance Refactor

This is an internal refactor and should not change the behavior of Vitess as seen by users. 

Developers will see a difference though: v16 changes the way we maintain vttablet's sidecar database schema (also referred to as the `_vt`
database). Instead of using the `WithDDL` package, introduced in [PR #6348](https://github.com/vitessio/vitess/pull/6348), we use a
declarative approach. Developers will now have to update the desired schema in the `go/vt/sidecardb/schema` directory.

The desired schema is specified, one per table. A new module `sidecardb`, compares this to the existing schema and
performs the required `create` or `alter` to reach it. This is done whenever a primary vttablet starts up.

The sidecar tables `local_metadata` and `shard_metadata` are no longer in use and all references to them are removed as
part of this refactor. They were used previously for Orchestrator support, which has been superseded by `vtorc`.
