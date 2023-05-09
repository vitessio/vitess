## Summary

- **[Known Issues](#known-issues)**
- **[Breaking Changes](#breaking-changes)**
  - [Flags](#flags)
  - [VTTablet Flag Deletions](#vttablet-flag-deletions)
  - [Vindex Interface](#vindex-interface)
- **[Deprecations](#deprecations)**
  - [LogStats Table and Keyspace Deprecated](#logstats-table-and-keyspace-deprecated)
  - [Orchestrator Integration Deprecation](#orchestrator-integration-deprecation)
  - [Connection Pool Prefill](#connection-pool-prefill)
  - [InitShardPrimary Deprecation](#initshardprimary-deprecation)
- **[Command-Line Syntax Deprecations](#command-line-syntax-deprecations)**
  - [VTTablet Startup Flag Deletions](#vttablet-startup-flag-deletions)
  - [VTTablet Startup Flag Deprecations](#vttablet-startup-flag-deprecations)
  - [VTBackup Flag Deprecations](#vtbackup-flag-deprecations)
- **[VTGate](#vtgate)**
  - [vtgate --mysql-server-pool-conn-read-buffers](#vtgate--mysql-server-pool-conn-read-buffers)
- **[VDiff2](#vdiff2)**
  - [Resume Workflow](#resume-workflow)
- **[New command line flags and behavior](#new-command-line)**
  - [vtctl GetSchema --table-schema-only](#vtctl-getschema--table-schema-only)
  - [Support for Additional Compressors and Decompressors During Backup & Restore](#support-for-additional-compressors-and-decompressors-during-backup-&-restore)
  - [Independent OLAP and OLTP Transactional Timeouts](#independant-olap-and-oltp-transactional-timeouts)
  - [Support for Specifying Group Information in Calls to VTGate](#support-for-specifying-group-information-in-calls-to-vtgate)
- **[Online DDL Changes](#online-ddl-changes)**
  - [Concurrent Vitess Migrations](#concurrent-vitess-migrations)
  - [VTCtl Command Changes](#vtctl-command-changes)
  - [New Syntax](#new-syntax)
- **[Tablet Throttler](#tablet-throttler)**
  - [API Changes](#api-changes)
- **[Mysql Compatibility](#mysql-compatibility)**
  - [System Settings](#system-settings)
  - [Lookup Vindexes](#lookup-vindexes)
- **[Durability Policy](#durability-policy)**
  - [Cross Cell](#cross-cell)
- **[New EXPLAIN Format](#new-explain-format)**
  - [FORMAT=vtexplain](#formatvtexplain)
- **[VTOrc](#vtorc)**
  - [Old UI Removal and Replacement](#old-ui-removal-and-replacement)
  - [Configuration Refactor and New Flags](#configuratoin-refactor-and-new-flags)
  - [Example Upgrade](#example-upgrade)
  - [Default Configuration Files](#default-configuration-files)
- **[Flags Restructure](#flags-restructure)**
  - [Flags Diff](#flags-diff)

## <a id="known-issues"/>Known Issues

- [Corrupted results for non-full-group-by queries with JOINs](https://github.com/vitessio/vitess/issues/11625). This can be resolved by using full-group-by queries.

## Major Changes

### <a id="breaking-changes"/>Breaking Changes

#### <a id="flags"/>Flags

- The deprecated `--cpu_profile` flag has been removed. Please use the `--pprof` flag instead.
- The deprecated `--mem-profile-rate` flag has been removed. Please use `--pprof=mem` instead.
- The deprecated `--mutex-profile-fraction` flag has been removed. Please use `--pprof=mutex` instead.
- The deprecated vtgate/vtexplain/vtcombo flag `--planner_version` has been removed. Please use `--planner-version` instead.
- The deprecated flag `--master_connect_retry` has been removed. Please use `--replication_connect_retry` instead.
- `vtctl` commands that take shard names and ranges as positional arguments (e.g. `vtctl Reshard ks.workflow -80 -40,40-80`) need to have their positional arguments separated from their flag arguments by a double-dash separator to avoid the new parsing library from mistaking them as flags (e.g. `vtctl Reshard ks.workflow -- -80 -40,40-80`).
- The `--cell` flag in the `vtgate` binary no longer has a default value. It is a required argument that has to be specified for the binary to run. Please explicitly specify the flag, if dependent on the flag's default value.
- The `--db-config-*-*` VTTablet flags were deprecated in `v3.0.0`. They have now been deleted as part of this release. You must use `--db_dba_*` now.

#### <a id="vttablet-flag-deletions"/>vttablet Flag Deletions
The following VTTablet flags were deprecated in 7.0. They have now been deleted
- `--queryserver-config-message-conn-pool-size`
- `--queryserver-config-message-conn-pool-prefill-parallelism`
- `--client-found-rows-pool-size` A different existing flag `--queryserver-config-transaction-cap` will be used instead
- `--transaction_shutdown_grace_period` Use `--shutdown_grace_period` instead
- `--queryserver-config-max-dml-rows`
- `--queryserver-config-allowunsafe-dmls`
- `--pool-name-prefix`
- `--enable-autocommit` Autocommit is always allowed

#### <a id="vindex-interface"/>Vindex Interface

All the vindex interface methods are changed by adding `context.Context` as an input parameter.

E.g:
```go
Map(vcursor VCursor, .... ) .... 
	To
Map(ctx context.Context, vcursor VCursor, .... ) ....
```

This only affects users who have added their own custom vindex implementation. 
They are required to change their implementation with these new interface method expectations.

### <a id="deprecations"/>Deprecations

#### <a id="logstats-table-and-keyspace-deprecated"/>LogStats Table and Keyspace deprecated

Information about which tables are used was being reported through the `Keyspace` and `Table` fields on LogStats.
For multi-table queries, this output can be confusing, so we have added `TablesUsed`, that is a string array, listing all tables and which keyspace they are on.
`Keyspace` and `Table` fields are deprecated and will be removed in the v16 release of Vitess.

#### <a id="orchestrator-integration-deprecation"/>Orchestrator Integration Deprecation

Orchestrator integration in `vttablet` has been deprecated. It will continue to work in this release but is liable to be removed in future releases.
Consider using VTOrc instead of Orchestrator as VTOrc goes GA in this release.

#### <a id="connection-pool-prefill"/>Connection Pool Prefill

The connection pool with prefilled connections have been removed. The pool now does lazy connection creation.

#### <a id="initshardprimary-deprecation"/>InitShardPrimary Deprecation

The vtcltd command InitShardPrimary has been deprecated. Please use PlannedReparentShard instead.

### <a id="command-line-syntax-deprecations"/>Command-line syntax deprecations

#### <a id="vttablet-startup-flag-deletions"/>vttablet startup flag deletions
The following VTTablet flags were deprecated in 7.0. They have now been deleted
- --queryserver-config-message-conn-pool-size
- --queryserver-config-message-conn-pool-prefill-parallelism
- --client-found-rows-pool-size --queryserver-config-transaction-cap will be used instead
- --transaction_shutdown_grace_period Use --shutdown_grace_period instead
- --queryserver-config-max-dml-rows
- --queryserver-config-allowunsafe-dmls
- --pool-name-prefix
- --enable-autocommit Autocommit is always allowed

#### <a id="vttablet-startup-flag-deprecations"/>vttablet startup flag deprecations
- `--enable-query-plan-field-caching` has been deprecated. It will be removed in v16.
- `--enable_semi_sync` has been deprecated. It will be removed in v16. Instead, set the correct durability policy using `SetKeyspaceDurabilityPolicy`
- `--queryserver-config-pool-prefill-parallelism`, `--queryserver-config-stream-pool-prefill-parallelism` and `--queryserver-config-transaction-prefill-parallelism` have all been deprecated. They will be removed in v16.
- `--backup_storage_hook` has been deprecated, consider using one of the builtin compression algorithms or `--external-compressor` and `--external-decompressor` instead.

#### <a id="vtbackup-flag-deprecations"/>vtbackup flag deprecations
- `--backup_storage_hook` has been deprecated, consider using one of the builtin compression algorithms or `--external-compressor` and `--external-decompressor` instead.

### <a id="vtgate"/>VTGate

#### <a id="vtgate--mysql-server-pool-conn-read-buffers"/>vtgate --mysql-server-pool-conn-read-buffers

`--mysql-server-pool-conn-read-buffers` enables pooling of buffers used to read from incoming
connections, similar to the way pooling happens for write buffers. Defaults to off.

### <a id="vdiff2"/>VDiff v2

#### <a id="resume-workflow"/>Resume Workflow

We introduced the ability to resume a VDiff2 workflow:
```
$ vtctlclient --server=localhost:15999 VDiff --v2 customer.commerce2customer resume 4c664dc2-eba9-11ec-9ef7-920702940ee0
VDiff 4c664dc2-eba9-11ec-9ef7-920702940ee0 resumed on target shards, use show to view progress

$ vtctlclient --server=localhost:15999 VDiff --v2 customer.commerce2customer show last

VDiff Summary for customer.commerce2customer (4c664dc2-eba9-11ec-9ef7-920702940ee0)
State:        completed
RowsCompared: 196
HasMismatch:  false
StartedAt:    2022-06-26 22:44:29
CompletedAt:  2022-06-26 22:44:31

Use "--format=json" for more detailed output.

$ vtctlclient --server=localhost:15999 VDiff --v2 --format=json customer.commerce2customer show last
{
	"Workflow": "commerce2customer",
	"Keyspace": "customer",
	"State": "completed",
	"UUID": "4c664dc2-eba9-11ec-9ef7-920702940ee0",
	"RowsCompared": 196,
	"HasMismatch": false,
	"Shards": "0",
	"StartedAt": "2022-06-26 22:44:29",
	"CompletedAt": "2022-06-26 22:44:31"
}
```

We leverage this resume capability to automatically restart a VDiff2 workflow that encountered a retryable error.

We also made a number of other enhancements like progress reporting and features needed to make it a full replacement for VDiff v1. You can see more details in the tracking ticket for the VDiff2 feature complete target: https://github.com/vitessio/vitess/issues/10494

Now that VDiff v2 is feature complete in 15.0, we hope to make it GA in 16.0.

Please see the VDiff2 [documentation](https://vitess.io/docs/15.0/reference/vreplication/vdiff2/) for additional information.

### <a id="new-command-line"/>New command line flags and behavior

#### <a id="vtctl-getschema--table-schema-only"/>vtctl GetSchema --table-schema-only

The new flag `--table-schema-only` skips column introspection. `GetSchema` only returns general schema analysis, and specifically it includes the `CREATE TABLE|VIEW` statement in the `schema` field.

#### <a id="support-for-additional-compressors-and-decompressors-during-backup-&-restore"/>Support for additional compressors and decompressors during backup & restore
Backup/Restore now allow you many more options for compression and decompression instead of relying on the default compressor(`pargzip`).
There are some built-in compressors which you can use out-of-the-box. Users will need to evaluate which option works best for their
use-case. Here are the flags that control this feature

- `--compression-engine-name`
- `--external-compressor`
- `--external-decompressor`
- `--external-compressor-extension`
- `--compression-level`

`--compression-engine-name` specifies the engine used for compression. It can have one of the following values

- pargzip (Default)
- pgzip
- lz4
- zstd
- external

If you want to use any of the built-in compressors, simply set one of the above values other than `external` for `--compression-engine-name`. The value
specified in `--compression-engine-name` is saved in the backup MANIFEST, which is later read by the restore process to decide which
engine to use for decompression. Default value for engine is 'pargzip'.

If you would like to use a custom command or external tool for compression/decompression then you need to provide the full command with
arguments to the `--external-compressor` and `--external-decompressor` flags. `--external-compressor-extension` flag also needs to be provided
so that compressed files are created with the correct extension. If the external command is not using any of the built-in compression engines
(i.e. pgzip, pargzip, lz4 or zstd) then you need to set `--compression-engine-name` to value 'external'.

Please note that if you want to keep the current behavior then you don't need to provide any of these flags.
You can read more about backup & restore [here] (https://vitess.io/docs/15.0/user-guides/operating-vitess/backup-and-restore/).

If you decided to switch from an external compressor to one of the built-in supported compressors (i.e. pgzip, pargzip, lz4 or zstd) at any point
in the future, you will need to do it in two steps.

- step #1, set `--external-compressor` and `--external-compressor-extension` flag values to empty and change `--compression-engine-name` to desired value.
- Step #2, after at least one cycle of backup with new configuration, you can set `--external-decompressor` flag value to empty.

The reason you cannot change all the values together is because the restore process will then have no way to find out which external decompressor
should be used to process the previous backup. Please make sure you have thought out all possible scenarios for restore before transitioning from one
compression engine to another.

#### <a id="independant-olap-and-oltp-transactional-timeouts"/>Independent OLAP and OLTP transactional timeouts

`--queryserver-config-olap-transaction-timeout` specifies the timeout applied
to a transaction created within an OLAP workload. The default value is `30`
seconds, but this can be raised, lowered, or set to zero to disable the timeout
altogether.

Until now, while OLAP queries would bypass the query timeout, transactions
created within an OLAP session would be rolled back
`--queryserver-config-transaction-timeout` seconds after the transaction was
started.

As of now, OLTP and OLAP transaction timeouts can be configured independently of each
other.

The main use case is to run queries spanning a long period of time which
require transactional guarantees such as consistency or atomicity.

#### <a id="support-for-specifying-group-information-in-calls-to-vtgate"/>Support for specifying group information in calls to VTGate

`--grpc-use-effective-groups` allows non-SSL callers to specify groups information for a caller.
Until now, you could only specify the caller-id for the security context used to authorize queries.
As of now, you can specify the principal of the caller, and any groups they belong to.

### <a id="online-ddl-changes"/>Online DDL changes

#### <a id="concurrent-vitess-migrations"/>Concurrent vitess migrations

All Online DDL migrations using the `vitess` strategy are now eligible to run concurrently, given `--allow-concurrent` DDL strategy flag. Until now, only `CREATE`, `DROP` and `REVERT` migrations were eligible, and now `ALTER` migrations are supported, as well. The terms for `ALTER` migrations concurrency:

- DDL strategy must be `vitess --allow-concurent ...`
- No two migrations can run concurrently on the same table
- No two `ALTER`s will copy table data concurrently
- A concurrent `ALTER` migration will not start if another `ALTER` is running and is not `ready_to_complete`

The main use case is to run multiple concurrent migrations, all with `--postpone-completion`. All table-copy operations will run sequentially, but no migration will actually cut-over, and eventually all migrations will be `ready_to_complete`, continuously tailing the binary logs and keeping up-to-date. A quick and iterative `ALTER VITESS_MIGRATION '...' COMPLETE` sequence of commands will cut-over all migrations _closely together_ (though not atomically together).

#### <a id="vtctl-command-changes"/>vtctl command changes. 
All `online DDL show` commands can now be run with a few additional parameters
- `--order` , order migrations in the output by either ascending or descending order of their `id` fields.
- `--skip`  , skip specified number of migrations in the output.
- `--limit` , limit results to a specified number of migrations in the output.

#### <a id="new-syntax"/>New syntax

The following is now supported:

```sql
ALTER VITESS_MIGRATION COMPLETE ALL
```

This works on all pending migrations (`queued`, `ready`, `running`) and internally issues a `ALTER VITESS_MIGRATION '<uuid>' COMPLETE` for each one. The command is useful for completing multiple concurrent migrations (see above) that are open-ended (`--postpone-completion`).

### <a id="tablet-throttler"/>Tablet Throttler

#### <a id="api-changes"/>API changes

API endpoint `/debug/vars` now exposes throttler metrics, such as number of hits and errors per app per check type. Example:

```shell
$ curl -s http://127.0.0.1:15100/debug/vars | jq . | grep Throttler
  "ThrottlerAggregatedMysqlSelf": 0.191718,
  "ThrottlerAggregatedMysqlShard": 0.960054,
  "ThrottlerCheckAnyError": 27,
  "ThrottlerCheckAnyMysqlSelfError": 13,
  "ThrottlerCheckAnyMysqlSelfTotal": 38,
  "ThrottlerCheckAnyMysqlShardError": 14,
  "ThrottlerCheckAnyMysqlShardTotal": 42,
  "ThrottlerCheckAnyTotal": 80,
  "ThrottlerCheckMysqlSelfSecondsSinceHealthy": 0,
  "ThrottlerCheckMysqlShardSecondsSinceHealthy": 0,
  "ThrottlerProbesLatency": 355523,
  "ThrottlerProbesTotal": 74,
```

### <a id="mysql-compatibility"/>Mysql Compatibility

#### <a id="system-settings"/>System Settings
Vitess has had support for system settings from release 7.0 onwards, but this support came with some caveats.
As soon as a client session changes a default system setting, a mysql connection gets reserved for it.
This can sometimes lead to clients running out of mysql connections. 
Users were instructed to minimize the use of this feature and to try to set the desired system settings as defaults in the mysql config.

With this release, Vitess can handle system settings changes in a much better way and clients can use them more freely.
Vitess now has the ability to pool changed settings without reserving connections for any particular session. 

This feature can be enabled by setting `queryserver-enable-settings-pool` flag on the vttablet. It is disabled by default.
In future releases, we will make this flag enabled by default.

#### <a id="lookup-vindexes"/>Lookup Vindexes

Lookup vindexes now support a new parameter `multi_shard_autocommit`. If this is set to `true`, lookup vindex dml queries will be sent as autocommit to all shards instead of being wrapped in a transaction.
This is different from the existing `autocommit` parameter where the query is sent in its own transaction separate from the ongoing transaction if any i.e. begin -> lookup query execs -> commit/rollback

### <a id="durability-policy"/>Durability Policy

#### <a id="cross-cell"/>Cross Cell

A new durability policy `cross_cell` is now supported. `cross_cell` durability policy only allows replica tablets from a different cell than the current primary to
send semi-sync ACKs. This ensures that any committed write exists in at least 2 tablets belonging to different cells.

### <a id="new-explain-format"/>New EXPLAIN format

#### <a id="format=vtexplain"/>FORMAT=vtexplain

With this new `explain` format, you can get an output that is very similar to the command line `vtexplain` app, but from a running `vtgate`, through a MySQL query.

### <a id="vtorc"/>VTOrc

#### <a id="old-ui-removal-and-replacement"/>Old UI Removal and Replacement

The old UI that VTOrc inherited from `Orchestrator` has been removed. A replacement UI, more consistent with the other Vitess binaries has been created.
In order to use the new UI, `--port` flag has to be provided. 

Along with the UI, the old APIs have also been deprecated. However, some of them have been ported over to the new UI - 

| Old API                          | New API                          | Additional notes                                                      |
|----------------------------------|----------------------------------|-----------------------------------------------------------------------|
| `/api/problems`                  | `/api/problems`                  | The new API also supports filtering using the keyspace and shard name |
| `/api/disable-global-recoveries` | `/api/disable-global-recoveries` | Functionally remains the same                                         |
| `/api/enable-global-recoveries`  | `/api/enable-global-recoveries`  | Functionally remains the same                                         |
| `/api/health`                    | `/debug/health`                  | Functionally remains the same                                         |
| `/api/replication-analysis`      | `/api/replication-analysis`      | Functionally remains the same. Output is now JSON format.             |

Apart from these APIs, we also now have `/debug/status`, `/debug/vars` and `/debug/liveness` available in the new UI.

#### <a id="configuratoin-refactor-and-new-flags"/>Configuration Refactor and New Flags 

Since VTOrc was forked from `Orchestrator`, it inherited a lot of configurations that don't make sense for the Vitess use-case.
All of such configurations have been removed.

VTOrc ignores the configurations that it doesn't understand. So old configurations can be kept around on upgrading and won't cause any issues.
They will just be ignored.

For all the configurations that are kept, flags have been added for them and the flags are the desired way to pass these configurations going forward.
The config file will be deprecated and removed in upcoming releases. The following is a list of all the configurations that are kept and the associated flags added.

|          Configurations Kept          |           Flags Introduced            |
|:-------------------------------------:|:-------------------------------------:|
|            SQLite3DataFile            |         `--sqlite-data-file`          |
|          InstancePollSeconds          |        `--instance-poll-time`         |
|    SnapshotTopologiesIntervalHours    |    `--snapshot-topology-interval`     |
|    ReasonableReplicationLagSeconds    |    `--reasonable-replication-lag`     |
|             AuditLogFile              |        `--audit-file-location`        |
|             AuditToSyslog             |         `--audit-to-backend`          |
|           AuditToBackendDB            |          `--audit-to-syslog`          |
|            AuditPurgeDays             |       `--audit-purge-duration`        |
|      RecoveryPeriodBlockSeconds       |  `--recovery-period-block-duration`   |
| PreventCrossDataCenterPrimaryFailover |    `--prevent-cross-cell-failover`    |
|        LockShardTimeoutSeconds        |        `--lock-shard-timeout`         |
|      WaitReplicasTimeoutSeconds       |       `--wait-replicas-timeout`       |
|     TopoInformationRefreshSeconds     | `--topo-information-refresh-duration` |
|          RecoveryPollSeconds          |      `--recovery-poll-duration`       |

Apart from configurations, some flags from VTOrc have also been removed -
- `sibling`
- `destination`
- `discovery`
- `skip-unresolve`
- `skip-unresolve-check`
- `noop`
- `binlog`
- `statement`
- `grab-election`
- `promotion-rule`
- `skip-continuous-registration`
- `enable-database-update`
- `ignore-raft-setup`
- `tag`

The ideal way to ensure backward compatibility is to remove the flags listed above while on the previous release. Then upgrade VTOrc.
After upgrading, remove the config file and instead pass the flags that are introduced.

#### <a id="example-upgrade"/>Example Upgrade

If you are running VTOrc with the flags `--ignore-raft-setup --clusters_to_watch="ks/0" --config="path/to/config"` and the following configuration
```json
{
  "Debug": true,
  "ListenAddress": ":6922",
  "MySQLTopologyUser": "orc_client_user",
  "MySQLTopologyPassword": "orc_client_user_password",
  "MySQLReplicaUser": "vt_repl",
  "MySQLReplicaPassword": "",
  "RecoveryPeriodBlockSeconds": 1,
  "InstancePollSeconds": 1,
  "PreventCrossDataCenterPrimaryFailover": true
}
```
First drop the flag `--ignore-raft-setup` while on the previous release. So, you'll be running VTOrc with `--clusters_to_watch="ks/0" --config="path/to/config"` and the same configuration listed above.

Now you can upgrade your VTOrc version continuing to use the same flags and configurations, and it will continue to work just the same. If you wish to use the new UI, then you can add the `--port` flag as well.

After upgrading, you can drop the configuration entirely and use the new flags like `--clusters_to_watch="ks/0" --recovery-period-block-duration=1s --instance-poll-time=1s --prevent-cross-cell-failover`

#### <a id="default-configuration-files"/>Default Configuration Files

The default files that VTOrc searches for configurations in have also changed from `"/etc/orchestrator.conf.json", "conf/orchestrator.conf.json", "orchestrator.conf.json"` to
`"/etc/vtorc.conf.json", "conf/vtorc.conf.json", "vtorc.conf.json"`.

### <a id="flags-restructure"/>Flags Restructure

#### <a id="flags-diff"/>Flags Diff

In addition to these major streams of work in release-15.0, we have made tremendous progress on [VEP-4, aka The Flag Situation](https://github.com/vitessio/enhancements/blob/main/veps/vep-4.md), reorganizing our code so that Vitess binaries and their flags are
clearly aligned in help text. An immediate win for usability, this positions us well to move on to a [viper](https://github.com/spf13/viper) implementation which will facilitate additional improvements including standardization of flag syntax and runtime configuration reloads.
We are also aligning with industry standards regarding the use of flags, ensuring a seamless experience for users migrating from or integrating with other platforms.
Below are the changes for each binary.
- [mysqlctl](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/mysqlctl.diff)
- [mysqlctld](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/mysqlctld.diff)
- [vtaclcheck](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtaclcheck.diff)
- [vtadmin](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtadmin.diff)
- [vtctlclient](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtctlclient.diff)
- [vtctld](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtctld.diff)
- [vtctldclient](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtctldclient.diff)
- [vtexplain](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtexplain.diff)
- [vtgate](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtgate.diff)
- [vtgtr](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtgtr.diff)
- [vtorc](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vtorc.diff)
- [vttablet](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vttablet.diff)
- [vttestserver](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vttestserver.diff)
- [vttlstest](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/vttlstest.diff)
- [zk](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/zk.diff)
- [zkctl](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/zkctl.diff)
- [zkctld](https://github.com/vitessio/vitess/blob/release-15.0/doc/flags/14.0-to-15.0-transition/zkctld.diff)
