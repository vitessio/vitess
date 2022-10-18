# Release of Vitess v15.0.0-rc1
## Summary

- [Vindex Interface](#vindex-interface)
- [LogStats Table and Keyspace deprecated](#logstats-table-and-keyspace-deprecated)
- [Command-line syntax deprecations](#command-line-syntax-deprecations)
- [New command line flags and behavior](#new-command-line-flags-and-behavior)
- [Online DDL changes](#online-ddl-changes)
- [Tablet throttler](#tablet-throttler)
- [VDiff2](#vdiff2)
- [Mysql Compatibility](#mysql-compatibility)
- [Durability Policy](#durability-policy)
- [New EXPLAIN format](#new-explain-format)

## Known Issues

## Major Changes

### Breaking Changes

#### Flags

- The deprecated `--cpu_profile` flag has been removed. Please use the `--pprof` flag instead.
- The deprecated `--mem-profile-rate` flag has been removed. Please use `--pprof=mem` instead.
- The deprecated `--mutex-profile-fraction` flag has been removed. Please use `--pprof=mutex` instead.
- The deprecated vtgate/vtexplain/vtcombo flag `--planner_version` has been removed. Please use `--planner-version` instead.
- The deprecated flag `--master_connect_retry` has been removed. Please use `--replication_connect_retry` instead.
- `vtctl` commands that take shard names and ranges as positional arguments (e.g. `vtctl Reshard ks.workflow -80 -40,40-80`) need to have their positional arguments separated from their flag arguments by a double-dash separator to avoid the new parsing library from mistaking them as flags (e.g. `vtctl Reshard ks.workflow -- -80 -40,40-80`).

#### Vindex Interface

All the vindex interface methods are changed by adding `context.Context` as an input parameter.

E.g:
```go
Map(vcursor VCursor, .... ) .... 
	To
Map(ctx context.Context, vcursor VCursor, .... ) ....
```

This only affects users who have added their own custom vindex implementation. 
They are required to change their implementation with these new interface method expectations.

#### LogStats Table and Keyspace deprecated

Information about which tables are used was being reported through the `Keyspace` and `Table` fields on LogStats.
For multi-table queries, this output can be confusing, so we have added `TablesUsed`, that is a string array, listing all tables and which keyspace they are on.
`Keyspace` and `Table` fields are deprecated and will be removed in the v16 release of Vitess.

#### Orchestrator Integration Deprecation

Orchestrator integration in `vttablet` has been deprecated. It will continue to work in this release but is liable to be removed in future releases.
Consider using VTOrc instead of Orchestrator as VTOrc goes GA in this release.

#### Connection Pool Prefill

The connection pool with prefilled connections have been removed. The pool now does lazy connection creation.
Following flags are deprecated: `queryserver-config-pool-prefill-parallelism`, `queryserver-config-stream-pool-prefill-parallelism`, `queryserver-config-transaction-prefill-parallelism`
and will be removed in future version.

### Command-line syntax deprecations

#### vttablet startup flag deletions
The following VTTablet flags were deprecated in 7.0. They have now been deleted
- --queryserver-config-message-conn-pool-size
- --queryserver-config-message-conn-pool-prefill-parallelism
- --client-found-rows-pool-size --queryserver-config-transaction-cap will be used instead
- --transaction_shutdown_grace_period Use --shutdown_grace_period instead
- --queryserver-config-max-dml-rows
- --queryserver-config-allowunsafe-dmls
- --pool-name-prefix
- --enable-autocommit Autocommit is always allowed

#### vttablet startup flag deprecations
- --enable-query-plan-field-caching is now deprecated. It will be removed in v16.
- --enable_semi_sync is now deprecated. It will be removed in v16. Instead, set the correct durability policy using `SetKeyspaceDurabilityPolicy`
- --queryserver-config-pool-prefill-parallelism, --queryserver-config-stream-pool-prefill-parallelism and --queryserver-config-transaction-prefill-parallelism have all been deprecated. They will be removed in v16.

#### vtbackup flag deprecations
- --backup_storage_hook has been deprecated, consider using one of the builtin compression algorithms or --external-compressor and --external-decompressor instead.

### New command line flags and behavior

#### vtgate --mysql-server-pool-conn-read-buffers

`--mysql-server-pool-conn-read-buffers` enables pooling of buffers used to read from incoming
connections, similar to the way pooling happens for write buffers. Defaults to off.

### VDiff2

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

Please see the VDiff2 [documentation](https://vitess.io/docs/15.0/reference/vreplication/vdiff2/) for additional information.

### New command line flags and behavior

#### vtctl GetSchema --table-schema-only

The new flag `--table-schema-only` skips column introspection. `GetSchema` only returns general schema analysis, and specifically it includes the `CREATE TABLE|VIEW` statement in the `schema` field.

#### Support for additional compressors and decompressors during backup & restore
Backup/Restore now allow you many more options for compression and decompression instead of relying on the default compressor(`pgzip`).
There are some built-in compressors which you can use out-of-the-box. Users will need to evaluate which option works best for their
use-case. Here are the flags that control this feature

- --compression-engine-name
- --external-compressor
- --external-decompressor
- --external-compressor-extension
- --compression-level

`--compression-engine-name` specifies the engine used for compression. It can have one of the following values

- pargzip (Default)
- pgzip
- lz4
- zstd
- external

where 'external' is set only when using a custom command or tool other than the ones that are already provided. 
If you want to use any of the built-in compressors, simply set one of the above values for `--compression-engine-name`. The value
specified in `--compression-engine-name` is saved in the backup MANIFEST, which is later read by the restore process to decide which
engine to use for decompression. Default value for engine is 'pgzip'.

If you would like to use a custom command or external tool for compression/decompression then you need to provide the full command with
arguments to the `--external-compressor` and `--external-decompressor` flags. `--external-compressor-extension` flag also needs to be provided
so that compressed files are created with the correct extension. If the external command is not using any of the built-in compression engines
(i-e pgzip, pargzip, lz4 or zstd) then you need to set `--compression-engine-name` to value 'external'.

Please note that if you want the current production behavior then you don't need to change any of these flags.
You can read more about backup & restore [here] (https://vitess.io/docs/15.0/user-guides/operating-vitess/backup-and-restore/).

If you decided to switch from an external compressor to one of the built-in supported compressors (i-e pgzip, pargzip, lz4 or zstd) at any point
in the future, you will need to do it in two steps.

- step #1, set `--external-compressor` and `--external-compressor-extension` flag values to empty and change `--compression-engine-name` to desired value.
- Step #2, after at least one cycle of backup with new configuration, you can set `--external-decompressor` flag value to empty.

The reason you cannot change all the values together is because the restore process will then have no way to find out which external decompressor
should be used to process the previous backup. Please make sure you have thought out all possible scenarios for restore before transitioning from one
compression engine to another.

#### Independent OLAP and OLTP transactional timeouts

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

#### Support for specifying group information in calls to VTGate

`--grpc-use-effective-groups` allows non-SSL callers to specify groups information for a caller.
Until now, you could only specify the caller-id for the security context used to authorize queries.
As of now, you can specify the principal of the caller, and any groups they belong to.

### Online DDL changes

#### Concurrent vitess migrations

All Online DDL migrations using the `vitess` strategy are now eligible to run concurrently, given `--allow-concurrent` DDL strategy flag. Until now, only `CREATE`, `DROP` and `REVERT` migrations were eligible, and now `ALTER` migrations are supported, as well. The terms for `ALTER` migrations concurrency:

- DDL strategy must be `vitess --allow-concurent ...`
- No two migrations can run concurrently on the same table
- No two `ALTER`s will copy table data concurrently
- A concurrent `ALTER` migration will not start if another `ALTER` is running and is not `ready_to_complete`

The main use case is to run multiple concurrent migrations, all with `--postpone-completion`. All table-copy operations will run sequentially, but no migration will actually cut-over, and eventually all migrations will be `ready_to_complete`, continuously tailing the binary logs and keeping up-to-date. A quick and iterative `ALTER VITESS_MIGRATION '...' COMPLETE` sequence of commands will cut-over all migrations _closely together_ (though not atomically together).

#### vtctl command changes. 
All `online DDL show` commands can now be run with a few additional parameters
- `--order` , order migrations in the output by either ascending or descending order of their `id` fields.
- `--skip`  , skip specified number of migrations in the output.
- `--limit` , limit results to a specified number of migrations in the output.

#### New syntax

The following is now supported:

```sql
ALTER VITESS_MIGRATION COMPLETE ALL
```

This works on all pending migrations (`queued`, `ready`, `running`) and internally issues a `ALTER VITESS_MIGRATION '<uuid>' COMPLETE` for each one. The command is useful for completing multiple concurrent migrations (see above) that are open-ended (`--postpone-completion`).

### Tablet throttler

#### API changes

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

### Mysql Compatibility

#### System Settings
Vitess supported system settings from release 7.0 onwards, but it was always with a pinch of salt.
As soon as a client session changes a default system setting, the mysql connection gets blocked for it.
This leads to clients running out of mysql connections. 
The clients were instructed to use this to a minimum and try to set those changed system settings as default on the mysql.

With this release, Vitess can handle system settings changes in a much better way and the clients can use it more freely.
Vitess now pools those changed settings and does not reserve it for any particular session. 

This feature can be enabled by setting `queryserver-enable-settings-pool` flag on the vttablet. It is disabled by default.
In future releases, we will make this flag enabled by default.

#### Lookup Vindexes

Lookup vindexes now support a new parameter `multi_shard_autocommit`. If this is set to `true`, lookup vindex dml queries will be sent as autocommit to all shards instead of being wrapped in a transaction.
This is different from the existing `autocommit` parameter where the query is sent in its own transaction separate from the ongoing transaction if any i.e. begin -> lookup query execs -> commit/rollback

### Durability Policy

#### Cross Cell

A new durability policy `cross_cell` is now supported. `cross_cell` durability policy only allows replica tablets from a different cell than the current primary to
send semi-sync ACKs. This ensures that any committed write exists in at least 2 tablets belonging to different cells.

### New EXPLAIN format

#### FORMAT=vtexplain

With this new `explain` format, you can get an output that is very similar to the command line `vtexplain` app, but from a running `vtgate`, through a MySQL query.

### VTOrc

#### Old UI Removal and Replacement

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

#### Configuration Refactor and New Flags 

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

#### Example Upgrade

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

#### Default Configuration Files

The default files that VTOrc searches for configurations in have also changed from `"/etc/orchestrator.conf.json", "conf/orchestrator.conf.json", "orchestrator.conf.json"` to
`"/etc/vtorc.conf.json", "conf/vtorc.conf.json", "vtorc.conf.json"`.

------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/doc/releasenotes/15_0_0_changelog.md).

The release includes 551 commits (excluding merges)

Thanks to all our contributors: @Abirdcfly, @DeathBorn, @GuptaManan100, @K-Kumar-01, @L3o-pold, @Phanatic, @Weijun-H, @ajm188, @arthurschreiber, @arvind-murty, @brirams, @dbussink, @deepthi, @dependabot[bot], @doeg, @frouioui, @harshit-gangal, @mattlord, @maxenglander, @mgale, @notfelineit, @ofiriluz, @olyazavr, @quinox, @rafer, @renatolabs, @rohit-nayak-ps, @rsajwani, @rvrangel, @saunderst, @shlomi-noach, @systay, @vitess-bot[bot], @vmg, @yoheimuta

