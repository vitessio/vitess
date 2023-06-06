# Release of Vitess v17.0.0-rc1
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
    - [Default Local Cell Preference for TabletPicker](#tablet-picker-cell-preference)
    - [Dedicated stats for VTGate Prepare operations](#dedicated-vtgate-prepare-stats)
    - [VTAdmin web migrated from create-react-app to vite](#migrated-vtadmin)
    - [Keyspace name validation in TopoServer](#keyspace-name-validation)
    - [Shard name validation in TopoServer](#shard-name-validation)
    - [Compression CLI flags removed from vtctld and vtctldclient binaries](#remove-compression-flags-from-vtctld-binaries)
    - [VtctldClient command RestoreFromBackup will now use the correct context](#VtctldClient-RestoreFromBackup)
  - **[New command line flags and behavior](#new-flag)**
    - [Builtin backup: read buffering flags](#builtin-backup-read-buffering-flags)
    - [Manifest backup external decompressor command](#manifest-backup-external-decompressor-command)
    - [Throttler config via topo enabled by default](#throttler-config-via-topo)
  - **[New stats](#new-stats)**
    - [Detailed backup and restore stats](#detailed-backup-and-restore-stats)
    - [VTtablet Error count with code](#vttablet-error-count-with-code)
    - [VReplication stream status for Prometheus](#vreplication-stream-status-for-prometheus)
  - **[Online DDL](#online-ddl)**
    - [--cut-over-threshold DDL strategy flag](#online-ddl-cut-over-threshold-flag)
  - **[VReplication](#vreplication)**
    - [Support for MySQL 8.0 `binlog_transaction_compression`](#binlog-compression)
    - [Support for the `noblob` binlog row image mode](#noblob)
  - **[VTTablet](#vttablet)**
    - [VTTablet: Initializing all replicas with super_read_only](#vttablet-initialization)
    - [Vttablet Schema Reload Timeout](#vttablet-schema-reload-timeout)
    - [Settings pool enabled](#settings-pool)
  - **[VTGate](#vtgate)**
    - [StreamExecute GRPC API](#stream-execute)
    - [Insert Planner Gen4](#insert-planner)
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deprecated Flags](#deprecated-flags)
    - [Deprecated Stats](#deprecated-stats)


## <a id="major-changes"/>Major Changes

### <a id="breaking-changes"/>Breaking Changes

#### <a id="tablet-picker-cell-preference"/>Default Local Cell Preference for TabletPicker

We added options to the `TabletPicker` that allow for specifying a cell preference in addition to making the default behavior to give priority to the local cell *and any alias it belongs to*. We are also introducing a new way to select tablet type preference which should eventually replace the `in_order:` hint currently used as a prefix for tablet types. The signature for creating a new `TabletPicker` now looks like:

```go
func NewTabletPicker(
	ctx context.Context,
	ts *topo.Server,
	cells []string,
	localCell, keyspace, shard, tabletTypesStr string,
	options TabletPickerOptions,
) (*TabletPicker, error) {...}
```

Where ctx, localCell, option are all new parameters.

`option` is of type `TabletPickerOptions` and includes two fields, `CellPreference` and `TabletOrder`.
CellPreference`: "PreferLocalWithAlias" (default) gives preference to vtgate's local cell, or "OnlySpecified" which only picks from the cells explicitly passed in by the client
`TabletOrder`: "Any" (default) for no ordering or random, or "InOrder" to use the order specified by the client

See [PR 12282 Description](https://github.com/vitessio/vitess/pull/12282) for examples on how this changes cell picking behavior.

#### <a id="vtgr-default-tls-version"/>Default TLS version changed for `vtgr`

When using TLS with `vtgr`, we now default to TLS 1.2 if no other explicit version is configured. Configuration flags are provided to explicitly configure the minimum TLS version to be used.

#### <a id="dedicated-vtgate-prepare-stats">Dedicated stats for VTGate Prepare operations

Prior to v17 Vitess incorrectly combined stats for VTGate Execute and Prepare operations under a single stats key (`Execute`). In v17 Execute and Prepare operations generate stats under independent stats keys.

Here is a (condensed) example of stats output:

```json
{
  "VtgateApi": {
    "Histograms": {
      "Execute.src.primary": {
        "500000": 5
      },
      "Prepare.src.primary": {
        "100000000": 0
      }
    }
  },
  "VtgateApiErrorCounts": {
    "Execute.src.primary.INVALID_ARGUMENT": 3,
    "Execute.src.primary.ALREADY_EXISTS": 1
  }
}
```

#### <a id="migrated-vtadmin"/>VTAdmin web migrated to vite

Previously, VTAdmin web used the Create React App framework to test, build, and serve the application. In v17, Create React App has been removed, and [Vite](https://vitejs.dev/) is used in its place. Some of the main changes include:
- Vite uses `VITE_*` environment variables instead of `REACT_APP_*` environment variables
- Vite uses `import.meta.env` in place of `process.env`
- [Vitest](https://vitest.dev/) is used in place of Jest for testing
- Our protobufjs generator now produces an es6 module instead of commonjs to better work with Vite's defaults
- `public/index.html` has been moved to root directory in web/vtadmin

#### <a id="keyspace-name-validation">Keyspace name validation in TopoServer

Prior to v17, it was possible to create a keyspace with invalid characters, which would then be inaccessible to various cluster management operations.

Keyspace names are restricted to using only ASCII characters, digits and `_` and `-`. TopoServer's `GetKeyspace` and `CreateKeyspace` methods return an error if given an invalid name.

#### <a id="shard-name-validation">Shard name validation in TopoServer

Prior to v17, it was possible to create a shard name with invalid characters, which would then be inaccessible to various cluster management operations.

Shard names are restricted to using only ASCII characters, digits and `_` and `-`. TopoServer's `GetShard` and `CreateShard` methods return an error if given an invalid name.

#### <a id="remove-compression-flags-from-vtctld-binaries">Compression CLI flags remove from vtctld and vtctldclient binaries

The CLI flags below were mistakenly added to `vtctld` and `vtctldclient` in v15. In v17, they are no longer present in those binaries.

 * `--compression-engine-name`
 * `--compression-level`
 * `--external-compressor`
 * `--external-compressor-extension`
 * `--external-decompressor`

#### <a id="VtctldClient-RestoreFromBackup">VtctldClient command RestoreFromBackup will now use the correct context

The VtctldClient command RestoreFromBackup initiates an asynchronous process on the specified tablet to restore data from either the latest backup or the closest one before the specified backup-timestamp.
Prior to v17, this asynchronous process could run indefinitely in the background since it was called using the background context. In v17 [PR#12830](https://github.com/vitessio/vitess/issues/12830),
this behavior was changed to use a context with a timeout of `action_timeout`. If you are using VtctldClient to initiate a restore, make sure you provide an appropriate value for action_timeout to give enough
time for the restore process to complete. Otherwise, the restore will throw an error if the context expires before it completes.

### <a id="Vttablet-TxThrottler">Vttablet's transaction throttler now also throttles DML outside of `BEGIN; ...; COMMIT;` blocks

Prior to v17, `vttablet`'s transaction throttler (enabled with `--enable-tx-throttler`) would only throttle requests done inside an explicit transaction, i.e., a `BEGIN; ...; COMMIT;` block.
In v17 [PR#13040](https://github.com/vitessio/vitess/issues/13037), this behavior was being changed so that it also throttles work outside of explicit transactions for `INSERT/UPDATE/DELETE/LOAD` queries.

### <a id="new-flag"/>New command line flags and behavior

#### <a id="builtin-backup-read-buffering-flags"/>Backup --builtinbackup-file-read-buffer-size and --builtinbackup-file-write-buffer-size

Prior to v17 the builtin Backup Engine does not use read buffering for restores, and for backups uses a hardcoded write buffer size of 2097152 bytes.

In v17 these defaults may be tuned with, respectively `--builtinbackup-file-read-buffer-size` and `--builtinbackup-file-write-buffer-size`.

- `--builtinbackup-file-read-buffer-size`:  read files using an IO buffer of this many bytes. Golang defaults are used when set to 0.
- `--builtinbackup-file-write-buffer-size`: write files using an IO buffer of this many bytes. Golang defaults are used when set to 0. (default 2097152)

These flags are applicable to the following programs:

- `vtbackup`
- `vtctld`
- `vttablet`
- `vttestserver`

#### <a id="manifest-backup-external-decompressor-command" />Manifest backup external decompressor command

Add a new builtin/xtrabackup flag `--manifest-external-decompressor`. When set the value of that flag is stored in the manifest field `ExternalDecompressor`. This manifest field may be consulted when decompressing a backup that was compressed with an external command.

This feature enables the following flow:

 1. Take a backup using an external compressor
    ```
     Backup --compression-engine=external \
            --external-compressor=zstd \
            --manifest-external-decompressor="zstd -d"
    ```
 2. Restore that backup with a mere `Restore` command, without having to specify `--external-decompressor`.

#### <a id="throttler-config-via-topo" />vttablet --throttler-config-via-topo

This flag was introduced in v16 and defaulted to `false`. In v17 it defaults to `true`, and there is no need to supply it.

Note that this flag overrides `--enable-lag-throttler` and `--throttle-threshold`, which now give warnings, and will be removed in v18.

### <a id="new-stats"/>New stats

#### <a id="detailed-backup-and-restore-stats"/>Detailed backup and restore stats

##### Backup metrics

Metrics related to backup operations are available in both Vtbackup and VTTablet.

**BackupBytes, BackupCount, BackupDurationNanoseconds**

Depending on the Backup Engine and Backup Storage in-use, a backup may be a complex pipeline of operations, including but not limited to:

* Reading files from disk.
* Compressing files.
* Uploading compress files to cloud object storage.

These operations are counted and timed, and the number of bytes consumed or produced by each stage of the pipeline are counted as well.

##### Restore metrics

Metrics related to restore operations are available in both Vtbackup and VTTablet.

**RestoreBytes, RestoreCount, RestoreDurationNanoseconds**

Depending on the Backup Engine and Backup Storage in-use, a restore may be a complex pipeline of operations, including but not limited to:

* Downloading compressed files from cloud object storage.
* Decompressing files.
* Writing decompressed files to disk.

These operations are counted and timed, and the number of bytes consumed or produced by each stage of the pipeline are counted as well.

##### Vtbackup metrics

Vtbackup exports some metrics which are not available elsewhere.

**DurationByPhaseSeconds**

Vtbackup fetches the last backup, restores it to an empty mysql installation, replicates recent changes into that installation, and then takes a backup of that installation.

_DurationByPhaseSeconds_ exports timings for these individual phases.

##### Example

**A snippet of vtbackup metrics after running it against the local example after creating the initial cluster**

(Processed with `jq` for readability.)

```json
{
  "BackupBytes": {
    "BackupEngine.Builtin.Source:Read": 4777,
    "BackupEngine.Builtin.Compressor:Write": 4616,
    "BackupEngine.Builtin.Destination:Write": 162,
    "BackupStorage.File.File:Write": 163
  },
  "BackupCount": {
    "-.-.Backup": 1,
    "BackupEngine.Builtin.Source:Open": 161,
    "BackupEngine.Builtin.Source:Close": 322,
    "BackupEngine.Builtin.Compressor:Close": 161,
    "BackupEngine.Builtin.Destination:Open": 161,
    "BackupEngine.Builtin.Destination:Close": 322
  },
  "BackupDurationNanoseconds": {
    "-.-.Backup": 4188508542,
    "BackupEngine.Builtin.Source:Open": 10649832,
    "BackupEngine.Builtin.Source:Read": 55901067,
    "BackupEngine.Builtin.Source:Close": 960826,
    "BackupEngine.Builtin.Compressor:Write": 278358826,
    "BackupEngine.Builtin.Compressor:Close": 79358372,
    "BackupEngine.Builtin.Destination:Open": 16456627,
    "BackupEngine.Builtin.Destination:Write": 11021043,
    "BackupEngine.Builtin.Destination:Close": 17144630,
    "BackupStorage.File.File:Write": 10743169
  },
  "DurationByPhaseSeconds": {
    "InitMySQLd": 2,
    "RestoreLastBackup": 6,
    "CatchUpReplication": 1,
    "TakeNewBackup": 4
  },
  "RestoreBytes": {
    "BackupEngine.Builtin.Source:Read": 1095,
    "BackupEngine.Builtin.Decompressor:Read": 950,
    "BackupEngine.Builtin.Destination:Write": 209,
    "BackupStorage.File.File:Read": 1113
  },
  "RestoreCount": {
    "-.-.Restore": 1,
    "BackupEngine.Builtin.Source:Open": 161,
    "BackupEngine.Builtin.Source:Close": 322,
    "BackupEngine.Builtin.Decompressor:Close": 161,
    "BackupEngine.Builtin.Destination:Open": 161,
    "BackupEngine.Builtin.Destination:Close": 322
  },
  "RestoreDurationNanoseconds": {
    "-.-.Restore": 6204765541,
    "BackupEngine.Builtin.Source:Open": 10542539,
    "BackupEngine.Builtin.Source:Read": 104658370,
    "BackupEngine.Builtin.Source:Close": 773038,
    "BackupEngine.Builtin.Decompressor:Read": 165692120,
    "BackupEngine.Builtin.Decompressor:Close": 51040,
    "BackupEngine.Builtin.Destination:Open": 22715122,
    "BackupEngine.Builtin.Destination:Write": 41679581,
    "BackupEngine.Builtin.Destination:Close": 26954624,
    "BackupStorage.File.File:Read": 102416075
  },
  "backup_duration_seconds": 4,
  "restore_duration_seconds": 6
}
```

Some notes to help understand these metrics:

* `BackupBytes["BackupStorage.File.File:Write"]` measures how many bytes were read from disk by the `file` Backup Storage implementation during the backup phase.
* `DurationByPhaseSeconds["CatchUpReplication"]` measures how long it took to catch-up replication after the restore phase.
* `DurationByPhaseSeconds["RestoreLastBackup"]` measures to the duration of the restore phase.
* `RestoreDurationNanoseconds["-.-.Restore"]` also measures to the duration of the restore phase.

#### <a id="vttablet-error-count-with-code"/>VTTablet error count with error code

##### VTTablet Error Count

We are introducing new error counter `QueryErrorCountsWithCode` for VTTablet. It is similar to existing [QueryErrorCounts](https://github.com/vitessio/vitess/blob/main/go/vt/vttablet/tabletserver/query_engine.go#L174) except it contains errorCode as additional dimension.
We will deprecate `QueryErrorCounts` in v18.

#### <a id="vreplication-stream-status-for-prometheus"/>VReplication stream status for Prometheus

VReplication publishes the `VReplicationStreamState` status which reports the state of VReplication streams. For example, here's what it looks like in the local cluster example after the MoveTables step:

```
"VReplicationStreamState": {
  "commerce2customer.1": "Running"
}
```

Prior to v17, this data was not available via the Prometheus backend. In v17, workflow states are also published as a Prometheus gauge with a `state` label and a value of `1.0`. For example:

```
# HELP vttablet_v_replication_stream_state State of vreplication workflow
# TYPE vttablet_v_replication_stream_state gauge
vttablet_v_replication_stream_state{counts="1",state="Running",workflow="commerce2customer"} 1
```

### <a id="vttablet"/>VTTablet

#### <a id="vttablet-initialization"/>Initializing all replicas with super_read_only

In order to prevent SUPER privileged users like `root` or `vt_dba` from producing errant GTIDs on replicas, all the replica MySQL servers are initialized with the MySQL
global variable `super_read_only` value set to `ON`. During failovers, we set `super_read_only` to `OFF` for the promoted primary tablet. This will allow the
primary to accept writes. All of the shard's tablets, except the current primary, will still have their global variable `super_read_only` set to `ON`. This will make sure that apart from
MySQL replication no other component, offline system or operator can write directly to a replica.

Reference PR for this change is [PR #12206](https://github.com/vitessio/vitess/pull/12206)

An important note regarding this change is how the default `init_db.sql` file has changed.
This is even more important if you are running Vitess on the vitess-operator.
You must ensure your `init_db.sql` is up-to-date with the new default for `v17.0.0`.
The default file can be found in `./config/init_db.sql`.

#### <a id="vttablet-schema-reload-timeout"/>Vttablet Schema Reload Timeout

A new flag, `--schema-change-reload-timeout` has been added to timeout the reload of the schema that Vttablet does periodically. This is required because sometimes this operation can get stuck after MySQL restarts, etc. More details available in the issue https://github.com/vitessio/vitess/issues/13001.

#### <a id="settings-pool"/>Settings Pool

This was introduced in v15 and it enables pooling the connection with modified connection settings.
To know more what it does read the [v15 release notes](https://github.com/vitessio/vitess/releases/tag/v15.0.0) or the [blog](https://vitess.io/blog/2023-03-27-connection-pooling-in-vitess/) or [docs](https://vitess.io/docs/17.0/reference/query-serving/reserved-conn/)

### <a id="online-ddl"/>Online DDL

#### <a id="online-ddl-cut-over-threshold-flag" />--cut-over-threshold DDL strategy flag

Online DDL's strategy now accepts `--cut-over-threshold` (type: `duration`) flag.

This flag stand for the timeout in a `vitess` migration's cut-over phase, which includes the final locking of tables before finalizing the migration.

The value of the cut-over threshold should be high enough to support the async nature of vreplication catchup phase, as well as accommodate some replication lag. But it mustn't be too high. While cutting over, the migrated table is being locked, causing app connection and query pileup, consuming query buffers, and holding internal mutexes.

Recommended range for this variable is `5s` - `30s`. Default: `10s`.

### <a id="vreplication"/>VReplication

#### <a id="noblob"/>Support for the `noblob` binlog row image mode 

The `noblob` binlog row image is now supported by the MoveTables and Reshard VReplication workflows. If the source 
or target database has this mode, other workflows like OnlineDDL, Materialize and CreateLookupVindex will error out.
The row events streamed by the VStream API, where blobs and text columns have not changed, will contain null values 
for those columns, indicated by a `length:-1`.

Reference PR for this change is [PR #12905](https://github.com/vitessio/vitess/pull/12905)

#### <a id="binlog-compression"/>Support for MySQL 8.0 binary log transaction compression

MySQL 8.0 added support for [binary log compression via transaction (GTID) compression in 8.0.20](https://dev.mysql.com/blog-archive/mysql-8-0-20-replication-enhancements/).
You can read more about this feature here: https://dev.mysql.com/doc/refman/8.0/en/binary-log-transaction-compression.html

This can — at the cost of increased CPU usage — dramatically reduce the amount of data sent over the wire for MySQL replication while also dramatically reducing the overall
storage space needed to retain binary logs (for replication, backup and recovery, CDC, etc). For larger installations this was a very desirable feature and while you could
technically use it with Vitess (the MySQL replica-sets making up each shard could use it fine) there was one very big limitation — [VReplication workflows](https://vitess.io/docs/reference/vreplication/vreplication/)
would not work. Given the criticality of VReplication workflows within Vitess, this meant that in practice this MySQL feature was not usable within Vitess clusters.

We have addressed this issue in [PR #12950](https://github.com/vitessio/vitess/pull/12950) by adding support for processing the compressed transaction events in VReplication,
without any (known) limitations.

### <a id="vtgate"/>VTGate

#### <a id="stream-execute"/>Modified StreamExecute GRPC API

Earlier VTGate grpc api for `StreamExecute` did not return the session in the response.
Even though the underlying implementation supported transactions and other features that requires session persistence.
With [PR #13131](https://github.com/vitessio/vitess/pull/13131) VTGate will return the session to the client
so that it can be persisted with the client and sent back to VTGate on the next api call.

This does not impact anyone using the mysql client library to connect to VTGate.
This could be a breaking change for grpc api users based on how they have implemented their grpc clients.

#### <a id="insert-planner"/> Insert Planning with Gen4

Gen4 planner was made default in v14 for `SELECT` queries. In v15 `UPDATE` and `DELETE` queries were moved to Gen4 framework.
With this release `INSERT` queries are moved to Gen4.

Clients can move to old v3 planner for inserts by using `V3Insert` planner version with `--planner-version` vtgate flag or with comment directive `/*vt+ planner=<planner_version>` for individual query.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

- The deprecated `automation` and `automationservice` protobuf definitions and associated client and server packages have been removed.
- Auto-population of DDL revert actions and tables at execution-time has been removed. This is now handled entirely at enqueue-time.
- Backwards-compatibility for failed migrations without a `completed_timestamp` has been removed (see https://github.com/vitessio/vitess/issues/8499).
- The deprecated `Key`, `Name`, `Up`, and `TabletExternallyReparentedTimestamp` fields were removed from the JSON representation of `TabletHealth` structures.
- The `MYSQL_FLAVOR` environment variable is no longer used.
- The `--enable-query-plan-field-caching`/`--enable_query_plan_field_caching` vttablet flag was deprecated in v15 and has now been removed.

#### <a id="deprecated-flags"/>Deprecated Command Line Flags

- Flag `vtctld_addr` has been deprecated and will be deleted in a future release. This affects `vtgate`, `vttablet` and `vtcombo`.
- The flag `schema_change_check_interval` used to accept either a Go duration value (e.g. `1m` or `30s`) or a bare integer, which was treated as seconds.
  This behavior was deprecated in v15.0.0 and has been removed.
  `schema_change_check_interval` now **only** accepts Go duration values. This affects `vtctld`.
- The flag `durability_policy` is no longer used by vtctld. Instead it reads the durability policies for all keyspaces from the topology server.
- The flag `use_super_read_only` is deprecated and will be removed in a later release. This affects `vttablet`.
- The flag `queryserver-config-schema-change-signal-interval` is deprecated and will be removed in a later release. This affects `vttablet`.
  Schema-tracking has been refactored in this release to not use polling anymore, therefore the signal interval isn't required anymore.

In `vttablet` various flags that took float values as seconds have updated to take the standard duration syntax as well.
Float-style parsing is now deprecated and will be removed in a later release.
For example, instead of `--queryserver-config-query-pool-timeout 12.2`, use `--queryserver-config-query-pool-timeout 12s200ms`.
Affected flags and YAML config keys:

- `degraded_threshold`
- `heartbeat_interval`
- `heartbeat_on_demand_duration`
- `health_check_interval`
- `queryserver-config-idle-timeout`
- `queryserver-config-pool-conn-max-lifetime`
- `queryserver-config-olap-transaction-timeout`
- `queryserver-config-query-timeout`
- `queryserver-config-query-pool-timeout`
- `queryserver-config-schema-reload-time`
- `queryserver-config-schema-change-signal-interval`
- `queryserver-config-stream-pool-timeout`
- `queryserver-config-stream-pool-idle-timeout`
- `queryserver-config-transaction-timeout`
- `queryserver-config-txpool-timeout`
- `queryserver-config-txpool-idle-timeout`
- `shutdown_grace_period`
- `unhealthy_threshold`

#### <a id="deprecated-stats"/>Deprecated Stats

These stats are deprecated in v17.

| Deprecated stat | Supported alternatives |
|-|-|
| `backup_duration_seconds` | `BackupDurationNanoseconds` |
| `restore_duration_seconds` | `RestoreDurationNanoseconds` |

------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/17.0/17.0.0/changelog.md).

The release includes 432 commits (excluding merges)

Thanks to all our contributors: @Ayman161803, @GuptaManan100, @L3o-pold, @Phanatic, @WilliamLu99, @adsr, @ajm188, @andylim-duo, @arthurschreiber, @austenLacy, @cuishuang, @dasl-, @dbussink, @deepthi, @dependabot[bot], @ejortegau, @fatih, @frouioui, @github-actions[bot], @harshit-gangal, @hkdsun, @jeremycole, @jhump, @johanstenberg92, @jwangace, @kevinpurwito, @kovyrin, @lixin963, @mattlord, @maxbrunet, @maxenglander, @mdlayher, @moberghammer, @notfelineit, @olyazavr, @pbibra, @pnacht, @rohit-nayak-ps, @rsajwani, @shlomi-noach, @systay, @timvaillancourt, @twthorn, @vbalys, @vinimdocarmo, @vitess-bot[bot], @vmg, @yoheimuta

