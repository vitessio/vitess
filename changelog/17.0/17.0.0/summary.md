## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
    - [Dedicated stats for VTGate Prepare operations](#dedicated-vtgate-prepare-stats)
    - [Keyspace name validation in TopoServer](#keyspace-name-validation)
  - **[New command line flags and behavior](#new-flag)**
    - [Builtin backup: read buffering flags](#builtin-backup-read-buffering-flags)
  - **[New stats](#new-stats)**
    - [Detailed backup and restore stats](#detailed-backup-and-restore-stats)
    - [VTtablet Error count with code ](#vttablet-error-count-with-code)
    - [VReplication stream status for Prometheus](#vreplication-stream-status-for-prometheus)
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deprecated Stats](#deprecated-stats)
  - **[VTTablet](#vttablet)**
    - [VTTablet: Initializing all replicas with super_read_only](#vttablet-initialization)
    - [Deprecated Flags](#deprecated-flags)

## <a id="major-changes"/> Major Changes

### <a id="breaking-changes"/>Breaking Changes

#### <a id="vtgr-default-tls-version"/>Default TLS version changed for `vtgr`

When using TLS with `vtgr`, we now default to TLS 1.2 if no other explicit version is configured. Configuration flags are provided to explicitly configure the minimum TLS version to be used.

#### <a id="dedicated-vtgate-prepare-stats"> Dedicated stats for VTGate Prepare operations

Prior to v17 Vitess incorrectly combined stats for VTGate Execute and Prepare operations under a single stats key (`Execute`). In v17 Execute and Prepare operations generate stats under independent stats keys.

Here is a (condensed) example of stats output:

```
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


#### <a id="keyspace-name-validation"> Keyspace name validation in TopoServer

Prior to v17, it was possible to create a keyspace with invalid characters, which would then be inaccessible to various cluster management operations.

Keyspace names may no longer contain the forward slash ("/") character, and TopoServer's `GetKeyspace` and `CreateKeyspace` methods return an error if given such a name.

### <a id="new-flag"/> New command line flags and behavior

#### <a id="builtin-backup-read-buffering-flags" /> Backup --builtinbackup-file-read-buffer-size and --builtinbackup-file-write-buffer-size

Prior to v17 the builtin Backup Engine does not use read buffering for restores, and for backups uses a hardcoded write buffer size of 2097152 bytes.

In v17 these defaults may be tuned with, respectively `--builtinbackup-file-read-buffer-size` and `--builtinbackup-file-write-buffer-size`.

- `--builtinbackup-file-read-buffer-size`:  read files using an IO buffer of this many bytes. Golang defaults are used when set to 0.
- `--builtinbackup-file-write-buffer-size`: write files using an IO buffer of this many bytes. Golang defaults are used when set to 0. (default 2097152)

These flags are applicable to the following programs:

- `vtbackup`
- `vtctld`
- `vttablet`
- `vttestserver`

### <a id="new-stats"/> New stats

#### <a id="detailed-backup-and-restore-stats"/> Detailed backup and restore stats

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

```
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

#### <a id="vttablet-error-count-with-code"/> VTTablet error count with error code

##### VTTablet Error Count

We are introducing new error counter `QueryErrorCountsWithCode` for VTTablet. It is similar to existing [QueryErrorCounts](https://github.com/vitessio/vitess/blob/main/go/vt/vttablet/tabletserver/query_engine.go#L174) except it contains errorCode as additional dimension.
We will deprecate `QueryErrorCounts` in v18.

#### <a id="vreplication-stream-status-for-prometheus"/> VReplication stream status for Prometheus

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

## <a id="deprecations-and-deletions"/> Deprecations and Deletions

* The deprecated `automation` and `automationservice` protobuf definitions and associated client and server packages have been removed.
* Auto-population of DDL revert actions and tables at execution-time has been removed. This is now handled entirely at enqueue-time.
* Backwards-compatibility for failed migrations without a `completed_timestamp` has been removed (see https://github.com/vitessio/vitess/issues/8499).
* The deprecated `Key`, `Name`, `Up`, and `TabletExternallyReparentedTimestamp` fields were removed from the JSON representation of `TabletHealth` structures.

### <a id="deprecated-stats"/>Deprecated Stats

These stats are deprecated in v17.

| Deprecated stat | Supported alternatives |
|-|-|
| `backup_duration_seconds` | `BackupDurationNanoseconds` |
| `restore_duration_seconds` | `RestoreDurationNanoseconds` |


### <a id="vttablet"/> VTTablet
#### <a id="vttablet-initialization"/> Initializing all replicas with super_read_only
In order to prevent SUPER privileged users like `root` or `vt_dba` from producing errant GTIDs on replicas, all the replica MySQL servers are initialized with the MySQL
global variable `super_read_only` value set to `ON`. During failovers, we set `super_read_only` to `OFF` for the promoted primary tablet. This will allow the
primary to accept writes. All of the shard's tablets, except the current primary, will still have their global variable `super_read_only` set to `ON`. This will make sure that apart from
MySQL replication no other component, offline system or operator can write directly to a replica.

Reference PR for this change is [PR #12206](https://github.com/vitessio/vitess/pull/12206)

An important note regarding this change is how the default `init_db.sql` file has changed.
This is even more important if you are running Vitess on the vitess-operator.
You must ensure your `init_db.sql` is up-to-date with the new default for `v17.0.0`.
The default file can be found in `./config/init_db.sql`.

#### <a id="deprecated-flags"/> Deprecated Flags
The flag `use_super_read_only` is deprecated and will be removed in a later release.
