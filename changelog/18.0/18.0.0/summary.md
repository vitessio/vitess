## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
  - **[New command line flags and behavior](#new-flag)**
    - [VTOrc flag `--allow-emergency-reparent`](#new-flag-toggle-ers)
  - **[New stats](#new-stats)**
    - [Backup and restore metrics for AWS S3](#backup-restore-metrics-aws-s3)
  - **[VTAdmin](#vtadmin)**
    - [Updated to node v18.16.0](#update-node)
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deleted `k8stopo`](#deleted-k8stopo)
    - [Deleted `vtgr`](#deleted-vtgr)

## <a id="major-changes"/>Major Changes

### <a id="breaking-changes"/>Breaking Changes

### <a id="new-flag"/>New command line flags and behavior

#### <a id="new-flag-toggle-ers"/>VTOrc flag `--allow-emergency-reparent`

VTOrc has a new flag `--allow-emergency-reparent` that allows the users to toggle the ability of VTOrc to run emergency reparent operations.
The users that want VTOrc to fix the replication issues, but don't want it to run any reparents should start using this flag.
By default, VTOrc will be able to run `EmergencyReparentShard`. The users must specify the flag to `false` to change the behaviour.

### <a id="new-stats"/>New stats

#### <a id="backup-restore-metrics-aws-s3"/>Backup and restore metrics for AWS S3

Requests to AWS S3 are instrumented in backup and restore metrics. For example:

```
vtbackup_backup_count{component="BackupStorage",implementation="S3",operation="AWS:Request:Send"} 823
vtbackup_backup_duration_nanoseconds{component="BackupStorage",implementation="S3",operation="AWS:Request:Send"} 1.33632421437e+11
vtbackup_restore_count{component="BackupStorage",implementation="S3",operation="AWS:Request:Send"} 165
vtbackup_restore_count{component="BackupStorage",implementation="S3",operation="AWS:Request:Send"} 165
```

#### <a id="new-flag-toggle-ers"/>VTOrc flag `--allow-emergency-reparent`

### <a id="vtadmin"/>VTAdmin
#### <a id="updated-node"/>vtadmin-web updated to node v18.16.0 (LTS)

Building vtadmin-web now requires node >= v18.16.0 (LTS). Breaking changes from v16 to v18 are listed
in https://nodejs.org/en/blog/release/v18.0.0, but none apply to VTAdmin. Full details on v18.16.0 are listed here https://nodejs.org/en/blog/release/v18.16.0.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

#### <a id="deleted-k8stopo"/>Deleted `k8stopo`

The `k8stopo` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13298. With Vitess 18 the `k8stopo` has been removed.

#### <a id="deleted-vtgr"/>Deleted `vtgr`

The `vtgr` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13300. With Vitess 18 `vtgr` has been removed.
