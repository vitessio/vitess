## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
  - **[New command line flags and behavior](#new-flag)**
    - [VTOrc flag `--allow-emergency-reparent`](#new-flag-toggle-ers)
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

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

#### <a id="deleted-k8stopo"/>Deleted `k8stopo`

The `k8stopo` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13298. With Vitess 18 the `k8stopo` has been removed.

#### <a id="deleted-vtgr"/>Deleted `vtgr`

The `vtgr` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13300. With Vitess 18 `vtgr` has been removed.