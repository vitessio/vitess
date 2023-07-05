## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
  - **[New command line flags and behavior](#new-flag)**
    - [VTOrc flag `--allow-emergency-reparent`](#new-flag-toggle-ers)
  - **[VTAdmin](#vtadmin)**
    - [Updated to node v18.16.0](#update-node)
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deprecated Flags](#deprecated-flags)
    - [Deleted `k8stopo`](#deleted-k8stopo)
    - [Deleted `vtgr`](#deleted-vtgr)
  - **[New stats](#new-stats)**
    - [VTGate Vindex unknown parameters](#vtgate-vindex-unknown-parameters)
  - **[VTTablet](#vttablet)**
    - [VTTablet: New ResetSequences RPC](#vttablet-new-rpc-reset-sequences)

## <a id="major-changes"/>Major Changes

### <a id="breaking-changes"/>Breaking Changes

### <a id="new-flag"/>New command line flags and behavior

#### <a id="new-flag-toggle-ers"/>VTOrc flag `--allow-emergency-reparent`

VTOrc has a new flag `--allow-emergency-reparent` that allows the users to toggle the ability of VTOrc to run emergency
reparent operations. The users that want VTOrc to fix the replication issues, but don't want it to run any reparents
should start using this flag. By default, VTOrc will be able to run `EmergencyReparentShard`. The users must specify the
flag to `false` to change the behaviour.

### <a id="vtadmin"/>VTAdmin

#### <a id="updated-node"/>vtadmin-web updated to node v18.16.0 (LTS)

Building vtadmin-web now requires node >= v18.16.0 (LTS). Breaking changes from v16 to v18 are listed
in https://nodejs.org/en/blog/release/v18.0.0, but none apply to VTAdmin. Full details on v18.16.0 are listed
here https://nodejs.org/en/blog/release/v18.16.0.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

#### <a id="deprecated-flags"/>Deprecated Command Line Flags

Throttler related `vttablet` flags:

- `--enable-lag-throttler` is now removed after being deprecated in `v17.0`
- `--throttle_threshold` is deprecated and will be removed in `v19.0`
- `--throttle_metrics_query` is deprecated and will be removed in `v19.0`
- `--throttle_metrics_threshold` is deprecated and will be removed in `v19.0`
- `--throttle_check_as_check_self` is deprecated and will be removed in `v19.0`
- `--throttler-config-via-topo` is deprecated after asummed `true` in `v17.0`. It will be removed in a future version.

#### <a id="deleted-k8stopo"/>Deleted `k8stopo`

The `k8stopo` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13298. With Vitess 18
the `k8stopo` has been removed.

#### <a id="deleted-vtgr"/>Deleted `vtgr`

The `vtgr` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13300. With Vitess 18 `vtgr` has been removed.

### <a id="new-stats"/>New stats

#### <a id="vtgate-vindex-unknown-parameters"/>VTGate Vindex unknown parameters

The VTGate stat `VindexUnknownParameters` gauges unknown Vindex parameters found in the latest VSchema pulled from the topology.

### <a id="vttablet"/>VTTablet

#### <a id="vttablet-new-rpc-reset-sequences"/>New ResetSequences rpc

A new vttablet RPC `ResetSequences` has been added, which is being used by `MoveTables` and `Migrate` for workflows
where a `sequence` table is being moved (https://github.com/vitessio/vitess/pull/13238). This has an impact on the
Vitess upgrade process from an earlier version if you need to use such a workflow before the entire cluster is upgraded.

Any MoveTables or Migrate workflow that moves a sequence table should only be run after all vitess components have been
upgraded, and no upgrade should be done while such a workflow is in progress.
