## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
    - [VTBackup stat `DurationByPhase` removed](#remove-vtbackup-stat-duration-by-phase)
  - **[New command line flags and behavior](#new-flag)**
    - [VTOrc flag `--allow-emergency-reparent`](#new-flag-toggle-ers)
  - **[New stats](#new-stats)**
    - [VTBackup stat `Phase`](#vtbackup-stat-phase)
  - **[VTAdmin](#vtadmin)**
    - [Updated to node v18.16.0](#update-node)
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deleted `k8stopo`](#deleted-k8stopo)
    - [Deleted `vtgr`](#deleted-vtgr)

## <a id="major-changes"/>Major Changes

### <a id="breaking-changes"/>Breaking Changes

#### <a id="remove-vtbackup-stat-duration-by-phase"/>VTbackup stat `DurationByPhase` removed

VTBackup stat `DurationByPhase` is removed. Use the binary-valued `Phase` stat instead.

### <a id="new-flag"/>New command line flags and behavior

#### <a id="new-flag-toggle-ers"/>VTOrc flag `--allow-emergency-reparent`

VTOrc has a new flag `--allow-emergency-reparent` that allows the users to toggle the ability of VTOrc to run emergency reparent operations.
The users that want VTOrc to fix the replication issues, but don't want it to run any reparents should start using this flag.
By default, VTOrc will be able to run `EmergencyReparentShard`. The users must specify the flag to `false` to change the behaviour.

### <a id="new-stats"/>New stats

#### <a id="vtbackup-stat-phase"/>VTBackup `Phase` stat

In v17, the `vtbackup` stat `DurationByPhase` stat was added measuring the time spent by `vtbackup` in each phase. This stat turned out to be awkward to use in production, and has been replaced in v18 by a binary-valued `Phase` stat.

`Phase` reports a 1 (active) or a 0 (inactive) for each of the following phases:

 * `CatchUpReplication`
 * `InitialBackup`
 * `RestoreLastBackup`
 * `TakeNewBackup`

To calculate how long `vtbackup` has spent in a given phase, sum the 1-valued data points over time and multiply by the data collection or reporting interval. For example, in Prometheus:

```
sum_over_time(vtbackup_phase{phase="TakeNewBackup"}) * <interval>
```

### <a id="vtadmin"/>VTAdmin
#### <a id="updated-node"/>vtadmin-web updated to node v18.16.0 (LTS)

Building vtadmin-web now requires node >= v18.16.0 (LTS). Breaking changes from v16 to v18 are listed
in https://nodejs.org/en/blog/release/v18.0.0, but none apply to VTAdmin. Full details on v18.16.0 are listed here https://nodejs.org/en/blog/release/v18.16.0.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

#### <a id="deleted-k8stopo"/>Deleted `k8stopo`

The `k8stopo` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13298. With Vitess 18 the `k8stopo` has been removed.

#### <a id="deleted-vtgr"/>Deleted `vtgr`

The `vtgr` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13300. With Vitess 18 `vtgr` has been removed.
