# Release of Vitess v26.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[Breaking Changes](#breaking-changes)**
        - [Legacy vtctld HTTP API flags removed](#vtctld-http-api-flags-removed)

## <a id="major-changes"/>Major Changes</a>

### <a id="breaking-changes"/>Breaking Changes</a>

#### <a id="vtctld-http-api-flags-removed"/>Legacy vtctld HTTP API flags removed</a>

The following flags have been removed from vtctld and vtcombo after being retained as deprecated no-ops in v25:

- `--cell`
- `--proxy-tablets`
- `--action-timeout`
- `--tablet-health-keep-alive`

The underscore spellings of these flags are also removed.

**Impact**: vtctld and vtcombo fail to start with an unknown-flag error if any of these flags are passed, even if they are set to their former default values.

**Migration**: remove these flags from startup scripts, service definitions, container arguments, and any configuration that supplies arguments to vtctld or vtcombo before upgrading to v26. No replacement flags are required.

Operator-managed deployments must first upgrade to a compatible vitess-operator release that no longer automatically passes `--cell` to vtctld. Also remove these flags from user-provided `vitessDashboard.extraFlags` and cluster-wide `extraVitessFlags` that reach vtctld. Removing user-provided flags alone is insufficient if the operator still generates `--cell`.

This removal applies only to vtctld and vtcombo. Similarly named flags on other binaries, such as `--cell` on vtgate and vtorc and `--action-timeout` on vtctldclient and vtctlclient, are unaffected.

See [#21170](https://github.com/vitessio/vitess/issues/21170) for the flag removal and [vitess-operator #874](https://github.com/planetscale/vitess-operator/pull/874) for the companion operator change.
