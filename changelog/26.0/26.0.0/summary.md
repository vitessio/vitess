# Release of Vitess v26.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[Breaking Changes](#breaking-changes)**
        - [vtctld and vtcombo HTTP API flags removed](#vtctld-http-api-flags-removed)

## <a id="major-changes"/>Major Changes</a>

### <a id="breaking-changes"/>Breaking Changes</a>

#### <a id="vtctld-http-api-flags-removed"/>vtctld and vtcombo HTTP API flags removed</a>

The vtctld and vtcombo flags `--cell`, `--proxy-tablets`, `--action-timeout`, and `--tablet-health-keep-alive` have been removed. They configured the [legacy vtctld HTTP API that was removed in v25](../../25.0/25.0.0/summary.md#vtctld-http-api-removed), and have been no-ops since then. No replacement flags are needed.

**Migration**: remove `--cell`, `--proxy-tablets`, `--action-timeout`, and `--tablet-health-keep-alive` from vtctld and vtcombo startup arguments before you upgrade.

**Impact**: vtctld and vtcombo fail to start with an `unknown flag` error if any of these flags is still passed.

For deployments managed by vitess-operator, the operator sets `--cell` on vtctld itself. Upgrade to a vitess-operator release that no longer sets this flag before you upgrade to v26, and remove these flags from any user-provided `extraFlags`.

The `--action-timeout` flag of `vtctldclient` and `vtctlclient` is unaffected.

See [#21828](https://github.com/vitessio/vitess/pull/21828) for the removal, tracked in [#21170](https://github.com/vitessio/vitess/issues/21170).
