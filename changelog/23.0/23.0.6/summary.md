# Release of Vitess v23.0.6

## Summary

### VRLog Feature Removed

The VRLog feature — a streaming log of VReplication events served at VTTablet's `/debug/vrlog` HTTP endpoint, [disabled by default since v22](../../22.0/22.0.0/changelog.md) — has been removed. The `--vreplication-enable-http-log` flag that enabled it is now a deprecated no-op and will be removed in v26.

**Migration**: remove `--vreplication-enable-http-log` from VTTablet startup arguments.

**Impact**: The `/debug/vrlog` endpoint no longer exists. Passing `--vreplication-enable-http-log` logs a deprecation warning and has no effect.

See [#20467](https://github.com/vitessio/vitess/pull/20467) for details.
