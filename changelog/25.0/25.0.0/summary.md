# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[Breaking Changes](#breaking-changes)**
        - [Underscored flag names are no longer accepted](#breaking-changes-underscore-flags)

## <a id="major-changes"/>Major Changes</a>

### <a id="breaking-changes"/>Breaking Changes</a>

#### <a id="breaking-changes-underscore-flags"/>Underscored flag names are no longer accepted</a>

Vitess binaries no longer accept flag names with underscores. The migration to the dashed flag-naming convention completed in v23 ([#18280](https://github.com/vitessio/vitess/pull/18280)), and v23 and v24 shipped a compatibility layer ([#18642](https://github.com/vitessio/vitess/pull/18642)) that translated underscored flag names to their dashed equivalents at parse time and emitted a deprecation warning. That compatibility layer has been removed in v25.

Passing a flag like `--foo_bar` will now fail with pflag's standard "unknown flag" error.

**Impact**: replace `_` with `-` in any flag names passed to Vitess binaries — on the command line, in YAML configs, in deployment scripts, and in any helper scripts. For example:

- `--cells_to_watch` → `--cells-to-watch`
- `--mysql_server_port` → `--mysql-server-port`
- `--topo_implementation` → `--topo-implementation`

The glog flags `--log_dir`, `--log_link`, and `--log_backtrace_at` are unaffected — they come from the upstream `github.com/golang/glog` library and continue to use underscores.
