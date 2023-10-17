## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [VTTablet Flags](#vttablet-flags)
  - **[Docker](#docker)**
    - [New MySQL Image](#mysql-image)

## <a id="major-changes"/>Major Changes

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

- The `MYSQL_FLAVOR` environment variable is now removed from all Docker Images.

#### <a id="vttablet-flags"/>VTTablet Flags

- The following flags — which were deprecated in Vitess 7.0 — have been removed:
`--vreplication_healthcheck_topology_refresh`, `--vreplication_healthcheck_retry_delay`, and `--vreplication_healthcheck_timeout`.
- The `--vreplication_tablet_type` flag is now deprecated and ignored.

### <a id="docker"/>Docker

#### <a id="mysql-image"/>New MySQL Image

In `v19.0` the Vitess team is shipping a new image: `vitess/mysql`.
This lightweight image is a replacement of `vitess/lite` to only run `mysqld`.

Several tags are available to let you choose what version of MySQL you want to use: `vitess/mysql:8.0.30`, `vitess/mysql:8.0.34`.
