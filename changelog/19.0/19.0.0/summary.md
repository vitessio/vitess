## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
  - **[Docker](#docker)**
    - [New MySQL Image](#mysql-image)
  - **[New stats](#new-stats)**
    - [VTTablet tablet type for Prometheus](#vttablet-tablet-type-for-prometheus)

## <a id="major-changes"/>Major Changes

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

- The `MYSQL_FLAVOR` environment variable is now removed from all Docker Images.

### <a id="docker"/>Docker

#### <a id="mysql-image"/>New MySQL Image

In `v19.0` the Vitess team is shipping a new image: `vitess/mysql`.
This lightweight image is a replacement of `vitess/lite` to only run `mysqld`.

Several tags are available to let you choose what version of MySQL you want to use: `vitess/mysql:8.0.30`, `vitess/mysql:8.0.34`.

### <a id="new-stats"/> New stats


#### <a id="vttablet-tablet-type-for-prometheus"/> VTTablet tablet type for Prometheus

VTTablet publishes the `TabletType` status which reports the current type of the tablet. For example, here's what it looks like in the local cluster example after setting up the initial cluster:

```
"TabletType": "primary"
```

Prior to v19, this data was not available via the Prometheus backend. In v19, this is also published as a Prometheus gauge with a `tablet_type` label and a value of `1.0`. For example:

```
$ curl -s localhost:15100/metrics | grep tablet_type{
vttablet_tablet_type{tablet_type="primary"} 1
```
