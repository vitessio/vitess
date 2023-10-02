## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Docker](#docker)**
      - [New MySQL Image](#mysql-image)

## <a id="major-changes"/>Major Changes

### <a id="docker"/>Docker

#### <a id="mysql-image"/>New MySQL Image

In `v19.0` the Vitess team is shipping a new image: `vitess/mysql`.
This lightweight image is a replacement of `vitess/lite` to only run `mysqld`.

Several tags are available to let you choose what version of MySQL you want to use: `vitess/mysql:8.0.30`, `vitess/mysql:8.0.34`.
