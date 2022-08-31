## Major Changes

### Upgrade to `go1.17.13`

Vitess `v12.0.6` now runs on `go1.17.13`.
The patch release of Go, `go1.17.13`, was the main reason for this release as it includes important security fixes to packages used by Vitess.
Below is a summary of this patch release. You can learn more [here](https://go.dev/doc/devel/release#go1.17).

> go1.17.13 (released 2022-08-01) includes security fixes to the encoding/gob and math/big packages, as well as bug fixes to the compiler and the runtime.

### End of life of MariadDB 10.2

Since the end-of-life of MariaDB 10.2, its Docker image is unavailable, and we decided to remove the unit tests using this version of MariaDB. The Pull Request doing this change is available [here](https://github.com/vitessio/vitess/pull/11074).
This change is documented on our website [here](https://vitess.io/docs/12.0/overview/supported-databases/#mariadb-versions-100-to-103).

