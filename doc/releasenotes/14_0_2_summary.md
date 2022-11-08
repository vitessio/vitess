## Known Issues

- [Corrupted results for non-full-group-by queries with JOINs](https://github.com/vitessio/vitess/issues/11625). This can be resolved by using full-group-by queries.

## Major Changes

### Upgrade to `go1.18.5`

Vitess `v14.0.2` now runs on `go1.18.5`.
The patch release of Go, `go1.18.5`, was one of the main reasons for this release as it includes important security fixes to packages used by Vitess.
Below is a summary of this patch release. You can learn more [here](https://go.dev/doc/devel/release#go1.18).

> go1.18.4 (released 2022-07-12) includes security fixes to the compress/gzip, encoding/gob, encoding/xml, go/parser, io/fs, net/http, and path/filepath packages, as well as bug fixes to the compiler, the go command, the linker, the runtime, and the runtime/metrics package.

### End of life of MariadDB 10.2

Since the end-of-life of MariaDB 10.2, its Docker image is unavailable, and we decided to remove the unit tests using this version of MariaDB. The Pull Request doing this change is available [here](https://github.com/vitessio/vitess/pull/11042).
You can find more information on the list of supported databases on our documentation website, [here](https://vitess.io/docs/14.0/overview/supported-databases/).
