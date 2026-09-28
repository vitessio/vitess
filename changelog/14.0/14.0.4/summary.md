## Major Changes

### Upgrade to `go1.18.7`

Vitess `v14.0.4` now runs on `go1.18.7`.
The patch release of Go, `go1.18.7`, was one of the main reasons for this release as it includes important security fixes to packages used by Vitess.
Below is a summary of this patch release. You can learn more [here](https://go.dev/doc/devel/release#go1.18).

> go1.18.7 (released 2022-10-04) includes security fixes to the archive/tar, net/http/httputil, and regexp packages, as well as bug fixes to the compiler, the linker, and the go/types package.

### Corrupted results for non-full-group-by queries with JOINs

An issue in versions `<= v14.0.3` and `<= v15.0.0` that generated corrupted results for non-full-group-by queries with a JOIN
is now fixed. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11625), and its fix [here](https://github.com/vitessio/vitess/pull/11633).