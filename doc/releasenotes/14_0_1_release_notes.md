# Release of Vitess v14.0.1
## Major Changes

### Upgrade to `go1.18.4`

Vitess `v14.0.1` now runs on `go1.18.4`.
The patch release of Go, `go1.18.4`, was one of main motivations for this release as it includes important security fixes to packages used by Vitess.
Below is a summary of this patch release. You can learn more [here](https://go.dev/doc/devel/release#go1.18).

> go1.18.4 (released 2022-07-12) includes security fixes to the compress/gzip, encoding/gob, encoding/xml, go/parser, io/fs, net/http, and path/filepath packages, as well as bug fixes to the compiler, the go command, the linker, the runtime, and the runtime/metrics package. [See the Go 1.18.4 milestone](https://github.com/golang/go/issues?q=milestone%3AGo1.18.4+label%3ACherryPickApproved) on our issue tracker for details.
------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/doc/releasenotes/14_0_1_changelog.md).

The release includes 25 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @deepthi, @frouioui, @harshit-gangal, @mattlord, @rohit-nayak-ps, @shlomi-noach, @vitess-bot[bot]
