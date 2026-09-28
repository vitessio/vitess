# Release of Vitess v15.0.2
## Major Changes

### Upgrade to `go1.18.9`

Vitess `v15.0.2` now runs on `go1.18.9`.
The patch release of Go, `go1.18.9`, was one of the main reasons for this release as it includes an important security fixe to `net/http` package, which is use extensively by Vitess.
Below is a summary of this patch release. You can learn more [here](https://groups.google.com/g/golang-announce/c/L_3rmdT0BMU).

> go1.18.9 (released 2022-12-06) includes security fixes to the net/http and os packages, as well as bug fixes to cgo, the compiler, the runtime, and the crypto/x509 and os/exec packages.


------------

The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/15.0/15.0.2/changelog.md).

The release includes 14 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @dbussink, @frouioui, @mattlord, @rsajwani, @shlomi-noach, @vitess-bot[bot]

