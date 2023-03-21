# Release of Vitess v13.0.2
## Major Changes

### Upgrade to `go1.17.12`

Vitess `v13.0.2` now runs on `go1.17.12`.
The patch release of Go, `go1.17.12`, was one of main motivations for this release as it includes important security fixes to packages used by Vitess.
Below is a summary of this patch release. You can learn more [here](https://go.dev/doc/devel/release#go1.17).

> go1.17.12 (released 2022-07-12) includes security fixes to the compress/gzip, encoding/gob, encoding/xml, go/parser, io/fs, net/http, and path/filepath packages, as well as bug fixes to the compiler, the go command, the runtime, and the runtime/metrics package. [See the Go 1.17.12 milestone](https://github.com/golang/go/issues?q=milestone%3AGo1.17.12+label%3ACherryPickApproved) on our issue tracker for details.
------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/13.0/13.0.2/changelog.md).
The release includes 28 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @aquarapid, @frouioui, @harshit-gangal, @mattlord, @rohit-nayak-ps, @systay, @vitess-bot[bot], @vmg