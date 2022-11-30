# Release of Vitess v15.0.1
## Major Changes

### Corrupted results for non-full-group-by queries with JOINs

An issue in versions `<= v14.0.3` and `<= v15.0.0` that generated corrupted results for non-full-group-by queries with a JOIN
is now fixed. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11625), and its fix [here](https://github.com/vitessio/vitess/pull/11633).
------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/doc/releasenotes/15_0_1_changelog.md).

The release includes 25 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @frouioui, @harshit-gangal, @rsajwani, @vitess-bot[bot]

