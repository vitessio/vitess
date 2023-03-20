# Release of Vitess v15.0.1
## Major Changes

### Corrupted results for non-full-group-by queries with JOINs

An issue in versions `<= v14.0.3` and `<= v15.0.0` that generated corrupted results for non-full-group-by queries with a JOIN
is now fixed. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11625), and its fix [here](https://github.com/vitessio/vitess/pull/11633).

### VtAdmin web folder is missing while installing Vitess with local method

When we try to install Vitess locally (https://vitess.io/docs/15.0/get-started/local/#install-vitess) on `v15.0`, we are getting the following error
```
npm ERR! enoent ENOENT: no such file or directory, open '/home/web/vtadmin/package.json'
```
This issue is fixed in 15.0.1. The full issue can be found [here](https://github.com/vitessio/vitess/issues/11679), and its fix [here](https://github.com/vitessio/vitess/pull/11683).

------------

The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/15.0/15.0.1/changelog.md).

The release includes 25 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @frouioui, @harshit-gangal, @rsajwani, @vitess-bot[bot]

