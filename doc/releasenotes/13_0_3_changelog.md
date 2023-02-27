# Changelog of Vitess v13.0.3

### Bug fixes
#### Backup and Restore
* BugFix: Using backups from v14 should work with v13 [#10917](https://github.com/vitessio/vitess/pull/10917)
#### Query Serving
* Only use the special NoRoute handling mode when needed [#11020](https://github.com/vitessio/vitess/pull/11020)
* Fix AST copying of basic types in release-13.0 [#11052](https://github.com/vitessio/vitess/pull/11052)
* fix: return when instructions are nil in checkThatPlanIsValid [#11070](https://github.com/vitessio/vitess/pull/11070)
#### VReplication
* VStreamer: recompute table plan if a new table is encountered for the same id [#9978](https://github.com/vitessio/vitess/pull/9978)
### CI/Build
#### Backup and Restore
* Backport: Pin MySQL at 8.0.29 for upgrade/downgrade manual backup test [#10922](https://github.com/vitessio/vitess/pull/10922)
#### Build/CI
* Add more robust go version handling [#11001](https://github.com/vitessio/vitess/pull/11001)
* Fix mariadb103 ci [#11015](https://github.com/vitessio/vitess/pull/11015)
* Add upgrade-downgrade tests for next releases (release-13.0) [#11040](https://github.com/vitessio/vitess/pull/11040)
* Remove MariaDB 10.2 test from release-13.0 [#11073](https://github.com/vitessio/vitess/pull/11073)
#### Query Serving
* CI Fix: Collation tests [#10839](https://github.com/vitessio/vitess/pull/10839)
### Enhancement
#### Build/CI
* Skip CI workflows on `push` for pull requests [#10768](https://github.com/vitessio/vitess/pull/10768)
#### General
* Upgrade go version to `1.17.13` on `release-13.0` [#11145](https://github.com/vitessio/vitess/pull/11145)
### Release
#### Documentation
* Addition of the release summary for `v13.0.3` [#11141](https://github.com/vitessio/vitess/pull/11141)
#### General
* Post release `v13.0.2` [#10848](https://github.com/vitessio/vitess/pull/10848)
* Include the compose examples in the `do_release` script [#11130](https://github.com/vitessio/vitess/pull/11130)
### Testing
#### Query Serving
* Remove an unwanted change made in #11078 to the upgrade downgrade tests [#11143](https://github.com/vitessio/vitess/pull/11143)

