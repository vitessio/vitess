# Changelog of Vitess v12.0.6

### Bug fixes
#### Query Serving
* Fix AST copying of basic types in release-12.0 [#11053](https://github.com/vitessio/vitess/pull/11053)
* fix: return when instructions are nil in checkThatPlanIsValid [#11070](https://github.com/vitessio/vitess/pull/11070)
#### VReplication
* VStreamer: recompute table plan if a new table is encountered for the same id [#9978](https://github.com/vitessio/vitess/pull/9978)
* VStream API: Fix vtgate memory leaks when context gets cancelled [#10571](https://github.com/vitessio/vitess/pull/10571)
* VStream API: Fix vtgate memory leaks when context gets cancelled [#10923](https://github.com/vitessio/vitess/pull/10923)
### CI/Build
#### Build/CI
* Fix mariadb102 and mariadb103 tests on release-12.0 [#11025](https://github.com/vitessio/vitess/pull/11025)
* Remove MariaDB 10.2 test from release-12.0 [#11074](https://github.com/vitessio/vitess/pull/11074)
### Enhancement
#### General
* Upgrade go version to `1.17.13` on `release-12.0` [#11129](https://github.com/vitessio/vitess/pull/11129)
### Release
#### Documentation
* Addition of the release summary for `v12.0.6` [#11140](https://github.com/vitessio/vitess/pull/11140)
#### General
* Release `v12.0.5` [#10872](https://github.com/vitessio/vitess/pull/10872)
* Include the compose examples in the `do_release` script [#11130](https://github.com/vitessio/vitess/pull/11130)
