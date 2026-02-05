# Changelog of Vitess v16.0.4

### Bug fixes 
#### Backup and Restore
 * Manual cherry-pick of 13339 [#13733](https://github.com/vitessio/vitess/pull/13733)
 * [release-16.0] Address vttablet memory usage with backups to Azure Blob Service (#13770) [#13774](https://github.com/vitessio/vitess/pull/13774) 
#### Online DDL
 * v16 backport: Fix closed channel panic in Online DDL cutover [#13732](https://github.com/vitessio/vitess/pull/13732)
 * v16 backport: Solve RevertMigration.Comment read/write concurrency issue [#13736](https://github.com/vitessio/vitess/pull/13736) 
#### Query Serving
 * planbuilder: Fix infinite recursion for subqueries [#13783](https://github.com/vitessio/vitess/pull/13783)
 * [release-16.0] vtgate: fix race condition iterating tables and views from schema tracker (#13673) [#13795](https://github.com/vitessio/vitess/pull/13795)
 * [16.0] bugfixes: collection of fixes to bugs found while fuzzing [#13805](https://github.com/vitessio/vitess/pull/13805)
### CI/Build 
#### Online DDL
 * [release-16.0] CI: fix onlineddl_scheduler flakiness (#13754) [#13759](https://github.com/vitessio/vitess/pull/13759)
### Release 
#### General
 * Back to dev mode after v16.0.3 [#13660](https://github.com/vitessio/vitess/pull/13660)
 * Release 16.0 code freeze for `v16.0.3` release [#13810](https://github.com/vitessio/vitess/pull/13810)
### Testing 
#### Build/CI
 * [release-16.0] Flakes: Delete VTDATAROOT files in reparent test teardown within CI (#13793) [#13797](https://github.com/vitessio/vitess/pull/13797)

