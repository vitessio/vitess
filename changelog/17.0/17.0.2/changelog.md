# Changelog of Vitess v17.0.2

### Bug fixes
#### Backup and Restore
 * [release-17.0] Address vttablet memory usage with backups to Azure Blob Service (#13770) [#13775](https://github.com/vitessio/vitess/pull/13775)
 * [release-17.0] Do not drain tablet in incremental backup (#13773) [#13789](https://github.com/vitessio/vitess/pull/13789)
#### Cluster management
 * [release-17.0] Flaky tests: Fix race in memory topo (#13559) [#13577](https://github.com/vitessio/vitess/pull/13577)
#### Evalengine
 * [release-17.0] Fix a number of encoding issues when evaluating expressions with the evalengine (#13509) [#13551](https://github.com/vitessio/vitess/pull/13551)
 * [release-17.0] fastparse: Fix bug in overflow detection (#13702) [#13705](https://github.com/vitessio/vitess/pull/13705)
#### Online DDL
 * v17 backport: Fix closed channel panic in Online DDL cutover [#13731](https://github.com/vitessio/vitess/pull/13731)
 * v17 backport: Solve RevertMigration.Comment read/write concurrency issue [#13734](https://github.com/vitessio/vitess/pull/13734)
#### Query Serving
 * [release-17.0] Fix flaky vtgate test TestInconsistentStateDetectedBuffering (#13560) [#13575](https://github.com/vitessio/vitess/pull/13575)
 * [release-17.0] vtgate: fix race condition iterating tables and views from schema tracker (#13673) [#13796](https://github.com/vitessio/vitess/pull/13796)
### CI/Build
#### Backup and Restore
 * [release-17.0] Fixing `backup_pitr` flaky tests via wait-for loop on topo reads (#13781) [#13790](https://github.com/vitessio/vitess/pull/13790)
#### Online DDL
 * [release-17.0] CI: fix onlineddl_scheduler flakiness (#13754) [#13760](https://github.com/vitessio/vitess/pull/13760)
### Release
#### General
 * Back to dev mode after v17.0.1 [#13663](https://github.com/vitessio/vitess/pull/13663)
### Testing
#### Build/CI
 * [release-17.0] Flakes: Delete VTDATAROOT files in reparent test teardown within CI (#13793) [#13798](https://github.com/vitessio/vitess/pull/13798)
