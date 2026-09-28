# Changelog of Vitess v20.0.7

### Bug fixes 
#### Backup and Restore
 * [release-20.0] Fix tablet selection in `vtctld BackupShard` (#18002) [#18024](https://github.com/vitessio/vitess/pull/18024)
 * Fix backup shard copy paste error [#18099](https://github.com/vitessio/vitess/pull/18099) 
#### Cluster management
 * [release-20.0] ValidateKeyspace: Add check for no shards  (#18055) [#18062](https://github.com/vitessio/vitess/pull/18062) 
#### Evalengine
 * [release-20.0] Implement temporal comparisons (#17826) [#17853](https://github.com/vitessio/vitess/pull/17853) 
#### Query Serving
 * backport: support subqueries inside subqueries when merging (Release 20.0) [#17810](https://github.com/vitessio/vitess/pull/17810)
 * [release-20.0] evalengine: normalize types during compilation (#17887) [#17895](https://github.com/vitessio/vitess/pull/17895)
 * [release-20.0] Fix DISTINCT on ENUM/SET columns by making enums/set hashable (#17936) [#17990](https://github.com/vitessio/vitess/pull/17990)
 * [release-20.0] Set proper join vars type for the RHS field query in OLAP (#18028) [#18037](https://github.com/vitessio/vitess/pull/18037)
 * [release-20.0] Bugfix: Missing data when running vtgate outer joins (#18036) [#18043](https://github.com/vitessio/vitess/pull/18043)
 * [release-20.0] bugfix: allow window functions when possible to push down (#18103) [#18104](https://github.com/vitessio/vitess/pull/18104) 
#### VReplication
 * [release-20.0] VReplication Atomic Copy Workflows: fix bugs around concurrent inserts (#17772) [#17792](https://github.com/vitessio/vitess/pull/17792)
 * [release-20.0] Multi-tenant workflow SwitchWrites: Don't add denied tables on cancelMigration() (#17782) [#17796](https://github.com/vitessio/vitess/pull/17796)
 * [release-20.0] VDiff: Fix logic for reconciling extra rows (#17950) [#18071](https://github.com/vitessio/vitess/pull/18071)
 * [release-20.0] VStream API: Reset stopPos in catchup (#18119) [#18121](https://github.com/vitessio/vitess/pull/18121) 
#### VTTablet
 * [release-20.0] fix: race on storing schema engine last changed time (#17914) [#17916](https://github.com/vitessio/vitess/pull/17916) 
#### vtctldclient
 * [release-20.0] Filter out tablets with unknown replication lag when electing a new primary (#18004) [#18074](https://github.com/vitessio/vitess/pull/18074)
 * [release-20.0] Fix `Reshard Cancel` behavior (#18020) [#18079](https://github.com/vitessio/vitess/pull/18079)
### CI/Build 
#### General
 * [release-20.0] Upgrade the Golang version to `go1.22.12` [#17702](https://github.com/vitessio/vitess/pull/17702)
### Performance 
#### Performance
 * [release-20.0] smartconnpool: Better handling for idle expiration (#17757) [#17780](https://github.com/vitessio/vitess/pull/17780) 
#### Query Serving
 * [release-20.0] pool: reopen connection closed by idle timeout (#17818) [#17830](https://github.com/vitessio/vitess/pull/17830)
 * [release-20.0] Fix: Separate Lock for Keyspace to Update Controller Mapping in Schema Tracking (#17873) [#17884](https://github.com/vitessio/vitess/pull/17884)
### Regression 
#### Query Serving
 * [release-20.0] Fix a potential connection pool leak. (#17807) [#17813](https://github.com/vitessio/vitess/pull/17813) 
#### VTTablet
 * [release-20.0] fix: App and Dba Pool metrics (#18048) [#18083](https://github.com/vitessio/vitess/pull/18083)
### Release 
#### General
 * [release-20.0] Bump to `v20.0.7-SNAPSHOT` after the `v20.0.6` release [#17768](https://github.com/vitessio/vitess/pull/17768)
### Testing 
#### Build/CI
 * [release-20.0] Use release branches for upgrade downgrade tests (#18029) [#18034](https://github.com/vitessio/vitess/pull/18034) 
#### Query Serving
 * [release-20.0] [release-21.0] DML test fix for duplicate column value  (#17980) [#17987](https://github.com/vitessio/vitess/pull/17987)
 * [release-20.0] Test: Increase query timeout to fix flaky test 'TestQueryTimeoutWithShardTargeting' (#18016) [#18039](https://github.com/vitessio/vitess/pull/18039)

