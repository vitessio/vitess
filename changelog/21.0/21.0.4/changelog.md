# Changelog of Vitess v21.0.4

### Bug fixes 
#### Backup and Restore
 * [release-21.0] Fix tablet selection in `vtctld BackupShard` (#18002) [#18025](https://github.com/vitessio/vitess/pull/18025)
 * Fix backup shard copy paste error [#18100](https://github.com/vitessio/vitess/pull/18100) 
#### Evalengine
 * [release-21.0] Implement temporal comparisons (#17826) [#17854](https://github.com/vitessio/vitess/pull/17854) 
#### Query Serving
 * [release-21.0] Fail assignment expressions with the correct message (#17752) [#17776](https://github.com/vitessio/vitess/pull/17776)
 * backport: support subqueries inside subqueries when merging (Release 21.0) [#17811](https://github.com/vitessio/vitess/pull/17811)
 * [release-21.0] evalengine: normalize types during compilation (#17887) [#17896](https://github.com/vitessio/vitess/pull/17896)
 * [release-21.0] Fix DISTINCT on ENUM/SET columns by making enums/set hashable (#17936) [#17991](https://github.com/vitessio/vitess/pull/17991)
 * [release-21.0] go/vt/vtgate: take routing rules into account for traffic mirroring (#17953) [#17994](https://github.com/vitessio/vitess/pull/17994)
 * [release-21.0] Set proper join vars type for the RHS field query in OLAP (#18028) [#18038](https://github.com/vitessio/vitess/pull/18038)
 * [release-21.0] Bugfix: Missing data when running vtgate outer joins (#18036) [#18044](https://github.com/vitessio/vitess/pull/18044)
 * [release-21.0] Fix: Ensure Consistent Lookup Vindex Handles Duplicate Rows in Single Query (#17974) [#18078](https://github.com/vitessio/vitess/pull/18078)
 * [release-21.0] bugfix: allow window functions when possible to push down (#18103) [#18105](https://github.com/vitessio/vitess/pull/18105) 
#### VReplication
 * [release-21.0] VReplication Atomic Copy Workflows: fix bugs around concurrent inserts (#17772) [#17793](https://github.com/vitessio/vitess/pull/17793)
 * [release-21.0] Multi-tenant workflow SwitchWrites: Don't add denied tables on cancelMigration() (#17782) [#17797](https://github.com/vitessio/vitess/pull/17797)
 * [release-21.0] VDiff: Fix logic for reconciling extra rows (#17950) [#18072](https://github.com/vitessio/vitess/pull/18072)
 * [release-21.0] VStream API: Reset stopPos in catchup (#18119) [#18122](https://github.com/vitessio/vitess/pull/18122) 
#### VTAdmin
 * [release-21.0] [VTAdmin] Insert into schema cache if exists already and not expired (#17908) [#17924](https://github.com/vitessio/vitess/pull/17924) 
#### VTCombo
 * [release-21.0] Fix vtcombo parsing flags incorrectly (#17743) [#17820](https://github.com/vitessio/vitess/pull/17820) 
#### VTTablet
 * [release-21.0] fix: race on storing schema engine last changed time (#17914) [#17917](https://github.com/vitessio/vitess/pull/17917) 
#### vtctldclient
 * [release-21.0] Filter out tablets with unknown replication lag when electing a new primary (#18004) [#18075](https://github.com/vitessio/vitess/pull/18075)
 * [release-21.0] Fix `Reshard Cancel` behavior (#18020) [#18080](https://github.com/vitessio/vitess/pull/18080)
### CI/Build 
#### General
 * [release-21.0] Upgrade the Golang version to `go1.23.6` [#17699](https://github.com/vitessio/vitess/pull/17699)
 * [release-21.0] Upgrade the Golang version to `go1.23.7` [#17901](https://github.com/vitessio/vitess/pull/17901)
 * [release-21.0] Upgrade the Golang version to `go1.23.8` [#18092](https://github.com/vitessio/vitess/pull/18092)
### Dependencies 
#### Build/CI
 * [release-21.0] Bump golang.org/x/net from 0.34.0 to 0.36.0 (#17958) [#17960](https://github.com/vitessio/vitess/pull/17960)
### Performance 
#### Performance
 * [release-21.0] smartconnpool: Better handling for idle expiration (#17757) [#17781](https://github.com/vitessio/vitess/pull/17781) 
#### Query Serving
 * [release-21.0] pool: reopen connection closed by idle timeout (#17818) [#17829](https://github.com/vitessio/vitess/pull/17829)
 * [release-21.0] Fix: Separate Lock for Keyspace to Update Controller Mapping in Schema Tracking (#17873) [#17885](https://github.com/vitessio/vitess/pull/17885)
### Regression 
#### Query Serving
 * [release-21.0] Fix a potential connection pool leak. (#17807) [#17814](https://github.com/vitessio/vitess/pull/17814) 
#### VTTablet
 * [release-21.0] fix: App and Dba Pool metrics (#18048) [#18084](https://github.com/vitessio/vitess/pull/18084)
### Release 
#### General
 * [release-21.0] Bump to `v21.0.4-SNAPSHOT` after the `v21.0.3` release [#17766](https://github.com/vitessio/vitess/pull/17766)
 * [release-21.0] Code Freeze for `v21.0.4` [#18135](https://github.com/vitessio/vitess/pull/18135)
### Testing 
#### Build/CI
 * [release-21.0] Use release branches for upgrade downgrade tests (#18029) [#18035](https://github.com/vitessio/vitess/pull/18035) 
#### Query Serving
 * [release-21.0] DML test fix for duplicate column value  [#17980](https://github.com/vitessio/vitess/pull/17980)
 * [release-21.0] Test: Increase query timeout to fix flaky test 'TestQueryTimeoutWithShardTargeting' (#18016) [#18040](https://github.com/vitessio/vitess/pull/18040)

