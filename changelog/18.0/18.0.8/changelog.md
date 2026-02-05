# Changelog of Vitess v18.0.8

### Bug fixes 
#### Backup and Restore
 * [release-18.0] Fail fast when builtinbackup fails to restore a single file (#16856) [#16866](https://github.com/vitessio/vitess/pull/16866) 
#### Query Serving
 * [release-18.0] fixes bugs around expression precedence and LIKE (#16934 & #16649) [#16944](https://github.com/vitessio/vitess/pull/16944)
 * [release-18.0] fix issue with aggregation inside of derived tables [#16948](https://github.com/vitessio/vitess/pull/16948)
 * [release-18.0] Planner Bug: Joins inside derived table (#14974) [#16962](https://github.com/vitessio/vitess/pull/16962)
 * [release-18.0] bugfix: add HAVING columns inside derived tables (#16976) [#16977](https://github.com/vitessio/vitess/pull/16977)
 * [release-18.0] Delegate Column Availability Checks to MySQL for Single-Route Queries (#17077) [#17084](https://github.com/vitessio/vitess/pull/17084)
 * bugfix: make sure to split predicates before planning them [#17099](https://github.com/vitessio/vitess/pull/17099)
 * [release-18.0] Bugfix for Panic on Joined Queries with Non-Authoritative Tables in Vitess 19.0 (#17103) [#17115](https://github.com/vitessio/vitess/pull/17115) 
#### VTAdmin
 * [release-18.0] VTAdmin: Fix serve-handler's path-to-regexp dep and add default schema refresh (#16778) [#16782](https://github.com/vitessio/vitess/pull/16782) 
#### VTGate
 * [release-18.0] Fix deadlock between health check and topology watcher (#16995) [#17007](https://github.com/vitessio/vitess/pull/17007) 
#### VTTablet
 * [release-18.0] Fix race in `replicationLagModule` of `go/vt/throttle` (#16078) [#16898](https://github.com/vitessio/vitess/pull/16898)
### Dependencies 
#### Java
 * [release-18.0] Bump com.google.protobuf:protobuf-java from 3.24.3 to 3.25.5 in /java (#16809) [#16836](https://github.com/vitessio/vitess/pull/16836)
 * [release-18.0] Bump commons-io:commons-io from 2.7 to 2.14.0 in /java (#16889) [#16929](https://github.com/vitessio/vitess/pull/16929) 
#### VTAdmin
 * [release-18.0] VTAdmin: Address security vuln in path-to-regexp node pkg (#16770) [#16771](https://github.com/vitessio/vitess/pull/16771)
### Documentation 
#### Build/CI
 * [Direct PR] [release-18.0]: Add `sidecardb` known issue [#17096](https://github.com/vitessio/vitess/pull/17096)
### Enhancement 
#### Online DDL
 * [release-18.0] Improve Schema Engine's TablesWithSize80 query (#17066) [#17088](https://github.com/vitessio/vitess/pull/17088)
### Internal Cleanup 
#### VTAdmin
 * [release-18.0] VTAdmin: Upgrade deps to address security vulns (#16843) [#16845](https://github.com/vitessio/vitess/pull/16845)
### Regression 
#### Backup and Restore
 * [release-18.0] Fix unreachable errors when taking a backup (#17062) [#17109](https://github.com/vitessio/vitess/pull/17109) 
#### Query Serving
 * [release-18.0] fix: route engine to handle column truncation for execute after lookup (#16981) [#16983](https://github.com/vitessio/vitess/pull/16983)
 * [release-18.0] Add support for `MultiEqual` opcode for lookup vindexes. (#16975) [#17038](https://github.com/vitessio/vitess/pull/17038)
### Release 
#### General
 * [release-18.0] Bump to `v18.0.8-SNAPSHOT` after the `v18.0.7` release [#16748](https://github.com/vitessio/vitess/pull/16748)
### Testing 
#### Cluster management
 * [release-18.0] Flaky test fixes (#16940) [#16957](https://github.com/vitessio/vitess/pull/16957)

