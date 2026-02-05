# Changelog of Vitess v20.0.3

### Bug fixes 
#### Backup and Restore
 * [release-20.0] Fail fast when builtinbackup fails to restore a single file (#16856) [#16868](https://github.com/vitessio/vitess/pull/16868) 
#### Query Serving
 * [release-20.0] VTGate MoveTables Buffering: Fix panic when buffering is disabled (#16922) [#16935](https://github.com/vitessio/vitess/pull/16935)
 * [release-20.0] fixes bugs around expression precedence and LIKE (#16934 & #16649) [#16946](https://github.com/vitessio/vitess/pull/16946)
 * [release-20.0] bugfix: add HAVING columns inside derived tables (#16976) [#16979](https://github.com/vitessio/vitess/pull/16979)
 * [release-20.0] bugfix: treat EXPLAIN like SELECT (#17054) [#17057](https://github.com/vitessio/vitess/pull/17057)
 * [release-20.0] Delegate Column Availability Checks to MySQL for Single-Route Queries (#17077) [#17086](https://github.com/vitessio/vitess/pull/17086) 
#### VReplication
 * [release-20.0] Migrate Workflow: Scope vindex names correctly when target and source keyspace have different names (#16769) [#16815](https://github.com/vitessio/vitess/pull/16815) 
#### VTAdmin
 * [release-20.0] VTAdmin: Fix serve-handler's path-to-regexp dep and add default schema refresh (#16778) [#16784](https://github.com/vitessio/vitess/pull/16784) 
#### VTGate
 * [release-20.0] Support passing filters to `discovery.NewHealthCheck(…)` (#16170) [#16872](https://github.com/vitessio/vitess/pull/16872)
 * [release-20.0] Fix deadlock between health check and topology watcher (#16995) [#17009](https://github.com/vitessio/vitess/pull/17009) 
#### VTTablet
 * [release-20.0] Fix race in `replicationLagModule` of `go/vt/throttle` (#16078) [#16900](https://github.com/vitessio/vitess/pull/16900)
### CI/Build 
#### General
 * [release-20.0] Upgrade Golang to 1.22.8 [#16894](https://github.com/vitessio/vitess/pull/16894)
### Dependencies 
#### Java
 * [release-20.0] Bump com.google.protobuf:protobuf-java from 3.24.3 to 3.25.5 in /java (#16809) [#16838](https://github.com/vitessio/vitess/pull/16838)
 * [release-20.0] Bump commons-io:commons-io from 2.7 to 2.14.0 in /java (#16889) [#16931](https://github.com/vitessio/vitess/pull/16931) 
#### VTAdmin
 * [release-20.0] VTAdmin: Address security vuln in path-to-regexp node pkg (#16770) [#16773](https://github.com/vitessio/vitess/pull/16773)
### Enhancement 
#### Build/CI
 * [release-20.0] Change upgrade test to still use the older version of tests (#16937) [#16969](https://github.com/vitessio/vitess/pull/16969) 
#### Online DDL
 * [release-20.0] Improve Schema Engine's TablesWithSize80 query (#17066) [#17090](https://github.com/vitessio/vitess/pull/17090)
### Internal Cleanup 
#### VTAdmin
 * [release-20.0] VTAdmin: Upgrade deps to address security vulns (#16843) [#16847](https://github.com/vitessio/vitess/pull/16847)
### Regression 
#### Backup and Restore
 * [release-20.0] Fix unreachable errors when taking a backup (#17062) [#17111](https://github.com/vitessio/vitess/pull/17111) 
#### Query Serving
 * [release-20.0] fix: route engine to handle column truncation for execute after lookup (#16981) [#16985](https://github.com/vitessio/vitess/pull/16985)
 * [release-20.0] Add support for `MultiEqual` opcode for lookup vindexes. (#16975) [#17040](https://github.com/vitessio/vitess/pull/17040)
### Release 
#### General
 * [release-20.0] Code Freeze for `v20.0.3` [#17145](https://github.com/vitessio/vitess/pull/17145)
### Testing 
#### Cluster management
 * [release-20.0] Flaky test fixes (#16940) [#16959](https://github.com/vitessio/vitess/pull/16959)

