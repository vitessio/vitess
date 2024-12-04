# Changelog of Vitess v19.0.7

### Bug fixes 
#### Backup and Restore
 * [release-19.0] Fail fast when builtinbackup fails to restore a single file (#16856) [#16867](https://github.com/vitessio/vitess/pull/16867) 
#### Query Serving
 * Backport: Fix ACL checks for CTEs (#16642) [#16776](https://github.com/vitessio/vitess/pull/16776)
 * [release-19.0] VTTablet: smartconnpool: notify all expired waiters (#16897) [#16901](https://github.com/vitessio/vitess/pull/16901)
 * [release-19.0] fixes bugs around expression precedence and LIKE (#16934 & #16649) [#16945](https://github.com/vitessio/vitess/pull/16945)
 * [release-19.0] bugfix: add HAVING columns inside derived tables (#16976) [#16978](https://github.com/vitessio/vitess/pull/16978)
 * [release-19.0] bugfix: treat EXPLAIN like SELECT (#17054) [#17056](https://github.com/vitessio/vitess/pull/17056)
 * [release-19.0] Delegate Column Availability Checks to MySQL for Single-Route Queries (#17077) [#17085](https://github.com/vitessio/vitess/pull/17085)
 * Bugfix for Panic on Joined Queries with Non-Authoritative Tables in Vitess 19.0 [#17103](https://github.com/vitessio/vitess/pull/17103) 
#### VTAdmin
 * [release-19.0] VTAdmin: Fix serve-handler's path-to-regexp dep and add default schema refresh (#16778) [#16783](https://github.com/vitessio/vitess/pull/16783) 
#### VTGate
 * [release-19.0] Support passing filters to `discovery.NewHealthCheck(...)` (#16170) [#16871](https://github.com/vitessio/vitess/pull/16871)
 * [release-19.0] Fix deadlock between health check and topology watcher (#16995) [#17008](https://github.com/vitessio/vitess/pull/17008) 
#### VTTablet
 * [release-19.0] Fix race in `replicationLagModule` of `go/vt/throttle` (#16078) [#16899](https://github.com/vitessio/vitess/pull/16899)
### CI/Build 
#### Docker
 * [release-19.0] Remove mysql57 from docker images [#16763](https://github.com/vitessio/vitess/pull/16763) 
#### General
 * [release-19.0] Upgrade Golang to 1.22.8 [#16895](https://github.com/vitessio/vitess/pull/16895)
### Dependencies 
#### Java
 * [release-19.0] Bump com.google.protobuf:protobuf-java from 3.24.3 to 3.25.5 in /java (#16809) [#16837](https://github.com/vitessio/vitess/pull/16837)
 * [release-19.0] Bump commons-io:commons-io from 2.7 to 2.14.0 in /java (#16889) [#16930](https://github.com/vitessio/vitess/pull/16930) 
#### VTAdmin
 * [release-19.0] VTAdmin: Address security vuln in path-to-regexp node pkg (#16770) [#16772](https://github.com/vitessio/vitess/pull/16772)
### Enhancement 
#### Online DDL
 * [release-19.0] Improve Schema Engine's TablesWithSize80 query (#17066) [#17089](https://github.com/vitessio/vitess/pull/17089)
### Internal Cleanup 
#### VTAdmin
 * [release-19.0] VTAdmin: Upgrade deps to address security vulns (#16843) [#16846](https://github.com/vitessio/vitess/pull/16846)
### Regression 
#### Backup and Restore
 * [release-19.0] Fix unreachable errors when taking a backup (#17062) [#17110](https://github.com/vitessio/vitess/pull/17110) 
#### Query Serving
 * [release-19.0] fix: route engine to handle column truncation for execute after lookup (#16981) [#16984](https://github.com/vitessio/vitess/pull/16984)
 * [release-19.0] Add support for `MultiEqual` opcode for lookup vindexes. (#16975) [#17039](https://github.com/vitessio/vitess/pull/17039)
### Release 
#### General
 * [release-19.0] Code Freeze for `v19.0.7` [#17148](https://github.com/vitessio/vitess/pull/17148)
### Testing 
#### Cluster management
 * [release-19.0] Flaky test fixes (#16940) [#16958](https://github.com/vitessio/vitess/pull/16958)

