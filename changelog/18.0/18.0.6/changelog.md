# Changelog of Vitess v18.0.6

### Bug fixes 
#### Docker
 * [release-18.0] Fix the install dependencies script in Docker (#16340) [#16345](https://github.com/vitessio/vitess/pull/16345) 
#### Query Serving
 * [release-18.0] fix: handle info_schema routing (#15899) [#15905](https://github.com/vitessio/vitess/pull/15905)
 * [release-18.0] fix: remove keyspace when merging subqueries (#16019) [#16026](https://github.com/vitessio/vitess/pull/16026)
 * [release-18.0] Fix Incorrect Optimization with LIMIT and GROUP BY (#16263) [#16266](https://github.com/vitessio/vitess/pull/16266)
 * [release-18.0] planner: Handle ORDER BY inside derived tables (#16353) [#16358](https://github.com/vitessio/vitess/pull/16358)
 * [release-18.0] Fix Join Predicate Cleanup Bug in Route Merging (#16386) [#16388](https://github.com/vitessio/vitess/pull/16388) 
#### VReplication
 * [release-18.0] vtctldclient: Apply (Shard | Keyspace| Table) Routing Rules commands don't work (#16096) [#16123](https://github.com/vitessio/vitess/pull/16123)
 * [release-18.0] VDiff CLI: Fix VDiff `show` bug (#16177) [#16197](https://github.com/vitessio/vitess/pull/16197)
 * [release-18.0] VReplication Workflow: set state correctly when restarting workflow streams in the copy phase (#16217) [#16221](https://github.com/vitessio/vitess/pull/16221)
 * [release-18.0] VReplication: Properly handle target shards w/o a primary in Reshard (#16283) [#16290](https://github.com/vitessio/vitess/pull/16290) 
#### VTTablet
 * [18.x] Fix `schemacopy` collation issues [#15859](https://github.com/vitessio/vitess/pull/15859) 
#### VTorc
 * [release-18.0] Add timeout to all the contexts used for RPC calls in vtorc (#15991) [#16104](https://github.com/vitessio/vitess/pull/16104) 
#### vtexplain
 * [release-18.0] Fix `vtexplain` not handling `UNION` queries with `weight_string` results correctly. (#16129) [#16156](https://github.com/vitessio/vitess/pull/16156)
### CI/Build 
#### Build/CI
 * [release-18.0] Add DCO workflow (#16052) [#16055](https://github.com/vitessio/vitess/pull/16055)
 * [release-18.0] Remove DCO workaround (#16087) [#16090](https://github.com/vitessio/vitess/pull/16090)
 * [release-18.0] CI: Fix for xtrabackup install failures (#16329) [#16331](https://github.com/vitessio/vitess/pull/16331) 
#### General
 * [release-18.0] Upgrade the Golang version to `go1.21.11` [#16063](https://github.com/vitessio/vitess/pull/16063)
 * [release-18.0] Upgrade the Golang version to `go1.21.12` [#16320](https://github.com/vitessio/vitess/pull/16320)
### Dependencies 
#### VTAdmin
 * [release-18.0] Update braces package (#16115) [#16117](https://github.com/vitessio/vitess/pull/16117)
### Internal Cleanup 
#### Examples
 * [release-18.0] Update env.sh so that is does not error when running on Mac (#15835) [#15914](https://github.com/vitessio/vitess/pull/15914)
### Performance 
#### VTTablet
 * [release-18.0] Do not load table stats when booting `vttablet`. (#15715) [#16099](https://github.com/vitessio/vitess/pull/16099)
### Regression 
#### Query Serving
 * [release-18.0] fix: insert on duplicate update to add list argument in the bind variables map (#15961) [#15966](https://github.com/vitessio/vitess/pull/15966)
### Release 
#### General
 * [release-18.0] Bump to `v18.0.6-SNAPSHOT` after the `v18.0.5` release [#15888](https://github.com/vitessio/vitess/pull/15888)
 * [release-18.0] Code Freeze for `v18.0.6` [#16444](https://github.com/vitessio/vitess/pull/16444)
### Testing 
#### Query Serving
 * [release-18.0] test: Cleaner plan tests output (#15922) [#15923](https://github.com/vitessio/vitess/pull/15923)
 * [release-18] Vitess tester workflow (#16127) [#16419](https://github.com/vitessio/vitess/pull/16419) 
#### VTCombo
 * [release-18.0] Fix flaky tests that use vtcombo (#16178) [#16211](https://github.com/vitessio/vitess/pull/16211) 
#### vtexplain
 * [release-18.0] Fix flakiness in `vtexplain` unit test case. (#16159) [#16166](https://github.com/vitessio/vitess/pull/16166)

