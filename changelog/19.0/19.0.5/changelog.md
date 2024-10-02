# Changelog of Vitess v19.0.5

### Bug fixes 
#### Cluster management
 * [release-19.0] Use default schema reload config values when config file is empty (#16393) [#16410](https://github.com/vitessio/vitess/pull/16410) 
#### Docker
 * [release-19.0] Fix the install dependencies script in Docker (#16340) [#16346](https://github.com/vitessio/vitess/pull/16346) 
#### Documentation
 * [release-19.0] Fix the `v19.0.0` release notes and use the `vitess/lite` image for the MySQL container (#16282) [#16285](https://github.com/vitessio/vitess/pull/16285) 
#### Online DDL
 * [release-19.0] Online DDL shadow table: rename referenced table name in self referencing FK (#16205) [#16207](https://github.com/vitessio/vitess/pull/16207) 
#### Query Serving
 * [release-19.0] fix: handle info_schema routing (#15899) [#15906](https://github.com/vitessio/vitess/pull/15906)
 * [release-19.0] connpool: Allow time out during shutdown (#15979) [#16003](https://github.com/vitessio/vitess/pull/16003)
 * [release-19.0] fix: remove keyspace when merging subqueries (#16019) [#16027](https://github.com/vitessio/vitess/pull/16027)
 * [release-19.0] Handle Nullability for Columns from Outer Tables (#16174) [#16185](https://github.com/vitessio/vitess/pull/16185)
 * [release-19.0] Fix vtgate crash in group concat  [#16254](https://github.com/vitessio/vitess/pull/16254)
 * [release-19.0] Fix Incorrect Optimization with LIMIT and GROUP BY (#16263) [#16267](https://github.com/vitessio/vitess/pull/16267)
 * [release-19.0] planner: Handle ORDER BY inside derived tables (#16353) [#16359](https://github.com/vitessio/vitess/pull/16359)
 * [release-19.0] fix issue with aggregation inside of derived tables (#16366) [#16384](https://github.com/vitessio/vitess/pull/16384)
 * [release-19.0] Fix Join Predicate Cleanup Bug in Route Merging (#16386) [#16389](https://github.com/vitessio/vitess/pull/16389)
 * [release-19.0] Fix panic in schema tracker in presence of keyspace routing rules (#16383) [#16406](https://github.com/vitessio/vitess/pull/16406)
 * [release-19.0] Fix subquery planning having an aggregation that is used in order by as long as we can merge it all into a single route (#16402) [#16407](https://github.com/vitessio/vitess/pull/16407) 
#### VReplication
 * [release-19.0] vtctldclient: Apply (Shard | Keyspace| Table) Routing Rules commands don't work (#16096) [#16124](https://github.com/vitessio/vitess/pull/16124)
 * [release-19.0] VDiff CLI: Fix VDiff `show` bug (#16177) [#16198](https://github.com/vitessio/vitess/pull/16198)
 * [release-19.0] VReplication Workflow: set state correctly when restarting workflow streams in the copy phase (#16217) [#16222](https://github.com/vitessio/vitess/pull/16222)
 * [release-19.0] VReplication: Properly handle target shards w/o a primary in Reshard (#16283) [#16291](https://github.com/vitessio/vitess/pull/16291) 
#### VTorc
 * [release-19.0] Add timeout to all the contexts used for RPC calls in vtorc (#15991) [#16103](https://github.com/vitessio/vitess/pull/16103) 
#### vtexplain
 * [release-19.0] Fix `vtexplain` not handling `UNION` queries with `weight_string` results correctly. (#16129) [#16157](https://github.com/vitessio/vitess/pull/16157)
### CI/Build 
#### Build/CI
 * [release-19.0] Add DCO workflow (#16052) [#16056](https://github.com/vitessio/vitess/pull/16056)
 * [release-19.0] Remove DCO workaround (#16087) [#16091](https://github.com/vitessio/vitess/pull/16091)
 * [release-19.0] CI: Fix for xtrabackup install failures (#16329) [#16332](https://github.com/vitessio/vitess/pull/16332) 
#### General
 * [release-19.0] Upgrade the Golang version to `go1.22.4` [#16061](https://github.com/vitessio/vitess/pull/16061)
 * [release-19.0] Upgrade the Golang version to `go1.22.5` [#16322](https://github.com/vitessio/vitess/pull/16322) 
#### VTAdmin
 * [release-19.0] Update VTAdmin build script (#15839) [#15850](https://github.com/vitessio/vitess/pull/15850)
### Dependencies 
#### VTAdmin
 * [release-19.0] Update braces package (#16115) [#16118](https://github.com/vitessio/vitess/pull/16118)
### Internal Cleanup 
#### Examples
 * [release-19.0] Update env.sh so that is does not error when running on Mac (#15835) [#15915](https://github.com/vitessio/vitess/pull/15915)
### Performance 
#### VTTablet
 * [release-19.0] Do not load table stats when booting `vttablet`. (#15715) [#16100](https://github.com/vitessio/vitess/pull/16100)
### Regression 
#### Query Serving
 * [release-19.0] fix: derived table join column expression to be part of add join predicate on rewrite (#15956) [#15960](https://github.com/vitessio/vitess/pull/15960)
 * [release-19.0] fix: insert on duplicate update to add list argument in the bind variables map (#15961) [#15967](https://github.com/vitessio/vitess/pull/15967)
 * [release-19.0] fix: order by subquery planning (#16049) [#16132](https://github.com/vitessio/vitess/pull/16132)
 * [release-19.0] feat: add a LIMIT 1 on EXISTS subqueries to limit network overhead (#16153) [#16191](https://github.com/vitessio/vitess/pull/16191)
### Release 
#### General
 * [release-19.0] Bump to `v19.0.5-SNAPSHOT` after the `v19.0.4` release [#15889](https://github.com/vitessio/vitess/pull/15889)
### Testing 
#### Build/CI
 * Run more test on release-19 branch [#16152](https://github.com/vitessio/vitess/pull/16152) 
#### Query Serving
 * [release-19.0] test: Cleaner plan tests output (#15922) [#15964](https://github.com/vitessio/vitess/pull/15964)
 * [release-19] Vitess tester workflow (#16127) [#16418](https://github.com/vitessio/vitess/pull/16418) 
#### VTCombo
 * [release-19.0] Fix flaky tests that use vtcombo (#16178) [#16212](https://github.com/vitessio/vitess/pull/16212) 
#### vtexplain
 * [release-19.0] Fix flakiness in `vtexplain` unit test case. (#16159) [#16167](https://github.com/vitessio/vitess/pull/16167)

