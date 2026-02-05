# Changelog of Vitess v16.0.3

### Bug fixes 
#### Cluster management
 * [release-16.0] Prevent resetting replication every time we set replication source (#13377) [#13392](https://github.com/vitessio/vitess/pull/13392)
 * [release-16.0] Don't run any reparent commands if the host is empty (#13396) [#13402](https://github.com/vitessio/vitess/pull/13402)
 * [release-16.0] Upgrade-Downgrade Fix: Schema-initialization stuck on semi-sync ACKs while upgrading (#13411) [#13441](https://github.com/vitessio/vitess/pull/13441)
 * [release-16.0] Flaky tests: Fix race in memory topo (#13559) [#13576](https://github.com/vitessio/vitess/pull/13576)
 * [release-16.0] ignore all error for views in engine reload (#13590) [#13593](https://github.com/vitessio/vitess/pull/13593)
 * [release-16.0] check keyspace snapshot time if none specified for backup restores (#13557) [#13634](https://github.com/vitessio/vitess/pull/13634) 
#### Examples
 * [release-16.0] `examples/compose`: fix `consul:latest` error w/`docker-compose up -d` (#13468) [#13472](https://github.com/vitessio/vitess/pull/13472) 
#### Operator
 * [release-16.0] Upgrade mysqld memory limits to 1024Mi (#13122) [#13204](https://github.com/vitessio/vitess/pull/13204) 
#### Query Serving
 * [release-16.0] Fix the resilientQuery to give correct results during initialization (#13080) [#13087](https://github.com/vitessio/vitess/pull/13087)
 * [16.0] evalengine: TypeOf for Columns should only use value type when we have a value [#13154](https://github.com/vitessio/vitess/pull/13154)
 * [release-16.0] Remove indentation limit in the sqlparser (#13158) [#13166](https://github.com/vitessio/vitess/pull/13166)
 * Fix: errant GTID in health streamer [#13184](https://github.com/vitessio/vitess/pull/13184)
 * [16.0] Fix: TabletServer ReserveBeginExecute to return transaction ID on error [#13193](https://github.com/vitessio/vitess/pull/13193)
 * [release-16.0] Bug fix: SQL queries erroring with message `unknown aggregation random` (#13330) [#13334](https://github.com/vitessio/vitess/pull/13334)
 * [release-16.0] ignore ongoing backfill vindex from routing selection (#13523) [#13607](https://github.com/vitessio/vitess/pull/13607) 
#### Schema Tracker
 * [release-16.0] Ignore error while reading table data in Schema.Engine reload (#13421) [#13424](https://github.com/vitessio/vitess/pull/13424)
 * Backport v16: schema.Reload(): ignore column reading errors for views only, error for tables #13442 [#13456](https://github.com/vitessio/vitess/pull/13456) 
#### TabletManager
 * [release-16.0] mysqlctl: Correctly encode database and table names (#13312) [#13323](https://github.com/vitessio/vitess/pull/13323) 
#### VReplication
 * [release-16.0] VReplication: Do not delete sharded target vschema table entries on Cancel (#13146) [#13155](https://github.com/vitessio/vitess/pull/13155)
 * [release-16.0] VReplication: Pass on --keep_routing_rules flag value for Cancel action (#13171) [#13194](https://github.com/vitessio/vitess/pull/13194)
 * [release-16.0] VReplication: Fix VDiff2 DeleteByUUID Query (#13255) [#13282](https://github.com/vitessio/vitess/pull/13282)
 * [release-16.0] VReplication: Ensure ROW events are sent within a transaction (#13547) [#13580](https://github.com/vitessio/vitess/pull/13580)
### CI/Build 
#### General
 * [release-16.0] Upgrade the Golang version to `go1.20.4` [#13053](https://github.com/vitessio/vitess/pull/13053)
### Documentation 
#### Documentation
 * [release-16.0] update link for reparenting guide (#13350) [#13356](https://github.com/vitessio/vitess/pull/13356)
### Enhancement 
#### Build/CI
 * [release-16.0] Set the number of threads for release notes generation with a flag [#13316](https://github.com/vitessio/vitess/pull/13316)
### Performance 
#### TabletManager
 * [release-16.0] BaseShowTablesWithSizes: optimize MySQL 8.0 query (#13375) [#13389](https://github.com/vitessio/vitess/pull/13389)
### Release 
#### Build/CI
 * [release-16.0] Optimize release notes generation to use GitHub Milestones (#13398) [#13621](https://github.com/vitessio/vitess/pull/13621) 
#### Documentation
 * [release-16.0] Fix format error in the `v16.0.2` release notes (#13057) [#13058](https://github.com/vitessio/vitess/pull/13058)
### Testing 
#### Backup and Restore
 * [release-16.0]: Fix `upgrade-downgrade` test setup and fix the `init_db.sql` [#13525](https://github.com/vitessio/vitess/pull/13525) 
#### Cluster management
 * [release-16.0] Deflake `TestPlannedReparentShardPromoteReplicaFail` (#13548) [#13549](https://github.com/vitessio/vitess/pull/13549)
 * [release-16.0] Flaky tests: Fix wrangler tests (#13568) [#13571](https://github.com/vitessio/vitess/pull/13571) 
#### General
 * TestFix: `Upgrade Downgrade Testing - Backups - Manual` [#13408](https://github.com/vitessio/vitess/pull/13408) 
#### Query Serving
 * [release-16.0] Fix benchmarks in `plan_test.go` (#13096) [#13126](https://github.com/vitessio/vitess/pull/13126)
 * [release-16.0] Deflake `TestQueryTimeoutWithDual` test (#13405) [#13409](https://github.com/vitessio/vitess/pull/13409)
 * [release-16.0] Fix `TestGatewayBufferingWhileReparenting` flakiness (#13469) [#13500](https://github.com/vitessio/vitess/pull/13500)
 * [release-16.0] fix TestQueryTimeoutWithTables flaky test (#13579) [#13585](https://github.com/vitessio/vitess/pull/13585) 
#### VTorc
 * [release-16.0]: Fix flakiness in VTOrc tests (#13489) [#13528](https://github.com/vitessio/vitess/pull/13528) 
#### vtctl
 * Fix new vtctl upgrade downgrade test on `release-16.0` [#13252](https://github.com/vitessio/vitess/pull/13252)

