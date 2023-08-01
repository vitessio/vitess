# Changelog of Vitess v15.0.4

### Bug fixes 
#### Build/CI
 * [release-15.0] Small fixes to the auto-upgrade golang tool (#12838) [#12847](https://github.com/vitessio/vitess/pull/12847)
 * [release-15.0] Add timeout to golangci-lint and bump its version (#12852) [#12853](https://github.com/vitessio/vitess/pull/12853)
 * [release-15.0] Remove recent golangci-lint version bump [#12910](https://github.com/vitessio/vitess/pull/12910) 
#### Cluster management
 * [release-15.0] Prevent resetting replication every time we set replication source (#13377) [#13393](https://github.com/vitessio/vitess/pull/13393)
 * [release-15.0] Don't run any reparent commands if the host is empty (#13396) [#13403](https://github.com/vitessio/vitess/pull/13403)
 * [release-15.0] ignore all error for views in engine reload (#13590) [#13592](https://github.com/vitessio/vitess/pull/13592) 
#### Examples
 * [release-15.0] `examples/compose`: fix `consul:latest` error w/`docker-compose up -d` (#13468) [#13471](https://github.com/vitessio/vitess/pull/13471) 
#### Online DDL
 * v15 backport: vitess Online DDL atomic cut-over [#13376](https://github.com/vitessio/vitess/pull/13376) 
#### Query Serving
 * [release-15.0] planbuilder bugfix - do not push aggregations into derived tables [#12824](https://github.com/vitessio/vitess/pull/12824)
 * [release-15.0] Fix `vtgate_schema_tracker` flaky tests (#12780) [#12850](https://github.com/vitessio/vitess/pull/12850)
 * [release-15.0] fix: union distinct between unsharded route and sharded join (#12968) [#12982](https://github.com/vitessio/vitess/pull/12982)
 * gen4 planner: allow last_insert_id with arguments (15.0) [#13035](https://github.com/vitessio/vitess/pull/13035)
 * [release-15.0] Fix the resilientQuery to give correct results during initialization (#13080) [#13086](https://github.com/vitessio/vitess/pull/13086)
 * [release-15.0] Remove indentation limit in the sqlparser (#13158) [#13167](https://github.com/vitessio/vitess/pull/13167)
 * [release-15.0] Fix: TabletServer ReserveBeginExecute to return transaction ID on error (#13193) [#13196](https://github.com/vitessio/vitess/pull/13196)
 * [15.0] Fix: errant GTID in health streamer (#13184) [#13226](https://github.com/vitessio/vitess/pull/13226) 
#### Schema Tracker
 * [release-15.0] Ignore error while reading table data in Schema.Engine reload (#13421) [#13425](https://github.com/vitessio/vitess/pull/13425)
 * Backport v15: schema.Reload(): ignore column reading errors for views only, error for tables #13442 [#13457](https://github.com/vitessio/vitess/pull/13457)
### Enhancement 
#### Build/CI
 * Use go1.20.3 in the upgrade downgrade tests [#12839](https://github.com/vitessio/vitess/pull/12839)
 * [release-15.0] Set the number of threads for release notes generation with a flag [#13315](https://github.com/vitessio/vitess/pull/13315) 
#### General
 * Use `go1.20.4` on `release-15.0` upgrade test [#13071](https://github.com/vitessio/vitess/pull/13071) 
#### Query Serving
 * [release-15.0] planner fix: scoping rules for JOIN ON expression inside a subquery [#12890](https://github.com/vitessio/vitess/pull/12890)
### Internal Cleanup 
#### Operator
 * Use vitess-operator `v2.8.4` in the examples [#12993](https://github.com/vitessio/vitess/pull/12993) 
#### VTorc
 * [release-15.0] Remove excessive logging in VTOrc APIs (#13459) [#13463](https://github.com/vitessio/vitess/pull/13463)
### Performance 
#### TabletManager
 * [release-15.0] BaseShowTablesWithSizes: optimize MySQL 8.0 query (#13375) [#13388](https://github.com/vitessio/vitess/pull/13388)
### Release 
#### Build/CI
 * [release-15.0] Optimize release notes generation to use GitHub Milestones (#13398) [#13620](https://github.com/vitessio/vitess/pull/13620) 
#### Documentation
 * Prepare release note `v15.0.4` [#13619](https://github.com/vitessio/vitess/pull/13619)
### Testing 
#### Build/CI
 * [release-15.0] fakedbclient: Add locking to avoid races (#12814) [#12821](https://github.com/vitessio/vitess/pull/12821) 
#### Cluster management
 * [release-15.0] Flaky tests: Fix wrangler tests (#13568) [#13570](https://github.com/vitessio/vitess/pull/13570) 
#### General
 * [release-15.0] Update Upgrade/Downgrade tests to use `go1.20.5` [#13271](https://github.com/vitessio/vitess/pull/13271) 
#### Query Serving
 * [release-15.0] Fix benchmarks in `plan_test.go` (#13096) [#13125](https://github.com/vitessio/vitess/pull/13125)
 * [release-15.0] Fix `TestGatewayBufferingWhileReparenting` flakiness (#13469) [#13502](https://github.com/vitessio/vitess/pull/13502) 
#### VTorc
 * [release-15.0]: Fix flakiness in VTOrc tests (#13489) [#13529](https://github.com/vitessio/vitess/pull/13529)

