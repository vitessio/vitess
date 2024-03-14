# Changelog of Vitess v17.0.6

### Bug fixes 
#### Evalengine
 * [release-17.0] evalengine: Fix week overflow (#14859) [#14860](https://github.com/vitessio/vitess/pull/14860)
 * [release-17.0] evalengine: Return evalTemporal types for current date / time (#15079) [#15083](https://github.com/vitessio/vitess/pull/15083) 
#### General
 * [release-17.0] Protect `ExecuteFetchAsDBA` against multi-statements, excluding a sequence of `CREATE TABLE|VIEW`. (#14954) [#14983](https://github.com/vitessio/vitess/pull/14983) 
#### Online DDL
 * [release-17.0] VReplication/OnlineDDL: reordering enum values (#15103) [#15350](https://github.com/vitessio/vitess/pull/15350) 
#### Query Serving
 * [release-17]: Vindexes: Pass context in consistent lookup handleDup (#14653) [#14912](https://github.com/vitessio/vitess/pull/14912)
 * [release-17.0] evalengine bugfix: handle nil evals correctly when coercing values (#14906) [#14913](https://github.com/vitessio/vitess/pull/14913)
 * [release-17.0] Fix Go routine leaks in streaming calls (#15293) [#15299](https://github.com/vitessio/vitess/pull/15299)
 * [release-17.0] SHOW VITESS_REPLICATION_STATUS: Only use replication tracker when it's enabled (#15348) [#15360](https://github.com/vitessio/vitess/pull/15360) 
#### Throttler
 * [release-17.0] examples: rm heartbeat flags (#14980) [#14998](https://github.com/vitessio/vitess/pull/14998) 
#### vtexplain
 * [release-17.0] vtexplain: Fix setting up the column information (#15275) [#15280](https://github.com/vitessio/vitess/pull/15280)
 * [release-17.0] vtexplain: Ensure memory topo is set up for throttler (#15279) [#15283](https://github.com/vitessio/vitess/pull/15283) 
#### vttestserver
 * [release-17.0] use proper mysql version in the `vttestserver` images (#15235) [#15237](https://github.com/vitessio/vitess/pull/15237)
### CI/Build 
#### Build/CI
 * [release-17.0] Fix relevant files listing for `endtoend` CI (#15104) [#15109](https://github.com/vitessio/vitess/pull/15109)
 * [release-17.0] Remove concurrency group for check labels workflow (#15197) [#15207](https://github.com/vitessio/vitess/pull/15207)
 * [release-17.0] Update all actions setup to latest versions (#15443) [#15444](https://github.com/vitessio/vitess/pull/15444)
### Dependabot 
#### General
 * [release-17.0] Bumps deps and use proper Go version in upgrade tests [#15408](https://github.com/vitessio/vitess/pull/15408) 
#### Java
 * [release-17.0] build(deps): bump io.netty:netty-handler from 4.1.93.Final to 4.1.94.Final in /java (#14863) [#14880](https://github.com/vitessio/vitess/pull/14880)
### Documentation 
#### Documentation
 * [release-17.0] 17.0.6 release notes: ExecuteFetchAsDBA breaking change [#15011](https://github.com/vitessio/vitess/pull/15011)
### Enhancement 
#### Build/CI
 * [release-17.0] Update paths filter action (#15254) [#15262](https://github.com/vitessio/vitess/pull/15262)
### Regression 
#### Throttler
 * [release-17.0] Enable 'heartbeat_on_demand_duration' in local/examples (#15204) [#15290](https://github.com/vitessio/vitess/pull/15290)
### Release 
### Testing 
#### Build/CI
 * [release-17.0] Bump upgrade tests to `go1.21.7` [#15160](https://github.com/vitessio/vitess/pull/15160)
 * [release-17.0] CI: Address data races on memorytopo Conn.closed (#15365) [#15369](https://github.com/vitessio/vitess/pull/15369) 
#### Query Serving
 * [release-17.0] Refactor Upgrade downgrade tests (#14782) [#14831](https://github.com/vitessio/vitess/pull/14831)

