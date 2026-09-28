# Changelog of Vitess v18.0.3

### Bug fixes 
#### CLI
 * [release-18.0] Fix some binaries to print the versions (#15306) [#15309](https://github.com/vitessio/vitess/pull/15309) 
#### Evalengine
 * [release-18.0] evalengine: Fix week overflow (#14859) [#14861](https://github.com/vitessio/vitess/pull/14861)
 * [release-18.0] evalengine: Return evalTemporal types for current date / time (#15079) [#15084](https://github.com/vitessio/vitess/pull/15084) 
#### General
 * [release-18.0] Protect `ExecuteFetchAsDBA` against multi-statements, excluding a sequence of `CREATE TABLE|VIEW`. (#14954) [#14984](https://github.com/vitessio/vitess/pull/14984) 
#### Online DDL
 * [release-18.0] VReplication/OnlineDDL: reordering enum values (#15103) [#15351](https://github.com/vitessio/vitess/pull/15351) 
#### Query Serving
 * [release-18]: Vindexes: Pass context in consistent lookup handleDup (#14653) [#14911](https://github.com/vitessio/vitess/pull/14911)
 * [release-18.0] evalengine bugfix: handle nil evals correctly when coercing values (#14906) [#14914](https://github.com/vitessio/vitess/pull/14914)
 * [release-18.0] bugfix: Columns alias expanding (#14935) [#14955](https://github.com/vitessio/vitess/pull/14955)
 * [release-18.0] Improve efficiency and accuracy of mysqld.GetVersionString (#15096) [#15111](https://github.com/vitessio/vitess/pull/15111)
 * [release-18.0] In the same sqltypes.Type, Copy expression types to avoid weight_strings and derived tables (#15069) [#15129](https://github.com/vitessio/vitess/pull/15129)
 * [release-18.0] make sure to handle unsupported collations well (#15134) [#15142](https://github.com/vitessio/vitess/pull/15142)
 * [release-18.0] fix: ignore internal tables in schema tracking (#15141) [#15146](https://github.com/vitessio/vitess/pull/15146)
 * [release-18.0] TxThrottler: dont throttle unless lag (#14789) [#15190](https://github.com/vitessio/vitess/pull/15190)
 * [release-18.0] Avoid rewriting unsharded queries and split semantic analysis in two (#15217) [#15229](https://github.com/vitessio/vitess/pull/15229)
 * [release-18.0] sqlparser: use integers instead of literals for Length/Precision  (#15256) [#15268](https://github.com/vitessio/vitess/pull/15268)
 * [release-18.0] Fix Go routine leaks in streaming calls (#15293) [#15300](https://github.com/vitessio/vitess/pull/15300)
 * [release-18.0] Column alias expanding on ORDER BY  (#15302) [#15331](https://github.com/vitessio/vitess/pull/15331)
 * [release-18.0] go/vt/discovery: use protobuf getters for SrvVschema (#15343) [#15345](https://github.com/vitessio/vitess/pull/15345)
 * [release-18.0] SHOW VITESS_REPLICATION_STATUS: Only use replication tracker when it's enabled (#15348) [#15361](https://github.com/vitessio/vitess/pull/15361)
 * [release-18.0] Bugfix: GROUP BY/HAVING alias resolution (#15344) [#15381](https://github.com/vitessio/vitess/pull/15381) 
#### Schema Tracker
 * [release-18.0] discovery: fix crash with nil server vschema (#15086) [#15092](https://github.com/vitessio/vitess/pull/15092) 
#### Throttler
 * [release-18.0] examples: rm heartbeat flags (#14980) [#14999](https://github.com/vitessio/vitess/pull/14999) 
#### VReplication
 * [release-18.0] VReplication: Make Target Sequence Initialization More Robust (#15289) [#15307](https://github.com/vitessio/vitess/pull/15307)
 * [release-18.0] VtctldClient Reshard: add e2e tests to confirm CLI options and fix discovered issues. (#15353) [#15471](https://github.com/vitessio/vitess/pull/15471) 
#### VTCombo
 * [release-18.0] Correctly set log_dir default in vtcombo (#15153) [#15154](https://github.com/vitessio/vitess/pull/15154) 
#### vtexplain
 * [release-18.0] vtexplain: Fix setting up the column information (#15275) [#15281](https://github.com/vitessio/vitess/pull/15281)
 * [release-18.0] vtexplain: Ensure memory topo is set up for throttler (#15279) [#15284](https://github.com/vitessio/vitess/pull/15284) 
#### vttestserver
 * [release-18.0] Revert unwanted logging change to `vttestserver` (#15148) [#15149](https://github.com/vitessio/vitess/pull/15149)
 * [release-18.0] use proper mysql version in the `vttestserver` images (#15235) [#15238](https://github.com/vitessio/vitess/pull/15238)
### CI/Build 
#### Build/CI
 * [release-18.0] Fix relevant files listing for `endtoend` CI (#15104) [#15110](https://github.com/vitessio/vitess/pull/15110)
 * [release-18.0] Remove concurrency group for check labels workflow (#15197) [#15208](https://github.com/vitessio/vitess/pull/15208)
 * [release-18.0] bump `github.com/golang/protobuf` to `v1.5.4` (#15426) [#15427](https://github.com/vitessio/vitess/pull/15427)
 * [release-18.0] Update all actions setup to latest versions (#15443) [#15445](https://github.com/vitessio/vitess/pull/15445) 
#### General
 * [release-18.0] Upgrade the Golang version to `go1.21.8` [#15407](https://github.com/vitessio/vitess/pull/15407)
### Dependabot 
#### Java
 * [release-18.0] build(deps): bump io.netty:netty-handler from 4.1.93.Final to 4.1.94.Final in /java (#14863) [#14881](https://github.com/vitessio/vitess/pull/14881)
### Documentation 
#### Documentation
 * [release-18.0] 18.0.3 release notes: ExecuteFetchAsDBA breaking change [#15013](https://github.com/vitessio/vitess/pull/15013)
 * [release-18.0] Fix docs for unmanaged tablets (#15437) [#15473](https://github.com/vitessio/vitess/pull/15473)
### Enhancement 
#### Build/CI
 * [release-18.0] Update paths filter action (#15254) [#15263](https://github.com/vitessio/vitess/pull/15263)
### Performance 
#### Throttler
 * [release-18.0] Throttler: Use tmclient pool for CheckThrottler tabletmanager RPC [#15087](https://github.com/vitessio/vitess/pull/15087)
### Regression 
#### Query Serving
 * [release-18.0] Subquery inside aggregration function (#14844) [#14845](https://github.com/vitessio/vitess/pull/14845)
 * [release-18.0] Fix routing rule query rewrite (#15253) [#15258](https://github.com/vitessio/vitess/pull/15258) 
#### Throttler
 * [release-18.0] Enable 'heartbeat_on_demand_duration' in local/examples (#15204) [#15291](https://github.com/vitessio/vitess/pull/15291) 
#### vttestserver
 * [release-18.0] Fix logging issue when running in Docker with the syslog daemon disabled (#15176) [#15185](https://github.com/vitessio/vitess/pull/15185)
### Release 
#### General
 * Back to dev mode after v18.0.2 [#14839](https://github.com/vitessio/vitess/pull/14839)
 * [release-18.0] Code Freeze for `v18.0.3` [#15480](https://github.com/vitessio/vitess/pull/15480)
### Testing 
#### Build/CI
 * [release-18.0] Use `go1.22.0` in upgrade tests [#15170](https://github.com/vitessio/vitess/pull/15170)
 * [release-18.0] CI: Address data races on memorytopo Conn.closed (#15365) [#15370](https://github.com/vitessio/vitess/pull/15370)

