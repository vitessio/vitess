# Changelog of Vitess v22.0.1

### Bug fixes 
#### Backup and Restore
 * [release-22.0] fix version issue when using --mysql-shell-speedup-restore=true (#18310) [#18356](https://github.com/vitessio/vitess/pull/18356) 
#### Evalengine
 * [release-22.0] fix: Preserve multi-column TupleExpr in tuple simplifier (#18216) [#18220](https://github.com/vitessio/vitess/pull/18220)
 * [release-22.0] Fix evalengine crashes on unexpected types (#18254) [#18258](https://github.com/vitessio/vitess/pull/18258) 
#### Query Serving
 * [release-22.0] make sure to give MEMBER OF the correct precedence (#18237) [#18245](https://github.com/vitessio/vitess/pull/18245)
 * [release-22.0] Fix subquery merging regression introduced in #11379 (#18260) [#18263](https://github.com/vitessio/vitess/pull/18263)
 * [release-22.0] Fix `SET` and `START TRANSACTION` in create procedure statements (#18279) [#18293](https://github.com/vitessio/vitess/pull/18293)
 * [release-22.0] fix: keep LIMIT/OFFSET even when merging UNION queries (#18361) [#18363](https://github.com/vitessio/vitess/pull/18363) 
#### Throttler
 * [release-22.0] Throttler: keep watching topo even on error (#18223) [#18322](https://github.com/vitessio/vitess/pull/18322) 
#### VReplication
 * [release-22.0] Atomic Copy: Handle error that was ignored while streaming tables and log it (#18313) [#18316](https://github.com/vitessio/vitess/pull/18316) 
#### VTTablet
 * [release-22.0] Fix deadlock in semi-sync monitor (#18276) [#18290](https://github.com/vitessio/vitess/pull/18290)
 * [release-22.0] Fix: Deadlock in `Close` and `write` in semi-sync monitor. (#18359) [#18368](https://github.com/vitessio/vitess/pull/18368)
### CI/Build 
#### General
 * [release-22.0] Upgrade the Golang version to `go1.24.3` [#18239](https://github.com/vitessio/vitess/pull/18239)
 * [release-22.0] Upgrade the Golang version to `go1.24.4` [#18329](https://github.com/vitessio/vitess/pull/18329) 
#### VReplication
 * [release-22.0] Split workflow with flaky vdiff2 e2e test. Skip flaky Migrate test. (#18300) [#18334](https://github.com/vitessio/vitess/pull/18334)
### Regression 
#### Query Serving
 * [release-22.0] fix: handle dml query for None opcode (#18326) [#18345](https://github.com/vitessio/vitess/pull/18345)
### Release 
#### General
 * [release-22.0] Bump to `v22.0.1-SNAPSHOT` after the `v22.0.0` release [#18225](https://github.com/vitessio/vitess/pull/18225)
 * [release-22.0] Code Freeze for `v22.0.1` [#18374](https://github.com/vitessio/vitess/pull/18374)
### Testing 
#### Query Serving
 * [release-22.0] test: TestQueryTimeoutWithShardTargeting fix flaky test (#18242) [#18250](https://github.com/vitessio/vitess/pull/18250)
 * [release-22.0] json array insert test (#18284) [#18286](https://github.com/vitessio/vitess/pull/18286)

