# Changelog of Vitess v21.0.5

### Bug fixes 
#### Evalengine
 * [release-21.0] fix: Preserve multi-column TupleExpr in tuple simplifier (#18216) [#18219](https://github.com/vitessio/vitess/pull/18219)
 * [release-21.0] Fix evalengine crashes on unexpected types (#18254) [#18257](https://github.com/vitessio/vitess/pull/18257) 
#### Query Serving
 * [release-21.0] make sure to give MEMBER OF the correct precedence (#18237) [#18244](https://github.com/vitessio/vitess/pull/18244)
 * [release-21.0] Fix subquery merging regression introduced in #11379 (#18260) [#18262](https://github.com/vitessio/vitess/pull/18262)
 * [release-21.0] fix: keep LIMIT/OFFSET even when merging UNION queries (#18361) [#18362](https://github.com/vitessio/vitess/pull/18362) 
#### Throttler
 * [release-21.0] Throttler: keep watching topo even on error (#18223) [#18321](https://github.com/vitessio/vitess/pull/18321) 
#### VReplication
 * [release-21.0] Atomic Copy: Handle error that was ignored while streaming tables and log it (#18313) [#18315](https://github.com/vitessio/vitess/pull/18315)
### CI/Build 
#### General
 * [release-21.0] Upgrade the Golang version to `go1.23.10` [#18328](https://github.com/vitessio/vitess/pull/18328)
### Dependencies 
#### General
 * [release-21.0] Bump golang.org/x/net from 0.36.0 to 0.38.0 (#18177) [#18188](https://github.com/vitessio/vitess/pull/18188)
### Release 
#### General
 * [release-21.0] Bump to `v21.0.5-SNAPSHOT` after the `v21.0.4` release [#18145](https://github.com/vitessio/vitess/pull/18145)
 * [release-21.0] Code Freeze for `v21.0.5` [#18372](https://github.com/vitessio/vitess/pull/18372)
### Testing 
#### Query Serving
 * Skip end-to-end test [#18166](https://github.com/vitessio/vitess/pull/18166)
 * [release-21.0] json array insert test [#18194](https://github.com/vitessio/vitess/pull/18194)
 * [release-21.0] test: TestQueryTimeoutWithShardTargeting fix flaky test (#18242) [#18249](https://github.com/vitessio/vitess/pull/18249)

