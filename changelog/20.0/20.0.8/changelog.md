# Changelog of Vitess v20.0.8

### Bug fixes 
#### Evalengine
 * [release-20.0] fix: Preserve multi-column TupleExpr in tuple simplifier (#18216) [#18218](https://github.com/vitessio/vitess/pull/18218)
 * [release-20.0] Fix evalengine crashes on unexpected types (#18254) [#18256](https://github.com/vitessio/vitess/pull/18256) 
#### Query Serving
 * [release-20.0] Fix: Ensure Consistent Lookup Vindex Handles Duplicate Rows in Single Query (#17974) [#18077](https://github.com/vitessio/vitess/pull/18077)
 * [release-20.0] make sure to give MEMBER OF the correct precedence (#18237) [#18243](https://github.com/vitessio/vitess/pull/18243)
 * [release-20.0] Fix subquery merging regression introduced in #11379 (#18260) [#18261](https://github.com/vitessio/vitess/pull/18261) 
#### Throttler
 * [release-20.0] Properly handle grpc dial errors in the throttler metric aggregation (#18073) [#18229](https://github.com/vitessio/vitess/pull/18229)
 * [release-20.0] Throttler: keep watching topo even on error (#18223) [#18320](https://github.com/vitessio/vitess/pull/18320) 
#### VReplication
 * [release-20.0] Atomic Copy: Handle error that was ignored while streaming tables and log it (#18313) [#18314](https://github.com/vitessio/vitess/pull/18314)
### Release 
#### General
 * [release-20.0] Bump to `v20.0.8-SNAPSHOT` after the `v20.0.7` release [#18147](https://github.com/vitessio/vitess/pull/18147)
 * [release-20.0] Code Freeze for `v20.0.8` [#18377](https://github.com/vitessio/vitess/pull/18377)
### Testing 
#### Query Serving
 * [release-20.0] test: TestQueryTimeoutWithShardTargeting fix flaky test (#18242) [#18248](https://github.com/vitessio/vitess/pull/18248)

