# Changelog of Vitess v20.0.1

### Bug fixes 
#### Cluster management
 * Fix Downgrade problem from v20 in semi-sync plugin [#16357](https://github.com/vitessio/vitess/pull/16357)
 * [release-20.0] Use default schema reload config values when config file is empty (#16393) [#16411](https://github.com/vitessio/vitess/pull/16411) 
#### Docker
 * [release-20.0] Fix the install dependencies script in Docker (#16340) [#16347](https://github.com/vitessio/vitess/pull/16347) 
#### Documentation
 * [release-20.0] Fix the `v19.0.0` release notes and use the `vitess/lite` image for the MySQL container (#16282) [#16286](https://github.com/vitessio/vitess/pull/16286) 
#### Query Serving
 * [release-20.0] Fix Incorrect Optimization with LIMIT and GROUP BY (#16263) [#16268](https://github.com/vitessio/vitess/pull/16268)
 * [release-20.0] planner: Handle ORDER BY inside derived tables (#16353) [#16360](https://github.com/vitessio/vitess/pull/16360)
 * [release-20.0] fix issue with aggregation inside of derived tables (#16366) [#16385](https://github.com/vitessio/vitess/pull/16385)
 * [release-20.0] Fix Join Predicate Cleanup Bug in Route Merging (#16386) [#16390](https://github.com/vitessio/vitess/pull/16390)
 * [release-20.0] Fix panic in user defined aggregation functions planning (#16398) [#16404](https://github.com/vitessio/vitess/pull/16404)
 * [release-20.0]  Fix panic in schema tracker in presence of keyspace routing rules (#16383) [#16405](https://github.com/vitessio/vitess/pull/16405)
 * [release-20.0] Fix subquery planning having an aggregation that is used in order by as long as we can merge it all into a single route (#16402) [#16408](https://github.com/vitessio/vitess/pull/16408) 
#### VReplication
 * [release-20.0] VReplication: Properly handle target shards w/o a primary in Reshard (#16283) [#16292](https://github.com/vitessio/vitess/pull/16292)
 * [release-20.0] VDiff: Copy non in_keyrange workflow filters to target tablet query (#16307) [#16315](https://github.com/vitessio/vitess/pull/16315)
### CI/Build 
#### Build/CI
 * [release-20.0] CI: Fix for xtrabackup install failures (#16329) [#16333](https://github.com/vitessio/vitess/pull/16333) 
#### General
 * [release-20.0] Upgrade the Golang version to `go1.22.5` [#16323](https://github.com/vitessio/vitess/pull/16323)
### Internal Cleanup 
#### Documentation
 * [release-20.0] Add a note on `QueryCacheHits` and `QueryCacheMisses` in the release notes (#16299) [#16306](https://github.com/vitessio/vitess/pull/16306) 
#### General
 * [release-20.0] Update post release `v20.0.0` [#16287](https://github.com/vitessio/vitess/pull/16287)
### Release 
#### General
 * [release-20.0-rc] Bump to `v20.0.1-SNAPSHOT` after the `v20.0.0` release [#16275](https://github.com/vitessio/vitess/pull/16275)
 * [release-20.0] Bump to `v20.0.1-SNAPSHOT` after the `v20.0.0` release [#16276](https://github.com/vitessio/vitess/pull/16276)
### Testing 
#### Query Serving
 * [release-20.0] fix flaky test TestQueryTimeoutWithShardTargeting (#16150) [#16179](https://github.com/vitessio/vitess/pull/16179)
 * [release-20] Vitess tester workflow (#16127) [#16417](https://github.com/vitessio/vitess/pull/16417)

