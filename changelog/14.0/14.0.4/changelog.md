# Changelog of Vitess v14.0.4

### Bug fixes 
#### Backup and Restore
 * Detect redo log location dynamically based on presence [#11555](https://github.com/vitessio/vitess/pull/11555) 
#### Build/CI
 * Fix the script `check_make_sizegen` [#11465](https://github.com/vitessio/vitess/pull/11465)
 * Skip `TestComparisonSemantics` test [#11474](https://github.com/vitessio/vitess/pull/11474)
 * Addition of a CI tool to detect dead links in test/config.json [#11668](https://github.com/vitessio/vitess/pull/11668)
 * Fix files changes filtering in CI [#11714](https://github.com/vitessio/vitess/pull/11714) 
#### Query Serving
 * fix: do not rewrite single columns in derived tables [#11419](https://github.com/vitessio/vitess/pull/11419)
 * Push down derived tables under route when possible [#11422](https://github.com/vitessio/vitess/pull/11422)
 * collations: fix coercion semantics according to 8.0.31 changes [#11487](https://github.com/vitessio/vitess/pull/11487)
 * [14.0] Fix JSON functions parsing [#11624](https://github.com/vitessio/vitess/pull/11624)
 * [bugfix] Allow VTExplain to handle shards that are not active during resharding [#11640](https://github.com/vitessio/vitess/pull/11640)
 * [release-14.0] Do not multiply `AggregateRandom` in JOINs [#11671](https://github.com/vitessio/vitess/pull/11671)
 * [14.0] Send errors in stream instead of a grpc error from streaming rpcs when transaction or reserved connection is acquired [#11688](https://github.com/vitessio/vitess/pull/11688)
 * Push down derived tables under route when possible [#11786](https://github.com/vitessio/vitess/pull/11786)
### CI/Build 
#### Build/CI
 * [release-14.0] Remove Launchable in the workflows [#11244](https://github.com/vitessio/vitess/pull/11244)
 * [release-14.0] Add automation to change vitess version in the docker-release script (#11682) [#11814](https://github.com/vitessio/vitess/pull/11814)
 * Remove Tests from Self-hosted runners [#11838](https://github.com/vitessio/vitess/pull/11838) 
#### Governance
 * codeowners: have at least two for almost every package [#11639](https://github.com/vitessio/vitess/pull/11639) 
#### VReplication
 * update jsonparser dependency [#11694](https://github.com/vitessio/vitess/pull/11694)
### Enhancement 
#### General
 * [release-14.0] Upgrade to `go1.18.7` [#11510](https://github.com/vitessio/vitess/pull/11510) 
#### Query Serving
 * Improve route merging for queries that have conditions on different vindexes, but can be merged via join predicates. [#10942](https://github.com/vitessio/vitess/pull/10942)
### Release 
#### Documentation
 * Prepare the release notes summary for `v14.0.4` [#11803](https://github.com/vitessio/vitess/pull/11803) 
#### General
 * Release of v14.0.3 [#11404](https://github.com/vitessio/vitess/pull/11404)
 * Back to dev mode after v14.0.3 [#11405](https://github.com/vitessio/vitess/pull/11405)
### Testing 
#### Query Serving
 * [V14] Better plan-tests [#11435](https://github.com/vitessio/vitess/pull/11435)

