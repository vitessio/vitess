# Changelog of Vitess v15.0.1

### Bug fixes 
#### Build/CI
 * Docker Image Context Fix [#11628](https://github.com/vitessio/vitess/pull/11628)
 * Addition of a CI tool to detect dead links in test/config.json [#11668](https://github.com/vitessio/vitess/pull/11668)
 * Fix files changes filtering in CI [#11714](https://github.com/vitessio/vitess/pull/11714) 
#### General
 * [release-15.0] Fix missing flag usage for vault credentials flags (#11582) [#11583](https://github.com/vitessio/vitess/pull/11583)
 * fix vdiff release notes [#11595](https://github.com/vitessio/vitess/pull/11595) 
#### Query Serving
 * collations: fix coercion semantics according to 8.0.31 changes [#11487](https://github.com/vitessio/vitess/pull/11487)
 * [bugfix] Allow VTExplain to handle shards that are not active during resharding [#11640](https://github.com/vitessio/vitess/pull/11640)
 * [release-15.0] Do not multiply `AggregateRandom` in JOINs [#11672](https://github.com/vitessio/vitess/pull/11672)
 * [15.0] Send errors in stream instead of a grpc error from streaming rpcs when transaction or reserved connection is acquired [#11687](https://github.com/vitessio/vitess/pull/11687)
 * improve handling of ORDER BY/HAVING rewriting [#11691](https://github.com/vitessio/vitess/pull/11691)
 * [release-15.0] Accept no more data in session state change as ok (#11796) [#11800](https://github.com/vitessio/vitess/pull/11800)
 * semantics: Use a BitSet [#11819](https://github.com/vitessio/vitess/pull/11819) 
#### VTAdmin
 * Add VTAdmin folder to release package [#11683](https://github.com/vitessio/vitess/pull/11683) 
#### vtctl
 * Switch ApplySchema `--sql` argument to be `StringArray` instead of `StringSlice` [#11790](https://github.com/vitessio/vitess/pull/11790)
### CI/Build 
#### Build/CI
 * [release-15.0] Remove Launchable in the workflows [#11669](https://github.com/vitessio/vitess/pull/11669)
 * Update test runners to run all tests including outside package [#11787](https://github.com/vitessio/vitess/pull/11787)
 * [release-15.0] Add automation to change vitess version in the docker-release script (#11682) [#11816](https://github.com/vitessio/vitess/pull/11816) 
#### Governance
 * codeowners: have at least two for almost every package [#11639](https://github.com/vitessio/vitess/pull/11639) 
#### Query Serving
 * [release-15.0] Consistent sorting in Online DDL Vrepl suite test (#11821) [#11828](https://github.com/vitessio/vitess/pull/11828) 
#### VReplication
 * update jsonparser dependency [#11694](https://github.com/vitessio/vitess/pull/11694)
### Release 
#### General
 * Release of v15.0.0 [#11573](https://github.com/vitessio/vitess/pull/11573)
 * Back to dev mode after v15.0.0 [#11574](https://github.com/vitessio/vitess/pull/11574)
 * fix anchors for release notes and summary [#11578](https://github.com/vitessio/vitess/pull/11578)
 * Mention the `--db-config-*-*` flag in the release notes [#11610](https://github.com/vitessio/vitess/pull/11610)
### Testing 
#### Build/CI
 * [release-15.0] Use `go1.19.3` in the upgrade/downgrade tests [#11676](https://github.com/vitessio/vitess/pull/11676)

