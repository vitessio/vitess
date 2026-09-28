# Changelog of Vitess v13.0.2

### Bug fixes
#### Build/CI
* Make go version check reliable by using double brackets [#10126](https://github.com/vitessio/vitess/pull/10126)
* Fixed the release notes CI check helper [#10574](https://github.com/vitessio/vitess/pull/10574)
#### Query Serving
* Backport: Only start SQL thread temporarily to WaitForPosition if needed [#10123](https://github.com/vitessio/vitess/pull/10123)
* Do not mutate replication state in WaitSourcePos and ignore tablets with SQL_Thread stopped in ERS [#10148](https://github.com/vitessio/vitess/pull/10148)
* Fix for empty results when no shards can be found to route to [#10187](https://github.com/vitessio/vitess/pull/10187)
* Fix for empty results when no shards can be found to route to [R13, v3] [#10202](https://github.com/vitessio/vitess/pull/10202)
* plancache: Lazy sysvar planner functions [#10248](https://github.com/vitessio/vitess/pull/10248)
* Do not cache plans that are invalid because of `--no_scatter` [#10283](https://github.com/vitessio/vitess/pull/10283)
* Backport R13: concatenate engine primitive [#10343](https://github.com/vitessio/vitess/pull/10343)
* BugFix: Keep predicates in join when pushing new ones [#10715](https://github.com/vitessio/vitess/pull/10715)
#### VReplication
* VStream API: Fix vtgate memory leaks when context gets cancelled [#10571](https://github.com/vitessio/vitess/pull/10571)
#### vtexplain
* fix: check that all keyspaces loaded successfully before using them [#10396](https://github.com/vitessio/vitess/pull/10396)
### CI/Build
#### Build/CI
* Fix upgrade-downgrade build on `release-13.0` [#10503](https://github.com/vitessio/vitess/pull/10503)
* Take into account `github.ref` when doing upgrade-downgrade tests [#10504](https://github.com/vitessio/vitess/pull/10504)
* Remove the review checklist workflow [#10656](https://github.com/vitessio/vitess/pull/10656)
#### General
* Upgrade to `go1.17.11` on release-13.0 branch [#10461](https://github.com/vitessio/vitess/pull/10461)
* Upgrade to `go1.17.12` [#10707](https://github.com/vitessio/vitess/pull/10707)
#### Governance
* Update the comment for review checklist with an item for CI workflows [#10471](https://github.com/vitessio/vitess/pull/10471)
### Regression
#### Query Serving
* [13.0] partial dml execution logic enhancement [#10284](https://github.com/vitessio/vitess/pull/10284)
### Release
#### Build/CI
* Rework how the `release notes` labels are handled by the CI [#10508](https://github.com/vitessio/vitess/pull/10508)
* Rework the generation of the release notes [#10510](https://github.com/vitessio/vitess/pull/10510)
#### General
* Release 13.0.1 [#10096](https://github.com/vitessio/vitess/pull/10096)
* [13] Release summary 13.0.2 [#10820](https://github.com/vitessio/vitess/pull/10820)
### Testing
#### Query Serving
* Flaky test fix: TestFoundRows [#10222](https://github.com/vitessio/vitess/pull/10222)
* unit test: fix mysql tests to run on MacOS [#10613](https://github.com/vitessio/vitess/pull/10613)
#### web UI
* Fixing flaky vtctld2 web test [#10541](https://github.com/vitessio/vitess/pull/10541)
