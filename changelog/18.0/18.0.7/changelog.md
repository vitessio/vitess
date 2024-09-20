# Changelog of Vitess v18.0.7

### Bug fixes 
#### Query Serving
 * [release-18.0] bugfix: don't treat join predicates as filter predicates (#16472) [#16473](https://github.com/vitessio/vitess/pull/16473)
 * [release-18.0] Fix RegisterNotifier to use a copy of the tables to prevent data races (#14716) [#16490](https://github.com/vitessio/vitess/pull/16490)
 * [release-18.0] fix: reference table join merge (#16488) [#16495](https://github.com/vitessio/vitess/pull/16495)
 * [release-18.0] simplify merging logic (#16525) [#16531](https://github.com/vitessio/vitess/pull/16531)
 * [release-18.0] Fix query plan cache misses metric (#16562) [#16626](https://github.com/vitessio/vitess/pull/16626)
 * [release-18.0] JSON Encoding: Use Type_RAW for marshalling json (#16637) [#16680](https://github.com/vitessio/vitess/pull/16680) 
#### Throttler
 * v18 backport: Throttler/vreplication: fix app name used by VPlayer (#16578) [#16581](https://github.com/vitessio/vitess/pull/16581) 
#### VReplication
 * [release-18.0] VStream API: validate that last PK has fields defined (#16478) [#16485](https://github.com/vitessio/vitess/pull/16485) 
#### VTAdmin
 * [release-18.0] VTAdmin: Upgrade websockets js package (#16504) [#16511](https://github.com/vitessio/vitess/pull/16511) 
#### VTGate
 * [release-18.0] Fix `RemoveTablet` during `TabletExternallyReparented` causing connection issues (#16371) [#16566](https://github.com/vitessio/vitess/pull/16566) 
#### VTorc
 * [release-18.0] FindErrantGTIDs: superset is not an errant GTID situation (#16725) [#16727](https://github.com/vitessio/vitess/pull/16727)
### CI/Build 
#### General
 * [release-18.0] Upgrade the Golang version to `go1.21.13` [#16545](https://github.com/vitessio/vitess/pull/16545)
 * [release-18.0] Bump upgrade tests to `go1.22.7` [#16722](https://github.com/vitessio/vitess/pull/16722) 
#### VTAdmin
 * [release-18.0] Update micromatch to 4.0.8 (#16660) [#16665](https://github.com/vitessio/vitess/pull/16665)
### Enhancement 
#### Build/CI
 * [release-18.0] Improve the queries upgrade/downgrade CI workflow by using same test code version as binary (#16494) [#16500](https://github.com/vitessio/vitess/pull/16500) 
#### Online DDL
 * v18 backport: Online DDL: avoid SQL's `CONVERT(...)`, convert programmatically if needed [#16604](https://github.com/vitessio/vitess/pull/16604)
 * [release-18.0] VReplication workflows: retry "wrong tablet type" errors (#16645) [#16651](https://github.com/vitessio/vitess/pull/16651)
### Internal Cleanup 
#### Build/CI
 * [release-18.0] Move from 4-cores larger runners to `ubuntu-latest` (#16714) [#16716](https://github.com/vitessio/vitess/pull/16716)
### Regression 
#### Query Serving
 * [release-18.0] bugfix: Allow cross-keyspace joins (#16520) [#16522](https://github.com/vitessio/vitess/pull/16522)
### Release 
#### General
 * [release-18.0] Bump to `v18.0.7-SNAPSHOT` after the `v18.0.6` release [#16454](https://github.com/vitessio/vitess/pull/16454)
### Testing 
#### Query Serving
 * [release-18.0] Replace ErrorContains checks with Error checks before running upgrade downgrade [#16701](https://github.com/vitessio/vitess/pull/16701)

