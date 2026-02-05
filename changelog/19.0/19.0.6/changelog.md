# Changelog of Vitess v19.0.6

### Bug fixes 
#### Query Serving
 * [release-19.0] bugfix: don't treat join predicates as filter predicates (#16472) [#16474](https://github.com/vitessio/vitess/pull/16474)
 * [release-19.0] fix: reference table join merge (#16488) [#16496](https://github.com/vitessio/vitess/pull/16496)
 * [release-19.0] simplify merging logic (#16525) [#16532](https://github.com/vitessio/vitess/pull/16532)
 * [release-19.0] Fix: Offset planning in hash joins (#16540) [#16551](https://github.com/vitessio/vitess/pull/16551)
 * [release-19.0] Fix query plan cache misses metric (#16562) [#16627](https://github.com/vitessio/vitess/pull/16627)
 * [release-19.0] JSON Encoding: Use Type_RAW for marshalling json (#16637) [#16681](https://github.com/vitessio/vitess/pull/16681) 
#### Throttler
 * v19 backport: Throttler/vreplication: fix app name used by VPlayer (#16578) [#16580](https://github.com/vitessio/vitess/pull/16580) 
#### VReplication
 * [release-19.0] VStream API: validate that last PK has fields defined (#16478) [#16486](https://github.com/vitessio/vitess/pull/16486) 
#### VTAdmin
 * [release-19.0] VTAdmin: Upgrade websockets js package (#16504) [#16512](https://github.com/vitessio/vitess/pull/16512) 
#### VTGate
 * [release-19.0] Fix `RemoveTablet` during `TabletExternallyReparented` causing connection issues (#16371) [#16567](https://github.com/vitessio/vitess/pull/16567) 
#### VTorc
 * [release-19.0] FindErrantGTIDs: superset is not an errant GTID situation (#16725) [#16728](https://github.com/vitessio/vitess/pull/16728)
### CI/Build 
#### General
 * [release-19.0] Upgrade the Golang version to `go1.22.6` [#16543](https://github.com/vitessio/vitess/pull/16543) 
#### VTAdmin
 * [release-19.0] Update micromatch to 4.0.8 (#16660) [#16666](https://github.com/vitessio/vitess/pull/16666)
### Enhancement 
#### Build/CI
 * [release-19.0] Improve the queries upgrade/downgrade CI workflow by using same test code version as binary (#16494) [#16501](https://github.com/vitessio/vitess/pull/16501) 
#### Online DDL
 * [release-19.0] VReplication workflows: retry "wrong tablet type" errors (#16645) [#16652](https://github.com/vitessio/vitess/pull/16652)
### Internal Cleanup 
#### Build/CI
 * [release-19.0] Move from 4-cores larger runners to `ubuntu-latest` (#16714) [#16717](https://github.com/vitessio/vitess/pull/16717) 
#### Docker
 * [release-19.0] Remove mysql57/percona57 bootstrap images (#16620) [#16622](https://github.com/vitessio/vitess/pull/16622)
### Performance 
#### Online DDL
 * v19 backport: Online DDL: avoid SQL's `CONVERT(...)`, convert programmatically if needed [#16603](https://github.com/vitessio/vitess/pull/16603)
### Regression 
#### Query Serving
 * [release-19.0] bugfix: Allow cross-keyspace joins (#16520) [#16523](https://github.com/vitessio/vitess/pull/16523)
### Release 
#### General
 * [release-19.0] Bump to `v19.0.6-SNAPSHOT` after the `v19.0.5` release [#16456](https://github.com/vitessio/vitess/pull/16456)
### Testing 
#### Query Serving
 * [release-19.0] Replace ErrorContains checks with Error checks before running upgrade downgrade [#16700](https://github.com/vitessio/vitess/pull/16700)

