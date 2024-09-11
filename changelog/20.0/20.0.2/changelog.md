# Changelog of Vitess v20.0.2

### Bug fixes 
#### Query Serving
 * [release-20.0] bugfix: don't treat join predicates as filter predicates (#16472) [#16475](https://github.com/vitessio/vitess/pull/16475)
 * [release-20.0] fix: reference table join merge (#16488) [#16497](https://github.com/vitessio/vitess/pull/16497)
 * [release-20.0] simplify merging logic (#16525) [#16533](https://github.com/vitessio/vitess/pull/16533)
 * [release-20.0] Fix: Offset planning in hash joins (#16540) [#16552](https://github.com/vitessio/vitess/pull/16552)
 * [release-20.0] Fix query plan cache misses metric (#16562) [#16628](https://github.com/vitessio/vitess/pull/16628)
 * [release-20.0] Fix race conditions in the concatenate engine streaming (#16640) [#16648](https://github.com/vitessio/vitess/pull/16648)
 * [release-20.0] JSON Encoding: Use Type_RAW for marshalling json (#16637) [#16682](https://github.com/vitessio/vitess/pull/16682) 
#### Throttler
 * v20 backport: Throttler/vreplication: fix app name used by VPlayer (#16578) [#16579](https://github.com/vitessio/vitess/pull/16579) 
#### VReplication
 * [release-20.0] VStream API: validate that last PK has fields defined (#16478) [#16487](https://github.com/vitessio/vitess/pull/16487)
 * [release-20.0] VReplication: Properly ignore errors from trying to drop tables that don't exist (#16505) [#16561](https://github.com/vitessio/vitess/pull/16561) 
#### VTAdmin
 * [release-20.0] VTAdmin: Upgrade websockets js package (#16504) [#16513](https://github.com/vitessio/vitess/pull/16513) 
#### VTGate
 * [release-20.0] Fix `RemoveTablet` during `TabletExternallyReparented` causing connection issues (#16371) [#16568](https://github.com/vitessio/vitess/pull/16568) 
#### VTorc
 * [release-20.0] FindErrantGTIDs: superset is not an errant GTID situation (#16725) [#16729](https://github.com/vitessio/vitess/pull/16729)
### CI/Build 
#### Docker
 * [release-20.0] Fix `docker_lite_push` make target (#16662) [#16668](https://github.com/vitessio/vitess/pull/16668) 
#### General
 * [release-20.0] Upgrade the Golang version to `go1.22.6` [#16546](https://github.com/vitessio/vitess/pull/16546)
 * [release-20.0] Upgrade the Golang version to `go1.22.7` [#16719](https://github.com/vitessio/vitess/pull/16719) 
#### VTAdmin
 * [release-20.0] Update micromatch to 4.0.8 (#16660) [#16667](https://github.com/vitessio/vitess/pull/16667)
### Enhancement 
#### Build/CI
 * [release-20.0] Improve the queries upgrade/downgrade CI workflow by using same test code version as binary (#16494) [#16502](https://github.com/vitessio/vitess/pull/16502) 
#### Online DDL
 * [release-20.0] VReplication workflows: retry "wrong tablet type" errors (#16645) [#16653](https://github.com/vitessio/vitess/pull/16653)
### Internal Cleanup 
#### Build/CI
 * [release-20.0] Move from 4-cores larger runners to `ubuntu-latest` (#16714) [#16718](https://github.com/vitessio/vitess/pull/16718) 
#### Docker
 * [release-20.0] Remove mysql57/percona57 bootstrap images (#16620) [#16623](https://github.com/vitessio/vitess/pull/16623)
### Performance 
#### Online DDL
 * v20 backport: Online DDL: avoid SQL's `CONVERT(...)`, convert programmatically if needed [#16602](https://github.com/vitessio/vitess/pull/16602)
### Regression 
#### Query Serving
 * [release-20.0] bugfix: Allow cross-keyspace joins (#16520) [#16524](https://github.com/vitessio/vitess/pull/16524)
### Testing 
#### Build/CI
 * [release-20.0] Fix error contain checks in vtgate package (#16672) [#16677](https://github.com/vitessio/vitess/pull/16677) 
#### Query Serving
 * [release-20.0] Replace ErrorContains checks with Error checks before running upgrade downgrade [#16699](https://github.com/vitessio/vitess/pull/16699)

