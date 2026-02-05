# Changelog of Vitess v14.0.5

### Bug fixes 
#### Observability
 * Reset the current lag when closing the replication lag reader. [#12683](https://github.com/vitessio/vitess/pull/12683) 
#### Online DDL
 * Mysqld.GetSchema: tolerate tables being dropped while inspecting schema [#12641](https://github.com/vitessio/vitess/pull/12641) 
#### Query Serving
 * Fix CheckMySQL by setting the correct wanted state [#11895](https://github.com/vitessio/vitess/pull/11895)
 * [release-14.0] Fix sending a ServerLost error when reading a packet fails (#11920) [#11928](https://github.com/vitessio/vitess/pull/11928)
 * Fix: Date math with Interval keyword [#12082](https://github.com/vitessio/vitess/pull/12082)
 * BugFix: Cast expression translation by evaluation engine [#12111](https://github.com/vitessio/vitess/pull/12111)
 * Fix aggregation on outer joins [#12298](https://github.com/vitessio/vitess/pull/12298)
 * [release-14.0] fix: added null safe operator precendence rule (#12297) [#12305](https://github.com/vitessio/vitess/pull/12305)
 * Fix scalar aggregation engine primitive for column truncation [#12468](https://github.com/vitessio/vitess/pull/12468)
 * [release-16.0] BugFix: Unsharded query using a derived table and a dual table [#12484](https://github.com/vitessio/vitess/pull/12484)
 * collations: fix sorting in UCA900 collations [#12555](https://github.com/vitessio/vitess/pull/12555)
 * [release-14.0] `ApplyVSchemaDDL`: escape Sequence names when writing the VSchema (#12519) [#12597](https://github.com/vitessio/vitess/pull/12597)
 * [gen4 planner] Make sure to not push down expressions when not possible [#12607](https://github.com/vitessio/vitess/pull/12607)
 * [release-14.0] Fix `panic` when executing a prepare statement with over `65,528` parameters [#12628](https://github.com/vitessio/vitess/pull/12628)
 * [planner bugfix] add expressions to HAVING [#12668](https://github.com/vitessio/vitess/pull/12668)
 * [release-14.0] Always add columns in the `Derived` operator [#12681](https://github.com/vitessio/vitess/pull/12681)
 * [planner fix] make unknown column an error only for sharded queries [#12704](https://github.com/vitessio/vitess/pull/12704)
### CI/Build 
#### Build/CI
 * Move towards MySQL 8.0 as the default template generation [#11153](https://github.com/vitessio/vitess/pull/11153)
 * Fix deprecated usage of set-output [#11844](https://github.com/vitessio/vitess/pull/11844)
 * Use `go1.18.9` in the next release upgrade downgrade E2E tests [#11925](https://github.com/vitessio/vitess/pull/11925)
 * [release-14.0] Make upgrade downgrade job names unique [#12497](https://github.com/vitessio/vitess/pull/12497)
 * v14 backport: CI: increase overall test timeouts for all OnlineDDL tests [#12590](https://github.com/vitessio/vitess/pull/12590) 
#### Online DDL
 * CI: extend timeouts in onlineddl_vrepl due to slow CI runners [#12583](https://github.com/vitessio/vitess/pull/12583) 
#### TabletManager
 * Fix closing the body for HTTP requests [#11842](https://github.com/vitessio/vitess/pull/11842)
### Enhancement 
#### Build/CI
 * Auto upgrade the Golang version [#12585](https://github.com/vitessio/vitess/pull/12585) 
#### Governance
 * [release-14.0] Add manan and florent to Docker files CODEOWNERS (#11981) [#11982](https://github.com/vitessio/vitess/pull/11982)
### Internal Cleanup 
#### General
 * [release-14.0] Fix release script for the version in the docker script (#12285) [#12290](https://github.com/vitessio/vitess/pull/12290)
### Performance 
#### Cluster management
 * Bug fix: Cache filtered out tablets in topology watcher to avoid unnecessary GetTablet calls to topo [#12194](https://github.com/vitessio/vitess/pull/12194)
### Release 
#### Build/CI
 * [release-14.0] Tooling improvements backports [#12526](https://github.com/vitessio/vitess/pull/12526) 
#### Documentation
 * Re-organize the `releasenotes` directory into `changelog` [#12566](https://github.com/vitessio/vitess/pull/12566) 
#### General
 * Back to dev mode after v14.0.4 [#11845](https://github.com/vitessio/vitess/pull/11845)
 * Release of v14.0.4 [#11846](https://github.com/vitessio/vitess/pull/11846)
 * Code freeze of `release-14.0` for `v14.0.5` [#12763](https://github.com/vitessio/vitess/pull/12763)

