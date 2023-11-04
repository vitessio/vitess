# Changelog of Vitess v15.0.3

### Bug fixes 
#### Backup and Restore
 * mysqlctl: flags should be added to vtbackup [#12048](https://github.com/vitessio/vitess/pull/12048) 
#### Build/CI
 * Fix `codeql` workflow timeout issue [#11760](https://github.com/vitessio/vitess/pull/11760)
 * [release-15.0] Use `go1.20.1` in upgrade/downgrade tests [#12512](https://github.com/vitessio/vitess/pull/12512) 
#### CLI
 * Purge logs without panicking [#12187](https://github.com/vitessio/vitess/pull/12187)
 * Fix `vtctldclient`'s Root command to return an error on unknown command [#12481](https://github.com/vitessio/vitess/pull/12481) 
#### Cluster management
 * Skip `TestReparentDoesntHangIfPrimaryFails` in vttablet v16 and above [#12387](https://github.com/vitessio/vitess/pull/12387)
 * Fix initialization code to also stop replication to prevent crash [#12534](https://github.com/vitessio/vitess/pull/12534) 
#### Observability
 * Reset the current lag when closing the replication lag reader. [#12683](https://github.com/vitessio/vitess/pull/12683) 
#### Online DDL
 * Bugfix/Backport to v15: Fix schema migrations requested_timestamp zero values [#12263](https://github.com/vitessio/vitess/pull/12263)
 * Mysqld.GetSchema: tolerate tables being dropped while inspecting schema [#12641](https://github.com/vitessio/vitess/pull/12641) 
#### Operator
 * Fix rbac config in the vtop example [#12034](https://github.com/vitessio/vitess/pull/12034) 
#### Query Serving
 * [release-15.0] only expand when we have full information (#11998) [#12002](https://github.com/vitessio/vitess/pull/12002)
 * Fix: Date math with Interval keyword [#12082](https://github.com/vitessio/vitess/pull/12082)
 * BugFix: Cast expression translation by evaluation engine [#12111](https://github.com/vitessio/vitess/pull/12111)
 * [Gen4] Fix lookup vindexes with `autocommit` enabled [#12172](https://github.com/vitessio/vitess/pull/12172)
 * VTGate: Ensure HealthCheck Cache Secondary Maps Stay in Sync With Authoritative Map on Tablet Delete [#12178](https://github.com/vitessio/vitess/pull/12178)
 * Fix aggregation on outer joins [#12298](https://github.com/vitessio/vitess/pull/12298)
 * [release-15.0] fix: added null safe operator precendence rule (#12297) [#12306](https://github.com/vitessio/vitess/pull/12306)
 * [release-15.0] Fix bug in vtexplain around JOINs (#12376) [#12383](https://github.com/vitessio/vitess/pull/12383)
 * Fix scalar aggregation engine primitive for column truncation [#12468](https://github.com/vitessio/vitess/pull/12468)
 * [release-16.0] BugFix: Unsharded query using a derived table and a dual table [#12484](https://github.com/vitessio/vitess/pull/12484)
 * [bug fix] USING planning on information_schema [#12542](https://github.com/vitessio/vitess/pull/12542)
 * handle filter on top of UNION [#12543](https://github.com/vitessio/vitess/pull/12543)
 * collations: fix sorting in UCA900 collations [#12555](https://github.com/vitessio/vitess/pull/12555)
 * VSchema DDL: Add grammar to accept qualified table names in Vindex option values [#12577](https://github.com/vitessio/vitess/pull/12577)
 * [release-15.0] `ApplyVSchemaDDL`: escape Sequence names when writing the VSchema (#12519) [#12598](https://github.com/vitessio/vitess/pull/12598)
 * [gen4 planner] Make sure to not push down expressions when not possible [#12607](https://github.com/vitessio/vitess/pull/12607)
 * Fix `panic` when executing a prepare statement with over `65,528` parameters [#12614](https://github.com/vitessio/vitess/pull/12614)
 * [planner bugfix] add expressions to HAVING [#12668](https://github.com/vitessio/vitess/pull/12668)
 * Use a left join to make sure that tables with tablespace=innodb_system are included in the schema [#12672](https://github.com/vitessio/vitess/pull/12672)
 * [release-15.0] Always add columns in the `Derived` operator [#12680](https://github.com/vitessio/vitess/pull/12680)
 * [planner fix] make unknown column an error only for sharded queries [#12704](https://github.com/vitessio/vitess/pull/12704) 
#### VReplication
 * VReplication Last Error: retry error if it happens after timeout [#12114](https://github.com/vitessio/vitess/pull/12114) 
#### VTorc
 * Fix unhandled error in VTOrc `recoverDeadPrimary` [#12511](https://github.com/vitessio/vitess/pull/12511)
### CI/Build 
#### Build/CI
 * [release-15.0] Make upgrade downgrade job names unique [#12498](https://github.com/vitessio/vitess/pull/12498)
 * v15 backport: CI: increase overall test timeouts for all OnlineDDL tests [#12591](https://github.com/vitessio/vitess/pull/12591) 
#### Online DDL
 * CI: extend timeouts in onlineddl_vrepl due to slow CI runners [#12583](https://github.com/vitessio/vitess/pull/12583) 
#### Query Serving
 * [release-15.0] Flakes: Properly Test HealthCheck Cache Response Handling (#12226) [#12227](https://github.com/vitessio/vitess/pull/12227)
### Dependabot 
#### Build/CI
 * Bump golang.org/x/net from 0.5.0 to 0.7.0 (#12390) [#12405](https://github.com/vitessio/vitess/pull/12405)
### Enhancement 
#### Build/CI
 * Auto upgrade the Golang version [#12585](https://github.com/vitessio/vitess/pull/12585) 
#### Governance
 * [release-15.0] Add manan and florent to Docker files CODEOWNERS (#11981) [#11983](https://github.com/vitessio/vitess/pull/11983) 
#### VTorc
 * Release-15: Cherry pick vtorc no cgo [#12223](https://github.com/vitessio/vitess/pull/12223)
### Internal Cleanup 
#### Build/CI
 * [15.0] CI: remove pitrtls test [#12064](https://github.com/vitessio/vitess/pull/12064) 
#### General
 * Remove removed flags from being used for v16+ binaries [#12128](https://github.com/vitessio/vitess/pull/12128)
 * [release-15.0] Fix release script for the version in the docker script [#12285](https://github.com/vitessio/vitess/pull/12285)
### Other 
#### Other
 * Code freeze of release-15.0 [#12764](https://github.com/vitessio/vitess/pull/12764)
### Performance 
#### Cluster management
 * Bug fix: Cache filtered out tablets in topology watcher to avoid unnecessary GetTablet calls to topo [#12194](https://github.com/vitessio/vitess/pull/12194)
### Release 
#### Build/CI
 * [release-15.0] Tooling improvements backports [#12527](https://github.com/vitessio/vitess/pull/12527) 
#### Documentation
 * Re-organize the `releasenotes` directory into `changelog` [#12566](https://github.com/vitessio/vitess/pull/12566) 
#### General
 * Release of v15.0.2 [#11961](https://github.com/vitessio/vitess/pull/11961)
 * Back to dev mode after v15.0.2 [#11962](https://github.com/vitessio/vitess/pull/11962)
### Testing 
#### General
 * Fix vtbackup upgrade/downgrade test  [#12437](https://github.com/vitessio/vitess/pull/12437)

