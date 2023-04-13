# Changelog of Vitess v16.0.1

### Bug fixes 
#### Build/CI
 * Fix `TestFuzz` that hangs on `go1.20.1` [#12514](https://github.com/vitessio/vitess/pull/12514)
 * Fix dubious ownership of git directory in `vitess/base` Docker build [#12530](https://github.com/vitessio/vitess/pull/12530) 
#### CLI
 * Purge logs without panicking [#12187](https://github.com/vitessio/vitess/pull/12187)
 * Fix `vtctldclient`'s Root command to return an error on unknown command [#12481](https://github.com/vitessio/vitess/pull/12481) 
#### Cluster management
 * Fix initialization code to also stop replication to prevent crash [#12534](https://github.com/vitessio/vitess/pull/12534)
 * [Backport] Update topo {Get,Create}Keyspace to prevent invalid keyspace names [#12732](https://github.com/vitessio/vitess/pull/12732)
#### General
 * Fixing backup tests flakiness [#12655](https://github.com/vitessio/vitess/pull/12655)
 * [release-16.0] Port two flaky test fixes #12603 and #12546 [#12745](https://github.com/vitessio/vitess/pull/12745) 
#### Observability
 * Reset the current lag when closing the replication lag reader. [#12683](https://github.com/vitessio/vitess/pull/12683) 
#### Online DDL
 * Throttler: Store Config in Global Keyspace Topo Record [#12520](https://github.com/vitessio/vitess/pull/12520)
 * v16: Online DDL: enforce ALGORITHM=COPY on shadow table [#12522](https://github.com/vitessio/vitess/pull/12522)
 * Mysqld.GetSchema: tolerate tables being dropped while inspecting schema [#12641](https://github.com/vitessio/vitess/pull/12641) 
#### Query Serving
 * collations: fix sorting in UCA900 collations [#12555](https://github.com/vitessio/vitess/pull/12555)
 * VSchema DDL: Add grammar to accept qualified table names in Vindex option values [#12577](https://github.com/vitessio/vitess/pull/12577)
 * [release-16.0] `ApplyVSchemaDDL`: escape Sequence names when writing the VSchema (#12519) [#12599](https://github.com/vitessio/vitess/pull/12599)
 * [gen4 planner] Make sure to not push down expressions when not possible [#12607](https://github.com/vitessio/vitess/pull/12607)
 * Fix `panic` when executing a prepare statement with over `65,528` parameters [#12614](https://github.com/vitessio/vitess/pull/12614)
 * Always add columns in the `Derived` operator [#12634](https://github.com/vitessio/vitess/pull/12634)
 * planner: fix predicate simplifier [#12650](https://github.com/vitessio/vitess/pull/12650)
 * [planner bugfix] add expressions to HAVING [#12668](https://github.com/vitessio/vitess/pull/12668)
 * Use a left join to make sure that tables with tablespace=innodb_system are included in the schema [#12672](https://github.com/vitessio/vitess/pull/12672)
 * [planner fix] make unknown column an error only for sharded queries [#12704](https://github.com/vitessio/vitess/pull/12704) 
#### VReplication
 * VStreamer: improve representation of integers in json data types [#12630](https://github.com/vitessio/vitess/pull/12630) 
#### VTorc
 * Fix unhandled error in VTOrc `recoverDeadPrimary` [#12510](https://github.com/vitessio/vitess/pull/12510)
### CI/Build 
#### Build/CI
 * [release-16.0] Make upgrade downgrade job names unique [#12499](https://github.com/vitessio/vitess/pull/12499) 
#### Examples
 * Examples, Flakes: Wait for Shard's VReplication Engine to Open [#12560](https://github.com/vitessio/vitess/pull/12560) 
#### General
 * [release-16.0] Upgrade the Golang version to `go1.20.2` [#12723](https://github.com/vitessio/vitess/pull/12723) 
#### Online DDL
 * CI: extend timeouts in onlineddl_vrepl due to slow CI runners [#12583](https://github.com/vitessio/vitess/pull/12583)
 * [release-16.0] CI: increase overall test timeouts for all OnlineDDL tests (#12584) [#12589](https://github.com/vitessio/vitess/pull/12589)
### Enhancement 
#### Build/CI
 * Auto upgrade the Golang version [#12585](https://github.com/vitessio/vitess/pull/12585)
### Internal Cleanup 
#### Build/CI
 * Run launchable only on PRs against `main`  [#12694](https://github.com/vitessio/vitess/pull/12694) 
#### General
 * Add a known issue into the release notes for xtrabackup and DDLs [#12536](https://github.com/vitessio/vitess/pull/12536)
### Release 
#### Build/CI
 * [release-16.0] Tooling improvements backports [#12528](https://github.com/vitessio/vitess/pull/12528) 
#### Documentation
 * Re-organize the `releasenotes` directory into `changelog` [#12566](https://github.com/vitessio/vitess/pull/12566)
 * Addition of the `v16.0.1` release summary [#12751](https://github.com/vitessio/vitess/pull/12751) 
#### General
 * Back to dev mode after v16.0.0 [#12515](https://github.com/vitessio/vitess/pull/12515)
 * Release 16.0 code freeze for 16.0.1 patch release [#12762](https://github.com/vitessio/vitess/pull/12762) 
#### VTAdmin
 * Add the vtadmin `web` directory to the release packages [#12639](https://github.com/vitessio/vitess/pull/12639)
### Testing 
#### General
 * Fix fullstatus test for backward compat [#12685](https://github.com/vitessio/vitess/pull/12685) 
#### VReplication
 * Flakes: Use new healthy shard check in vreplication e2e tests [#12502](https://github.com/vitessio/vitess/pull/12502)

