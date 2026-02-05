# Changelog of Vitess v19.0.4

### Bug fixes 
#### Evalengine
 * [release-19.0] projection: Return correct collation information (#15801) [#15804](https://github.com/vitessio/vitess/pull/15804) 
#### General
 * [release-19.0] GRPC: Address potential segfault in dedicated connection pooling (#15751) [#15753](https://github.com/vitessio/vitess/pull/15753)
 * [release-19.0] Properly unescape keyspace name in FindAllShardsInKeyspace (#15765) [#15786](https://github.com/vitessio/vitess/pull/15786) 
#### Query Serving
 * [release-19.0] Fix TPCH test by providing the correct field information in evalengine (#15623) [#15655](https://github.com/vitessio/vitess/pull/15655)
 * [release-19.0] fix: don't forget DISTINCT for derived tables (#15672) [#15678](https://github.com/vitessio/vitess/pull/15678)
 * [release-19.0] Fix panic in aggregation (#15728) [#15736](https://github.com/vitessio/vitess/pull/15736)
 * [release-19.0] Fix wrong assignment to `sql_id_opt` in the parser (#15862) [#15869](https://github.com/vitessio/vitess/pull/15869) 
#### Topology
 * [release-19.0] discovery: Fix tablets removed from healthcheck when topo server GetTablet call fails (#15633) [#15681](https://github.com/vitessio/vitess/pull/15681)
 * [release-19.0] Fix ZooKeeper Topology connection locks not being cleaned up correctly (#15757) [#15764](https://github.com/vitessio/vitess/pull/15764) 
#### VReplication
 * [release-19.0] VReplication: Take replication lag into account in VStreamManager healthcheck result processing (#15761) [#15774](https://github.com/vitessio/vitess/pull/15774) 
#### VTAdmin
 * [release-19.0] [VTAdmin API] Fix schema cache flag, add documentation (#15704) [#15720](https://github.com/vitessio/vitess/pull/15720) 
#### VTorc
 * [release-19.0]: VTOrc optimize TMC usage (#15356) [#15759](https://github.com/vitessio/vitess/pull/15759)
### CI/Build 
#### General
 * [release-19.0] Upgrade the Golang version to `go1.22.3` [#15864](https://github.com/vitessio/vitess/pull/15864) 
#### VReplication
 * [release-19.0] VReplication: Get workflowFlavorVtctl endtoend testing working properly again (#15636) [#15667](https://github.com/vitessio/vitess/pull/15667)
### Internal Cleanup 
#### General
 * [release-19.0] changelogs: squash 19.0.2/19.0.3 into just 19.0.3 and remove 19.0.2 (#15665) [#15668](https://github.com/vitessio/vitess/pull/15668)
### Performance 
#### VTTablet
 * [release-19.0] Improve performance for `BaseShowTablesWithSizes` query. (#15713) [#15795](https://github.com/vitessio/vitess/pull/15795)
### Regression 
#### Query Serving
 * [release-19.0] Fix regression where inserts into reference tables with a different name on sharded keyspaces were not routed correctly. (#15796) [#15860](https://github.com/vitessio/vitess/pull/15860)
### Release 
#### General
 * [release-19.0] Bump to `v19.0.4-SNAPSHOT` after the `v19.0.3` release [#15662](https://github.com/vitessio/vitess/pull/15662)
 * [release-19.0] Code Freeze for `v19.0.4` [#15874](https://github.com/vitessio/vitess/pull/15874)

