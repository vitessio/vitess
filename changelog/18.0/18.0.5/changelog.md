# Changelog of Vitess v18.0.5

### Bug fixes 
#### Query Serving
 * [release-18.0] fix: don't forget DISTINCT for derived tables (#15672) [#15677](https://github.com/vitessio/vitess/pull/15677)
 * [release-18.0] Fix panic in aggregation (#15728) [#15735](https://github.com/vitessio/vitess/pull/15735)
 * [release-18.0] Fix wrong assignment to `sql_id_opt` in the parser (#15862) [#15868](https://github.com/vitessio/vitess/pull/15868) 
#### Topology
 * [release-18.0] Fix ZooKeeper Topology connection locks not being cleaned up correctly (#15757) [#15763](https://github.com/vitessio/vitess/pull/15763) 
#### VReplication
 * [release-18.0] VReplication: Take replication lag into account in VStreamManager healthcheck result processing (#15761) [#15773](https://github.com/vitessio/vitess/pull/15773) 
#### VTAdmin
 * [release-18.0] [VTAdmin API] Fix schema cache flag, add documentation (#15704) [#15719](https://github.com/vitessio/vitess/pull/15719)
 * [VTAdmin] Remove vtctld web link, improve local example (#15607) [#15825](https://github.com/vitessio/vitess/pull/15825)
### CI/Build 
#### General
 * [release-18.0] Upgrade the Golang version to `go1.21.10` [#15866](https://github.com/vitessio/vitess/pull/15866) 
#### VReplication
 * [release-18.0] VReplication: Get workflowFlavorVtctl endtoend testing working properly again (#15636) [#15666](https://github.com/vitessio/vitess/pull/15666) 
#### VTAdmin
 * [release-18.0] Update VTAdmin build script (#15839) [#15849](https://github.com/vitessio/vitess/pull/15849)
### Performance 
#### VTTablet
 * [release-18.0] Improve performance for `BaseShowTablesWithSizes` query. (#15713) [#15793](https://github.com/vitessio/vitess/pull/15793)
### Regression 
#### Query Serving
 * [release-18.0] Direct PR. Fix regression where reference tables with a different name on sharded keyspaces were not routed correctly. [#15788](https://github.com/vitessio/vitess/pull/15788)
### Release 
#### General
 * [release-18.0] Bump to `v18.0.5-SNAPSHOT` after the `v18.0.4` release [#15660](https://github.com/vitessio/vitess/pull/15660)
 * [release-18.0] Code Freeze for `v18.0.5` [#15876](https://github.com/vitessio/vitess/pull/15876)

