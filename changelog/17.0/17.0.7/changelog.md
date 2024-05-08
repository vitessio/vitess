# Changelog of Vitess v17.0.7

### Bug fixes 
#### Query Serving
 * [release-17.0] TxThrottler: dont throttle unless lag (#14789) [#15189](https://github.com/vitessio/vitess/pull/15189)
 * [release-17.0] Fix aliasing in routes that have a derived table (#15550) [#15552](https://github.com/vitessio/vitess/pull/15552)
 * [release-17.0] fix: don't forget DISTINCT for derived tables (#15672) [#15676](https://github.com/vitessio/vitess/pull/15676)
 * [release-17.0] Fix wrong assignment to `sql_id_opt` in the parser (#15862) [#15867](https://github.com/vitessio/vitess/pull/15867) 
#### Topology
 * [release-17.0] Fix ZooKeeper Topology connection locks not being cleaned up correctly (#15757) [#15762](https://github.com/vitessio/vitess/pull/15762) 
#### VReplication
 * [release-17.0] VReplication: Take replication lag into account in VStreamManager healthcheck result processing (#15761) [#15772](https://github.com/vitessio/vitess/pull/15772) 
#### VTAdmin
 * [release-17.0] [VTAdmin API] Fix schema cache flag, add documentation (#15704) [#15718](https://github.com/vitessio/vitess/pull/15718)
### CI/Build 
#### Build/CI
 * [release-17.0] Update to latest CodeQL (#15530) [#15532](https://github.com/vitessio/vitess/pull/15532)
 * [release-17.0] Upgrade go version in upgrade tests to `go1.21.9` [#15640](https://github.com/vitessio/vitess/pull/15640) 
#### General
 * [release-17.0] Upgrade Golang from `v1.20.13` to `v1.21.9` [#15669](https://github.com/vitessio/vitess/pull/15669)
 * [release-17.0] Upgrade the Golang version to `go1.21.10` [#15863](https://github.com/vitessio/vitess/pull/15863)
### Performance 
#### VTTablet
 * [release-17.0] Improve performance for `BaseShowTablesWithSizes` query. (#15713) [#15792](https://github.com/vitessio/vitess/pull/15792)
### Release 
#### General
 * [release-17.0] Bump to `v17.0.7-SNAPSHOT` after the `v17.0.6` release [#15487](https://github.com/vitessio/vitess/pull/15487)
 * [release-17.0] Code Freeze for `v17.0.7` [#15878](https://github.com/vitessio/vitess/pull/15878)

