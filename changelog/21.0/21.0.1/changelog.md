# Changelog of Vitess v21.0.1

### Bug fixes 
#### Backup and Restore
 * [release-21.0] Fix how we cancel the context in the builtin backup engine (#17285) [#17291](https://github.com/vitessio/vitess/pull/17291)
 * [release-21.0] S3: optional endpoint resolver and correct retrier [#17307](https://github.com/vitessio/vitess/pull/17307) 
#### Cluster management
 * [release-21.0] Fix panic in vttablet when closing topo server twice (#17094) [#17122](https://github.com/vitessio/vitess/pull/17122) 
#### Online DDL
 * [release-21.0] Online DDL: fix defer function, potential connection pool exhaustion (#17207) [#17210](https://github.com/vitessio/vitess/pull/17210) 
#### Query Serving
 * [release-21.0] bugfix: treat EXPLAIN like SELECT (#17054) [#17058](https://github.com/vitessio/vitess/pull/17058)
 * [release-21.0] Delegate Column Availability Checks to MySQL for Single-Route Queries (#17077) [#17087](https://github.com/vitessio/vitess/pull/17087)
 * [release-21.0] bugfix: Handle CTEs with columns named in the CTE def (#17179) [#17181](https://github.com/vitessio/vitess/pull/17181)
 * [release-21.0] Use proper keyspace when updating the query graph of a reference DML (#17226) [#17258](https://github.com/vitessio/vitess/pull/17258) 
#### Topology
 * [release-21.0] Close zookeeper topo connection on disconnect (#17136) [#17193](https://github.com/vitessio/vitess/pull/17193) 
#### VReplication
 * [release-21.0] VReplication: Qualify and SQL escape tables in created AutoIncrement VSchema definitions (#17174) [#17176](https://github.com/vitessio/vitess/pull/17176) 
#### VTTablet
 * [release-21.0] Fix deadlock in messager and health streamer (#17230) [#17235](https://github.com/vitessio/vitess/pull/17235)
 * [release-21.0] Fix potential deadlock in health streamer (#17261) [#17270](https://github.com/vitessio/vitess/pull/17270)
### CI/Build 
#### Build/CI
 * [release-21.0] Specify Ubuntu 24.04 for all jobs (#17278) [#17282](https://github.com/vitessio/vitess/pull/17282) 
#### Cluster management
 * [release-21.0] Fix flakiness in `TestListenerShutdown` (#17024) [#17189](https://github.com/vitessio/vitess/pull/17189) 
#### General
 * [release-21.0] Upgrade the Golang version to `go1.23.3` [#17211](https://github.com/vitessio/vitess/pull/17211)
### Dependencies 
#### Java
 * [release-21.0] java package updates for grpc and protobuf and release plugins (#17100) [#17105](https://github.com/vitessio/vitess/pull/17105)
### Documentation 
#### Documentation
 * [Direct PR][release-21.0] Add RPC changes segment in the summary doc [#17034](https://github.com/vitessio/vitess/pull/17034)
### Enhancement 
#### Online DDL
 * [release-21.0] Improve Schema Engine's TablesWithSize80 query (#17066) [#17091](https://github.com/vitessio/vitess/pull/17091) 
#### Query Serving
 * [release-21.0] Fix to prevent stopping buffering prematurely (#17013) [#17205](https://github.com/vitessio/vitess/pull/17205) 
#### VReplication
 * [release-21.0] Binlog: Improve ZstdInMemoryDecompressorMaxSize management (#17220) [#17241](https://github.com/vitessio/vitess/pull/17241)
### Internal Cleanup 
#### Build/CI
 * [release-21.0] Change the name of the vitess-tester repository (#16917) [#17030](https://github.com/vitessio/vitess/pull/17030)
### Regression 
#### Backup and Restore
 * [release-21.0] Fix unreachable errors when taking a backup (#17062) [#17112](https://github.com/vitessio/vitess/pull/17112)
### Release 
#### General
 * [release-21.0] Bump to `v21.0.1-SNAPSHOT` after the `v21.0.0` release [#17098](https://github.com/vitessio/vitess/pull/17098)
### Testing 
#### Build/CI
 * [release-21.0] Flakes: Address flakiness in TestZkConnClosedOnDisconnect (#17194) [#17197](https://github.com/vitessio/vitess/pull/17197) 
#### Query Serving
 * [release-21.0] fix: flaky test on twopc transaction (#17068) [#17070](https://github.com/vitessio/vitess/pull/17070)

