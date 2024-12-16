# Changelog of Vitess v20.0.4

### Bug fixes 
#### Query Serving
 * [release-20.0] Use proper keyspace when updating the query graph of a reference DML (#17226) [#17257](https://github.com/vitessio/vitess/pull/17257) 
#### Topology
 * [release-20.0] Close zookeeper topo connection on disconnect (#17136) [#17192](https://github.com/vitessio/vitess/pull/17192) 
#### VTTablet
 * [release-20.0] Fix deadlock in messager and health streamer (#17230) [#17234](https://github.com/vitessio/vitess/pull/17234)
 * [release-20.0] Fix potential deadlock in health streamer (#17261) [#17269](https://github.com/vitessio/vitess/pull/17269)
### CI/Build 
#### Build/CI
 * [release-20.0] Specify Ubuntu 24.04 for all jobs (#17278) [#17281](https://github.com/vitessio/vitess/pull/17281) 
#### Cluster management
 * [release-20.0] Fix flakiness in `TestListenerShutdown` (#17024) [#17188](https://github.com/vitessio/vitess/pull/17188) 
#### General
 * [release-20.0] Upgrade the Golang version to `go1.22.9` [#17212](https://github.com/vitessio/vitess/pull/17212)
### Enhancement 
#### Query Serving
 * [release-20.0] Fix to prevent stopping buffering prematurely (#17013) [#17204](https://github.com/vitessio/vitess/pull/17204)
### Internal Cleanup 
#### Build/CI
 * [release-20.0] Change the name of the vitess-tester repository (#16917) [#17029](https://github.com/vitessio/vitess/pull/17029)
### Testing 
#### Build/CI
 * [release-20.0] Flakes: Address flakiness in TestZkConnClosedOnDisconnect (#17194) [#17196](https://github.com/vitessio/vitess/pull/17196)

