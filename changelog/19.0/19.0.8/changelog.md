# Changelog of Vitess v19.0.8

### Bug fixes 
#### Topology
 * [release-19.0] Close zookeeper topo connection on disconnect (#17136) [#17191](https://github.com/vitessio/vitess/pull/17191) 
#### VTTablet
 * [release-19.0] Fix deadlock in messager and health streamer (#17230) [#17233](https://github.com/vitessio/vitess/pull/17233)
 * [release-19.0] Fix potential deadlock in health streamer (#17261) [#17268](https://github.com/vitessio/vitess/pull/17268)
### CI/Build 
#### Build/CI
 * [release-19.0] Specify Ubuntu 24.04 for all jobs (#17278) [#17280](https://github.com/vitessio/vitess/pull/17280) 
#### Cluster management
 * [release-19.0] Fix flakiness in `TestListenerShutdown` (#17024) [#17187](https://github.com/vitessio/vitess/pull/17187) 
#### General
 * [release-19.0] Upgrade the Golang version to `go1.22.9` [#17214](https://github.com/vitessio/vitess/pull/17214)
### Enhancement 
#### Query Serving
 * [release-19.0] Fix to prevent stopping buffering prematurely (#17013) [#17203](https://github.com/vitessio/vitess/pull/17203)
### Internal Cleanup 
#### Build/CI
 * [release-19.0] Change the name of the vitess-tester repository (#16917) [#17028](https://github.com/vitessio/vitess/pull/17028)
### Release 
#### General
 * [release-19.0] Bump to `v19.0.8-SNAPSHOT` after the `v19.0.7` release [#17158](https://github.com/vitessio/vitess/pull/17158)
 * [release-19.0] Code Freeze for `v19.0.8` [#17310](https://github.com/vitessio/vitess/pull/17310)
### Testing 
#### Build/CI
 * [release-19.0] Flakes: Address flakiness in TestZkConnClosedOnDisconnect (#17194) [#17195](https://github.com/vitessio/vitess/pull/17195)

