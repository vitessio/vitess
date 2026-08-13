# Changelog of Vitess v24.0.2

### Bug fixes 
#### Backup and Restore
 * [release-24.0] mysqlctl: propagate remote shutdown timeout (#20059) [#20075](https://github.com/vitessio/vitess/pull/20075)
 * [release-24.0] fix(backup): Clear lastErr on repl healthy (#20111) [#20139](https://github.com/vitessio/vitess/pull/20139) 
#### Cluster management
 * [release-24.0] discovery: fix keyspaceState leak for keyspaces missing in localCell (#19993) [#20109](https://github.com/vitessio/vitess/pull/20109) 
#### Query Serving
 * [release-24.0] smartconnpool: don't hand returned connections to expired waiters (#20308) [#20355](https://github.com/vitessio/vitess/pull/20355) 
#### Topology
 * [release-24.0] etcd2topo: bound lock-acquisition cleanup RPCs (#20149) [#20152](https://github.com/vitessio/vitess/pull/20152) 
#### VReplication
 * [release-24.0] tests: give vtctl/workflow tests their own port range (#20150) [#20156](https://github.com/vitessio/vitess/pull/20156)
 * [release-24.0] binlog: preserve leading zero on DECIMAL JSON values like 0.1 (#20228) [#20231](https://github.com/vitessio/vitess/pull/20231)
 * [release-24.0] binlog: trim leading zeroes for DECIMALs the 0.1 fix missed (#20232) [#20237](https://github.com/vitessio/vitess/pull/20237) 
#### VTGate
 * [release-24.0] vtgate: use `ToBoolean()` in the `Filter` streaming path (#20245) [#20250](https://github.com/vitessio/vitess/pull/20250)
 * [release-24.0] vtgate: build specialized plans for prepared statements in OLAP/streaming mode (#20266) [#20305](https://github.com/vitessio/vitess/pull/20305)
 * [release-24.0] vtgate: fix vstream StopOnReshard hang on reshard convergence (#20262) [#20312](https://github.com/vitessio/vitess/pull/20312)
 * [release-24.0] vtgate healthcheck: drop stale updates from a replaced tablet's canceled checkConn (#20328) [#20375](https://github.com/vitessio/vitess/pull/20375) 
#### VTOrc
 * [release-24.0] VTOrc: fix `PrimaryIsReadOnly` recovery deadlock against `PrimarySemiSyncBlocked` (#20015) [#20083](https://github.com/vitessio/vitess/pull/20083)
 * [release-24.0] vtorc: reset `CurrentErrantGTIDCount` gauge when errant GTIDs are resolved (#20259) [#20292](https://github.com/vitessio/vitess/pull/20292) 
#### VTTablet
 * [release-24.0] smartconnpool: fix MaxLifetime jitter (#20118) [#20128](https://github.com/vitessio/vitess/pull/20128)
 * [release-24.0] smartconnpool: keep refresh worker running after reopen (#20119) [#20129](https://github.com/vitessio/vitess/pull/20129)
 * [release-24.0] smartconnpool: avoid idle reopen stalls (#20079) [#20134](https://github.com/vitessio/vitess/pull/20134)
 * [release-24.0] smartconnpool: avoid close deadlock with refresh reopen (#20157) [#20181](https://github.com/vitessio/vitess/pull/20181)
 * [release-24.0] smartconnpool: unblock Close and preserve Setting on reopen (#20122) [#20201](https://github.com/vitessio/vitess/pull/20201)
 * [release-24.0] smartconnpool: reject SetCapacity on a closed pool + shutdown stress tests (#20158) [#20207](https://github.com/vitessio/vitess/pull/20207)
 * [release-24.0] vttablet: stop Stream from clobbering non-keyspace field schemas (#20265) [#20303](https://github.com/vitessio/vitess/pull/20303)
### CI/Build 
#### Build/CI
 * [release-24.0] ci: fall back to GitHub-hosted runners on forks (#20165) [#20183](https://github.com/vitessio/vitess/pull/20183)
 * [release-24.0] tests: stop passing --watch-replication-stream in tests [#20194](https://github.com/vitessio/vitess/pull/20194)
 * [release-24.0] ci: bump codecov-action to v6.0.2 to fix GPG verification failure (#20278) [#20288](https://github.com/vitessio/vitess/pull/20288) 
#### VTTablet
 * [release-24.0] vstreamer: make `TestSpec.Close()` nil-safe (#20246) [#20252](https://github.com/vitessio/vitess/pull/20252)
### Dependencies 
#### Build/CI
 * [release-24.0] build: move dev tools into per-tool Go modules (#20293) [#20298](https://github.com/vitessio/vitess/pull/20298) 
#### Docker
 * [release-24.0] Upgrade the Golang version to `go1.26.3` [#20065](https://github.com/vitessio/vitess/pull/20065)
 * [release-24.0] Upgrade the Golang version to `go1.26.4` [#20336](https://github.com/vitessio/vitess/pull/20336) 
#### VTAdmin
 * [release-24.0] Resolve outstanding VTAdmin audit warnings (#20072) [#20077](https://github.com/vitessio/vitess/pull/20077)
### Release 
#### General
 * [release-24.0] Code Freeze for `v24.0.2` [#20390](https://github.com/vitessio/vitess/pull/20390)
### Security 
#### VTGate
 * [release-24.0] servenv: constant-time password compare in static grpc auth plugin (#20264) [#20272](https://github.com/vitessio/vitess/pull/20272) 
#### VTTablet
 * [release-24.0] tabletserver: escape reflected form values in twopcz handler (#20255) [#20274](https://github.com/vitessio/vitess/pull/20274)
### Testing 
#### Backup and Restore
 * [release-24.0] mysqlctl: prevent test hang on missing remote shutdown RPC (#20164) [#20172](https://github.com/vitessio/vitess/pull/20172) 
#### VTTablet
 * [release-24.0] vreplication: fix TestPlayerDDL flake on multi-source GTID positions (#20229) [#20235](https://github.com/vitessio/vitess/pull/20235)

