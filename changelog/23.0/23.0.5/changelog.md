# Changelog of Vitess v23.0.5

### Bug fixes 
#### Backup and Restore
 * [release-23.0] mysqlctl: propagate remote shutdown timeout (#20059) [#20074](https://github.com/vitessio/vitess/pull/20074) 
#### Cluster management
 * [release-23.0] discovery: fix keyspaceState leak for keyspaces missing in localCell (#19993) [#20108](https://github.com/vitessio/vitess/pull/20108) 
#### Query Serving
 * [release-23.0] smartconnpool: don't hand returned connections to expired waiters (#20308) [#20354](https://github.com/vitessio/vitess/pull/20354) 
#### Topology
 * [release-23.0] etcd2topo: bound lock-acquisition cleanup RPCs (#20149) [#20151](https://github.com/vitessio/vitess/pull/20151) 
#### VReplication
 * [release-23.0] tests: give vtctl/workflow tests their own port range (#20150) [#20155](https://github.com/vitessio/vitess/pull/20155)
 * [release-23.0] binlog: preserve leading zero on DECIMAL JSON values like 0.1 (#20228) [#20230](https://github.com/vitessio/vitess/pull/20230)
 * [release-23.0] binlog: trim leading zeroes for DECIMALs the 0.1 fix missed (#20232) [#20236](https://github.com/vitessio/vitess/pull/20236) 
#### VTGate
 * [release-23.0] planner: substitute merged DML IN/NOT IN subqueries via ListArg placeholder (#19973) [#20029](https://github.com/vitessio/vitess/pull/20029)
 * [release-23.0] vtgate: use `ToBoolean()` in the `Filter` streaming path (#20245) [#20249](https://github.com/vitessio/vitess/pull/20249)
 * [release-23.0] vtgate: build specialized plans for prepared statements in OLAP/streaming mode (#20266) [#20304](https://github.com/vitessio/vitess/pull/20304)
 * [release-23.0] vtgate: fix vstream StopOnReshard hang on reshard convergence (#20262) [#20311](https://github.com/vitessio/vitess/pull/20311)
 * [release-23.0] vtgate healthcheck: drop stale updates from a replaced tablet's canceled checkConn (#20328) [#20374](https://github.com/vitessio/vitess/pull/20374) 
#### VTOrc
 * [release-23.0] vtorc: fix data race in forgetAliases cache initialization (#19843) [#19846](https://github.com/vitessio/vitess/pull/19846)
 * [release-23.0] VTOrc: fix `PrimaryIsReadOnly` recovery deadlock against `PrimarySemiSyncBlocked` (#20015) [#20082](https://github.com/vitessio/vitess/pull/20082)
 * [release-23.0] vtorc: reset `CurrentErrantGTIDCount` gauge when errant GTIDs are resolved (#20259) [#20291](https://github.com/vitessio/vitess/pull/20291) 
#### VTTablet
 * [release-23.0] smartconnpool: fix MaxLifetime jitter (#20118) [#20126](https://github.com/vitessio/vitess/pull/20126)
 * [release-23.0] smartconnpool: keep refresh worker running after reopen (#20119) [#20127](https://github.com/vitessio/vitess/pull/20127)
 * [release-23.0] smartconnpool: avoid idle reopen stalls (#20079) [#20133](https://github.com/vitessio/vitess/pull/20133)
 * [release-23.0] smartconnpool: avoid close deadlock with refresh reopen (#20157) [#20180](https://github.com/vitessio/vitess/pull/20180)
 * [release-23.0] smartconnpool: unblock Close and preserve Setting on reopen (#20122) [#20200](https://github.com/vitessio/vitess/pull/20200)
 * [release-23.0] smartconnpool: reject SetCapacity on a closed pool + shutdown stress tests (#20158) [#20206](https://github.com/vitessio/vitess/pull/20206)
 * [release-23.0] vttablet: stop Stream from clobbering non-keyspace field schemas (#20265) [#20302](https://github.com/vitessio/vitess/pull/20302)
### CI/Build 
#### Build/CI
 * [release-23.0] ci: fall back to GitHub-hosted runners on forks (#20165) [#20182](https://github.com/vitessio/vitess/pull/20182)
 * [release-23.0] ci: bump codecov-action to v5.5.5 to fix GPG verification failure (#20278) [#20287](https://github.com/vitessio/vitess/pull/20287) 
#### VTTablet
 * [release-23.0] vstreamer: make `TestSpec.Close()` nil-safe (#20246) [#20251](https://github.com/vitessio/vitess/pull/20251)
### Dependencies 
#### Build/CI
 * [release-23.0] build: move dev tools into per-tool Go modules (#20293) [#20296](https://github.com/vitessio/vitess/pull/20296) 
#### Docker
 * [release-23.0] Upgrade the Golang version to `go1.25.10` [#20064](https://github.com/vitessio/vitess/pull/20064)
 * [release-23.0] Upgrade the Golang version to `go1.25.11` [#20338](https://github.com/vitessio/vitess/pull/20338) 
#### VTAdmin
 * [release-23.0] Resolve outstanding VTAdmin audit warnings (#20072) [#20076](https://github.com/vitessio/vitess/pull/20076)
### Release 
#### General
 * [release-23.0] Bump to `v23.0.5-SNAPSHOT` after the `v23.0.4` release [#20053](https://github.com/vitessio/vitess/pull/20053)
 * [release-23.0] Code Freeze for `v23.0.5` [#20386](https://github.com/vitessio/vitess/pull/20386)
### Security 
#### VTGate
 * [release-23.0] servenv: constant-time password compare in static grpc auth plugin (#20264) [#20271](https://github.com/vitessio/vitess/pull/20271) 
#### VTTablet
 * [release-23.0] tabletserver: escape reflected form values in twopcz handler (#20255) [#20273](https://github.com/vitessio/vitess/pull/20273)
### Testing 
#### Backup and Restore
 * [release-23.0] mysqlctl: prevent test hang on missing remote shutdown RPC (#20164) [#20171](https://github.com/vitessio/vitess/pull/20171) 
#### VTTablet
 * [release-23.0] vreplication: fix TestPlayerDDL flake on multi-source GTID positions (#20229) [#20234](https://github.com/vitessio/vitess/pull/20234)

