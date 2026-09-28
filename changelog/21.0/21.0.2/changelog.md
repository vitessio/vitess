# Changelog of Vitess v21.0.2

### Bug fixes 
#### Evalengine
 * [release-21.0] Fix week number for date_format evalengine function (#17432) [#17452](https://github.com/vitessio/vitess/pull/17452) 
#### Java
 * [release-21.0] Use proper `groupId` for mysql connector in java (#17540) [#17541](https://github.com/vitessio/vitess/pull/17541) 
#### Query Serving
 * [release-21.0] Fix Data race in semi-join (#17417) [#17447](https://github.com/vitessio/vitess/pull/17447)
 * [release-21.0] vexplain to protect the log fields from concurrent writes (#17460) [#17463](https://github.com/vitessio/vitess/pull/17463)
 * [release-21.0] Reference Table DML Join Fix (#17414) [#17474](https://github.com/vitessio/vitess/pull/17474)
 * [release-21.0] Fix crash in the evalengine (#17487) [#17490](https://github.com/vitessio/vitess/pull/17490)
 * [release-21.0] Always return a valid timezone in cursor (#17546) [#17551](https://github.com/vitessio/vitess/pull/17551)
 * [release-21.0] sizegen: do not ignore type aliases (#17556) [#17557](https://github.com/vitessio/vitess/pull/17557) 
#### VReplication
 * [release-21.0] LookupVindex: fix CLI to allow creating non-unique lookups with single column (#17301) [#17349](https://github.com/vitessio/vitess/pull/17349)
 * [release-21.0] SwitchTraffic: use separate context while canceling a migration (#17340) [#17366](https://github.com/vitessio/vitess/pull/17366)
 * [release-21.0] LookupVindex bug fix: Fix typos from PR 17301 (#17423) [#17438](https://github.com/vitessio/vitess/pull/17438)
 * [release-21.0] Tablet picker: Handle the case where a primary tablet is not setup for a shard (#17573) [#17576](https://github.com/vitessio/vitess/pull/17576) 
#### VTorc
 * [release-21.0] Use uint64 for binary log file position (#17472) [#17507](https://github.com/vitessio/vitess/pull/17507) 
#### schema management
 * [release-21.0] schemadiff: skip keys with expressions in Online DDL analysis (#17475) [#17480](https://github.com/vitessio/vitess/pull/17480)
### CI/Build 
#### Build/CI
 * [release-21.0] use newer versions of actions in scorecard workflow (#17373) [#17374](https://github.com/vitessio/vitess/pull/17374)
 * [release-21.0] split upgrade downgrade queries test to 2 CI workflows (#17464) [#17494](https://github.com/vitessio/vitess/pull/17494) 
#### General
 * [release-21.0] Bump go version to 1.23.4 [#17336](https://github.com/vitessio/vitess/pull/17336)
 * [release-21.0] Upgrade the Golang version to `go1.23.5` [#17561](https://github.com/vitessio/vitess/pull/17561)
### Dependencies 
#### Build/CI
 * [release-21.0] Bump golang.org/x/crypto from 0.29.0 to 0.31.0 (#17376) [#17383](https://github.com/vitessio/vitess/pull/17383) 
#### General
 * [release-21.0] Bump golang.org/x/net from 0.29.0 to 0.33.0 (#17416) [#17422](https://github.com/vitessio/vitess/pull/17422)
 * [release-21.0] CVE Fix: Update glog to v1.2.4 (#17524) [#17534](https://github.com/vitessio/vitess/pull/17534) 
#### Java
 * [release-21.0] [Java]: Bump mysql-connector-java version from 8.0.33 to mysql-connector-j 8.4.0 (#17522) [#17527](https://github.com/vitessio/vitess/pull/17527) 
#### VTAdmin
 * [release-21.0] Bump nanoid from 3.3.7 to 3.3.8 in /web/vtadmin (#17375) [#17379](https://github.com/vitessio/vitess/pull/17379)
### Enhancement 
#### Documentation
 * [Direct PR] [V21 backport] CobraDocs: Remove commit hash from docs. Fix issue with workdir replacement (#17392) [#17444](https://github.com/vitessio/vitess/pull/17444) 
#### VTorc
 * [release-21.0] `vtorc`: require topo for `Healthy: true` in `/debug/health` (#17129) [#17353](https://github.com/vitessio/vitess/pull/17353)
### Internal Cleanup 
#### Build/CI
 * [release-21.0] Security improvements to GitHub Actions (#17520) [#17531](https://github.com/vitessio/vitess/pull/17531)
### Regression 
#### Java
 * [release-21.0] [Java] Fix dependency issues in Java package  (#17481) [#17484](https://github.com/vitessio/vitess/pull/17484)
### Release 
#### General
 * [release-21.0] Bump to `v21.0.2-SNAPSHOT` after the `v21.0.1` release [#17325](https://github.com/vitessio/vitess/pull/17325)
### Testing 
#### General
 * [release-21.0] Remove broken panic handler (#17354) [#17360](https://github.com/vitessio/vitess/pull/17360) 
#### Query Serving
 * [release-21.0] Ensure PRS runs for all the shards in `TestSemiSyncRequiredWithTwoPC` (#17384) [#17385](https://github.com/vitessio/vitess/pull/17385) 
#### VReplication
 * [release-21.0] Flaky test fix: TestMoveTablesSharded and TestMoveTablesUnsharded (#17343) [#17363](https://github.com/vitessio/vitess/pull/17363)
 * [release-21.0] Flaky TestMoveTables(Un)sharded: Handle race condition  (#17440) [#17455](https://github.com/vitessio/vitess/pull/17455)
 * [release-21.0] Flaky TestTickSkip: Remove inherently flaky test (#17504) [#17513](https://github.com/vitessio/vitess/pull/17513)

