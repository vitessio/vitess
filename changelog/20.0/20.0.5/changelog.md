# Changelog of Vitess v20.0.5

### Bug fixes 
#### Query Serving
 * [release-20.0] Fix Data race in semi-join (#17417) [#17446](https://github.com/vitessio/vitess/pull/17446)
 * [release-20.0] Reference Table DML Join Fix (#17414) [#17473](https://github.com/vitessio/vitess/pull/17473)
 * [release-20.0] Fix crash in the evalengine (#17487) [#17489](https://github.com/vitessio/vitess/pull/17489)
 * [release-20.0] Always return a valid timezone in cursor (#17546) [#17550](https://github.com/vitessio/vitess/pull/17550) 
#### VReplication
 * [release-20.0] LookupVindex: fix CLI to allow creating non-unique lookups with single column (#17301) [#17348](https://github.com/vitessio/vitess/pull/17348)
 * [release-20.0] SwitchTraffic: use separate context while canceling a migration (#17340) [#17365](https://github.com/vitessio/vitess/pull/17365)
 * [release-20.0] LookupVindex bug fix: Fix typos from PR 17301 (#17423) [#17437](https://github.com/vitessio/vitess/pull/17437)
 * [release-20.0] Tablet picker: Handle the case where a primary tablet is not setup for a shard (#17573) [#17575](https://github.com/vitessio/vitess/pull/17575) 
#### VTorc
 * [release-20.0] Use uint64 for binary log file position (#17472) [#17506](https://github.com/vitessio/vitess/pull/17506) 
#### vttestserver
 * [release-20.0] parse transaction timeout as duration (#16338) [#17405](https://github.com/vitessio/vitess/pull/17405)
### CI/Build 
#### General
 * [release-20.0] Bump go version to 1.22.10 [#17337](https://github.com/vitessio/vitess/pull/17337)
 * [release-20.0] Upgrade the Golang version to `go1.22.11` [#17562](https://github.com/vitessio/vitess/pull/17562)
### Dependencies 
#### Build/CI
 * [release-20.0] Bump golang.org/x/crypto from 0.29.0 to 0.31.0 (#17376) [#17382](https://github.com/vitessio/vitess/pull/17382) 
#### General
 * [release-20.0] Bump golang.org/x/net from 0.25.0 to 0.33.0 (#17416) [#17421](https://github.com/vitessio/vitess/pull/17421)
 * [release-20.0] CVE Fix: Update glog to v1.2.4 (#17524) [#17533](https://github.com/vitessio/vitess/pull/17533) 
#### VTAdmin
 * [release-20.0] Bump nanoid from 3.3.7 to 3.3.8 in /web/vtadmin (#17375) [#17378](https://github.com/vitessio/vitess/pull/17378)
### Enhancement 
#### Documentation
 * [release-20.0] [Direct PR] [V21 backport] CobraDocs: Remove commit hash from docs. Fix issue with workdir replacement (#17392) (#17444) [#17451](https://github.com/vitessio/vitess/pull/17451) 
#### VTorc
 * [release-20.0] `vtorc`: require topo for `Healthy: true` in `/debug/health` (#17129) [#17352](https://github.com/vitessio/vitess/pull/17352)
### Internal Cleanup 
#### Build/CI
 * [release-20.0] Security improvements to GitHub Actions (#17520) [#17530](https://github.com/vitessio/vitess/pull/17530)
### Release 
#### General
 * [release-20.0] Bump to `v20.0.5-SNAPSHOT` after the `v20.0.4` release [#17323](https://github.com/vitessio/vitess/pull/17323)
### Testing 
#### General
 * [release-20.0] Remove broken panic handler (#17354) [#17359](https://github.com/vitessio/vitess/pull/17359) 
#### VReplication
 * [release-20.0] Flaky TestTickSkip: Remove inherently flaky test (#17504) [#17512](https://github.com/vitessio/vitess/pull/17512)

