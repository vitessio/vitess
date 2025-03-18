# Changelog of Vitess v19.0.9

### Bug fixes 
#### Query Serving
 * [release-19.0] Fix Data race in semi-join (#17417) [#17445](https://github.com/vitessio/vitess/pull/17445)
 * [release-19.0] Always return a valid timezone in cursor (#17546) [#17549](https://github.com/vitessio/vitess/pull/17549) 
#### VReplication
 * [release-19.0] LookupVindex: fix CLI to allow creating non-unique lookups with single column (#17301) [#17347](https://github.com/vitessio/vitess/pull/17347)
 * [release-19.0] SwitchTraffic: use separate context while canceling a migration (#17340) [#17364](https://github.com/vitessio/vitess/pull/17364)
 * [release-19.0] LookupVindex bug fix: Fix typos from PR 17301 (#17423) [#17436](https://github.com/vitessio/vitess/pull/17436)
 * [Direct PR] Fix merge issue in backport  [#17509](https://github.com/vitessio/vitess/pull/17509)
 * [release-19.0] Tablet picker: Handle the case where a primary tablet is not setup for a shard (#17573) [#17574](https://github.com/vitessio/vitess/pull/17574) 
#### VTorc
 * [release-19.0] Use uint64 for binary log file position (#17472) [#17505](https://github.com/vitessio/vitess/pull/17505) 
#### vttestserver
 * [release-19.0] parse transaction timeout as duration (#16338) [#17406](https://github.com/vitessio/vitess/pull/17406)
### CI/Build 
#### General
 * [release-19.0] Bump go version to 1.22.10 [#17338](https://github.com/vitessio/vitess/pull/17338)
 * [release-19.0] Upgrade the Golang version to `go1.22.11` [#17564](https://github.com/vitessio/vitess/pull/17564)
### Dependencies 
#### Build/CI
 * [release-19.0] Bump golang.org/x/crypto from 0.29.0 to 0.31.0 (#17376) [#17381](https://github.com/vitessio/vitess/pull/17381) 
#### General
 * [release-19.0] Bump golang.org/x/net from 0.25.0 to 0.33.0 (#17416) [#17420](https://github.com/vitessio/vitess/pull/17420)
 * [release-19.0] CVE Fix: Update glog to v1.2.4 (#17524) [#17532](https://github.com/vitessio/vitess/pull/17532) 
#### VTAdmin
 * [release-19.0] Bump nanoid from 3.3.7 to 3.3.8 in /web/vtadmin (#17375) [#17377](https://github.com/vitessio/vitess/pull/17377)
### Enhancement 
#### Documentation
 * [release-19.0] [Direct PR] [V21 backport] CobraDocs: Remove commit hash from docs. Fix issue with workdir replacement (#17392) (#17444) [#17450](https://github.com/vitessio/vitess/pull/17450) 
#### VTorc
 * [release-19.0] `vtorc`: require topo for `Healthy: true` in `/debug/health` (#17129) [#17351](https://github.com/vitessio/vitess/pull/17351)
### Internal Cleanup 
#### Build/CI
 * [release-19.0] Security improvements to GitHub Actions (#17520) [#17528](https://github.com/vitessio/vitess/pull/17528)
### Regression 
#### Query Serving
 * Backport v19: Fixing Column aliases in outer join queries (#15384) [#17418](https://github.com/vitessio/vitess/pull/17418)
### Release 
#### General
 * [release-19.0] Bump to `v19.0.9-SNAPSHOT` after the `v19.0.8` release [#17321](https://github.com/vitessio/vitess/pull/17321)
 * [release-19.0] Code Freeze for `v19.0.9` [#17587](https://github.com/vitessio/vitess/pull/17587)
### Testing 
#### General
 * [release-19.0] Remove broken panic handler (#17354) [#17358](https://github.com/vitessio/vitess/pull/17358) 
#### VReplication
 * [release-19.0] Flaky TestTickSkip: Remove inherently flaky test (#17504) [#17511](https://github.com/vitessio/vitess/pull/17511)

