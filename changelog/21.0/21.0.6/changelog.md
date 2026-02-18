# Changelog of Vitess v21.0.6

### Bug fixes 
#### Build/CI
 * [release-21.0] [CI] Use the draft state from the event payload instead of calling `curl`. (#18650) [#18651](https://github.com/vitessio/vitess/pull/18651) 
#### Online DDL
 * [release-21.0] Online DDL: resume vreplication after cut-over/RENAME failure (#18428) [#18436](https://github.com/vitessio/vitess/pull/18436)
 * [release-21.0] copy_state: use a mediumblob instead of a smaller varbinary for lastpk (#18852) [#18857](https://github.com/vitessio/vitess/pull/18857) 
#### Query Serving
 * [release-21.0] bugfix: INSERT IGNORE not inserting rows (#18151) [#18164](https://github.com/vitessio/vitess/pull/18164)
 * [release-21.0] Fix watcher storm during topo outages (#18434) [#18439](https://github.com/vitessio/vitess/pull/18439)
 * [release-21.0] Fix scalar aggregation with literals in empty result sets (#18477) [#18490](https://github.com/vitessio/vitess/pull/18490)
 * [release-21.0] Fix for simple projection showing no fields (#18489) [#18492](https://github.com/vitessio/vitess/pull/18492)
 * [release-21.0] bugfix: Plan group by only on top of derived tables correctly (#18505) [#18510](https://github.com/vitessio/vitess/pull/18510)
 * [release-21.0]  Fix foreign key relation with routed tables (#18537) [#18540](https://github.com/vitessio/vitess/pull/18540)
 * [release-21.0] fix: remove database qualifier after building query in operator to sql (#18602) [#18604](https://github.com/vitessio/vitess/pull/18604)
 * [release-21.0] Introduce aliases for foreign keys verify operations (#18601) [#18613](https://github.com/vitessio/vitess/pull/18613)
 * [release-21.0] CONNPOOL: Fix race condition when waiting for connection (#18713) [#18720](https://github.com/vitessio/vitess/pull/18720)
 * [release-21.0] Fix handling of tuple bind variables in filtering operations. (#18736) [#18745](https://github.com/vitessio/vitess/pull/18745) 
#### VDiff
 * [release-21.0] Fix a panic in VDiff when reconciling extra rows. (#18585) [#18595](https://github.com/vitessio/vitess/pull/18595) 
#### VReplication
 * [release-21.0] VReplication: Fix bug while reading _vt.vreplication record (#18478) [#18482](https://github.com/vitessio/vitess/pull/18482)
 * [release-21.0] Avoid terminating atomic copy workflows on error if they are out of copy phase (#18475) [#18486](https://github.com/vitessio/vitess/pull/18486) 
#### VTGate
 * [release-21.0] fix: ensure callbacks are not called after `VStream` returns (#18689) [#18704](https://github.com/vitessio/vitess/pull/18704) 
#### VTTablet
 * [release-21.0] [Bugfix] Broken Heartbeat system in Row Streamer (#18390) [#18397](https://github.com/vitessio/vitess/pull/18397)
 * [release-21.0] Reset in-memory sequence info on vttablet on UpdateSequenceTables request (#18415) [#18444](https://github.com/vitessio/vitess/pull/18444)
 * [release-21.0] Fix `vttablet` not being marked as not serving when MySQL stalls (#17883) [#18453](https://github.com/vitessio/vitess/pull/18453) 
#### vtctl
 * [release-21.0] fix: Fix `GenerateShardRanges` returning shard names that don't cover the full range (#18641) [#18653](https://github.com/vitessio/vitess/pull/18653)
### CI/Build 
#### Build/CI
 * [release-21.0] Try updating the create PR workflow step (#18563) [#18570](https://github.com/vitessio/vitess/pull/18570)
 * [release-21.0] ci: use the newest mysql apt config package (#18790) [#18792](https://github.com/vitessio/vitess/pull/18792)
### Dependencies 
#### VTAdmin
 * [release-21.0] Bump vite from 4.5.9 to 4.5.14 in /web/vtadmin (#18485) [#18499](https://github.com/vitessio/vitess/pull/18499)
 * [release-21.0] Bump @babel/runtime from 7.26.0 to 7.27.6 in /web/vtadmin (#18467) [#18501](https://github.com/vitessio/vitess/pull/18501)
 * [release-21.0] Bump form-data from 4.0.1 to 4.0.4 in /web/vtadmin (#18473) [#18503](https://github.com/vitessio/vitess/pull/18503)
### Documentation 
#### Governance
 * [release-21.0] Update codeowners and maintainers. [#18677](https://github.com/vitessio/vitess/pull/18677)
 * [release-21.0] Update CODEOWNERS (#18697) [#18698](https://github.com/vitessio/vitess/pull/18698)
### Enhancement 
#### Build/CI
 * [release-21.0] Simplify workflow files. (#18649) [#18655](https://github.com/vitessio/vitess/pull/18655) 
#### Query Serving
 * [release-21.0] bugfix: Fix impossible query for UNION (#18463) [#18464](https://github.com/vitessio/vitess/pull/18464)
### Internal Cleanup 
#### Build/CI
 * [release-21.0] ci: Replace `always()` with `!cancelled()`. (#18659) [#18661](https://github.com/vitessio/vitess/pull/18661)
 * [release-21.0] ci: Disable man-db auto updates. (#18665) [#18667](https://github.com/vitessio/vitess/pull/18667)
 * [release-21.0] ci: Bump `actions/setup-go` to `v5.5.0`. (#18660) [#18669](https://github.com/vitessio/vitess/pull/18669)
 * [release-21.0] ci: don't run codecov twice. (#18680) [#18681](https://github.com/vitessio/vitess/pull/18681)
### Performance 
#### VTTablet
 * [release-21.0] Fix: Improve VDiff internal query performance (#18579) [#18631](https://github.com/vitessio/vitess/pull/18631)
### Regression 
#### Schema Tracker
 * [release-21.0] Backport: Fix GetSchema RPC to prevent returning view definitions when EnableViews is disabled [#18734](https://github.com/vitessio/vitess/pull/18734)
### Release 
#### General
 * [release-21.0] Bump to `v21.0.6-SNAPSHOT` after the `v21.0.5` release [#18382](https://github.com/vitessio/vitess/pull/18382)
 * [release-21.0] Code Freeze for `v21.0.6` [#18875](https://github.com/vitessio/vitess/pull/18875)
### Security 
#### Backup and Restore
 * [release-21.0] Address dir traversal in file backup storage `GetBackups` RPC (#18814) [#18816](https://github.com/vitessio/vitess/pull/18816)
### Testing 
#### VReplication
 * [release-21.0] test: Fix race condition in TestStreamRowsHeartbeat (#18414) [#18419](https://github.com/vitessio/vitess/pull/18419)
 * [release-21.0] CI: Fix `VDiff2` flaky e2e test (#18494) [#18525](https://github.com/vitessio/vitess/pull/18525) 
#### VTTablet
 * [release-21.0] connpool: Bump the hang detection timeout to fix flakiness (#18722) [#18723](https://github.com/vitessio/vitess/pull/18723)

