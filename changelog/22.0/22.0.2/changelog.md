# Changelog of Vitess v22.0.2

### Bug fixes 
#### Backup and Restore
 * [release-22.0] BuiltinBackupEngine: Retry file close and fail backup when we cannot (#18848) [#18861](https://github.com/vitessio/vitess/pull/18861) 
#### Build/CI
 * [release-22.0] [CI] Use the draft state from the event payload instead of calling `curl`. (#18650) [#18652](https://github.com/vitessio/vitess/pull/18652) 
#### Online DDL
 * [release-22.0] Online DDL: resume vreplication after cut-over/RENAME failure (#18428) [#18437](https://github.com/vitessio/vitess/pull/18437)
 * [release-22.0] copy_state: use a mediumblob instead of a smaller varbinary for lastpk (#18852) [#18858](https://github.com/vitessio/vitess/pull/18858) 
#### Query Serving
 * [release-22.0] Fix watcher storm during topo outages (#18434) [#18440](https://github.com/vitessio/vitess/pull/18440)
 * [release-22.0] Fix scalar aggregation with literals in empty result sets (#18477) [#18491](https://github.com/vitessio/vitess/pull/18491)
 * [release-22.0] Fix for simple projection showing no fields (#18489) [#18493](https://github.com/vitessio/vitess/pull/18493)
 * [release-22.0] bugfix: Plan group by only on top of derived tables correctly (#18505) [#18511](https://github.com/vitessio/vitess/pull/18511)
 * [release-22.0]  Fix foreign key relation with routed tables (#18537) [#18541](https://github.com/vitessio/vitess/pull/18541)
 * [release-22.0] fix: remove database qualifier after building query in operator to sql (#18602) [#18605](https://github.com/vitessio/vitess/pull/18605)
 * [release-22.0] Introduce aliases for foreign keys verify operations (#18601) [#18614](https://github.com/vitessio/vitess/pull/18614)
 * [release-22.0] CONNPOOL: Fix race condition when waiting for connection (#18713) [#18721](https://github.com/vitessio/vitess/pull/18721)
 * [release-22.0] Fix handling of tuple bind variables in filtering operations. (#18736) [#18746](https://github.com/vitessio/vitess/pull/18746) 
#### Topology
 * [release-22.0] Topo: Add NamedLock test for zk2 and consul and get them passing (#18407) [#18410](https://github.com/vitessio/vitess/pull/18410) 
#### VDiff
 * [release-22.0] Fix a panic in VDiff when reconciling extra rows. (#18585) [#18596](https://github.com/vitessio/vitess/pull/18596) 
#### VReplication
 * [release-22.0] VReplication: Fix bug while reading _vt.vreplication record (#18478) [#18483](https://github.com/vitessio/vitess/pull/18483)
 * [release-22.0] Avoid terminating atomic copy workflows on error if they are out of copy phase (#18475) [#18487](https://github.com/vitessio/vitess/pull/18487)
 * [release-22.0] VReplication: Ensure proper handling of keyspace/database names with dashes (#18762) [#18772](https://github.com/vitessio/vitess/pull/18772)
 * [release-22.0] VReplication: Treat ER_BINLOG_CREATE_ROUTINE_NEED_SUPER as unrecoverable (#18784) [#18819](https://github.com/vitessio/vitess/pull/18819) 
#### VTGate
 * [release-22.0] fix: ensure callbacks are not called after `VStream` returns (#18689) [#18705](https://github.com/vitessio/vitess/pull/18705) 
#### VTTablet
 * [release-22.0] [Bugfix] Broken Heartbeat system in Row Streamer (#18390) [#18398](https://github.com/vitessio/vitess/pull/18398)
 * [release-22.0] Reset in-memory sequence info on vttablet on UpdateSequenceTables request (#18415) [#18445](https://github.com/vitessio/vitess/pull/18445)
 * [release-22.0] Fix `vttablet` not being marked as not serving when MySQL stalls (#17883) [#18454](https://github.com/vitessio/vitess/pull/18454)
 * [release-22.0] repltracker: reset replica lag when we are primary (#18800) [#18806](https://github.com/vitessio/vitess/pull/18806)
 * [release-22.0] Fix bug where query consolidator returns empty result without error when the waiter cap exceeded (#18782) [#18832](https://github.com/vitessio/vitess/pull/18832) 
#### vtctl
 * [release-22.0] fix: Fix `GenerateShardRanges` returning shard names that don't cover the full range (#18641) [#18654](https://github.com/vitessio/vitess/pull/18654)
### CI/Build 
#### Build/CI
 * [release-22.0] Try updating the create PR workflow step (#18563) [#18571](https://github.com/vitessio/vitess/pull/18571)
 * [release-22.0] ci: use the newest mysql apt config package (#18790) [#18793](https://github.com/vitessio/vitess/pull/18793)
 * [release-22.0] ci: extract os tuning (#18824) [#18826](https://github.com/vitessio/vitess/pull/18826)
 * [release-22.0] ci: DRY up MySQL Setup (#18815) [#18836](https://github.com/vitessio/vitess/pull/18836) 
#### General
 * [release-22.0] Upgrade the Golang version to `go1.24.7` [#18621](https://github.com/vitessio/vitess/pull/18621)
 * [release-22.0] Upgrade the Golang version to `go1.24.9` [#18737](https://github.com/vitessio/vitess/pull/18737) 
#### Java
 * [release-22.0] update java packages to use central instead of ossrh (#18765) [#18766](https://github.com/vitessio/vitess/pull/18766)
### Compatibility Bug 
#### VTGate
 * [release-22.0] fix sqlSelectLimit propagating to subqueries (#18716) [#18872](https://github.com/vitessio/vitess/pull/18872)
### Dependencies 
#### VTAdmin
 * [release-22.0] Bump vite from 4.5.9 to 4.5.14 in /web/vtadmin (#18485) [#18500](https://github.com/vitessio/vitess/pull/18500)
 * [release-22.0] Bump @babel/runtime from 7.26.0 to 7.27.6 in /web/vtadmin (#18467) [#18502](https://github.com/vitessio/vitess/pull/18502)
 * [release-22.0] Bump form-data from 4.0.1 to 4.0.4 in /web/vtadmin (#18473) [#18504](https://github.com/vitessio/vitess/pull/18504)
### Documentation 
#### Governance
 * [release-22.0] Update codeowners and maintainers. [#18676](https://github.com/vitessio/vitess/pull/18676)
 * [release-22.0] Update CODEOWNERS (#18697) [#18699](https://github.com/vitessio/vitess/pull/18699)
### Enhancement 
#### Build/CI
 * [release-22.0] Simplify workflow files. (#18649) [#18656](https://github.com/vitessio/vitess/pull/18656) 
#### Query Serving
 * [release-22.0] bugfix: Fix impossible query for UNION (#18463) [#18465](https://github.com/vitessio/vitess/pull/18465)
### Internal Cleanup 
#### Build/CI
 * [release-22.0] ci: Replace `always()` with `!cancelled()`. (#18659) [#18662](https://github.com/vitessio/vitess/pull/18662)
 * [release-22.0] ci: Disable man-db auto updates. (#18665) [#18668](https://github.com/vitessio/vitess/pull/18668)
 * [release-22.0] ci: Bump `actions/setup-go` to `v5.5.0`. (#18660) [#18670](https://github.com/vitessio/vitess/pull/18670)
 * [release-22.0] ci: don't run codecov twice. (#18680) [#18682](https://github.com/vitessio/vitess/pull/18682)
### Performance 
#### VTTablet
 * [release-22.0] Fix: Improve VDiff internal query performance (#18579) [#18632](https://github.com/vitessio/vitess/pull/18632)
### Regression 
#### General
 * [release-22.0] Fix regression in v22 around new flag setup (#18507) [#18509](https://github.com/vitessio/vitess/pull/18509) 
#### Schema Tracker
 * [release-22.0] Fix GetSchema RPC to prevent returning view definitions when EnableViews is disabled (#18513) [#18517](https://github.com/vitessio/vitess/pull/18517)
### Release 
#### General
 * [release-22.0] Bump to `v22.0.2-SNAPSHOT` after the `v22.0.1` release [#18380](https://github.com/vitessio/vitess/pull/18380)
 * [release-22.0] Code Freeze for `v22.0.2` [#18876](https://github.com/vitessio/vitess/pull/18876)
### Security 
#### Backup and Restore
 * [release-22.0] Address dir traversal in file backup storage `GetBackups` RPC (#18814) [#18817](https://github.com/vitessio/vitess/pull/18817) 
#### Java
 * [release-22.0] Resolve `commons-lang` vulnerability in Java driver (#18768) [#18796](https://github.com/vitessio/vitess/pull/18796) 
#### VTAdmin
 * [release-22.0] vtadmin: upgrade vite to the latest (#18803) [#18811](https://github.com/vitessio/vitess/pull/18811) 
#### vtctldclient
 * [release-22.0] Potential fix for code scanning alert no. 2992: Clear-text logging of sensitive information (#18754) [#18759](https://github.com/vitessio/vitess/pull/18759)
 * [release-22.0] `vtctldclient GetPermissions`: hide `authentication_string` from response (#18771) [#18798](https://github.com/vitessio/vitess/pull/18798)
### Testing 
#### General
 * [release-22.0] Fix flaky tests (#18835) [#18838](https://github.com/vitessio/vitess/pull/18838) 
#### VReplication
 * [release-22.0] test: Fix race condition in TestStreamRowsHeartbeat (#18414) [#18420](https://github.com/vitessio/vitess/pull/18420)
 * [release-22.0] CI: Fix `VDiff2` flaky e2e test (#18494) [#18526](https://github.com/vitessio/vitess/pull/18526) 
#### VTTablet
 * [release-22.0] connpool: Bump the hang detection timeout to fix flakiness (#18722) [#18724](https://github.com/vitessio/vitess/pull/18724)

