# Changelog of Vitess v23.0.3

### Bug fixes 
#### Docker
 * [release-23.0] docker: install mysql-shell from Oracle repo and fix shellcheck warnings (#19456) [#19464](https://github.com/vitessio/vitess/pull/19464) 
#### Online DDL
 * [release-23.0] vreplication: fix infinite retry loop when terminal error message contains binary data (#19423) [#19438](https://github.com/vitessio/vitess/pull/19438) 
#### VDiff
 * [release-23.0] Address a few VDiff concerns (#19413) [#19448](https://github.com/vitessio/vitess/pull/19448) 
#### VReplication
 * [release-23.0] Bug fix: Add missing db_name filters to vreplication and vdiff queries #19378 [#19429](https://github.com/vitessio/vitess/pull/19429)
 * [release-23.0] Normalize the --on-ddl param for MoveTables (#19445) [#19452](https://github.com/vitessio/vitess/pull/19452) 
#### VTGate
 * [release-23.0] vtgate: Add bounds check in `visitUnion` for mismatched column counts (#19476) [#19483](https://github.com/vitessio/vitess/pull/19483) 
#### VTOrc
 * [release-23.0] vtorc: Add a timeout to `DemotePrimary` RPC (#19432) [#19450](https://github.com/vitessio/vitess/pull/19450) 
#### schema management
 * [release-23.0] sidecardb: make ALTER TABLE algorithm version-aware (#19358) [#19404](https://github.com/vitessio/vitess/pull/19404)
### CI/Build 
#### Build/CI
 * Backport to v23: Support Go 1.26 and later with Swiss maps always enabled (#19088) [#19367](https://github.com/vitessio/vitess/pull/19367)
 * [release-23.0] try to fix setup mysql (#19371) [#19376](https://github.com/vitessio/vitess/pull/19376)
 * [release-23.0] CI: Fix workflows that install xtrabackup (#19383) [#19385](https://github.com/vitessio/vitess/pull/19385)
### Compatibility Bug 
#### Query Serving
 * [release-23.0] fix streaming binary row corruption in prepared statements (#19381) [#19415](https://github.com/vitessio/vitess/pull/19415)
### Release 
#### Documentation
 * Add summary for 23.0.3 patch release [#19503](https://github.com/vitessio/vitess/pull/19503) 
#### General
 * [release-23.0] Code Freeze for `v23.0.3` [#19504](https://github.com/vitessio/vitess/pull/19504)
### Security 
#### Backup and Restore
 * [release-23.0] Restore: make loading compressor commands from `MANIFEST` opt-in (#19460) [#19474](https://github.com/vitessio/vitess/pull/19474)
 * [release-23.0] `backupengine`: disallow path traversals via backup `MANIFEST` on restore (#19470) [#19478](https://github.com/vitessio/vitess/pull/19478)
 * [release-23.0] `mysqlshellbackupengine`: use `fileutil.SafePathJoin(...)` to build path (#19484) [#19491](https://github.com/vitessio/vitess/pull/19491) 
#### VTTablet
 * [release-23.0] `filebackupstorage`: use `fileutil.SafePathJoin` for all path building (#19479) [#19481](https://github.com/vitessio/vitess/pull/19481)
 * [release-23.0] `vttablet`: harden `ExecuteHook` RPC and backup engine flag inputs (#19486) [#19501](https://github.com/vitessio/vitess/pull/19501)
### Testing 
#### Build/CI
 * [release-23.0] CI: Deflake Code Coverage workflow (#19388) [#19394](https://github.com/vitessio/vitess/pull/19394)
 * [release-23.0] CI: Deflake two flaky tests (#19364) [#19412](https://github.com/vitessio/vitess/pull/19412)
 * [release-23.0] CI: Use larger runners for vreplication workflows (#19433) [#19435](https://github.com/vitessio/vitess/pull/19435)

