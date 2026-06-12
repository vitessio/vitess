# Changelog of Vitess v22.0.4

### Bug fixes 
#### Backup and Restore
 * [release-22.0] fix(backup): propagate file hashes to manifest after retry (#19336) [#19343](https://github.com/vitessio/vitess/pull/19343) 
#### Docker
 * [release-22.0] docker: install mysql-shell from Oracle repo and fix shellcheck warnings (#19456) [#19463](https://github.com/vitessio/vitess/pull/19463) 
#### Online DDL
 * [release-22.0] vreplication: fix infinite retry loop when terminal error message contains binary data (#19423) [#19437](https://github.com/vitessio/vitess/pull/19437) 
#### Query Serving
 * [release-22.0] vtgate: defer implicit transaction start until after query planning (#19277) [#19341](https://github.com/vitessio/vitess/pull/19341) 
#### VDiff
 * [release-22.0] Address a few VDiff concerns (#19413) [#19447](https://github.com/vitessio/vitess/pull/19447) 
#### VReplication
 * [release-22.0] Bug fix: Add missing db_name filters to vreplication and vdiff queries #19378 [#19430](https://github.com/vitessio/vitess/pull/19430)
 * [release-22.0] Normalize the --on-ddl param for MoveTables (#19445) [#19451](https://github.com/vitessio/vitess/pull/19451) 
#### VTGate
 * [release-22.0] vtgate: Add bounds check in `visitUnion` for mismatched column counts (#19476) [#19482](https://github.com/vitessio/vitess/pull/19482) 
#### VTOrc
 * [release-22.0] vtorc: Add a timeout to `DemotePrimary` RPC (#19432) [#19449](https://github.com/vitessio/vitess/pull/19449) 
#### schema management
 * [release-22.0] sidecardb: make ALTER TABLE algorithm version-aware (#19358) [#19403](https://github.com/vitessio/vitess/pull/19403)
### CI/Build 
#### Build/CI
 * [release-22.0] Consolidate CI test workflows (#19259) [#19272](https://github.com/vitessio/vitess/pull/19272)
 * [release-22.0] Run tests with gotestsum (#19076) [#19292](https://github.com/vitessio/vitess/pull/19292)
 * [release-22.0] Fix go upgrade tool (#19290) [#19298](https://github.com/vitessio/vitess/pull/19298)
 * [release-22.0] Switch gotestsum output format (#19215) [#19302](https://github.com/vitessio/vitess/pull/19302)
 * [release-22.0] Build boostrap image for local/region example CI (#19310) [#19316](https://github.com/vitessio/vitess/pull/19316)
 * [release-22.0] Don't add "Skip CI" label for Go upgrade PRs (#19307) [#19322](https://github.com/vitessio/vitess/pull/19322)
 * [release-22.0] Explicitly pass local image tags in example CI (#19320) [#19325](https://github.com/vitessio/vitess/pull/19325)
 * [release-22.0] Add lite image build CI job (#19321) [#19329](https://github.com/vitessio/vitess/pull/19329)
 * [release-22.0] try to fix setup mysql (#19371) [#19375](https://github.com/vitessio/vitess/pull/19375)
 * [release-22.0] CI: Fix workflows that install xtrabackup (#19383) [#19384](https://github.com/vitessio/vitess/pull/19384) 
#### Docker
 * [release-22.0] Build bootstrap image locally in ci (#19255) [#19265](https://github.com/vitessio/vitess/pull/19265)
### Compatibility Bug 
#### Query Serving
 * [release-22.0] fix streaming binary row corruption in prepared statements (#19381) [#19414](https://github.com/vitessio/vitess/pull/19414) 
#### VTGate
 * [release-22.0] vtgate: fix handling of session variables on targeted connections (#19318) [#19334](https://github.com/vitessio/vitess/pull/19334)
### Dependencies 
#### Docker
 * [release-22.0] Upgrade the Golang version to `go1.24.13` [#19305](https://github.com/vitessio/vitess/pull/19305)
### Enhancement 
#### VTGate
 * [release-22.0] Performance: use `IsSingleShard()` check in `pushDerived` instead of just `engine.EqualUnique` opcode (#18974) [#19345](https://github.com/vitessio/vitess/pull/19345)
### Release 
#### Build/CI
 * [release-22.0] Code Freeze for `v22.0.4` [#19509](https://github.com/vitessio/vitess/pull/19509) 
#### Documentation
 * Add release summary for v22.0.4 [#19508](https://github.com/vitessio/vitess/pull/19508) 
#### General
 * [release-22.0] Bump to `v22.0.4-SNAPSHOT` after the `v22.0.3` release [#19284](https://github.com/vitessio/vitess/pull/19284)
### Security 
#### Backup and Restore
 * [release-22.0] Restore: make loading compressor commands from `MANIFEST` opt-in (#19460) [#19473](https://github.com/vitessio/vitess/pull/19473)
 * [release-22.0] `backupengine`: disallow path traversals via backup `MANIFEST` on restore (#19470) [#19477](https://github.com/vitessio/vitess/pull/19477)
 * [release-22.0] `mysqlshellbackupengine`: use `fileutil.SafePathJoin(...)` to build path (#19484) [#19490](https://github.com/vitessio/vitess/pull/19490) 
#### VTTablet
 * [release-22.0] `filebackupstorage`: use `fileutil.SafePathJoin` for all path building (#19479) [#19480](https://github.com/vitessio/vitess/pull/19480)
 * [release-22.0] `vttablet`: harden `ExecuteHook` RPC and backup engine flag inputs (#19486) [#19500](https://github.com/vitessio/vitess/pull/19500)
### Testing 
#### Build/CI
 * [release-22.0] Generate race unit tests (#19078) [#19295](https://github.com/vitessio/vitess/pull/19295)
 * [release-22.0] CI: Deflake Code Coverage workflow (#19388) [#19393](https://github.com/vitessio/vitess/pull/19393)
 * [release-22.0] CI: Deflake two flaky tests (#19364) [#19411](https://github.com/vitessio/vitess/pull/19411)
 * [release-22.0] CI: Use larger runners for vreplication workflows (#19433) [#19434](https://github.com/vitessio/vitess/pull/19434)

