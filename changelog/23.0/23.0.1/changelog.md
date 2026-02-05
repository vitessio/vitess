# Changelog of Vitess v23.0.1

### Bug fixes 
#### Backup and Restore
 * [release-23.0] BuiltinBackupEngine: Retry file close and fail backup when we cannot (#18848) [#18862](https://github.com/vitessio/vitess/pull/18862) 
#### CLI
 * [release-23.0] `vtbench`: add `--db-credentials-*` flags (#18913) [#18922](https://github.com/vitessio/vitess/pull/18922) 
#### Cluster management
 * [release-23.0] Improve Semi-Sync Monitor Behavior to Prevent Errant ERS (#18884) [#18907](https://github.com/vitessio/vitess/pull/18907) 
#### Evalengine
 * [release-23.0] evalengine: Fix `NULL` document handling in JSON functions (#19052) [#19231](https://github.com/vitessio/vitess/pull/19231)
 * [release-23.0] evalengine: make `JSON_EXTRACT` work with non-static arguments (#19035) [#19254](https://github.com/vitessio/vitess/pull/19254) 
#### General
 * [release-23.0] Escape control bytes in JSON strings (#19270) [#19275](https://github.com/vitessio/vitess/pull/19275) 
#### Online DDL
 * [release-23.0] copy_state: use a mediumblob instead of a smaller varbinary for lastpk (#18852) [#18859](https://github.com/vitessio/vitess/pull/18859) 
#### Query Serving
 * [release-23.0] [Querythrottler]Remove noisy config loading error log (#18904) [#18912](https://github.com/vitessio/vitess/pull/18912)
 * [release-23.0] Properly Strip Keyspace Table Qualifiers in FK Constraints (#18926) [#18935](https://github.com/vitessio/vitess/pull/18935)
 * [release-23.0] Fix cross shard/keyspace joins with derived tables containing a `UNION`. (#19046) [#19137](https://github.com/vitessio/vitess/pull/19137)
 * [release-23.0] Fix column offset tracking for `UNION`s to be case insensitive. (#19139) [#19162](https://github.com/vitessio/vitess/pull/19162) 
#### TabletManager
 * [release-23.0] Fix `ReloadSchema` incorrectly using `DisableBinlogs` value in `grpctmclient` (#19085) [#19130](https://github.com/vitessio/vitess/pull/19130) 
#### VDiff
 * [release-23.0] VDiff: Prevent division by 0 when reconciling mismatches for reference tables (#19160) [#19165](https://github.com/vitessio/vitess/pull/19165) 
#### VReplication
 * [release-23.0] Run VStream copy only when VGTID requires it, use TablesToCopy in those cases (#18938) [#18940](https://github.com/vitessio/vitess/pull/18940)
 * [release-23.0] VDiff: Handle the case where a workflow's table has been dropped on the source (#18985) [#18989](https://github.com/vitessio/vitess/pull/18989)
 * [release-23.0] VReplication: Properly Handle Sequence Table Initialization For Empty Tables (#19226) [#19228](https://github.com/vitessio/vitess/pull/19228) 
#### VTTablet
 * [release-23.0] connpool: fix connection leak during idle connection reopen (#18967) [#18971](https://github.com/vitessio/vitess/pull/18971)
 * [release-23.0] Change connection pool idle expiration logic (#19004) [#19014](https://github.com/vitessio/vitess/pull/19014)
 * [release-23.0] binlog_json: fix opaque value parsing to read variable-length (#19102) [#19110](https://github.com/vitessio/vitess/pull/19110) 
#### VTorc
 * [release-23.0] `vtorc`: detect errant GTIDs for replicas not connected to primary (#19224) [#19234](https://github.com/vitessio/vitess/pull/19234)
 * [release-23.0] vtorc: add `StaleTopoPrimary` analysis and recovery (#19173) [#19237](https://github.com/vitessio/vitess/pull/19237) 
#### vtctl
 * [release-23.0] vschema revert: initialize as nil so that nil checks do not pass later (#19114) [#19118](https://github.com/vitessio/vitess/pull/19118)
### CI/Build 
#### Build/CI
 * [release-23.0] ci: extract os tuning (#18824) [#18827](https://github.com/vitessio/vitess/pull/18827)
 * [release-23.0] ci: DRY up MySQL Setup (#18815) [#18837](https://github.com/vitessio/vitess/pull/18837)
 * `release-23.0`: Add version conditional for tablet missing error message [#19057](https://github.com/vitessio/vitess/pull/19057)
 * Pin GitHub Actions and Docker images by hash [#19152](https://github.com/vitessio/vitess/pull/19152)
 * [release-23.0] Update go-upgrade to update docker image digests (#19178) [#19189](https://github.com/vitessio/vitess/pull/19189)
 * [release-23.0] Fix major upgrade logic in go upgrade tool [#19212](https://github.com/vitessio/vitess/pull/19212)
 * [release-23.0] Fix go upgrade workflow (#19216) [#19220](https://github.com/vitessio/vitess/pull/19220)
 * [release-23.0] switch end-to-end tests to gotestsum (#19182) [#19245](https://github.com/vitessio/vitess/pull/19245) 
#### General
 * [release-23.0] Upgrade the Golang version to `go1.25.6` [#19229](https://github.com/vitessio/vitess/pull/19229) 
#### VTAdmin
 * [release-23.0] Fix vtadmin package-lock.json (#18919) [#18924](https://github.com/vitessio/vitess/pull/18924)
### Compatibility Bug 
#### VTGate
 * [release-23.0] fix sqlSelectLimit propagating to subqueries (#18716) [#18873](https://github.com/vitessio/vitess/pull/18873)
### Dependencies 
#### Operator
 * upgrade operator.yaml [#18870](https://github.com/vitessio/vitess/pull/18870)
### Enhancement 
#### Build/CI
 * [release-23.0] Don't hardcode the go version to use for upgrade/downgrade tests. (#18920) [#18956](https://github.com/vitessio/vitess/pull/18956) 
#### TabletManager
 * [release-23.0] Add new `force` flag to `DemotePrimary` to force a demotion even when blocked on waiting for semi-sync acks (#18714) [#19239](https://github.com/vitessio/vitess/pull/19239) 
#### VDiff
 * [release-23.0] vdiff: do not sort by table name in summary, it is not necessary (#18972) [#18978](https://github.com/vitessio/vitess/pull/18978)
### Internal Cleanup 
#### Docker
 * [release-23.0] `ci`: use `etcd` v3.5.25, add retries (#19015) [#19022](https://github.com/vitessio/vitess/pull/19022)
### Release 
#### General
 * [release-23.0] Bump to `v23.0.1-SNAPSHOT` after the `v23.0.0` release [#18866](https://github.com/vitessio/vitess/pull/18866)
 * [release-23.0] Code Freeze for `v23.0.1` [#19285](https://github.com/vitessio/vitess/pull/19285)
### Security 
#### General
 * [release-23.0] Bump golang.org/x/crypto from 0.42.0 to 0.45.0 (#18918) [#18923](https://github.com/vitessio/vitess/pull/18923) 
#### Java
 * [release-23.0] Bump org.apache.logging.log4j:log4j-core from 2.24.1 to 2.25.3 in /java (#19063) [#19066](https://github.com/vitessio/vitess/pull/19066) 
#### VTAdmin
 * [release-23.0] Bump js-yaml from 4.1.0 to 4.1.1 in /web/vtadmin (#18908) [#18911](https://github.com/vitessio/vitess/pull/18911)
 * [release-23.0] Drop dependency on `npm`, bump version of `glob`. (#18931) [#18958](https://github.com/vitessio/vitess/pull/18958)
 * [release-23.0] Potential fix for code scanning alert no. 3944: Database query built … [#18963](https://github.com/vitessio/vitess/pull/18963)
### Testing 
#### Build/CI
 * [release-23.0] Stop using Equinix Metal self hosted runners (#18942) [#18944](https://github.com/vitessio/vitess/pull/18944)
 * [release-23.0] CI: Improve reliability of codecov workflow with larger runner (#18992) [#18995](https://github.com/vitessio/vitess/pull/18995)
 * [release-23.0] Skip flaky `TestRedial` test (#19106) [#19108](https://github.com/vitessio/vitess/pull/19108)
 * [release-23.0] CI: Look for expected log message rather than code in Backup tests (#19199) [#19201](https://github.com/vitessio/vitess/pull/19201) 
#### General
 * [release-23.0] Fix flaky tests (#18835) [#18839](https://github.com/vitessio/vitess/pull/18839) 
#### VTGate
 * [release-23.0] Fix sporadic TestServingKeyspaces panic on context cancellation (#19163) [#19187](https://github.com/vitessio/vitess/pull/19187)

