# Changelog of Vitess v22.0.3

### Bug fixes 
#### Build/CI
 * [release-22.0] Fix major upgrade logic in go upgrade tool [#19211](https://github.com/vitessio/vitess/pull/19211) 
#### CLI
 * [release-22.0] `vtbench`: add `--db-credentials-*` flags (#18913) [#18921](https://github.com/vitessio/vitess/pull/18921) 
#### Cluster management
 * [release-22.0] Improve Semi-Sync Monitor Behavior to Prevent Errant ERS (#18884) [#18906](https://github.com/vitessio/vitess/pull/18906) 
#### Evalengine
 * [release-22.0] evalengine: Fix `NULL` document handling in JSON functions (#19052) [#19230](https://github.com/vitessio/vitess/pull/19230)
 * [release-22.0] evalengine: make `JSON_EXTRACT` work with non-static arguments (#19035) [#19253](https://github.com/vitessio/vitess/pull/19253) 
#### General
 * [release-22.0] Escape control bytes in JSON strings (#19270) [#19274](https://github.com/vitessio/vitess/pull/19274) 
#### Query Serving
 * [release-22.0] Properly Strip Keyspace Table Qualifiers in FK Constraints (#18926) [#18934](https://github.com/vitessio/vitess/pull/18934)
 * [release-22.0] Fix cross shard/keyspace joins with derived tables containing a `UNION`. (#19046) [#19136](https://github.com/vitessio/vitess/pull/19136)
 * [release-22.0] Fix column offset tracking for `UNION`s to be case insensitive. (#19139) [#19161](https://github.com/vitessio/vitess/pull/19161) 
#### TabletManager
 * [release-22.0] Fix `ReloadSchema` incorrectly using `DisableBinlogs` value in `grpctmclient` (#19085) [#19129](https://github.com/vitessio/vitess/pull/19129) 
#### VDiff
 * [release-22.0] VDiff: Prevent division by 0 when reconciling mismatches for reference tables (#19160) [#19164](https://github.com/vitessio/vitess/pull/19164) 
#### VReplication
 * [release-22.0] VDiff: Handle the case where a workflow's table has been dropped on the source (#18985) [#18988](https://github.com/vitessio/vitess/pull/18988)
 * [release-22.0] VReplication: Properly Handle Sequence Table Initialization For Empty Tables (#19226) [#19227](https://github.com/vitessio/vitess/pull/19227) 
#### VTGate
 * [release-22.0] workflows: avoid accidental deletion to routing rules (#19121) [#19135](https://github.com/vitessio/vitess/pull/19135) 
#### VTTablet
 * [release-22.0] connpool: fix connection leak during idle connection reopen (#18967) [#18970](https://github.com/vitessio/vitess/pull/18970)
 * [release-22.0] Change connection pool idle expiration logic (#19004) [#19013](https://github.com/vitessio/vitess/pull/19013)
 * [release-22.0] binlog_json: fix opaque value parsing to read variable-length (#19102) [#19109](https://github.com/vitessio/vitess/pull/19109) 
#### VTorc
 * [release-22.0] `vtorc`: detect errant GTIDs for replicas not connected to primary (#19224) [#19233](https://github.com/vitessio/vitess/pull/19233)
 * [release-22.0] vtorc: add `StaleTopoPrimary` analysis and recovery (#19173) [#19236](https://github.com/vitessio/vitess/pull/19236) 
#### vtctl
 * [release-22.0] vschema revert: initialize as nil so that nil checks do not pass later (#19114) [#19117](https://github.com/vitessio/vitess/pull/19117)
### CI/Build 
#### Build/CI
 * Pin GitHub Actions and Docker images by hash [#19151](https://github.com/vitessio/vitess/pull/19151)
 * [release-22.0] Update go-upgrade to update docker image digests (#19178) [#19188](https://github.com/vitessio/vitess/pull/19188)
 * [release-22.0] Fix go upgrade workflow (#19216) [#19219](https://github.com/vitessio/vitess/pull/19219)
 * [release-22.0] switch end-to-end tests to gotestsum (#19182) [#19244](https://github.com/vitessio/vitess/pull/19244) 
#### General
 * [release-22.0] Upgrade the Golang version to `go1.24.10` [#18897](https://github.com/vitessio/vitess/pull/18897)
 * [release-22.0] Upgrade the Golang version to `go1.24.12` [#19222](https://github.com/vitessio/vitess/pull/19222)
### Enhancement 
#### Build/CI
 * [release-22.0] Don't hardcode the go version to use for upgrade/downgrade tests. (#18920) [#18955](https://github.com/vitessio/vitess/pull/18955) 
#### TabletManager
 * [release-22.0] Add new `force` flag to `DemotePrimary` to force a demotion even when blocked on waiting for semi-sync acks (#18714) [#19238](https://github.com/vitessio/vitess/pull/19238) 
#### VDiff
 * [release-22.0] vdiff: do not sort by table name in summary, it is not necessary (#18972) [#18977](https://github.com/vitessio/vitess/pull/18977)
### Internal Cleanup 
#### Docker
 * [release-22.0] `ci`: use `etcd` v3.5.25, add retries (#19015) [#19021](https://github.com/vitessio/vitess/pull/19021)
### Release 
#### General
 * [release-22.0] Code Freeze for `v22.0.3` [#19281](https://github.com/vitessio/vitess/pull/19281)
### Security 
#### Java
 * [release-22.0] Bump org.apache.logging.log4j:log4j-core from 2.24.1 to 2.25.3 in /java (#19063) [#19065](https://github.com/vitessio/vitess/pull/19065) 
#### VTAdmin
 * [release-22.0] Bump js-yaml from 4.1.0 to 4.1.1 in /web/vtadmin (#18908) [#18910](https://github.com/vitessio/vitess/pull/18910)
 * [release-22.0] Drop dependency on `npm`, bump version of `glob`. (#18931) [#18957](https://github.com/vitessio/vitess/pull/18957)
 * [release-22.0] Potential fix for code scanning alert no. 3944: Database query built … [#18962](https://github.com/vitessio/vitess/pull/18962)
### Testing 
#### Build/CI
 * [release-22.0] Stop using Equinix Metal self hosted runners (#18942) [#18943](https://github.com/vitessio/vitess/pull/18943)
 * [release-22.0] CI: Improve reliability of codecov workflow with larger runner (#18992) [#18994](https://github.com/vitessio/vitess/pull/18994)
 * [release-22.0] Skip flaky `TestRedial` test (#19106) [#19107](https://github.com/vitessio/vitess/pull/19107)
 * [release-22.0] CI: Look for expected log message rather than code in Backup tests (#19199) [#19200](https://github.com/vitessio/vitess/pull/19200) 
#### VTGate
 * [release-22.0] Fix sporadic TestServingKeyspaces panic on context cancellation (#19163) [#19186](https://github.com/vitessio/vitess/pull/19186)

