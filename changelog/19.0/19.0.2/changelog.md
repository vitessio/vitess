# Changelog of Vitess v19.0.2

### Bug fixes 
#### Backup and Restore
 * [release-19.0] Configurable incremental restore files path (#15451) [#15564](https://github.com/vitessio/vitess/pull/15564) 
#### Build/CI
 * CI: Upgrade/Downgrade use N+1 version of vtctld when testing with N+1 of vttablet [#15631](https://github.com/vitessio/vitess/pull/15631) 
#### Evalengine
 * [release-19.0] evalengine: Ensure to pass down the precision (#15611) [#15612](https://github.com/vitessio/vitess/pull/15612)
 * [release-19.0] evalengine: Fix additional time type handling (#15614) [#15616](https://github.com/vitessio/vitess/pull/15616) 
#### Query Serving
 * [release-19.0] Fix aliasing in routes that have a derived table (#15550) [#15554](https://github.com/vitessio/vitess/pull/15554)
 * [release-19.0] bugfix: handling of ANDed join predicates (#15551) [#15557](https://github.com/vitessio/vitess/pull/15557)
 * [release-19.0] Fail insert when primary vindex cannot be mapped to a shard (#15500) [#15573](https://github.com/vitessio/vitess/pull/15573) 
#### Throttler
 * [release-19.0] Dedicated poolDialer logic for VTOrc, throttler (#15562) [#15567](https://github.com/vitessio/vitess/pull/15567) 
#### VReplication
 * [release-19.0] VReplication: Fix workflow update changed handling (#15621) [#15629](https://github.com/vitessio/vitess/pull/15629)
### CI/Build 
#### Build/CI
 * [release-19.0] Update to latest CodeQL (#15530) [#15534](https://github.com/vitessio/vitess/pull/15534) 
#### General
 * [release-19.0] Upgrade go version to go1.22.2 [#15641](https://github.com/vitessio/vitess/pull/15641)
### Regression 
#### Query Serving
 * [release-19.0] fix: remove keyspace from column during query builder (#15514) [#15517](https://github.com/vitessio/vitess/pull/15517)
### Release 
#### General
 * [release-19.0] Bump to `v19.0.2-SNAPSHOT` after the `v19.0.1` release [#15491](https://github.com/vitessio/vitess/pull/15491)
 * [release-19.0] Code Freeze for `v19.0.2` [#15644](https://github.com/vitessio/vitess/pull/15644)
### Testing 
#### VReplication
 * [release-19.0] Fix vtctldclient SwitchReads related bugs and move the TestBasicV2Workflows e2e test to vtctldclient (#15579) [#15584](https://github.com/vitessio/vitess/pull/15584)

