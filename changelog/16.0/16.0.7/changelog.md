# Changelog of Vitess v16.0.7

### Bug fixes 
#### Build/CI
 * [release-16.0] Update create_release.sh (#14492) [#14514](https://github.com/vitessio/vitess/pull/14514) 
#### Cluster management
 * [release-16.0] Fix Panic in PRS due to a missing nil check (#14656) [#14674](https://github.com/vitessio/vitess/pull/14674) 
#### Query Serving
 * [release-16.0] expression rewriting: enable more rewrites and limit CNF rewrites (#14560) [#14574](https://github.com/vitessio/vitess/pull/14574)
 * [release-16.0] fix concurrency on stream execute engine primitives (#14586) [#14590](https://github.com/vitessio/vitess/pull/14590)
 * [16.0] bug fix: stop all kinds of expressions from cnf-exploding [#14595](https://github.com/vitessio/vitess/pull/14595)
 * [release-16.0] tabletserver: do not consolidate streams on primary tablet when consolidator mode is `notOnPrimary` (#14332) [#14683](https://github.com/vitessio/vitess/pull/14683) 
#### VReplication
 * Revert "[release-16.0] Replace use of `WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS` with `WAIT_FOR_EXECUTED_GTID_SET` (#14612)" [#14743](https://github.com/vitessio/vitess/pull/14743)
 * [release-16.0] VReplication: Update singular workflow in traffic switcher (#14826) [#14827](https://github.com/vitessio/vitess/pull/14827)
### CI/Build 
#### Build/CI
 * [release-16.0] Update MySQL apt package and GPG signature (#14785) [#14790](https://github.com/vitessio/vitess/pull/14790) 
#### Docker
 * [release-16.0] Build and push Docker Images from GitHub Actions [#14513](https://github.com/vitessio/vitess/pull/14513) 
#### General
 * [release-16.0] Upgrade the Golang version to `go1.20.12` [#14691](https://github.com/vitessio/vitess/pull/14691)
### Dependabot 
#### General
 * [release-16.0] build(deps): bump golang.org/x/crypto from 0.16.0 to 0.17.0 (#14814) [#14818](https://github.com/vitessio/vitess/pull/14818)
### Enhancement 
#### Build/CI
 * [release-16.0] Add step to static check to ensure consistency of GHA workflows (#14724) [#14725](https://github.com/vitessio/vitess/pull/14725)
### Internal Cleanup 
#### TabletManager
 * [release-16.0] Replace use of `WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS` with `WAIT_FOR_EXECUTED_GTID_SET` (#14612) [#14620](https://github.com/vitessio/vitess/pull/14620)
### Performance 
#### Query Serving
 * [release-16.0] vindexes: fix pooled collator buffer memory leak (#14621) [#14622](https://github.com/vitessio/vitess/pull/14622)
### Release 
#### General
 * [release-16.0] Code Freeze for `v16.0.7` [#14808](https://github.com/vitessio/vitess/pull/14808)
### Testing 
#### Backup and Restore
 * [release-16.0] Add a retry to remove the vttablet directory during upgrade/downgrade backup tests (#14753) [#14756](https://github.com/vitessio/vitess/pull/14756)
 * [release-16.0] Backup flaky test [#14819](https://github.com/vitessio/vitess/pull/14819)

