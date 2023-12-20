# Changelog of Vitess v17.0.5

### Bug fixes 
#### Build/CI
 * [release-17.0] Update create_release.sh (#14492) [#14515](https://github.com/vitessio/vitess/pull/14515) 
#### Cluster management
 * [release-17.0] Fix Panic in PRS due to a missing nil check (#14656) [#14675](https://github.com/vitessio/vitess/pull/14675)
 * Revert "[release-17.0] Replace use of `WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS` with `WAIT_FOR_EXECUTED_GTID_SET` (#14612)" [#14744](https://github.com/vitessio/vitess/pull/14744) 
#### Evalengine
 * [release-17.0] Fix nullability checks in evalengine (#14556) [#14563](https://github.com/vitessio/vitess/pull/14563) 
#### Query Serving
 * [release-17.0] expression rewriting: enable more rewrites and limit CNF rewrites (#14560) [#14575](https://github.com/vitessio/vitess/pull/14575)
 * [release-17.0] fix concurrency on stream execute engine primitives (#14586) [#14591](https://github.com/vitessio/vitess/pull/14591)
 * [17.0] bug fix: stop all kinds of expressions from cnf-exploding [#14594](https://github.com/vitessio/vitess/pull/14594)
 * [release-17.0] tabletserver: do not consolidate streams on primary tablet when consolidator mode is `notOnPrimary` (#14332) [#14678](https://github.com/vitessio/vitess/pull/14678)
 * Fix accepting bind variables in time related function calls. [#14763](https://github.com/vitessio/vitess/pull/14763) 
#### VReplication
 * [release-17.0] VReplication: Update singular workflow in traffic switcher (#14826) [#14828](https://github.com/vitessio/vitess/pull/14828)
### CI/Build 
#### Build/CI
 * [release-17.0] Update MySQL apt package and GPG signature (#14785) [#14791](https://github.com/vitessio/vitess/pull/14791) 
#### Docker
 * [release-17.0] Build and push Docker Images from GitHub Actions [#14512](https://github.com/vitessio/vitess/pull/14512) 
#### General
 * [release-17.0] Upgrade the Golang version to `go1.20.11` [#14489](https://github.com/vitessio/vitess/pull/14489)
 * [release-17.0] Upgrade the Golang version to `go1.20.12` [#14692](https://github.com/vitessio/vitess/pull/14692)
### Dependabot 
#### General
 * [release-17.0] build(deps): bump golang.org/x/crypto from 0.16.0 to 0.17.0 (#14814) [#14816](https://github.com/vitessio/vitess/pull/14816) 
#### VTAdmin
 * [release-17.0] Bump @adobe/css-tools from 4.3.1 to 4.3.2 in /web/vtadmin (#14654) [#14667](https://github.com/vitessio/vitess/pull/14667)
### Enhancement 
#### Build/CI
 * [release-17.0] Add step to static check to ensure consistency of GHA workflows (#14724) [#14726](https://github.com/vitessio/vitess/pull/14726)
### Internal Cleanup 
#### TabletManager
 * [release-17.0] Replace use of `WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS` with `WAIT_FOR_EXECUTED_GTID_SET` (#14612) [#14619](https://github.com/vitessio/vitess/pull/14619) 
#### vtctldclient
 * [release-17.0] Fix typo for `--cells` flag help description in `ApplyRoutingRules` (#14721) [#14722](https://github.com/vitessio/vitess/pull/14722)
### Performance 
#### Query Serving
 * [release-17.0] vindexes: fix pooled collator buffer memory leak (#14621) [#14623](https://github.com/vitessio/vitess/pull/14623)
### Release 
#### General
 * [release-17.0] Code Freeze for `v17.0.5` [#14806](https://github.com/vitessio/vitess/pull/14806)
### Testing 
#### Backup and Restore
 * [release-17.0] Add a retry to remove the vttablet directory during upgrade/downgrade backup tests (#14753) [#14757](https://github.com/vitessio/vitess/pull/14757)

