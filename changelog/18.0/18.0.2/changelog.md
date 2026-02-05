# Changelog of Vitess v18.0.2

### Bug fixes 
#### Cluster management
 * [release-18.0] Fix Panic in PRS due to a missing nil check (#14656) [#14676](https://github.com/vitessio/vitess/pull/14676)
 * Revert "[release-18.0] Replace use of `WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS` with `WAIT_FOR_EXECUTED_GTID_SET` (#14612)" [#14742](https://github.com/vitessio/vitess/pull/14742) 
#### Evalengine
 * [release-18.0] evalengine: Fix the min / max calculation for decimals (#14614) [#14616](https://github.com/vitessio/vitess/pull/14616) 
#### Query Serving
 * [release-18.0] fix concurrency on stream execute engine primitives (#14586) [#14592](https://github.com/vitessio/vitess/pull/14592)
 * [18.0] bug fix: stop all kinds of expressions from cnf-exploding [#14593](https://github.com/vitessio/vitess/pull/14593)
 * [release-18.0] bugfix: do not rewrite an expression twice (#14641) [#14643](https://github.com/vitessio/vitess/pull/14643)
 * [release-18.0] tabletserver: do not consolidate streams on primary tablet when consolidator mode is `notOnPrimary` (#14332) [#14679](https://github.com/vitessio/vitess/pull/14679)
 * [release-18.0] TabletServer: Handle nil targets properly everywhere (#14734) [#14741](https://github.com/vitessio/vitess/pull/14741) 
#### VReplication
 * [release-18.0] VReplication TableStreamer: Only stream tables in tablestreamer (ignore views) (#14646) [#14649](https://github.com/vitessio/vitess/pull/14649)
 * [release-18.0] VDiff: Fix vtctldclient limit bug (#14778) [#14780](https://github.com/vitessio/vitess/pull/14780)
 * [release-18.0] Backport: VReplication SwitchWrites: Properly return errors in SwitchWrites #14800 [#14824](https://github.com/vitessio/vitess/pull/14824)
 * [release-18.0] VReplication: Update singular workflow in traffic switcher (#14826) [#14829](https://github.com/vitessio/vitess/pull/14829)
### CI/Build 
#### Build/CI
 * [release-18.0] Update MySQL apt package and GPG signature (#14785) [#14792](https://github.com/vitessio/vitess/pull/14792) 
#### General
 * [release-18.0] Upgrade the Golang version to `go1.21.5` [#14690](https://github.com/vitessio/vitess/pull/14690)
### Dependabot 
#### General
 * [release-18.0] build(deps): bump golang.org/x/crypto from 0.16.0 to 0.17.0 (#14814) [#14817](https://github.com/vitessio/vitess/pull/14817) 
#### VTAdmin
 * [release-18.0] Bump @adobe/css-tools from 4.3.1 to 4.3.2 in /web/vtadmin (#14654) [#14668](https://github.com/vitessio/vitess/pull/14668)
### Enhancement 
#### Backup and Restore
 * [release-18.0] increase vtctlclient backupShard command success rate (#14604) [#14639](https://github.com/vitessio/vitess/pull/14639) 
#### Build/CI
 * [release-18.0] Add step to static check to ensure consistency of GHA workflows (#14724) [#14727](https://github.com/vitessio/vitess/pull/14727) 
#### Query Serving
 * [release-18.0] planbuilder: push down ordering through filter (#14583) [#14584](https://github.com/vitessio/vitess/pull/14584)
### Internal Cleanup 
#### TabletManager
 * [release-18.0] Replace use of `WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS` with `WAIT_FOR_EXECUTED_GTID_SET` (#14612) [#14617](https://github.com/vitessio/vitess/pull/14617) 
#### vtctldclient
 * [release-18.0] Fix typo for `--cells` flag help description in `ApplyRoutingRules` (#14721) [#14723](https://github.com/vitessio/vitess/pull/14723)
### Performance 
#### Query Serving
 * vindexes: fix pooled collator buffer memory leak [#14621](https://github.com/vitessio/vitess/pull/14621)
### Regression 
#### Query Serving
 * [release-18.0] plabuilder: use OR for not in comparisons (#14607) [#14615](https://github.com/vitessio/vitess/pull/14615)
 * [release-18.0] fix: insert on duplicate key update missing BindVars (#14728) [#14755](https://github.com/vitessio/vitess/pull/14755)
### Release 
#### General
 * Back to dev mode after v18.0.1 [#14580](https://github.com/vitessio/vitess/pull/14580)
 * [release-18.0] Code Freeze for `v18.0.2` [#14804](https://github.com/vitessio/vitess/pull/14804)
### Testing 
#### Backup and Restore
 * [release-18.0] Add a retry to remove the vttablet directory during upgrade/downgrade backup tests (#14753) [#14758](https://github.com/vitessio/vitess/pull/14758)

