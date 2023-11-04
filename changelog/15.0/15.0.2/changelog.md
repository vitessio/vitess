# Changelog of Vitess v15.0.2

### Bug fixes 
#### Query Serving
 * Online DDL: fix 'vtctlclient OnlineDDL' template queries [#11889](https://github.com/vitessio/vitess/pull/11889)
 * Fix CheckMySQL by setting the correct wanted state [#11895](https://github.com/vitessio/vitess/pull/11895)
 * bugfix: allow predicates without dependencies with derived tables to be handled correctly [#11911](https://github.com/vitessio/vitess/pull/11911)
 * [release-15.0] Fix sending a ServerLost error when reading a packet fails (#11920) [#11930](https://github.com/vitessio/vitess/pull/11930)
 * Skip `TestSubqueriesExists` during upgrade-downgrade tests [#11953](https://github.com/vitessio/vitess/pull/11953) 
#### VReplication
 * VReplication: Prevent Orphaned VDiff2 Jobs [#11768](https://github.com/vitessio/vitess/pull/11768)
### CI/Build 
#### Build/CI
 * Fix deprecated usage of set-output [#11844](https://github.com/vitessio/vitess/pull/11844)
 * Use `go1.19.4` in the next release upgrade downgrade E2E tests [#11924](https://github.com/vitessio/vitess/pull/11924) 
#### TabletManager
 * Fix closing the body for HTTP requests [#11842](https://github.com/vitessio/vitess/pull/11842)
### Enhancement 
#### General
 * Upgrade to `go1.18.9` [#11897](https://github.com/vitessio/vitess/pull/11897)
### Release 
#### General
 * Release of v15.0.1 [#11847](https://github.com/vitessio/vitess/pull/11847)
 * Back to dev mode after v15.0.1 [#11848](https://github.com/vitessio/vitess/pull/11848)
 * updating summary and release notes for v15.0.1 [#11852](https://github.com/vitessio/vitess/pull/11852)
 * Update the release `15.0.2` summary doc [#11954](https://github.com/vitessio/vitess/pull/11954)

