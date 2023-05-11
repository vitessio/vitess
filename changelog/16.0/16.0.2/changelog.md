# Changelog of Vitess v16.0.2

### Bug fixes 
#### Build/CI
 * Small fixes to the auto-upgrade golang tool [#12838](https://github.com/vitessio/vitess/pull/12838)
 * Add timeout to `golangci-lint` and bump its version [#12852](https://github.com/vitessio/vitess/pull/12852)
 * [release-16.0] Remove recent golangci-lint version bump [#12909](https://github.com/vitessio/vitess/pull/12909) 
#### Cluster management
 * Backport: [topo] Disallow the slash character in shard names #12843 [#12858](https://github.com/vitessio/vitess/pull/12858) 
#### Query Serving
 * Fix `vtgate_schema_tracker` flaky tests [#12780](https://github.com/vitessio/vitess/pull/12780)
 * [planbuilder bugfix] do not push aggregations into derived tables [#12810](https://github.com/vitessio/vitess/pull/12810)
 * [16.0] Fix: reset transaction session when no reserved connection [#12877](https://github.com/vitessio/vitess/pull/12877)
 * [release-16.0] fix: union distinct between unsharded route and sharded join (#12968) [#12974](https://github.com/vitessio/vitess/pull/12974)
### CI/Build 
#### General
 * Do not fail build on incorrect Go version [#12809](https://github.com/vitessio/vitess/pull/12809)
 * [release-16.0] Upgrade the Golang version to `go1.20.3` [#12832](https://github.com/vitessio/vitess/pull/12832)
### Documentation 
#### Query Serving
 * update v16 release notes about VTGate Advertised MySQL Version [#12957](https://github.com/vitessio/vitess/pull/12957)
### Enhancement 
#### Build/CI
 * Remove unnecessary code bits in workflows [#12756](https://github.com/vitessio/vitess/pull/12756) 
#### General
 * Automatically add milestone to new Pull Request [#12759](https://github.com/vitessio/vitess/pull/12759) 
#### Query Serving
 * [release-16.0] planner fix: scoping rules for JOIN ON expression inside a subquery [#12891](https://github.com/vitessio/vitess/pull/12891)
### Internal Cleanup 
#### CLI
 * Cleanup TODOs in vtorc flag parsing code from v15 [#12787](https://github.com/vitessio/vitess/pull/12787) 
#### TabletManager
 * Table GC: remove spammy log entry [#12625](https://github.com/vitessio/vitess/pull/12625)
### Regression 
#### ACL
 * vtgate : Disable Automatically setting immediateCallerID to user from static authentication context  [#12961](https://github.com/vitessio/vitess/pull/12961) 
#### Query Serving
 * gen4 planner: allow last_insert_id with arguments [#13026](https://github.com/vitessio/vitess/pull/13026)
### Release 
#### Documentation
 * Fix incorrect path during release notes generation [#12769](https://github.com/vitessio/vitess/pull/12769) 
#### General
 * Back to dev mode after v16.0.1 [#12783](https://github.com/vitessio/vitess/pull/12783)
 * Summary changes and code freeze for release of v16.0.2 [#13049](https://github.com/vitessio/vitess/pull/13049)
### Testing 
#### Build/CI
 * [release-16.0] Throttler: Expose Tablet's Config & Leverage to Deflake Tests [#12791](https://github.com/vitessio/vitess/pull/12791)
 * fakedbclient: Add locking to avoid races [#12814](https://github.com/vitessio/vitess/pull/12814)
 * [release-16.0] test: fix cfc flaky test (#12941) [#12960](https://github.com/vitessio/vitess/pull/12960)

