# Changelog of Vitess v16.0.6

### Bug fixes 
#### CLI
 * [release-16.0] Fix anonymous paths in cobra code-gen (#14185) [#14236](https://github.com/vitessio/vitess/pull/14236) 
#### Online DDL
 * [Release 16.0]: Online DDL: timeouts for all gRPC calls (#14182) [#14191](https://github.com/vitessio/vitess/pull/14191) 
#### Query Serving
 * [release-16.0] Rewrite `USING` to `ON` condition for joins (#13931) [#13940](https://github.com/vitessio/vitess/pull/13940) 
#### Throttler
 * [release-16.0] Tablet throttler: fix race condition by removing goroutine call (#14179) [#14200](https://github.com/vitessio/vitess/pull/14200)
### CI/Build 
#### General
 * [release-16.0] Upgrade the Golang version to `go1.20.9` [#14194](https://github.com/vitessio/vitess/pull/14194)
 * [release-16.0] Upgrade the Golang version to `go1.20.10` [#14228](https://github.com/vitessio/vitess/pull/14228) 
#### Online DDL
 * [release-16.0] OnlineDDL: reduce vrepl_stress workload in forks (#14302) [#14347](https://github.com/vitessio/vitess/pull/14347)
### Dependabot 
#### General
 * [release-16.0] Bump github.com/cyphar/filepath-securejoin from 0.2.3 to 0.2.4 (#14239) [#14251](https://github.com/vitessio/vitess/pull/14251)
 * [release-16.0] Bump golang.org/x/net from 0.14.0 to 0.17.0 (#14260) [#14262](https://github.com/vitessio/vitess/pull/14262) 
#### VTAdmin
 * [release-16.0] Bump postcss from 8.4.21 to 8.4.31 in /web/vtadmin (#14173) [#14256](https://github.com/vitessio/vitess/pull/14256)
 * [release-16.0] Bump @babel/traverse from 7.21.4 to 7.23.2 in /web/vtadmin (#14304) [#14306](https://github.com/vitessio/vitess/pull/14306)
### Enhancement 
#### Build/CI
 * [release-16.0] Automatic approval of `vitess-bot` clean backports (#14352) [#14355](https://github.com/vitessio/vitess/pull/14355)
### Release 
#### General
 * Code freeze of release-16.0 [#14409](https://github.com/vitessio/vitess/pull/14409)

