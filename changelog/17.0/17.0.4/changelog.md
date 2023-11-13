# Changelog of Vitess v17.0.4

### Bug fixes 
#### CLI
 * [release-17.0] Fix anonymous paths in cobra code-gen (#14185) [#14237](https://github.com/vitessio/vitess/pull/14237) 
#### Evalengine
 * [release-17.0] evalengine: Misc bugs (#14351) [#14353](https://github.com/vitessio/vitess/pull/14353) 
#### Examples
 * [release-17.0] examples: fix flag syntax for zkctl (#14469) [#14486](https://github.com/vitessio/vitess/pull/14486) 
#### General
 * [release-17.0] viper: register dynamic config with both disk and live (#14453) [#14454](https://github.com/vitessio/vitess/pull/14454) 
#### Online DDL
 * [Release 17.0]: Online DDL: timeouts for all gRPC calls (#14182) [#14190](https://github.com/vitessio/vitess/pull/14190)
 * [release-17.0] schemadiff: fix missing `DROP CONSTRAINT` in duplicate/redundant constraints scenario. (#14387) [#14390](https://github.com/vitessio/vitess/pull/14390) 
#### Query Serving
 * [release-17.0] Make column resolution closer to MySQL (#14426) [#14429](https://github.com/vitessio/vitess/pull/14429)
 * [release-17.0] vtgate/engine: Fix race condition in join logic (#14435) [#14440](https://github.com/vitessio/vitess/pull/14440)
 * [release-17.0] Ensure hexval and int don't share BindVar after Normalization (#14451) [#14478](https://github.com/vitessio/vitess/pull/14478) 
#### Throttler
 * [release-17.0] Tablet throttler: fix race condition by removing goroutine call (#14179) [#14199](https://github.com/vitessio/vitess/pull/14199) 
#### VReplication
 * [release-17.0] VDiff: wait for shard streams of one table diff to complete for before starting that of the next table (#14345) [#14381](https://github.com/vitessio/vitess/pull/14381)
### CI/Build 
#### General
 * [release-17.0] Upgrade the Golang version to `go1.20.9` [#14196](https://github.com/vitessio/vitess/pull/14196)
 * [release-17.0] Upgrade the Golang version to `go1.20.10` [#14229](https://github.com/vitessio/vitess/pull/14229) 
#### Online DDL
 * [release-17.0] OnlineDDL: reduce vrepl_stress workload in forks (#14302) [#14348](https://github.com/vitessio/vitess/pull/14348)
### Dependabot 
#### General
 * [release-17.0] Bump github.com/cyphar/filepath-securejoin from 0.2.3 to 0.2.4 (#14239) [#14252](https://github.com/vitessio/vitess/pull/14252)
 * [release-17.0] Bump golang.org/x/net from 0.14.0 to 0.17.0 (#14260) [#14263](https://github.com/vitessio/vitess/pull/14263)
 * [release-17.0] Bump google.golang.org/grpc from 1.55.0-dev to 1.59.0 (#14364) [#14497](https://github.com/vitessio/vitess/pull/14497) 
#### VTAdmin
 * [release-17.0] Bump postcss from 8.4.21 to 8.4.31 in /web/vtadmin (#14173) [#14257](https://github.com/vitessio/vitess/pull/14257)
 * [release-17.0] Bump @babel/traverse from 7.21.4 to 7.23.2 in /web/vtadmin (#14304) [#14307](https://github.com/vitessio/vitess/pull/14307)
### Enhancement 
#### Build/CI
 * [release-17.0] Automatic approval of `vitess-bot` clean backports (#14352) [#14356](https://github.com/vitessio/vitess/pull/14356)
### Release 
#### General
 * Code freeze of release-17.0 [#14407](https://github.com/vitessio/vitess/pull/14407)
### Testing 
#### Cluster management
 * Fix Upgrade downgrade reparent tests in release-17.0 [#14507](https://github.com/vitessio/vitess/pull/14507) 
#### Query Serving
 * [release-17.0] vtgate: Allow more errors for the warning check (#14421) [#14422](https://github.com/vitessio/vitess/pull/14422)

