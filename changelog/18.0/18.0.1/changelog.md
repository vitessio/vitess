# Changelog of Vitess v18.0.1

### Bug fixes 
#### Backup and Restore
 * [release 18.0]: `ReadBinlogFilesTimestamps` backwards compatibility [#14526](https://github.com/vitessio/vitess/pull/14526) 
#### Build/CI
 * [release-18.0] Update create_release.sh (#14492) [#14516](https://github.com/vitessio/vitess/pull/14516) 
#### Evalengine
 * [release-18.0] Fix nullability checks in evalengine (#14556) [#14564](https://github.com/vitessio/vitess/pull/14564) 
#### Examples
 * [release-18.0] examples: fix flag syntax for zkctl (#14469) [#14487](https://github.com/vitessio/vitess/pull/14487) 
#### Observability
 * [release-18.0] Fix #14414:  resilient_server metrics name/prefix logic is inverted, leading to no metrics being recorded (#14415) [#14527](https://github.com/vitessio/vitess/pull/14527) 
#### Query Serving
 * [release-18.0] Make column resolution closer to MySQL (#14426) [#14430](https://github.com/vitessio/vitess/pull/14430)
 * [release-18.0] Bug fix: Use target tablet from health stats cache when checking replication status (#14436) [#14456](https://github.com/vitessio/vitess/pull/14456)
 * [release-18.0] Ensure hexval and int don't share BindVar after Normalization (#14451) [#14479](https://github.com/vitessio/vitess/pull/14479)
 * [release-18.0] planbuilder bugfix: expose columns through derived tables (#14501) [#14504](https://github.com/vitessio/vitess/pull/14504)
 * [release-18.0] expression rewriting: enable more rewrites and limit CNF rewrites (#14560) [#14576](https://github.com/vitessio/vitess/pull/14576) 
#### vtctldclient
 * [release-18.0] vtctldclient: Apply tablet type filtering for keyspace+shard in GetTablets (#14467) [#14470](https://github.com/vitessio/vitess/pull/14470)
### CI/Build 
#### Docker
 * [release-18.0] Build and push Docker Images from GitHub Actions [#14511](https://github.com/vitessio/vitess/pull/14511)
### Dependabot 
#### General
 * [release-18.0] Bump google.golang.org/grpc from 1.55.0-dev to 1.59.0 (#14364) [#14498](https://github.com/vitessio/vitess/pull/14498)
### Documentation 
#### Documentation
 * [release-18.0] release notes: add FK import to summary (#14518) [#14519](https://github.com/vitessio/vitess/pull/14519)
### Internal Cleanup 
#### Query Serving
 * [release-18.0] Remove excessive VTGate logging of default planner selection (#14554) [#14561](https://github.com/vitessio/vitess/pull/14561)
### Release 
#### General
 * [release-18.0] Code Freeze for `v18.0.1` [#14549](https://github.com/vitessio/vitess/pull/14549)
### Testing 
#### Query Serving
 * [release-18.0] vtgate: Allow additional errors in warnings test (#14461) [#14465](https://github.com/vitessio/vitess/pull/14465)

