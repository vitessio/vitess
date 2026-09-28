# Changelog of Vitess v17.0.1

### Bug fixes 
#### Cluster management
 * [release-17.0] Upgrade-Downgrade Fix: Schema-initialization stuck on semi-sync ACKs while upgrading [#13411](https://github.com/vitessio/vitess/pull/13411)
 * [release-17.0] ignore all error for views in engine reload (#13590) [#13594](https://github.com/vitessio/vitess/pull/13594)
 * [release-17.0] check keyspace snapshot time if none specified for backup restores (#13557) [#13635](https://github.com/vitessio/vitess/pull/13635) 
#### Examples
 * [release-17.0] Local example 101: idempotent on existing clusters (#13373) [#13383](https://github.com/vitessio/vitess/pull/13383)
 * [release-17.0] Examples: only terminate vtadmin if it was started (#13433) [#13443](https://github.com/vitessio/vitess/pull/13443)
 * [release-17.0] `examples/compose`: fix `consul:latest` error w/`docker-compose up -d` (#13468) [#13473](https://github.com/vitessio/vitess/pull/13473) 
#### Schema Tracker
 * [release-17.0] Ignore error while reading table data in Schema.Engine reload (#13421) [#13423](https://github.com/vitessio/vitess/pull/13423)
 * Backport v17: schema.Reload(): ignore column reading errors for views only, error for tables #13442 [#13455](https://github.com/vitessio/vitess/pull/13455) 
#### Throttler
 * [release-17.0] Tablet throttler: only start watching SrvKeyspace once it's confirmed to exist (#13384) [#13399](https://github.com/vitessio/vitess/pull/13399) 
#### VReplication
 * [release-17.0] VReplication: Ensure ROW events are sent within a transaction (#13547) [#13581](https://github.com/vitessio/vitess/pull/13581) 
#### VTorc
 * [release-17.0] Ensure to call `servenv.Init` when needed (#13638) [#13643](https://github.com/vitessio/vitess/pull/13643)
### CI/Build 
#### Build/CI
 * Backport v17: Replace deprecated github.com/golang/mock with go.uber.org/mock #13512 [#13601](https://github.com/vitessio/vitess/pull/13601)
### Internal Cleanup 
#### VTorc
 * [release-17.0] Remove excessive logging in VTOrc APIs (#13459) [#13461](https://github.com/vitessio/vitess/pull/13461)
### Performance 
#### TabletManager
 * [release-17.0] BaseShowTablesWithSizes: optimize MySQL 8.0 query (#13375) [#13390](https://github.com/vitessio/vitess/pull/13390)
### Release 
#### Build/CI
 * [release-17.0] Optimize release notes generation to use GitHub Milestones (#13398) [#13622](https://github.com/vitessio/vitess/pull/13622) 
#### General
 * Back to dev mode after v17.0.0 [#13386](https://github.com/vitessio/vitess/pull/13386)
### Testing 
#### Cluster management
 * [release-17.0] Deflake `TestPlannedReparentShardPromoteReplicaFail` (#13548) [#13550](https://github.com/vitessio/vitess/pull/13550)
 * [release-17.0] Flaky tests: Fix wrangler tests (#13568) [#13572](https://github.com/vitessio/vitess/pull/13572) 
#### General
 * [release-17.0] Upgrade-downgrade test fix: Remove throttler flags in `vttablet-up.sh` [#13516](https://github.com/vitessio/vitess/pull/13516) 
#### Query Serving
 * [release-17.0] Deflake `TestQueryTimeoutWithDual` test (#13405) [#13410](https://github.com/vitessio/vitess/pull/13410)
 * [release-17.0] Fix `TestGatewayBufferingWhileReparenting` flakiness (#13469) [#13499](https://github.com/vitessio/vitess/pull/13499)
 * [release-17.0] fix TestQueryTimeoutWithTables flaky test (#13579) [#13586](https://github.com/vitessio/vitess/pull/13586) 
#### VTorc
 * [release-17.0]: Fix flakiness in VTOrc tests (#13489) [#13527](https://github.com/vitessio/vitess/pull/13527)

