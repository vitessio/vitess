# Changelog of Vitess v24.0.4

### Bug fixes 
#### Evalengine
 * [release-24.0] mysql/json: read numbers the way MySQL does (#20722) [#20737](https://github.com/vitessio/vitess/pull/20737) 
#### VDiff
 * [release-24.0] fix(vdiff): map source PK columns to their SELECT positions in getSourcePKCols (#20603) [#21215](https://github.com/vitessio/vitess/pull/21215)
 * [release-24.0] VDiff: accept a target primary key that extends a unique source key (#21275) [#21317](https://github.com/vitessio/vitess/pull/21317) 
#### VReplication
 * [release-24.0]  vreplication: Fix --config-overrides silently erasing existing config keys (#21017) [#21043](https://github.com/vitessio/vitess/pull/21043)
 * [release-24.0] vtctl/workflow: use tablet db name override when reading current sequence value in SwitchTraffic dry run (#21166) [#21179](https://github.com/vitessio/vitess/pull/21179) 
#### VTAdmin
 * [release-24.0] flagutil: keep the first value passed to StringSetFlag.Set (#21037) [#21045](https://github.com/vitessio/vitess/pull/21045)
 * [release-24.0] vtadmin: send a non-empty :authority so gRPC works behind a proxy (#21191) [#21197](https://github.com/vitessio/vitess/pull/21197) 
#### VTGate
 * [release-24.0] vtgate: report RowsAffected for stored-procedure calls over the streaming path (#20402) [#20749](https://github.com/vitessio/vitess/pull/20749)
 * [release-24.0] sqlparser: fix panic on WITH followed by parenthesized query (#21125) [#21184](https://github.com/vitessio/vitess/pull/21184)
 * [release-24.0] vtgate: list the session's SET_VAR hint variables in a fixed order (#21282) [#21286](https://github.com/vitessio/vitess/pull/21286) 
#### VTOrc
 * [release-24.0] vtorc: pass a heartbeat interval to `SetReplicationSource` only for `ReplicaMisconfigured` (#21251) [#21315](https://github.com/vitessio/vitess/pull/21315) 
#### VTTablet
 * [release-24.0] vttablet: reset connection settings with the DEFAULT keyword (#21026) [#21041](https://github.com/vitessio/vitess/pull/21041)
 * [release-24.0] smartconnpool: re-check capacity after getNew's CAS (#21190) [#21201](https://github.com/vitessio/vitess/pull/21201)
 * [release-24.0] vttablet: reset foreign_key_checks and unique_checks settings to the global value (#21167) [#21203](https://github.com/vitessio/vitess/pull/21203)
 * [release-24.0] tabletenv: Fix infinite recursion in OltpConfig.UnmarshalJSON (#21240) [#21272](https://github.com/vitessio/vitess/pull/21272)
 * [release-24.0] VTTablet: discard a transaction's connection after a CALL ran on it (#21256) [#21306](https://github.com/vitessio/vitess/pull/21306)
### CI/Build 
#### Build/CI
 * [release-24.0] go-upgrade: pin the multi-platform index digest of the Go image (#21007) [#21012](https://github.com/vitessio/vitess/pull/21012)
 * [release-24.0] ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099) [#21102](https://github.com/vitessio/vitess/pull/21102)
 * [release-24.0] CI Improvements from CNCF Staff (#21164) [#21174](https://github.com/vitessio/vitess/pull/21174)
 * [release-24.0] CI: close gaps in the #21164 gating and stop spending runners on no-op work (#21181) [#21207](https://github.com/vitessio/vitess/pull/21207) 
#### Documentation
 * [release-24.0] changelog: limit the release summary to changes users and operators act on (#21258) [#21260](https://github.com/vitessio/vitess/pull/21260)
### Dependencies 
#### Backup and Restore
 * [release-24.0] mysqlctl: upgrade pierrec/lz4 to v4 to fix broken amd64 block decoding (#20778) [#21035](https://github.com/vitessio/vitess/pull/21035)
 * [release-24.0] CI: Replace MinIO server with MicroCeph RGW (#21086) [#21090](https://github.com/vitessio/vitess/pull/21090) 
#### Docker
 * [release-24.0] Upgrade the Golang version to `go1.26.8` [#20980](https://github.com/vitessio/vitess/pull/20980)
### Enhancement 
#### VDiff
 * [release-24.0] Only negotiate multi statement support on connections that send batches (#21221) [#21274](https://github.com/vitessio/vitess/pull/21274)
### Performance 
#### Query Serving
 * [release-24.0] vtgate: only scan shard records for MoveTables state when routing rules reference the keyspace (#20464) [#21082](https://github.com/vitessio/vitess/pull/21082)
### Release 
#### General
 * [release-24.0] Bump to `v24.0.4-SNAPSHOT` after the `v24.0.3` release [#20999](https://github.com/vitessio/vitess/pull/20999)
 * [release-24.0] Code Freeze for `v24.0.4` [#21331](https://github.com/vitessio/vitess/pull/21331)
### Security 
#### Authn/z
 * [release-24.0] vttls: Enforce CRLs in every SSL mode and on resumed TLS sessions (#21054) [#21113](https://github.com/vitessio/vitess/pull/21113)
 * [release-24.0] vttls: Refuse a server-side CRL when the server is not configured for TLS (#21153) [#21231](https://github.com/vitessio/vitess/pull/21231)
 * [release-24.0] grpcoptionaltls: Say that plain-text connections are unauthenticated, and count connections by transport (#21162) [#21319](https://github.com/vitessio/vitess/pull/21319) 
#### Build/CI
 * [release-24.0] Add `zizmor` check to the static checks workflow (#19149) [#20811](https://github.com/vitessio/vitess/pull/20811) 
#### Documentation
 * [release-24.0] vtctld: Remove the unused legacy HTTP API served under `/api/` (#21171) [#21199](https://github.com/vitessio/vitess/pull/21199) 
#### VTGate
 * [release-24.0] VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139) [#21220](https://github.com/vitessio/vitess/pull/21220) 
#### VTTablet
 * [release-24.0] vttablet: derive table ACL permissions the way MySQL resolves CTE names (#21091) [#21105](https://github.com/vitessio/vitess/pull/21105)
 * [release-24.0] VTTablet: fail closed under strict table ACL when a statement's table set cannot be determined (#21053) [#21149](https://github.com/vitessio/vitess/pull/21149)
 * [release-24.0] VTTablet: Discard the pooled connection after CALL so procedure session state cannot leak (#21062) [#21218](https://github.com/vitessio/vitess/pull/21218)
### Testing 
#### VTTablet
 * [release-24.0] test: cut TestPlayerStalls from 75s to 10s per suite pass (#20546) [#21227](https://github.com/vitessio/vitess/pull/21227)
 * [release-24.0] test: deflake TestStartWrites in the semi-sync monitor (#21276) [#21304](https://github.com/vitessio/vitess/pull/21304)
 * [release-24.0] test: make TestPlayerStalls' heartbeat subtest exercise the heartbeat path (#21229) [#21313](https://github.com/vitessio/vitess/pull/21313) 
#### vttestserver
 * [release-24.0] vttestserver: fail, don't panic, on a vschema missing a table or vindex (#20740) [#20745](https://github.com/vitessio/vitess/pull/20745)

