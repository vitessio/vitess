# Changelog of Vitess v23.0.7

### Bug fixes 
#### Evalengine
 * [release-23.0] mysql/json: read numbers the way MySQL does (#20722) [#20736](https://github.com/vitessio/vitess/pull/20736) 
#### Query Serving
 * [release-23.0] charset: ensure DecodeRune always advances on malformed input (#20753) [#20765](https://github.com/vitessio/vitess/pull/20765) 
#### VDiff
 * [release-23.0] fix(vdiff): map source PK columns to their SELECT positions in getSourcePKCols (#20603) [#21214](https://github.com/vitessio/vitess/pull/21214)
 * [release-23.0] VDiff: accept a target primary key that extends a unique source key (#21275) [#21316](https://github.com/vitessio/vitess/pull/21316) 
#### VReplication
 * [release-23.0]  vreplication: Fix --config-overrides silently erasing existing config keys (#21017) [#21042](https://github.com/vitessio/vitess/pull/21042)
 * [release-23.0] vtctl/workflow: use tablet db name override when reading current sequence value in SwitchTraffic dry run (#21166) [#21178](https://github.com/vitessio/vitess/pull/21178) 
#### VTAdmin
 * [release-23.0] flagutil: keep the first value passed to StringSetFlag.Set (#21037) [#21044](https://github.com/vitessio/vitess/pull/21044)
 * [release-23.0] vtadmin: send a non-empty :authority so gRPC works behind a proxy (#21191) [#21196](https://github.com/vitessio/vitess/pull/21196) 
#### VTGate
 * [release-23.0] vtgate: report RowsAffected for stored-procedure calls over the streaming path (#20402) [#20748](https://github.com/vitessio/vitess/pull/20748)
 * [release-23.0] sqlparser: fix panic on WITH followed by parenthesized query (#21125) [#21183](https://github.com/vitessio/vitess/pull/21183)
 * [release-23.0] vtgate: list the session's SET_VAR hint variables in a fixed order (#21282) [#21285](https://github.com/vitessio/vitess/pull/21285) 
#### VTOrc
 * [release-23.0] vtorc: pass a heartbeat interval to `SetReplicationSource` only for `ReplicaMisconfigured` (#21251) [#21314](https://github.com/vitessio/vitess/pull/21314) 
#### VTTablet
 * [release-23.0] vttablet: reset connection settings with the DEFAULT keyword (#21026) [#21040](https://github.com/vitessio/vitess/pull/21040)
 * [release-23.0] smartconnpool: re-check capacity after getNew's CAS (#21190) [#21200](https://github.com/vitessio/vitess/pull/21200)
 * [release-23.0] vttablet: reset foreign_key_checks and unique_checks settings to the global value (#21167) [#21202](https://github.com/vitessio/vitess/pull/21202)
 * [release-23.0] tabletenv: Fix infinite recursion in OltpConfig.UnmarshalJSON (#21240) [#21271](https://github.com/vitessio/vitess/pull/21271)
 * [release-23.0] VTTablet: discard a transaction's connection after a CALL ran on it (#21256) [#21305](https://github.com/vitessio/vitess/pull/21305)
### CI/Build 
#### Build/CI
 * [release-23.0] go-upgrade: pin the multi-platform index digest of the Go image (#21007) [#21011](https://github.com/vitessio/vitess/pull/21011)
 * [release-23.0] ci: bound bootstrap download retries (#21099) [#21101](https://github.com/vitessio/vitess/pull/21101)
 * [release-23.0] CI Improvements from CNCF Staff (#21164) [#21173](https://github.com/vitessio/vitess/pull/21173)
 * [release-23.0] CI: close gaps in the #21164 gating and stop spending runners on no-op work (#21181) [#21206](https://github.com/vitessio/vitess/pull/21206) 
#### Documentation
 * [release-23.0] changelog: limit the release summary to changes users and operators act on (#21258) [#21259](https://github.com/vitessio/vitess/pull/21259)
### Dependencies 
#### Backup and Restore
 * [release-23.0] mysqlctl: upgrade pierrec/lz4 to v4 to fix broken amd64 block decoding (#20778) [#21034](https://github.com/vitessio/vitess/pull/21034)
 * [release-23.0] CI: Replace MinIO server with MicroCeph RGW (#21086) [#21089](https://github.com/vitessio/vitess/pull/21089)
### Documentation 
#### Documentation
 * docs: changelog: add v23.0.7 entry for removal of the legacy vtctld /api/ HTTP API [#21252](https://github.com/vitessio/vitess/pull/21252)
### Enhancement 
#### VDiff
 * [release-23.0] Only negotiate multi statement support on connections that send batches (#21221) [#21273](https://github.com/vitessio/vitess/pull/21273)
### Performance 
#### Query Serving
 * [release-23.0] vtgate: only scan shard records for MoveTables state when routing rules reference the keyspace (#20464) [#21081](https://github.com/vitessio/vitess/pull/21081)
### Release 
#### General
 * [release-23.0] Bump to `v23.0.7-SNAPSHOT` after the `v23.0.6` release [#20995](https://github.com/vitessio/vitess/pull/20995)
 * [release-23.0] Code Freeze for `v23.0.7` [#21332](https://github.com/vitessio/vitess/pull/21332)
### Security 
#### Authn/z
 * [release-23.0] vttls: Enforce CRLs in every SSL mode and on resumed TLS sessions (#21054) [#21112](https://github.com/vitessio/vitess/pull/21112)
 * [release-23.0] vttls: Refuse a server-side CRL when the server is not configured for TLS (#21153) [#21230](https://github.com/vitessio/vitess/pull/21230)
 * [release-23.0] grpcoptionaltls: Say that plain-text connections are unauthenticated, and count connections by transport (#21162) [#21318](https://github.com/vitessio/vitess/pull/21318) 
#### Build/CI
 * [release-23.0] Add `zizmor` check to the static checks workflow (#19149) [#20810](https://github.com/vitessio/vitess/pull/20810) 
#### Documentation
 * [release-23.0] vtctld: Remove the unused legacy HTTP API served under `/api/` (#21171) [#21198](https://github.com/vitessio/vitess/pull/21198) 
#### VTGate
 * [release-23.0] VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139) [#21219](https://github.com/vitessio/vitess/pull/21219) 
#### VTTablet
 * [release-23.0] vttablet: derive table ACL permissions the way MySQL resolves CTE names (#21091) [#21104](https://github.com/vitessio/vitess/pull/21104)
 * [release-23.0] VTTablet: fail closed under strict table ACL when a statement's table set cannot be determined (#21053) [#21148](https://github.com/vitessio/vitess/pull/21148)
 * [release-23.0] VTTablet: Discard the pooled connection after CALL so procedure session state cannot leak (#21062) [#21217](https://github.com/vitessio/vitess/pull/21217)
### Testing 
#### VTGate
 * [release-23.0] test: deflake TestVStreamsMetrics [#21277](https://github.com/vitessio/vitess/pull/21277) 
#### VTTablet
 * [release-23.0] test: cut TestPlayerStalls from 75s to 10s per suite pass (#20546) [#21226](https://github.com/vitessio/vitess/pull/21226)
 * [release-23.0] test: deflake TestStartWrites in the semi-sync monitor (#21276) [#21303](https://github.com/vitessio/vitess/pull/21303)
 * [release-23.0] test: make TestPlayerStalls' heartbeat subtest exercise the heartbeat path (#21229) [#21312](https://github.com/vitessio/vitess/pull/21312) 
#### vttestserver
 * [release-23.0] vttestserver: fail, don't panic, on a vschema missing a table or vindex (#20740) [#20744](https://github.com/vitessio/vitess/pull/20744)

