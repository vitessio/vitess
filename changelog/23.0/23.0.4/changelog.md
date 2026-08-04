# Changelog of Vitess v23.0.4

### Bug fixes 
#### Query Serving
 * [release-23.0] planbuilder: fix panic when SELECT has duplicate subqueries (#19581) [#19606](https://github.com/vitessio/vitess/pull/19606) 
#### VReplication
 * [release-23.0] workflow: finish switch traffic after post-journal cancel (#19672) [#19682](https://github.com/vitessio/vitess/pull/19682) 
#### VTGate
 * [release-23.0] vtgate: set ServerStatusAutocommit in handshake status flags (#19628) [#19646](https://github.com/vitessio/vitess/pull/19646)
 * [release-23.0] VTGate: fix warming reads timeout context (#19674) [#19728](https://github.com/vitessio/vitess/pull/19728)
 * [release-23.0] proto: fix incorrect flag bits on `RAW` and `ROW_TUPLE` types (#19920) [#19949](https://github.com/vitessio/vitess/pull/19949)
 * [release-23.0] vtgate: preserve target keyspace when routing rule rewrites table AST (#19948) [#19952](https://github.com/vitessio/vitess/pull/19952)
 * [release-23.0] vtgate: prevent buffer restart after shutdown (#19954) [#19961](https://github.com/vitessio/vitess/pull/19961)
 * [release-23.0] vtgate: rebuild routing rules after schema-tracker updates (port of #19104) [#20014](https://github.com/vitessio/vitess/pull/20014) 
#### VTOrc
 * [release-23.0] vtorc: add timeout helpers for remaining recovery topo/tmc calls (#19520) [#19559](https://github.com/vitessio/vitess/pull/19559)
 * [release-23.0] VTOrc: Address panic uncovered by Antithesis (#19904) [#19909](https://github.com/vitessio/vitess/pull/19909)
 * [release-23.0] VTOrc: fix `ReplicationStopped` + `PrimarySemiSyncBlocked` recovery deadlock (#19925) [#19982](https://github.com/vitessio/vitess/pull/19982) 
#### VTTablet
 * [release-23.0] tabletmanager: handle nil Cnf in MysqlHostMetrics to prevent panic (#19752) [#19754](https://github.com/vitessio/vitess/pull/19754)
 * [release-23.0] vttablet: handle applier metadata init failures in relay-log recovery (#19560) [#19789](https://github.com/vitessio/vitess/pull/19789)
 * [release-23.0] `go/mysql`, `vreplication`: fix flaky unit tests with shared root cause (#19990) [#19997](https://github.com/vitessio/vitess/pull/19997) 
#### schema management
 * [release-23.0] Fix `DROP CONSTRAINT` to work the same way as MySQL (#19183) [#19241](https://github.com/vitessio/vitess/pull/19241) 
#### vtctl
 * [release-23.0] Restart IO threads on replicas after ERS failure (#19805) [#19823](https://github.com/vitessio/vitess/pull/19823)
 * [release-23.0] `EmergencyReparentShard`: fix nil pointer panic in errant GTID detection (#19848) [#19857](https://github.com/vitessio/vitess/pull/19857)
 * [release-23.0] `EmergencyReparentShard`: fix cancellation in `reparentReplicas()` (#19849) [#19860](https://github.com/vitessio/vitess/pull/19860)
### CI/Build 
#### Build/CI
 * [release-23.0] Fix some linting issues (#19246) [#19248](https://github.com/vitessio/vitess/pull/19248)
 * [release-23.0] `e2e`: fix race in `TestFailingReplication` (#19547) [#19548](https://github.com/vitessio/vitess/pull/19548)
 * [release-23.0] `ci`: run code coverage CI only on go packages that had changes (#19431) [#19591](https://github.com/vitessio/vitess/pull/19591)
 * [release-23.0] `ci`: only cache `action/setup-go` action on `main` (#19634) [#19638](https://github.com/vitessio/vitess/pull/19638)
 * [release-23.0] ci: use `bash -e {0}` in composite actions  (#19707) [#19710](https://github.com/vitessio/vitess/pull/19710)
 * [release-23.0] `ci`: skip Code Coverage CI on backports (#19726) [#19737](https://github.com/vitessio/vitess/pull/19737)
 * [release-23.0] docker: use shared buildkit cache scope for bootstrap images (#19770) [#19779](https://github.com/vitessio/vitess/pull/19779)
 * [release-23.0] ci: add `setup-go` composite action (#19784) [#19804](https://github.com/vitessio/vitess/pull/19804)
 * [release-23.0] go-upgrade: fix Go image digest rewrite matching (#19820) [#19829](https://github.com/vitessio/vitess/pull/19829) 
#### General
 * [release-23.0] Upgrade the Golang version to `go1.25.8` [#19598](https://github.com/vitessio/vitess/pull/19598)
### Compatibility Bug 
#### Query Serving
 * [release-23.0] vtgate: Reject unqualified `*` after comma in `SELECT` list (#19475) [#19584](https://github.com/vitessio/vitess/pull/19584)
### Dependencies 
#### Docker
 * [release-23.0] `vtorc`: support analysis ordering, improve semi-sync rollout (#19427) [#19472](https://github.com/vitessio/vitess/pull/19472) 
#### General
 * [release-23.0] Upgrade the Golang version to `go1.25.9` [#19818](https://github.com/vitessio/vitess/pull/19818)
### Enhancement 
#### Online DDL
 * [release-23.0] OnlineDDL: always close lock connection (#19586) [#19721](https://github.com/vitessio/vitess/pull/19721) 
#### Query Serving
 * [release-23.0] sqlparser: enforce bare `*` restriction in grammar (#19585) [#19719](https://github.com/vitessio/vitess/pull/19719) 
#### Topology
 * [release-23.0] Add ZooKeeper connection metrics to zk2topo (#19757) [#19792](https://github.com/vitessio/vitess/pull/19792) 
#### VTOrc
 * [release-23.0] `vtorc`: improve logging in `DiscoverInstance`, remove old metric (#19010) [#19517](https://github.com/vitessio/vitess/pull/19517)
 * [release-23.0] `vtorc`: improvements to analysis ordering, handle semi-sync disable (#19488) [#19551](https://github.com/vitessio/vitess/pull/19551) 
#### VTTablet
 * [release-23.0] OnlineDDL: set `wait_timeout` on cutover connections (#19630) [#19761](https://github.com/vitessio/vitess/pull/19761) 
#### vtctl
 * [release-23.0] `EmergencyReparentShard`: require stop replication error to be from `PRIMARY` (#19515) [#19608](https://github.com/vitessio/vitess/pull/19608)
### Regression 
#### Driver
 * [release-23.0] vitessdriver: return string for binary result values (#19527) [#19534](https://github.com/vitessio/vitess/pull/19534)
### Release 
#### General
 * [release-23.0] Bump to `v23.0.4-SNAPSHOT` after the `v23.0.3` release [#19507](https://github.com/vitessio/vitess/pull/19507)
 * [release-23.0] Code Freeze for `v23.0.4` [#20020](https://github.com/vitessio/vitess/pull/20020)
### Testing 
#### General
 * [release-23.0] Move `TabletManagerClient` mock to `tmclient/mock` package (#19698) [#19711](https://github.com/vitessio/vitess/pull/19711) 
#### VReplication
 * [release-23.0] CI: Address our two flakiest tests (#19587) [#19595](https://github.com/vitessio/vitess/pull/19595) 
#### vttestserver
 * [release-23.0] vttest: avoid 10-minute hang when vtcombo exits during startup (#20041) [#20043](https://github.com/vitessio/vitess/pull/20043)

