# Changelog of Vitess v22.0.0

### Bug fixes 
#### ACL
 * Don't exit tablet server on reloading invalid ACL [#17485](https://github.com/vitessio/vitess/pull/17485) 
#### Backup and Restore
 * fix releasing the global read lock when mysqlshell backup fails [#17000](https://github.com/vitessio/vitess/pull/17000)
 * Fix how we cancel the context in the builtin backup engine [#17285](https://github.com/vitessio/vitess/pull/17285)
 * Replace uses of os.Create with os2.Create within backup/restore workflows [#17648](https://github.com/vitessio/vitess/pull/17648)
 * Fix tablet selection in `vtctld BackupShard` [#18002](https://github.com/vitessio/vitess/pull/18002) 
#### Build/CI
 * Fix typos in MySQL 8.4 Dockerfile [#17601](https://github.com/vitessio/vitess/pull/17601)
 * Fix golang dependencies upgrade workflow [#17692](https://github.com/vitessio/vitess/pull/17692) 
#### CLI
 * Fix config file not found handling [#17578](https://github.com/vitessio/vitess/pull/17578) 
#### Cluster management
 * Fix Errant GTID detection logic in `SetReplicationSource` [#17031](https://github.com/vitessio/vitess/pull/17031)
 * Fix panic in vttablet when closing topo server twice [#17094](https://github.com/vitessio/vitess/pull/17094)
 * Fixes for avoidance of hosts taking backup in PRS & ERS [#17300](https://github.com/vitessio/vitess/pull/17300)
 * Clear `demotePrimaryStalled` field after the function ends [#17823](https://github.com/vitessio/vitess/pull/17823)
 * fix: fix incorrect nil return value [#17881](https://github.com/vitessio/vitess/pull/17881)
 * Fix semi-sync monitor bug of expecting incorrect number of fields [#17939](https://github.com/vitessio/vitess/pull/17939)
 * ValidateKeyspace: Add check for no shards  [#18055](https://github.com/vitessio/vitess/pull/18055)
 * [release-22.0] Add code to clear orphaned files for a deleted keyspace (#18098) [#18115](https://github.com/vitessio/vitess/pull/18115) 
#### Evalengine
 * Fix week number for date_format evalengine function [#17432](https://github.com/vitessio/vitess/pull/17432)
 * [release-20.0] Fix week number for date_format evalengine function (#17432) [#17453](https://github.com/vitessio/vitess/pull/17453)
 * [release-19.0] Fix week number for date_format evalengine function (#17432) [#17454](https://github.com/vitessio/vitess/pull/17454)
 * Implement temporal comparisons [#17826](https://github.com/vitessio/vitess/pull/17826) 
#### Java
 * Use proper `groupId` for mysql connector in java [#17540](https://github.com/vitessio/vitess/pull/17540) 
#### Online DDL
 * Online DDL: fix defer function, potential connection pool exhaustion [#17207](https://github.com/vitessio/vitess/pull/17207)
 * Propagate CallerID through vtctld OnlineDDL RPCs [#17935](https://github.com/vitessio/vitess/pull/17935) 
#### Query Serving
 * fix issue with json unmarshalling of operators with space in them [#16905](https://github.com/vitessio/vitess/pull/16905)
 * VTGate MoveTables Buffering: Fix panic when buffering is disabled [#16922](https://github.com/vitessio/vitess/pull/16922)
 * fixes bugs around expression precedence and LIKE [#16934](https://github.com/vitessio/vitess/pull/16934)
 * Fix `GetUnresolvedTransactions` command for Vtctld [#16961](https://github.com/vitessio/vitess/pull/16961)
 * bugfix: add HAVING columns inside derived tables [#16976](https://github.com/vitessio/vitess/pull/16976)
 * Handle encoding binary data separately [#16988](https://github.com/vitessio/vitess/pull/16988)
 * bugfix: treat EXPLAIN like SELECT [#17054](https://github.com/vitessio/vitess/pull/17054)
 * Fix: Update found rows pool transaction capacity dynamically via vttablet debug/env [#17055](https://github.com/vitessio/vitess/pull/17055)
 * Delegate Column Availability Checks to MySQL for Single-Route Queries [#17077](https://github.com/vitessio/vitess/pull/17077)
 * bugfix: Handle CTEs with columns named in the CTE def [#17179](https://github.com/vitessio/vitess/pull/17179)
 * Use proper keyspace when updating the query graph of a reference DML [#17226](https://github.com/vitessio/vitess/pull/17226)
 * Reference Table DML Join Fix [#17414](https://github.com/vitessio/vitess/pull/17414)
 * Fix Data race in semi-join [#17417](https://github.com/vitessio/vitess/pull/17417)
 * vexplain to protect the log fields from concurrent writes [#17460](https://github.com/vitessio/vitess/pull/17460)
 * Fix crash in the evalengine [#17487](https://github.com/vitessio/vitess/pull/17487)
 * Fix how we generate the query serving error documentation [#17516](https://github.com/vitessio/vitess/pull/17516)
 * fix: add missing MySQL 8.4 keywords [#17538](https://github.com/vitessio/vitess/pull/17538)
 * Always return a valid timezone in cursor [#17546](https://github.com/vitessio/vitess/pull/17546)
 * sizegen: do not ignore type aliases [#17556](https://github.com/vitessio/vitess/pull/17556)
 * `sendBinlogDumpCommand`: apply BinlogThroughGTID flag [#17580](https://github.com/vitessio/vitess/pull/17580)
 * `parseComBinlogDumpGTID`: GTID payload is always 5.6 flavor [#17605](https://github.com/vitessio/vitess/pull/17605)
 * Fix panic inside schema tracker [#17659](https://github.com/vitessio/vitess/pull/17659)
 * smartconnpool: do not allow connections to starve [#17675](https://github.com/vitessio/vitess/pull/17675)
 * Fail assignment expressions with the correct message [#17752](https://github.com/vitessio/vitess/pull/17752)
 * bugfix: support subqueries inside subqueries when merging [#17764](https://github.com/vitessio/vitess/pull/17764)
 * evalengine: normalize types during compilation [#17887](https://github.com/vitessio/vitess/pull/17887)
 * Fix DISTINCT on ENUM/SET columns by making enums/set hashable [#17936](https://github.com/vitessio/vitess/pull/17936)
 * go/vt/vtgate: take routing rules into account for traffic mirroring [#17953](https://github.com/vitessio/vitess/pull/17953)
 * Fix TEXT and BLOB column types not supporting length parameter in CREATE TABLE [#17973](https://github.com/vitessio/vitess/pull/17973)
 * Fix: Ensure Consistent Lookup Vindex Handles Duplicate Rows in Single Query [#17974](https://github.com/vitessio/vitess/pull/17974)
 * Set proper join vars type for the RHS field query in OLAP [#18028](https://github.com/vitessio/vitess/pull/18028)
 * Bugfix: Missing data when running vtgate outer joins [#18036](https://github.com/vitessio/vitess/pull/18036)
 * Fix `SHOW VITESS_SHARDS` bug that led to incomplete output [#18069](https://github.com/vitessio/vitess/pull/18069)
 * [release-22.0] bugfix: allow window functions when possible to push down (#18103) [#18106](https://github.com/vitessio/vitess/pull/18106) 
#### Schema Tracker
 * `engine.Reload()`: fix file size aggregation for partitioned tables [#18058](https://github.com/vitessio/vitess/pull/18058) 
#### Topology
 * Close zookeeper topo connection on disconnect [#17136](https://github.com/vitessio/vitess/pull/17136)
 * Limit rare race condition in `topo.Server` [#17165](https://github.com/vitessio/vitess/pull/17165) 
#### VReplication
 * VReplication: Support reversing read-only traffic in vtctldclient [#16920](https://github.com/vitessio/vitess/pull/16920)
 * fix: Infinite logs in case of non-existent stream logs [#17004](https://github.com/vitessio/vitess/pull/17004)
 * Use proper zstd decoder pool for binlog event compression handling [#17042](https://github.com/vitessio/vitess/pull/17042)
 * VReplication: Fixes for generated column handling [#17107](https://github.com/vitessio/vitess/pull/17107)
 * VReplication: Qualify and SQL escape tables in created AutoIncrement VSchema definitions [#17174](https://github.com/vitessio/vitess/pull/17174)
 * VReplication: recover from closed connection [#17249](https://github.com/vitessio/vitess/pull/17249)
 * LookupVindex: fix CLI to allow creating non-unique lookups with single column [#17301](https://github.com/vitessio/vitess/pull/17301)
 * SwitchTraffic: use separate context while canceling a migration [#17340](https://github.com/vitessio/vitess/pull/17340)
 * LookupVindex bug fix: Fix typos from PR 17301 [#17423](https://github.com/vitessio/vitess/pull/17423)
 * VReplication: Handle --vreplication_experimental_flags options properly for partial images [#17428](https://github.com/vitessio/vitess/pull/17428)
 * VDiff: Save lastpk value for source and target [#17493](https://github.com/vitessio/vitess/pull/17493)
 * Tablet picker: Handle the case where a primary tablet is not setup for a shard [#17573](https://github.com/vitessio/vitess/pull/17573)
 * VReplication: Address SwitchTraffic bugs around replication lag and cancel on error [#17616](https://github.com/vitessio/vitess/pull/17616)
 * VDiff: fix race when a vdiff resumes on vttablet restart [#17638](https://github.com/vitessio/vitess/pull/17638)
 * Atomic Copy: Fix panics when the copy phase starts in some clusters [#17717](https://github.com/vitessio/vitess/pull/17717)
 * VReplication Atomic Copy Workflows: fix bugs around concurrent inserts [#17772](https://github.com/vitessio/vitess/pull/17772)
 * Multi-tenant workflow SwitchWrites: Don't add denied tables on cancelMigration() [#17782](https://github.com/vitessio/vitess/pull/17782)
 * VReplication: Align VReplication and VTGate VStream Retry Logic [#17783](https://github.com/vitessio/vitess/pull/17783)
 * VDiff: Fix logic for reconciling extra rows [#17950](https://github.com/vitessio/vitess/pull/17950)
 * VDiff: Fix bug with handling tables with no pks but only a unique key. [#17968](https://github.com/vitessio/vitess/pull/17968)
 * [release-22.0] VStream API: Reset stopPos in catchup (#18119) [#18123](https://github.com/vitessio/vitess/pull/18123) 
#### VTAdmin
 * VTAdmin: Support for multiple DataTable accessing same URL [#17036](https://github.com/vitessio/vitess/pull/17036)
 * fix SchemaCacheConfig.DefaultExpiration [#17609](https://github.com/vitessio/vitess/pull/17609)
 * [VTAdmin] Insert into schema cache if exists already and not expired [#17908](https://github.com/vitessio/vitess/pull/17908)
 * vtadmin: allow cluster IDs with underscores by sanitizing resolver scheme [#17962](https://github.com/vitessio/vitess/pull/17962) 
#### VTCombo
 * Fix vtcombo parsing flags incorrectly [#17743](https://github.com/vitessio/vitess/pull/17743) 
#### VTGate
 * Fix deadlock between health check and topology watcher [#16995](https://github.com/vitessio/vitess/pull/16995)
 * Increase health check channel buffer [#17821](https://github.com/vitessio/vitess/pull/17821) 
#### VTTablet
 * Fix deadlock in messager and health streamer [#17230](https://github.com/vitessio/vitess/pull/17230)
 * Fix potential deadlock in health streamer [#17261](https://github.com/vitessio/vitess/pull/17261)
 * fix: race on storing schema engine last changed time [#17914](https://github.com/vitessio/vitess/pull/17914)
 * Fix data race in Schema Engine  [#17921](https://github.com/vitessio/vitess/pull/17921)
 * Schema Engine Reload: check for file and allocated size columns to be available  [#17967](https://github.com/vitessio/vitess/pull/17967)
 * Allow the use of db credentials when verifying unmanaged tablets config [#17984](https://github.com/vitessio/vitess/pull/17984) 
#### VTorc
 * Errant GTID detection fix for VTOrc [#17287](https://github.com/vitessio/vitess/pull/17287)
 * Use uint64 for binary log file position [#17472](https://github.com/vitessio/vitess/pull/17472)
 * `vtorc`: improve handling of partial cell topo results [#17718](https://github.com/vitessio/vitess/pull/17718)
 * Take into account the tablet type not being set while checking for primary tablet mismatch recovery [#18032](https://github.com/vitessio/vitess/pull/18032) 
#### schema management
 * `Applyschema` uses `ExecuteMultiFetchAsDba` [#17078](https://github.com/vitessio/vitess/pull/17078)
 * `schemadiff`: consistent key ordering in table diff [#17141](https://github.com/vitessio/vitess/pull/17141)
 * schemadiff: skip keys with expressions in Online DDL analysis [#17475](https://github.com/vitessio/vitess/pull/17475)
 * `schemadiff`: explicit `ALGORITHM=INSTANT` counts as instant-able [#17942](https://github.com/vitessio/vitess/pull/17942)
 * `schemadiff`: normalize `TEXT(length)` and `BLOB(length)` columns [#18030](https://github.com/vitessio/vitess/pull/18030) 
#### vtclient
 * Fix vtclient vtgate missing flags (specifically --grpc_*) [#17800](https://github.com/vitessio/vitess/pull/17800)
 * Fix how we generate `vtclient` CLI docs [#17897](https://github.com/vitessio/vitess/pull/17897) 
#### vtctldclient
 * Fix flag name typo from #16852 [#16921](https://github.com/vitessio/vitess/pull/16921)
 * Correct WriteTopologyPath args to match help text [#17735](https://github.com/vitessio/vitess/pull/17735)
 * Filter out tablets with unknown replication lag when electing a new primary [#18004](https://github.com/vitessio/vitess/pull/18004)
 * Fix `Reshard Cancel` behavior [#18020](https://github.com/vitessio/vitess/pull/18020)
 * [release-22.0] Fix deadlock in `ValidateKeyspace` (#18114) [#18117](https://github.com/vitessio/vitess/pull/18117) 
#### vttestserver
 * vttestserver should only pass a BindAddressGprc if one is passed in [#17457](https://github.com/vitessio/vitess/pull/17457)
### CI/Build 
#### Build/CI
 * Flakes: Address flakes in TestMoveTables* unit tests [#16942](https://github.com/vitessio/vitess/pull/16942)
 * CI: Correct etcd and ncurses package names for Ubuntu 24.04 [#16966](https://github.com/vitessio/vitess/pull/16966)
 * Flakes: Address TestServerStats flakiness [#16991](https://github.com/vitessio/vitess/pull/16991)
 * Specify Ubuntu 24.04 for all jobs [#17278](https://github.com/vitessio/vitess/pull/17278)
 * use newer versions of actions in scorecard workflow [#17373](https://github.com/vitessio/vitess/pull/17373)
 * split upgrade downgrade queries test to 2 CI workflows [#17464](https://github.com/vitessio/vitess/pull/17464)
 * CI: fix to Online DDL flow test, do not t.Log from within a goroutine [#17496](https://github.com/vitessio/vitess/pull/17496)
 * Revert using the oracle runners [#18015](https://github.com/vitessio/vitess/pull/18015) 
#### Cluster management
 * Fix flakiness in `TestListenerShutdown` [#17024](https://github.com/vitessio/vitess/pull/17024) 
#### Docker
 * Build `vttestserver` in GHA and send Slack message on Docker Build failure [#16963](https://github.com/vitessio/vitess/pull/16963)
 * feat: add mysql 8.4 docker images [#17529](https://github.com/vitessio/vitess/pull/17529) 
#### General
 * [main] Upgrade the Golang version to `go1.23.3` [#17199](https://github.com/vitessio/vitess/pull/17199)
 * [main] Bump go version to 1.23.4 [#17335](https://github.com/vitessio/vitess/pull/17335)
 * [main] Upgrade the Golang version to `go1.23.5` [#17563](https://github.com/vitessio/vitess/pull/17563)
 * Add Shlomi & Frances to VReplication CODEOWNERS [#17878](https://github.com/vitessio/vitess/pull/17878)
 * [main] Upgrade the Golang version to `go1.24.1` [#17902](https://github.com/vitessio/vitess/pull/17902)
 * Fix `sync` import error in keyspace_shard_discovery.go [#17972](https://github.com/vitessio/vitess/pull/17972)
 * [release-22.0] Upgrade the Golang version to `go1.24.2` [#18097](https://github.com/vitessio/vitess/pull/18097) 
#### Query Serving
 * Flakes: Setup new fake server if it has gone away [#17023](https://github.com/vitessio/vitess/pull/17023) 
#### Throttler
 * Fix flakiness in throttler's `TestDormant` [#17940](https://github.com/vitessio/vitess/pull/17940) 
#### VReplication
 * VReplication: Restore previous minimal e2e test behavior [#17016](https://github.com/vitessio/vitess/pull/17016) 
#### VTAdmin
 * Update to latest Nodejs LTS and NPM [#17606](https://github.com/vitessio/vitess/pull/17606)
### Dependencies 
#### Build/CI
 * Bump golang.org/x/crypto from 0.29.0 to 0.31.0 [#17376](https://github.com/vitessio/vitess/pull/17376)
 * Update bundled MySQL version to 8.0.40 [#17552](https://github.com/vitessio/vitess/pull/17552)
 * Update dependencies manually [#17691](https://github.com/vitessio/vitess/pull/17691)
 * Update to Go 1.24.0 [#17790](https://github.com/vitessio/vitess/pull/17790)
 * Bump golang.org/x/net from 0.34.0 to 0.36.0 [#17958](https://github.com/vitessio/vitess/pull/17958) 
#### General
 * go mod update [#17227](https://github.com/vitessio/vitess/pull/17227)
 * Bump golang.org/x/net from 0.31.0 to 0.33.0 [#17416](https://github.com/vitessio/vitess/pull/17416)
 * CVE Fix: Update glog to v1.2.4 [#17524](https://github.com/vitessio/vitess/pull/17524)
 * Upgrade the Golang Dependencies [#17695](https://github.com/vitessio/vitess/pull/17695)
 * Upgrade the Golang Dependencies [#17803](https://github.com/vitessio/vitess/pull/17803) 
#### Java
 * Bump commons-io:commons-io from 2.7 to 2.14.0 in /java [#16889](https://github.com/vitessio/vitess/pull/16889)
 * java package updates for grpc and protobuf and release plugins [#17100](https://github.com/vitessio/vitess/pull/17100)
 * [Java]: Bump mysql-connector-java version from 8.0.33 to mysql-connector-j 8.4.0 [#17522](https://github.com/vitessio/vitess/pull/17522)
 * Bump io.netty:netty-handler from 4.1.110.Final to 4.1.118.Final in /java [#17730](https://github.com/vitessio/vitess/pull/17730) 
#### Operator
 * Upgrade `sigs.k8s.io/controller-tools` and `KUBE_VERSION` [#17889](https://github.com/vitessio/vitess/pull/17889) 
#### VTAdmin
 * VTAdmin: Upgrade msw package to address security vuln [#17108](https://github.com/vitessio/vitess/pull/17108)
 * Bump nanoid from 3.3.7 to 3.3.8 in /web/vtadmin [#17375](https://github.com/vitessio/vitess/pull/17375)
### Documentation 
#### Documentation
 * Add missing changelog for PR #16852 [#17002](https://github.com/vitessio/vitess/pull/17002) 
#### General
 * Reorganize the `v22.0.0` release notes [#18052](https://github.com/vitessio/vitess/pull/18052) 
#### Governance
 * Move Andrew Mason to emeritus [#17745](https://github.com/vitessio/vitess/pull/17745)
 * Get some CLOMonitor checks passing [#17773](https://github.com/vitessio/vitess/pull/17773)
 * Update email address for mattlord [#17961](https://github.com/vitessio/vitess/pull/17961) 
#### Query Serving
 * Update Atomic Distributed Transaction Design [#17005](https://github.com/vitessio/vitess/pull/17005)
### Enhancement 
#### Authn/z
 * Add support for hashed caching sha2 passwords [#17948](https://github.com/vitessio/vitess/pull/17948) 
#### Backup and Restore
 * add S3 minimum part size defined by the user [#17171](https://github.com/vitessio/vitess/pull/17171)
 * Implement a per-file retry mechanism in the `BuiltinBackupEngine` [#17271](https://github.com/vitessio/vitess/pull/17271)
 * Fail VTBackup early when replication or MySQL is failing [#17356](https://github.com/vitessio/vitess/pull/17356)
 * Allow for specifying a specific MySQL shutdown timeout [#17923](https://github.com/vitessio/vitess/pull/17923) 
#### Build/CI
 * Change upgrade test to still use the older version of tests [#16937](https://github.com/vitessio/vitess/pull/16937) 
#### Cluster management
 * Improve errant GTID detection in ERS to handle more cases. [#16926](https://github.com/vitessio/vitess/pull/16926)
 * PRS and ERS don't promote replicas taking backups [#16997](https://github.com/vitessio/vitess/pull/16997)
 * go/vt/mysqlctl: add postflight_mysqld_start and preflight_mysqld_shutdown hooks [#17652](https://github.com/vitessio/vitess/pull/17652)
 * Add semi-sync monitor to unblock primaries blocked on semi-sync ACKs [#17763](https://github.com/vitessio/vitess/pull/17763)
 * [Feature]: Sort results of getTablets for consistency in output/readability [#17771](https://github.com/vitessio/vitess/pull/17771)
 * Change healthcheck to only reload specific keyspace shard tablets [#17872](https://github.com/vitessio/vitess/pull/17872)
 * Rebuild `SrvKeyspace` and `SrvVSchema` during vtgate init if required [#17915](https://github.com/vitessio/vitess/pull/17915) 
#### Documentation
 * CobraDocs: Remove commit hash from docs. Fix issue with workdir replacement [#17392](https://github.com/vitessio/vitess/pull/17392)
 * Fix `vtbackup` help output [#17892](https://github.com/vitessio/vitess/pull/17892) 
#### Evalengine
 * Improved Compatibility Around LAST_INSERT_ID - evalengine [#17409](https://github.com/vitessio/vitess/pull/17409) 
#### Examples
 * Added the initial script for the 40x example for backup and restore  [#16893](https://github.com/vitessio/vitess/pull/16893)
 * Feature Request: Change varbinary to varchar in schemas used in examples folder [#17318](https://github.com/vitessio/vitess/pull/17318) 
#### General
 * MySQL errors: `ERWrongParamcountToNativeFct` [#16914](https://github.com/vitessio/vitess/pull/16914)
 * Standardize topo flags to use hyphen-based naming [#17975](https://github.com/vitessio/vitess/pull/17975) 
#### Observability
 * Improve vtgate logging for buffering [#17654](https://github.com/vitessio/vitess/pull/17654)
 * Add more vstream metrics for vstream manager [#17858](https://github.com/vitessio/vitess/pull/17858) 
#### Online DDL
 * Online DDL: detect `vreplication` errors via `vreplication_log` history [#16925](https://github.com/vitessio/vitess/pull/16925)
 * Online DDL: better error messages in cut-over phase [#17052](https://github.com/vitessio/vitess/pull/17052)
 * Improve Schema Engine's TablesWithSize80 query [#17066](https://github.com/vitessio/vitess/pull/17066)
 * Online DDL: dynamic cut-over threshold via `ALTER VITESS_MIGRATION ... CUTOVER_THRESHOLD ...` command [#17126](https://github.com/vitessio/vitess/pull/17126)
 * Online DDL: `--singleton-table` DDL strategy flag [#17169](https://github.com/vitessio/vitess/pull/17169)
 * Online DDL: `ANALYZE` the shadow table before cut-over [#17201](https://github.com/vitessio/vitess/pull/17201)
 * Online DDL: publish `vreplication_lag_seconds` from vreplication progress [#17263](https://github.com/vitessio/vitess/pull/17263)
 * Online DDL `--analyze-table`: use non-local `ANALYZE TABLE` [#17462](https://github.com/vitessio/vitess/pull/17462)
 * Online DDL forced cut-over: terminate transactions holding  metadata locks on table [#17535](https://github.com/vitessio/vitess/pull/17535)
 * Online DDL: consider UUID throttle ratios in `user_throttle_ratio` [#17850](https://github.com/vitessio/vitess/pull/17850) 
#### Operator
 * Update example operator.yaml to support vtgate HPA [#16805](https://github.com/vitessio/vitess/pull/16805)
 * `operator.yaml`: pass on the `sidecarDbName` when creating a keyspace [#17739](https://github.com/vitessio/vitess/pull/17739)
 * Use `vtbackup` in scheduled backups [#17869](https://github.com/vitessio/vitess/pull/17869)
 * Support multiple namespaces with the vitess-operator [#17928](https://github.com/vitessio/vitess/pull/17928)
 * Add deployment strategy to vtgate in `operator.yaml` [#18063](https://github.com/vitessio/vitess/pull/18063) 
#### Query Serving
 * Support settings changes with atomic transactions [#16974](https://github.com/vitessio/vitess/pull/16974)
 * Fix to prevent stopping buffering prematurely [#17013](https://github.com/vitessio/vitess/pull/17013)
 * Atomic Transaction `StartCommit` api to return the commit state [#17116](https://github.com/vitessio/vitess/pull/17116)
 * --consolidator-query-waiter-cap to set the max number of waiter for consolidated query [#17244](https://github.com/vitessio/vitess/pull/17244)
 * Handle MySQL handler error codes [#17252](https://github.com/vitessio/vitess/pull/17252)
 * Track shard session affecting change inside the transaction  [#17266](https://github.com/vitessio/vitess/pull/17266)
 * Allow for dynamic configuration of DDL settings [#17328](https://github.com/vitessio/vitess/pull/17328)
 * Allow dynamic configuration of vschema acl [#17333](https://github.com/vitessio/vitess/pull/17333)
 * Range Query Optimization (For sequential Vindex types) [#17342](https://github.com/vitessio/vitess/pull/17342)
 * Add missing tables to globally routed list in schema tracker only if they are not already present in a VSchema [#17371](https://github.com/vitessio/vitess/pull/17371)
 * Add linearizable support to SQL VSchema management [#17401](https://github.com/vitessio/vitess/pull/17401)
 * Improved Compatibility Around LAST_INSERT_ID [#17408](https://github.com/vitessio/vitess/pull/17408)
 * Make transaction mode a dynamic configuration [#17419](https://github.com/vitessio/vitess/pull/17419)
 * Views: VTGate changes for seamless integration [#17439](https://github.com/vitessio/vitess/pull/17439)
 * VALUES statement AST and parsing [#17500](https://github.com/vitessio/vitess/pull/17500)
 * Remove shard targeted keyspace from database qualifier on the query [#17503](https://github.com/vitessio/vitess/pull/17503)
 * Add support for global routing in describe statement [#17510](https://github.com/vitessio/vitess/pull/17510)
 * Engine primitive for join-values [#17518](https://github.com/vitessio/vitess/pull/17518)
 * Improve sizegen to handle more types [#17583](https://github.com/vitessio/vitess/pull/17583)
 * Views global routing [#17633](https://github.com/vitessio/vitess/pull/17633)
 * Handle Percona compressed column extension [#17660](https://github.com/vitessio/vitess/pull/17660)
 * Wrap fatal TX errors in a new `vterrors` code [#17669](https://github.com/vitessio/vitess/pull/17669)
 * Parse SRID attribute for virtual columns [#17680](https://github.com/vitessio/vitess/pull/17680)
 * Vtgate metrics: Added plan and query type metrics [#17727](https://github.com/vitessio/vitess/pull/17727)
 * Add a Visitable interface to support AST nodes declared outside sqlparser [#17875](https://github.com/vitessio/vitess/pull/17875)
 * Unsharded support for `CREATE PROCEDURE` and `DROP PROCEDURE` statement  [#17946](https://github.com/vitessio/vitess/pull/17946)
 * Ensure DML Queries with Large Input Rows Succeed When Passthrough DML is Enabled [#17949](https://github.com/vitessio/vitess/pull/17949)
 * Pass through predicates when trying to merge apply join. [#17955](https://github.com/vitessio/vitess/pull/17955)
 * Planner: Allow Arbitrary Expression Pushdown in UNION Queries [#18000](https://github.com/vitessio/vitess/pull/18000)
 * Make it possible to use paths with Visitable nodes [#18053](https://github.com/vitessio/vitess/pull/18053) 
#### Throttler
 * Tablet throttler: read and use MySQL host metrics [#16904](https://github.com/vitessio/vitess/pull/16904)
 * Multi-metrics throttler: post v21 deprecations and changes [#16915](https://github.com/vitessio/vitess/pull/16915)
 * Multi-metrics throttler: adding InnoDB `history_list_length` metric [#17262](https://github.com/vitessio/vitess/pull/17262) 
#### Topology
 * Avoid flaky topo concurrency test [#17407](https://github.com/vitessio/vitess/pull/17407) 
#### VReplication
 * fix: Avoid creation of workflows with non-empty tables in target keyspace [#16874](https://github.com/vitessio/vitess/pull/16874)
 * VReplication: Properly support cancel and delete for multi-tenant MoveTables [#16906](https://github.com/vitessio/vitess/pull/16906)
 * VDiff: Also generate default new UUID in VtctldServer VDiffCreate [#17003](https://github.com/vitessio/vitess/pull/17003)
 * VReplication: Relax restrictions on Cancel and ReverseTraffic when writes not involved [#17128](https://github.com/vitessio/vitess/pull/17128)
 * Binlog: Improve ZstdInMemoryDecompressorMaxSize management [#17220](https://github.com/vitessio/vitess/pull/17220)
 * VReplication: Support for filter using `IN` operator [#17296](https://github.com/vitessio/vitess/pull/17296)
 * VReplication: Support binlog_row_value_options=PARTIAL_JSON [#17345](https://github.com/vitessio/vitess/pull/17345)
 * Export `BuildSummary` func for VDiff show [#17413](https://github.com/vitessio/vitess/pull/17413)
 * VReplication: Improve error handling in VTGate VStreams [#17558](https://github.com/vitessio/vitess/pull/17558)
 * VReplication Workflow command: Allow stop/start on specific shards [#17581](https://github.com/vitessio/vitess/pull/17581)
 * VReplication: Support for `BETWEEN`/`NOT BETWEEN` filter in VStream [#17721](https://github.com/vitessio/vitess/pull/17721)
 * VReplication: Support ignoring the source Keyspace on MoveTables cancel/complete [#17729](https://github.com/vitessio/vitess/pull/17729)
 * VReplication: Support excluding lagging tablets and use this in vstream manager [#17835](https://github.com/vitessio/vitess/pull/17835)
 * VStream: Add `IS NULL` filter support [#17848](https://github.com/vitessio/vitess/pull/17848)
 * VTTablet schema reload: include stats only on periodic reload for all flavors. Default to "base table" query for the "with sizes" query for 5.7 only [#17855](https://github.com/vitessio/vitess/pull/17855)
 * Refactor vstream_dynamic_packet_size and vstream_packet_size flags to use dashes [#17964](https://github.com/vitessio/vitess/pull/17964)
 * VReplication Workflows: make defer-secondary-keys true by default [#18006](https://github.com/vitessio/vitess/pull/18006) 
#### VTAdmin
 * VTAdmin: Support for Workflow Actions [#16816](https://github.com/vitessio/vitess/pull/16816)
 * VTAdmin: Support for conclude txn and abandon age param [#16834](https://github.com/vitessio/vitess/pull/16834)
 * VTAdmin(web): Add refresh compatibility to workflow screen (all tabs) [#16865](https://github.com/vitessio/vitess/pull/16865)
 * VTAdmin: Add support for Create Reshard [#16903](https://github.com/vitessio/vitess/pull/16903)
 * VTAdmin: Support for `Materialize` Create [#16941](https://github.com/vitessio/vitess/pull/16941)
 * VTAdmin: Support for `VDiff` create and show last [#16943](https://github.com/vitessio/vitess/pull/16943)
 * VTAdmin: Support for schema migrations view/create [#17134](https://github.com/vitessio/vitess/pull/17134)
 * Add GetTransactionInfo in VTadmin API and page [#17142](https://github.com/vitessio/vitess/pull/17142)
 * VTAdmin(web): Add simpler topology tree structure [#17245](https://github.com/vitessio/vitess/pull/17245)
 * vtadmin: enable sorting in all tables [#17468](https://github.com/vitessio/vitess/pull/17468)
 * VTAdmin to use VTGate's vexplain [#17508](https://github.com/vitessio/vitess/pull/17508)
 * VTAdmin: Add advanced workflow `switchtraffic` options [#17658](https://github.com/vitessio/vitess/pull/17658)
 * VTAdmin: update logo and favicon for the new Vitess logos [#17715](https://github.com/vitessio/vitess/pull/17715)
 * vtadmin: display transaction participant list as comma separated [#17986](https://github.com/vitessio/vitess/pull/17986) 
#### VTGate
 * Store the health check subscriber name to improve error message [#17863](https://github.com/vitessio/vitess/pull/17863) 
#### VTTablet
 * vttablet debugenv changes [#17161](https://github.com/vitessio/vitess/pull/17161)
 * Fail loading an ACL config if the provided file is empty and enforceTableACLConfig is true [#17274](https://github.com/vitessio/vitess/pull/17274)
 * Refactor Disk Stall implementation and mark tablet not serving if disk is stalled [#17624](https://github.com/vitessio/vitess/pull/17624) 
#### VTorc
 * Improve efficiency of `vtorc` topo calls  [#17071](https://github.com/vitessio/vitess/pull/17071)
 * `vtorc`: require topo for `Healthy: true` in `/debug/health` [#17129](https://github.com/vitessio/vitess/pull/17129)
 * `vtorc`: make SQL formatting consistent [#17154](https://github.com/vitessio/vitess/pull/17154)
 * Fix errant GTID detection in VTOrc to also work with a replica not connected to any primary [#17267](https://github.com/vitessio/vitess/pull/17267)
 * Add StalledDiskPrimary analysis and recovery to vtorc [#17470](https://github.com/vitessio/vitess/pull/17470)
 * Use prefix in all vtorc check and recover logs [#17526](https://github.com/vitessio/vitess/pull/17526)
 * Add stats for shards watched by VTOrc, purge stale shards [#17815](https://github.com/vitessio/vitess/pull/17815)
 * Add VTOrc recovery for mismatch in tablet type [#17870](https://github.com/vitessio/vitess/pull/17870)
 * `vtorc`: add tablets watched stats [#17911](https://github.com/vitessio/vitess/pull/17911)
 * `vtorc`: add stats to shard locks and instance discovery [#17977](https://github.com/vitessio/vitess/pull/17977) 
#### schema management
 * Only run sidecardb change detection on serving primary tablets [#17051](https://github.com/vitessio/vitess/pull/17051)
 * `engine.Reload()`: read InnoDB tables sizes including `FULLTEXT` index volume [#17118](https://github.com/vitessio/vitess/pull/17118)
 * `schemadiff`: validate uniqueness of `CHECK` and of `FOREIGN KEY` constraints [#17627](https://github.com/vitessio/vitess/pull/17627)
 * `schemadiff` textual annotation fix + tests [#17630](https://github.com/vitessio/vitess/pull/17630)
 * Supporting `InnoDBParallelReadThreadsCapability` [#17689](https://github.com/vitessio/vitess/pull/17689)
 * Ignore execution time errors for schemadiff view analysis [#17704](https://github.com/vitessio/vitess/pull/17704)
 * schemadiff: `ViewDependencyUnresolvedError` lists missing referenced entities [#17711](https://github.com/vitessio/vitess/pull/17711) 
#### vtctldclient
 * Add RPC to read the statements to be executed in an unresolved prepared transaction [#17131](https://github.com/vitessio/vitess/pull/17131) 
#### vttestserver
 * Add support for receiving `grpc_bind_adress` on `vttestserver` [#17231](https://github.com/vitessio/vitess/pull/17231) 
#### web UI
 * VTAdmin(web): Better visualization for JSON screens [#17459](https://github.com/vitessio/vitess/pull/17459)
 * VTAdmin: Show Hostname Alongside Tablet ID on tablet selection drop-downs [#17982](https://github.com/vitessio/vitess/pull/17982)
### Feature 
#### Cluster management
 * Tool to determine mapping from vindex and value to shard [#17290](https://github.com/vitessio/vitess/pull/17290) 
#### Query Serving
 * Add savepoint support to atomic distributed transaction [#16863](https://github.com/vitessio/vitess/pull/16863)
 * feat: add metrics for atomic distributed transactions [#16939](https://github.com/vitessio/vitess/pull/16939)
 * connection pool: max idle connections implementation [#17443](https://github.com/vitessio/vitess/pull/17443) 
#### VReplication
 * LookupVindex: Implement `internalize` command for lookup vindexes [#17429](https://github.com/vitessio/vitess/pull/17429)
 * VReplication: Support passing VStream filters down to MySQL [#17677](https://github.com/vitessio/vitess/pull/17677)
 * VReplication SwitchTraffic: for a dry run check if sequences need to be updated  [#17842](https://github.com/vitessio/vitess/pull/17842) 
#### VTGate
 * Add option to log error queries only [#17554](https://github.com/vitessio/vitess/pull/17554) 
#### VTTablet
 * Add a way to know if DemotePrimary is blocked and send it in the health stream [#17289](https://github.com/vitessio/vitess/pull/17289)
 * Add index and table metrics to vttablet [#17570](https://github.com/vitessio/vitess/pull/17570) 
#### VTorc
 * Improve VTOrc config handling to support dynamic variables [#17218](https://github.com/vitessio/vitess/pull/17218)
 * Support KeyRange in `--clusters_to_watch` flag [#17604](https://github.com/vitessio/vitess/pull/17604)
### Internal Cleanup 
#### ACL
 * Remove unused code from go/acl [#17741](https://github.com/vitessio/vitess/pull/17741) 
#### Backup and Restore
 * Remove binlog-server point in time recoveries code & tests [#17361](https://github.com/vitessio/vitess/pull/17361) 
#### Build/CI
 * Add release-21.0 to the Golang Upgrade workflow [#16916](https://github.com/vitessio/vitess/pull/16916)
 * Change the name of the vitess-tester repository [#16917](https://github.com/vitessio/vitess/pull/16917)
 * Security improvements to GitHub Actions [#17520](https://github.com/vitessio/vitess/pull/17520)
 * Rename docker clusters for more clarity and group all java test together [#17542](https://github.com/vitessio/vitess/pull/17542)
 * Upgrade default etcd version to 3.5.17 [#17653](https://github.com/vitessio/vitess/pull/17653)
 * test: replace `t.Errorf` and `t.Fatalf` with `assert` and `require` [#17720](https://github.com/vitessio/vitess/pull/17720)
 * Simply changing GH Actions runner [#17788](https://github.com/vitessio/vitess/pull/17788)
 * nit: move new `const`s to own section [#17802](https://github.com/vitessio/vitess/pull/17802)
 * Add support for `GOPRIVATE` in CI templates [#17806](https://github.com/vitessio/vitess/pull/17806)
 * Allow build git envs to be set in `docker/lite` [#17827](https://github.com/vitessio/vitess/pull/17827)
 * Set 16 core runners to oci gh arc runners [#17879](https://github.com/vitessio/vitess/pull/17879)
 * Add @frouioui to CODEOWNERS for backups [#17927](https://github.com/vitessio/vitess/pull/17927)
 * Migrate all but one of the other jobs to use cncf-hosted gha runners [#17943](https://github.com/vitessio/vitess/pull/17943)
 * re-replace most runners with the cncf-hosted runners [#17993](https://github.com/vitessio/vitess/pull/17993) 
#### CLI
 * Remove deprecated items before `v22.0.0` [#17894](https://github.com/vitessio/vitess/pull/17894) 
#### Cluster management
 * Remove legacy way of considering SQL thread being stopped on a replica as unreachable [#17918](https://github.com/vitessio/vitess/pull/17918) 
#### General
 * sqltypes: add ToTime conversion funcs [#17178](https://github.com/vitessio/vitess/pull/17178)
 * Fix integer parsing logic [#17650](https://github.com/vitessio/vitess/pull/17650)
 * Remove unused code from go/mysql package [#17740](https://github.com/vitessio/vitess/pull/17740)
 * Remove bits of unused code across various packages [#17785](https://github.com/vitessio/vitess/pull/17785)
 * sqltypes: move datetime handling to Value method [#17929](https://github.com/vitessio/vitess/pull/17929)
 * Remove direct usage of archived github.com/pkg/errors (#17933) [#17934](https://github.com/vitessio/vitess/pull/17934)
 * Update to golangci-lint 2 [#18050](https://github.com/vitessio/vitess/pull/18050) 
#### Java
 * Update the how to release java docs [#17603](https://github.com/vitessio/vitess/pull/17603) 
#### Observability
 * Remove unused go/stats code [#17742](https://github.com/vitessio/vitess/pull/17742) 
#### Online DDL
 * Online DDL: removal of `gh-ost` and `pt-osc` strategies [#17626](https://github.com/vitessio/vitess/pull/17626) 
#### Query Serving
 * Parse enum/set values with `sqlparser` [#17133](https://github.com/vitessio/vitess/pull/17133)
 * Deprecate `twopc_enable` flag and change input type for `twopc_abandon_age` to time.Duration [#17279](https://github.com/vitessio/vitess/pull/17279)
 * Improve logging in buffering [#17294](https://github.com/vitessio/vitess/pull/17294)
 * refactor: rename DefaultKeyspace() to CurrentKeyspace() [#17303](https://github.com/vitessio/vitess/pull/17303)
 * refactor: VTGate executor with a runnable context package [#17305](https://github.com/vitessio/vitess/pull/17305)
 * Clean up duplicate datetime parsing code [#17582](https://github.com/vitessio/vitess/pull/17582)
 * Make sure no AST types are bare slices [#17674](https://github.com/vitessio/vitess/pull/17674)
 * Small planner refactoring [#17851](https://github.com/vitessio/vitess/pull/17851)
 * Refactor Join Predicate Handling in Planner [#17877](https://github.com/vitessio/vitess/pull/17877)
 * VTGate: Session in StreamExecute response as default [#17907](https://github.com/vitessio/vitess/pull/17907)
 * Refactor transitive closure handling [#17978](https://github.com/vitessio/vitess/pull/17978) 
#### TabletManager
 * Use non-deprecated flags to skip replication [#17400](https://github.com/vitessio/vitess/pull/17400)
 * Add forward compatibility for caching_sha2_password and replication [#18033](https://github.com/vitessio/vitess/pull/18033) 
#### Throttler
 * throttler: formal gRPC calls in endtoend tests, removing HTTP API calls [#16530](https://github.com/vitessio/vitess/pull/16530)
 * e2e framework change to find empty throttler config [#17228](https://github.com/vitessio/vitess/pull/17228)
 * Multi-metrics throttler post v21 cleanup: remove unthrottled entry from topo [#17283](https://github.com/vitessio/vitess/pull/17283) 
#### VReplication
 * refac: Remove duplicate `forAllShards` methods from `vt/vtctl/workflow` [#17025](https://github.com/vitessio/vitess/pull/17025)
 * VDiff: Comment the VDiffCreate proto msg and set reasonable server side defaults [#17026](https://github.com/vitessio/vitess/pull/17026)
 * refac: Refactor `Server.GetWorkflows()` [#17092](https://github.com/vitessio/vitess/pull/17092)
 * Refactor `Server.LookupVindexCreate` [#17242](https://github.com/vitessio/vitess/pull/17242)
 * Remove unused code for old START_GTID logic [#17265](https://github.com/vitessio/vitess/pull/17265)
 * VDiff: Remove extra % sign in vdiff text report template [#17568](https://github.com/vitessio/vitess/pull/17568)
 * VReplication: CODEOWNERS and unit test housekeeping [#17646](https://github.com/vitessio/vitess/pull/17646)
 * Always make sure to escape all strings [#17649](https://github.com/vitessio/vitess/pull/17649) 
#### VTGate
 * Change keys of the discovery flags such that they conform to the convention [#17430](https://github.com/vitessio/vitess/pull/17430) 
#### VTTablet
 * Remove deprecated `--disable_active_reparents` flag [#17919](https://github.com/vitessio/vitess/pull/17919) 
#### VTorc
 * `vtorc`: cleanup init db handling [#17198](https://github.com/vitessio/vitess/pull/17198)
 * Cleanup legacy MariaDB bits from vtorc [#17415](https://github.com/vitessio/vitess/pull/17415)
 * Make all durability policy names constants [#17448](https://github.com/vitessio/vitess/pull/17448)
 * Remove unused code in discovery queue creation [#17515](https://github.com/vitessio/vitess/pull/17515)
 * `vtorc`: use `golang.org/x/sync/semaphore`, add flag for db concurrency [#17837](https://github.com/vitessio/vitess/pull/17837)
 * `vtorc`: use `errgroup` in keyspace/shard discovery [#17857](https://github.com/vitessio/vitess/pull/17857)
 * `vtorc`: add stats for discovery workers [#17937](https://github.com/vitessio/vitess/pull/17937) 
#### vtctldclient
 * Move all e2e tests to vtctldclient [#17441](https://github.com/vitessio/vitess/pull/17441)
 * Add vtctldclient missing cmds and remove remaining vtctl[client] usage in e2e tests [#17442](https://github.com/vitessio/vitess/pull/17442)
### Performance 
#### Backup and Restore
 * go/stats: improve performance of safeJoinLabels [#16953](https://github.com/vitessio/vitess/pull/16953) 
#### Performance
 * `vtorc`: fetch all tablets from cells once + filter during refresh [#17388](https://github.com/vitessio/vitess/pull/17388)
 * smartconnpool: Better handling for idle expiration [#17757](https://github.com/vitessio/vitess/pull/17757) 
#### Query Serving
 * grpc: upgrade to 1.66.2 and use Codec v2 [#16790](https://github.com/vitessio/vitess/pull/16790)
 * Benchmark Prepared statement and expected Improvement [#17449](https://github.com/vitessio/vitess/pull/17449)
 * Reduce VTGate Normalizer multiple AST walks  [#17619](https://github.com/vitessio/vitess/pull/17619)
 * Optimise AST rewriting [#17623](https://github.com/vitessio/vitess/pull/17623)
 * Faster dependency copying [#17708](https://github.com/vitessio/vitess/pull/17708)
 * Use ast-paths for subquery planning to improve performance [#17738](https://github.com/vitessio/vitess/pull/17738)
 * Faster Prepared Statement Execution by Using Raw SQL for Caching [#17777](https://github.com/vitessio/vitess/pull/17777)
 * pool: reopen connection closed by idle timeout [#17818](https://github.com/vitessio/vitess/pull/17818)
 * Fix: Separate Lock for Keyspace to Update Controller Mapping in Schema Tracking [#17873](https://github.com/vitessio/vitess/pull/17873)
 * Implement Deferred Optimization for Prepared Statements [#17992](https://github.com/vitessio/vitess/pull/17992)
 * Only sort when receiving results from multiple shards [#17998](https://github.com/vitessio/vitess/pull/17998) 
#### Throttler
 * Throttler: reduce regexp/string allocations by pre-computing pascal case [#17817](https://github.com/vitessio/vitess/pull/17817) 
#### VReplication
 * VReplication: Optimize replication on target tablets [#17166](https://github.com/vitessio/vitess/pull/17166)
 * VStreamer: For larger compressed transaction payloads, stream the internal contents [#17239](https://github.com/vitessio/vitess/pull/17239)
 * VReplication: Disable /debug/vrlog by default [#17832](https://github.com/vitessio/vitess/pull/17832) 
#### VTGate
 * Ensure all topo read calls consider `--topo_read_concurrency` [#17276](https://github.com/vitessio/vitess/pull/17276) 
#### VTTablet
 * Skip Field Query for Views During Schema Refresh to Improve Reload Time [#18066](https://github.com/vitessio/vitess/pull/18066) 
#### VTorc
 * Move to native sqlite3 queries [#17124](https://github.com/vitessio/vitess/pull/17124)
 * `vtorc`: cleanup discover queue, add concurrency flag [#17825](https://github.com/vitessio/vitess/pull/17825)
 * `vtorc`: remove duplicate instance read from backend [#17834](https://github.com/vitessio/vitess/pull/17834)
 * `vtorc`: add index for `inst.ReadInstanceClusterAttributes` table scan [#17866](https://github.com/vitessio/vitess/pull/17866)
 * `vtorc`: skip unnecessary backend read in `logic.LockShard(...)` [#17900](https://github.com/vitessio/vitess/pull/17900)
### Regression 
#### Backup and Restore
 * Fix unreachable errors when taking a backup [#17062](https://github.com/vitessio/vitess/pull/17062) 
#### Java
 * [Java] Fix dependency issues in Java package  [#17481](https://github.com/vitessio/vitess/pull/17481) 
#### Query Serving
 * Add support for `MultiEqual` opcode for lookup vindexes. [#16975](https://github.com/vitessio/vitess/pull/16975)
 * fix: route engine to handle column truncation for execute after lookup [#16981](https://github.com/vitessio/vitess/pull/16981)
 * Fix release 18 again [#17069](https://github.com/vitessio/vitess/pull/17069)
 * Fix a potential connection pool leak. [#17807](https://github.com/vitessio/vitess/pull/17807) 
#### VTTablet
 * fix: App and Dba Pool metrics [#18048](https://github.com/vitessio/vitess/pull/18048)
### Release 
#### Build/CI
 * Fix the release workflow [#16964](https://github.com/vitessio/vitess/pull/16964)
 * v19 EOL: Remove v19 from Golang Update Version CI workflow [#17932](https://github.com/vitessio/vitess/pull/17932) 
#### Documentation
 * Update the EOL documentation [#17215](https://github.com/vitessio/vitess/pull/17215) 
#### General
 * Bump to `v22.0.0-SNAPSHOT` after the `v21.0.0-RC1` release [#16913](https://github.com/vitessio/vitess/pull/16913)
 * [main] Copy `v21.0.0-RC1` release notes [#16954](https://github.com/vitessio/vitess/pull/16954)
 * [main] Copy `v21.0.0-RC2` release notes [#17048](https://github.com/vitessio/vitess/pull/17048)
 * [main] Copy `v21.0.0` release notes [#17097](https://github.com/vitessio/vitess/pull/17097)
 * [main] Copy `v18.0.8` release notes [#17155](https://github.com/vitessio/vitess/pull/17155)
 * [main] Copy `v19.0.7` release notes [#17157](https://github.com/vitessio/vitess/pull/17157)
 * [main] Copy `v20.0.3` release notes [#17159](https://github.com/vitessio/vitess/pull/17159)
 * [main] Copy `v19.0.8` release notes [#17320](https://github.com/vitessio/vitess/pull/17320)
 * [main] Copy `v20.0.4` release notes [#17322](https://github.com/vitessio/vitess/pull/17322)
 * [main] Copy `v21.0.1` release notes [#17324](https://github.com/vitessio/vitess/pull/17324)
 * [main] Copy `v19.0.9` release notes [#17595](https://github.com/vitessio/vitess/pull/17595)
 * [main] Copy `v20.0.5` release notes [#17597](https://github.com/vitessio/vitess/pull/17597)
 * [main] Copy `v21.0.2` release notes [#17599](https://github.com/vitessio/vitess/pull/17599)
 * [main] Copy `v21.0.3` release notes [#17765](https://github.com/vitessio/vitess/pull/17765)
 * [main] Copy `v20.0.6` release notes [#17767](https://github.com/vitessio/vitess/pull/17767)
 * [main] Copy `v19.0.10` release notes [#17769](https://github.com/vitessio/vitess/pull/17769)
 * [release-22.0] Code Freeze for `v22.0.0-RC1` [#18087](https://github.com/vitessio/vitess/pull/18087)
 * Bump to `v23.0.0-SNAPSHOT` after the `v22.0.0-RC1` release [#18088](https://github.com/vitessio/vitess/pull/18088)
### Testing 
#### Backup and Restore
 * fix flaky test on mysqlshell backup engine [#17037](https://github.com/vitessio/vitess/pull/17037)
 * [release-21.0] fix flaky test on mysqlshell backup engine  [#17981](https://github.com/vitessio/vitess/pull/17981) 
#### Build/CI
 * Fix flakiness in `TestDisruptions` for two pc testing [#17106](https://github.com/vitessio/vitess/pull/17106)
 * Flakes: Address flakiness in TestZkConnClosedOnDisconnect [#17194](https://github.com/vitessio/vitess/pull/17194)
 * A couple `endtoend` cluster tests enhancement [#17247](https://github.com/vitessio/vitess/pull/17247)
 * Fix fuzzer paths [#17380](https://github.com/vitessio/vitess/pull/17380)
 * Use release branches for upgrade downgrade tests [#18029](https://github.com/vitessio/vitess/pull/18029) 
#### Cluster management
 * Flaky test fixes [#16940](https://github.com/vitessio/vitess/pull/16940) 
#### General
 * Remove broken panic handler [#17354](https://github.com/vitessio/vitess/pull/17354) 
#### Java
 * Remove deprecated syntax [#17631](https://github.com/vitessio/vitess/pull/17631) 
#### Operator
 * Add CI for VTop example [#16007](https://github.com/vitessio/vitess/pull/16007) 
#### Query Serving
 * fix: flaky test on twopc transaction [#17068](https://github.com/vitessio/vitess/pull/17068)
 * Fix flaky test `TestShardSync` [#17095](https://github.com/vitessio/vitess/pull/17095)
 * Run plan tests in end to end configuration [#17117](https://github.com/vitessio/vitess/pull/17117)
 * List join predicates used in `GetVExplainKeys` [#17130](https://github.com/vitessio/vitess/pull/17130)
 * Fix flakiness in `TestReadTransactionStatus` [#17277](https://github.com/vitessio/vitess/pull/17277)
 * Add multi table updates in the 2pc fuzzer testing  [#17293](https://github.com/vitessio/vitess/pull/17293)
 * Add test for vindexes in atomic transactions package [#17308](https://github.com/vitessio/vitess/pull/17308)
 * Fix flakiness in `TestSemiSyncRequiredWithTwoPC` [#17332](https://github.com/vitessio/vitess/pull/17332)
 * test fix: make sure to keep the skipE2E field around [#17357](https://github.com/vitessio/vitess/pull/17357)
 * Ensure PRS runs for all the shards in `TestSemiSyncRequiredWithTwoPC` [#17384](https://github.com/vitessio/vitess/pull/17384)
 * Consistent lookup vindex tests for atomic distributed transactions [#17393](https://github.com/vitessio/vitess/pull/17393)
 * benchmark: TwoPC commit mode [#17397](https://github.com/vitessio/vitess/pull/17397)
 * test: make it easier to run tests without a main keyspace [#17501](https://github.com/vitessio/vitess/pull/17501)
 * Fix TestTrackerNoLock flaky test by increasing time to mark failure [#17886](https://github.com/vitessio/vitess/pull/17886)
 * Test: Increase query timeout to fix flaky test 'TestQueryTimeoutWithShardTargeting' [#18016](https://github.com/vitessio/vitess/pull/18016) 
#### TabletManager
 * Skip `TestRunFailsToStartTabletManager` for now [#17167](https://github.com/vitessio/vitess/pull/17167)
 * Fix flaky mysqlctl blackbox test [#17387](https://github.com/vitessio/vitess/pull/17387) 
#### VReplication
 * VReplication fix Flaky e2e test TestMoveTablesBuffering [#17180](https://github.com/vitessio/vitess/pull/17180)
 * VReplication Flaky Test fix: TestVtctldMigrateSharded [#17182](https://github.com/vitessio/vitess/pull/17182)
 * test: Add missing unit tests in `vtctl/workflow` [#17304](https://github.com/vitessio/vitess/pull/17304)
 * test: Add missing tests for `traffic_switcher.go` [#17334](https://github.com/vitessio/vitess/pull/17334)
 * VReplication: Enable VPlayerBatching in unit tests [#17339](https://github.com/vitessio/vitess/pull/17339)
 * Flaky test fix: TestMoveTablesSharded and TestMoveTablesUnsharded [#17343](https://github.com/vitessio/vitess/pull/17343)
 * test: Include unit tests for `switcher-dry-run` [#17403](https://github.com/vitessio/vitess/pull/17403)
 * Flaky TestMoveTables(Un)sharded: Handle race condition  [#17440](https://github.com/vitessio/vitess/pull/17440)
 * Move VDiff related workflow server APIs to `vdiff.go` and add unit tests [#17466](https://github.com/vitessio/vitess/pull/17466)
 * Fix flaky vreplication tests: correct logic that checks for workflow state in test helper [#17498](https://github.com/vitessio/vitess/pull/17498)
 * Flaky TestTickSkip: Remove inherently flaky test [#17504](https://github.com/vitessio/vitess/pull/17504)
 * test: Add unit tests for `vtctl/workflow` [#17618](https://github.com/vitessio/vitess/pull/17618)
 * test: Add more unit tests for `server.go` [#17679](https://github.com/vitessio/vitess/pull/17679)
 * CI: Add some randomness to the ports used in VReplication e2e tests [#17712](https://github.com/vitessio/vitess/pull/17712) 
#### VTTablet
 * Fix data race in `TestIsServingLocked` [#17728](https://github.com/vitessio/vitess/pull/17728) 
#### VTorc
 * Remove mysql parameters from VTOrc setup [#16996](https://github.com/vitessio/vitess/pull/16996)
 * Fix flakiness in checking for drained tablets in Vtorc test [#18008](https://github.com/vitessio/vitess/pull/18008) 
#### schema management
 * `schemadiff`: more index expression validations (tests only) [#17483](https://github.com/vitessio/vitess/pull/17483)

