# Changelog of Vitess v21.0.0

### Bug fixes 
#### ACL
 * Fix ACL checks for CTEs [#16642](https://github.com/vitessio/vitess/pull/16642) 
#### Backup and Restore
 * fixing issue with xtrabackup and long gtids [#16304](https://github.com/vitessio/vitess/pull/16304)
 * Fix `xtrabackup`/`builtin` context when doing `AddFiles` [#16806](https://github.com/vitessio/vitess/pull/16806)
 * Fail fast when builtinbackup fails to restore a single file [#16856](https://github.com/vitessio/vitess/pull/16856)
 * [release-21.0] fix releasing the global read lock when mysqlshell backup fails (#17000) [#17012](https://github.com/vitessio/vitess/pull/17012) 
#### Build/CI
 * Fix echo command in build docker images workflow [#16350](https://github.com/vitessio/vitess/pull/16350)
 * CI: Remove build docker images CI action until we figure out if that it is affecting our rate limits [#16759](https://github.com/vitessio/vitess/pull/16759)
 * Fix golang upgrade workflow [#16890](https://github.com/vitessio/vitess/pull/16890) 
#### Cluster management
 * Use default schema reload config values when config file is empty [#16393](https://github.com/vitessio/vitess/pull/16393)
 * fix: Use authentication_policy to specify default auth plugin for MySQL 8.4 [#16426](https://github.com/vitessio/vitess/pull/16426)
 * [Direct PR] [release-21.0] Revert Errant GTID detection in vttablets [#17019](https://github.com/vitessio/vitess/pull/17019) 
#### Docker
 * Fix the install dependencies script in Docker [#16340](https://github.com/vitessio/vitess/pull/16340) 
#### Documentation
 * Fix the `v19.0.0` release notes and use the `vitess/lite` image for the MySQL container [#16282](https://github.com/vitessio/vitess/pull/16282) 
#### Examples
 * Example operator 401_scheduled_backups.yaml incorrect keyspace [#16732](https://github.com/vitessio/vitess/pull/16732)
 * small fixes for docker-compose setup [#16818](https://github.com/vitessio/vitess/pull/16818) 
#### Observability
 * VReplication: Estimate lag when workflow fully throttled [#16577](https://github.com/vitessio/vitess/pull/16577) 
#### Online DDL
 * Online DDL: Fail a --in-order-completion migration, if a _prior_ migration within the same context is 'failed' or 'cancelled' [#16071](https://github.com/vitessio/vitess/pull/16071)
 * Ensure that we check the correct collation for foreign keys [#16109](https://github.com/vitessio/vitess/pull/16109)
 * Online DDL shadow table: rename referenced table name in self referencing FK [#16205](https://github.com/vitessio/vitess/pull/16205)
 * Online DDL: do not update last_throttled_timestamp with zero value [#16395](https://github.com/vitessio/vitess/pull/16395)
 * Support `SHOW VITESS_MIGRATIONS` from inside a transaction [#16399](https://github.com/vitessio/vitess/pull/16399) 
#### Query Serving
 * Fix dual merging in outer join queries [#15959](https://github.com/vitessio/vitess/pull/15959)
 * Handle single sharded keyspaces for analysis [#16068](https://github.com/vitessio/vitess/pull/16068)
 * Fix schemadiff semantics handling [#16073](https://github.com/vitessio/vitess/pull/16073)
 * `schemadiff`: only compare column collations if of textual type [#16138](https://github.com/vitessio/vitess/pull/16138)
 * Handle Nullability for Columns from Outer Tables [#16174](https://github.com/vitessio/vitess/pull/16174)
 * fix: rows affected count for multi table update for non-literal column value [#16181](https://github.com/vitessio/vitess/pull/16181)
 * Correct Handling of UNION Queries with Literals in the Vitess Planner [#16227](https://github.com/vitessio/vitess/pull/16227)
 * Parsing `Json_arrayagg` and `Json_objectagg` to allow some queries to work [#16251](https://github.com/vitessio/vitess/pull/16251)
 * Fix Incorrect Optimization with LIMIT and GROUP BY [#16263](https://github.com/vitessio/vitess/pull/16263)
 * planner: Handle ORDER BY inside derived tables [#16353](https://github.com/vitessio/vitess/pull/16353)
 * fix issue with aggregation inside of derived tables [#16366](https://github.com/vitessio/vitess/pull/16366)
 * Fix Join Predicate Cleanup Bug in Route Merging [#16386](https://github.com/vitessio/vitess/pull/16386)
 * Fix panic in user defined aggregation functions planning [#16398](https://github.com/vitessio/vitess/pull/16398)
 * Fix subquery planning having an aggregation that is used in order by as long as we can merge it all into a single route [#16402](https://github.com/vitessio/vitess/pull/16402)
 * bugfix: don't treat join predicates as filter predicates [#16472](https://github.com/vitessio/vitess/pull/16472)
 * fix: reference table join merge [#16488](https://github.com/vitessio/vitess/pull/16488)
 * fix: sequence table next value acl permission to writer role  [#16509](https://github.com/vitessio/vitess/pull/16509)
 * fix: show tables to use any keyspace on system schema [#16521](https://github.com/vitessio/vitess/pull/16521)
 * simplify merging logic [#16525](https://github.com/vitessio/vitess/pull/16525)
 * Fix: Offset planning in hash joins [#16540](https://github.com/vitessio/vitess/pull/16540)
 * Fix query plan cache misses metric [#16562](https://github.com/vitessio/vitess/pull/16562)
 * Atomic Transaction bug fix with PRS disruption [#16576](https://github.com/vitessio/vitess/pull/16576)
 * JSON Encoding: Use Type_RAW for marshalling json [#16637](https://github.com/vitessio/vitess/pull/16637)
 * Fix race conditions in the concatenate engine streaming [#16640](https://github.com/vitessio/vitess/pull/16640)
 * Fix Precedence rule for Tilda op [#16649](https://github.com/vitessio/vitess/pull/16649)
 * fix sizegen so it handles type aliases [#16650](https://github.com/vitessio/vitess/pull/16650)
 * Fix race condition that prevents queries from being buffered after vtgate startup [#16655](https://github.com/vitessio/vitess/pull/16655)
 * Fix data race in keyspace event watcher [#16683](https://github.com/vitessio/vitess/pull/16683)
 * fix: DDL comments to be recorded in AST [#16774](https://github.com/vitessio/vitess/pull/16774)
 * vtgate: Use the time zone setting correctly [#16824](https://github.com/vitessio/vitess/pull/16824)
 * VTTablet: smartconnpool: notify all expired waiters [#16897](https://github.com/vitessio/vitess/pull/16897)
 * [release-21.0] fix issue with json unmarshalling of operators with space in them [#16933](https://github.com/vitessio/vitess/pull/16933)
 * [release-21.0] VTGate MoveTables Buffering: Fix panic when buffering is disabled (#16922) [#16936](https://github.com/vitessio/vitess/pull/16936)
 * [release-21.0] fixes bugs around expression precedence and LIKE (#16934) [#16947](https://github.com/vitessio/vitess/pull/16947)
 * [release-21.0] [Direct PR] bugfix: add HAVING columns inside derived tables (#16976) [#16980](https://github.com/vitessio/vitess/pull/16980) 
#### Schema Tracker
 * Log and ignore the error in reading udfs in schema tracker [#16630](https://github.com/vitessio/vitess/pull/16630) 
#### Throttler
 * Tablet throttler: remove cached metric associated with removed tablet [#16555](https://github.com/vitessio/vitess/pull/16555)
 * Throttler/vreplication: fix app name used by VPlayer [#16578](https://github.com/vitessio/vitess/pull/16578) 
#### VReplication
 * vtctldclient: Fix Apply (Shard | Keyspace| Table) Routing Rules commands  [#16096](https://github.com/vitessio/vitess/pull/16096)
 * CI Bug: Rename shard name back to match existing workflow file for vreplication_migrate_vdiff2_convert_tz [#16148](https://github.com/vitessio/vitess/pull/16148)
 * VReplication: handle escaped identifiers in vschema when initializing sequence tables [#16169](https://github.com/vitessio/vitess/pull/16169)
 * VDiff CLI: Fix VDiff `show` bug [#16177](https://github.com/vitessio/vitess/pull/16177)
 * CI flaky test: Fix flakiness in vreplication_migrate_vdiff2_convert_tz [#16180](https://github.com/vitessio/vitess/pull/16180)
 * VReplication Workflow: set state correctly when restarting workflow streams in the copy phase [#16217](https://github.com/vitessio/vitess/pull/16217)
 * VReplication: Properly handle target shards w/o a primary in Reshard [#16283](https://github.com/vitessio/vitess/pull/16283)
 * VDiff: Copy non in_keyrange workflow filters to target tablet query [#16307](https://github.com/vitessio/vitess/pull/16307)
 * VReplication: disable use of `session_track_gtids` [#16424](https://github.com/vitessio/vitess/pull/16424)
 * VStream API: validate that last PK has fields defined [#16478](https://github.com/vitessio/vitess/pull/16478)
 * VReplication: Properly ignore errors from trying to drop tables that don't exist [#16505](https://github.com/vitessio/vitess/pull/16505)
 * VReplication: Return lock error everywhere that LockName fails [#16560](https://github.com/vitessio/vitess/pull/16560)
 * VReplication: Gather source positions once we know all writes are done during traffic switch [#16572](https://github.com/vitessio/vitess/pull/16572)
 * VTGate VStream: Ensure reasonable delivery time for reshard journal event  [#16639](https://github.com/vitessio/vitess/pull/16639)
 * Workflow Status: change logic to determine whether `MoveTables` writes are switched [#16731](https://github.com/vitessio/vitess/pull/16731)
 * Migrate Workflow: Scope vindex names correctly when target and source keyspace have different names [#16769](https://github.com/vitessio/vitess/pull/16769)
 * VReplication: Support both vindex col def formats when initing target sequences [#16862](https://github.com/vitessio/vitess/pull/16862)
 * [release-21.0] fix: Infinite logs in case of non-existent stream logs (#17004) [#17014](https://github.com/vitessio/vitess/pull/17014)
 * [release-21.0] VReplication: Support reversing read-only traffic in vtctldclient (#16920) [#17015](https://github.com/vitessio/vitess/pull/17015)
 * [release-21.0] Use proper zstd decoder pool for binlog event compression handling (#17042) [#17045](https://github.com/vitessio/vitess/pull/17045) 
#### VTAdmin
 * VTAdmin: Upgrade websockets js package [#16504](https://github.com/vitessio/vitess/pull/16504)
 * VTAdmin: Fix serve-handler's path-to-regexp dep and add default schema refresh [#16778](https://github.com/vitessio/vitess/pull/16778) 
#### VTCombo
 * vtcombo: close query service on drop database [#16606](https://github.com/vitessio/vitess/pull/16606) 
#### VTGate
 * Fix `RemoveTablet` during `TabletExternallyReparented` causing connection issues [#16371](https://github.com/vitessio/vitess/pull/16371)
 * Fix panic in schema tracker in presence of keyspace routing rules [#16383](https://github.com/vitessio/vitess/pull/16383)
 * [release-21.0] Fix deadlock between health check and topology watcher (#16995) [#17010](https://github.com/vitessio/vitess/pull/17010) 
#### VTTablet
 * Fix race in `replicationLagModule` of `go/vt/throttle` [#16078](https://github.com/vitessio/vitess/pull/16078) 
#### VTorc
 * FindErrantGTIDs: superset is not an errant GTID situation [#16725](https://github.com/vitessio/vitess/pull/16725)
 * Remember all tablets in VTOrc [#16888](https://github.com/vitessio/vitess/pull/16888) 
#### schema management
 * `schemadiff`: fix index/foreign-key apply scenario [#16698](https://github.com/vitessio/vitess/pull/16698) 
#### vtctldclient
 * [release-21.0] Fix flag name typo from #16852 (#16921) [#16923](https://github.com/vitessio/vitess/pull/16923) 
#### vtexplain
 * Fix `vtexplain` not handling `UNION` queries with `weight_string` results correctly. [#16129](https://github.com/vitessio/vitess/pull/16129) 
#### vttestserver
 * parse transaction timeout as duration [#16338](https://github.com/vitessio/vitess/pull/16338)
### CI/Build 
#### Backup and Restore
 * `backup_pitr` CI: validate rejoining replication stream [#16807](https://github.com/vitessio/vitess/pull/16807) 
#### Build/CI
 * Add DCO workflow [#16052](https://github.com/vitessio/vitess/pull/16052)
 * Remove DCO workaround [#16087](https://github.com/vitessio/vitess/pull/16087)
 * CI: Fix for xtrabackup install failures [#16329](https://github.com/vitessio/vitess/pull/16329)
 * CI: rename `TestSchemaChange` to distinguish the tests [#16694](https://github.com/vitessio/vitess/pull/16694)
 * CI: Use valid tag for the ossf-scorecard action [#16730](https://github.com/vitessio/vitess/pull/16730)
 * `endtoend`: better error reporting in Online DDL MySQL execution tests [#16786](https://github.com/vitessio/vitess/pull/16786)
 * Docker Images Build CI action: add updated version from release-20.0 back in prep for v21 release [#16799](https://github.com/vitessio/vitess/pull/16799)
 * Use `go-version-file: go.mod` in CI  [#16841](https://github.com/vitessio/vitess/pull/16841)
 * [release-21.0] Flakes: Address flakes in TestMoveTables* unit tests (#16942) [#16951](https://github.com/vitessio/vitess/pull/16951) 
#### Docker
 * Docker: Update node vtadmin version [#16147](https://github.com/vitessio/vitess/pull/16147)
 * Fix `docker_lite_push` make target [#16662](https://github.com/vitessio/vitess/pull/16662)
 * [release-21.0] Build `vttestserver` in GHA and send Slack message on Docker Build failure (#16963) [#17047](https://github.com/vitessio/vitess/pull/17047) 
#### General
 * [main] Upgrade the Golang version to `go1.22.4` [#16062](https://github.com/vitessio/vitess/pull/16062)
 * [main] Upgrade the Golang version to `go1.22.5` [#16319](https://github.com/vitessio/vitess/pull/16319)
 * Update Golang to `1.23.0` [#16599](https://github.com/vitessio/vitess/pull/16599)
 * [main] Upgrade the Golang version to `go1.23.1` [#16720](https://github.com/vitessio/vitess/pull/16720)
 * [main] Upgrade the Golang version to `go1.23.2` [#16891](https://github.com/vitessio/vitess/pull/16891) 
#### Online DDL
 * CI: increase timeout for  Online DDL foreign key stress tests [#16203](https://github.com/vitessio/vitess/pull/16203)
 * CI: wait-for rather than 'assume' in Online DDL flow [#16210](https://github.com/vitessio/vitess/pull/16210)
 * A couple changes in Online DDL CI [#16272](https://github.com/vitessio/vitess/pull/16272) 
#### VReplication
 * CI: Lower resources used for TestVtctldMigrateSharded [#16875](https://github.com/vitessio/vitess/pull/16875)
 * [release-21.0] VReplication: Restore previous minimal e2e test behavior (#17016) [#17017](https://github.com/vitessio/vitess/pull/17017) 
#### VTAdmin
 * Update micromatch to 4.0.8 [#16660](https://github.com/vitessio/vitess/pull/16660) 
#### VTGate
 * CI: testing self referencing tables in foreign key stress tests [#16216](https://github.com/vitessio/vitess/pull/16216)
### Dependencies 
#### General
 * Upgrade the Golang Dependencies [#16194](https://github.com/vitessio/vitess/pull/16194)
 * Upgrade the Golang Dependencies [#16302](https://github.com/vitessio/vitess/pull/16302)
 * Upgrade the Golang Dependencies [#16379](https://github.com/vitessio/vitess/pull/16379)
 * Upgrade the Golang Dependencies [#16514](https://github.com/vitessio/vitess/pull/16514)
 * Upgrade the Golang Dependencies [#16600](https://github.com/vitessio/vitess/pull/16600)
 * Upgrade the Golang Dependencies [#16691](https://github.com/vitessio/vitess/pull/16691)
 * Upgrade the Golang Dependencies [#16785](https://github.com/vitessio/vitess/pull/16785) 
#### Java
 * Bump com.google.protobuf:protobuf-java from 3.24.3 to 3.25.5 in /java [#16809](https://github.com/vitessio/vitess/pull/16809)
 * [release-21.0] Bump commons-io:commons-io from 2.7 to 2.14.0 in /java (#16889) [#16932](https://github.com/vitessio/vitess/pull/16932) 
#### VTAdmin
 * Update braces package [#16115](https://github.com/vitessio/vitess/pull/16115)
 * VTAdmin: Address security vuln in path-to-regexp node pkg [#16770](https://github.com/vitessio/vitess/pull/16770)
### Documentation 
#### Documentation
 * Changelog 20.0: Fix broken links [#16048](https://github.com/vitessio/vitess/pull/16048)
 * copy editing changes to summary [#16880](https://github.com/vitessio/vitess/pull/16880)
 * Add blurb about experimental 8.4 support [#16886](https://github.com/vitessio/vitess/pull/16886)
 * [release-21.0] Add missing changelog for PR #16852 (#17002) [#17006](https://github.com/vitessio/vitess/pull/17006) 
#### General
 * release notes: update dml related release notes [#16241](https://github.com/vitessio/vitess/pull/16241) 
#### Query Serving
 * [Direct PR][release note]: Atomic Distributed Transaction [#17079](https://github.com/vitessio/vitess/pull/17079) 
#### VReplication
 * Release docs: Add vreplication related entries to the v20 summary [#16259](https://github.com/vitessio/vitess/pull/16259) 
#### VTTablet
 * clarify collations are also supported for `db_charset` [#16423](https://github.com/vitessio/vitess/pull/16423)
### Enhancement 
#### Build/CI
 * Improve the queries upgrade/downgrade CI workflow by using same test code version as binary [#16494](https://github.com/vitessio/vitess/pull/16494)
 * [release-21.0] Change upgrade test to still use the older version of tests (#16937) [#16970](https://github.com/vitessio/vitess/pull/16970) 
#### CLI
 * Support specifying expected primary in ERS/PRS [#16852](https://github.com/vitessio/vitess/pull/16852) 
#### Cluster management
 * Prefer replicas that have innodb buffer pool populated in PRS [#16374](https://github.com/vitessio/vitess/pull/16374)
 * Allow cross cell promotion in PRS [#16461](https://github.com/vitessio/vitess/pull/16461)
 * Fix: Errant GTID detection on the replicas when they set replication source [#16833](https://github.com/vitessio/vitess/pull/16833)
 * [release-21.0] [Direct PR] Add RPC to read the reparent journal position [#16982](https://github.com/vitessio/vitess/pull/16982)
 * [Direct PR] [release-21.0] Augment `PrimaryStatus` to also send Server UUID [#17032](https://github.com/vitessio/vitess/pull/17032) 
#### Docker
 * Remove the `bootstrap` dependency on the Docker images we ship [#16339](https://github.com/vitessio/vitess/pull/16339) 
#### Evalengine
 * evalengine: Implement `PERIOD_ADD` [#16492](https://github.com/vitessio/vitess/pull/16492)
 * evalengine: Implement `PERIOD_DIFF` [#16557](https://github.com/vitessio/vitess/pull/16557) 
#### General
 * Add MySQL 8.4 unit tests [#16440](https://github.com/vitessio/vitess/pull/16440)
 * Added the scorecard github action and its badge [#16538](https://github.com/vitessio/vitess/pull/16538) 
#### Online DDL
 * `schemadiff`: improved diff ordering with various foreign key strategies [#16081](https://github.com/vitessio/vitess/pull/16081)
 * Online DDL: `ALTER VITESS_MIGRATION CLEANUP ALL` [#16314](https://github.com/vitessio/vitess/pull/16314)
 * Online DDL: ensure high `lock_wait_timeout` in Vreplication cut-over [#16601](https://github.com/vitessio/vitess/pull/16601)
 * Online DDL: new `message_timestamp` column in `schema_migrations` table [#16633](https://github.com/vitessio/vitess/pull/16633)
 * Online DDL: better support for subsecond `--force-cut-over-after` DDL strategy flag value. [#16635](https://github.com/vitessio/vitess/pull/16635)
 * VReplication workflows: retry "wrong tablet type" errors [#16645](https://github.com/vitessio/vitess/pull/16645) 
#### Query Serving
 * Add parsing support for `ANY`/`SOME`/`ALL` comparison modifiers. [#16080](https://github.com/vitessio/vitess/pull/16080)
 * VIndexes: Stop recommending md5 based vindex types as md5 is considered insecure [#16113](https://github.com/vitessio/vitess/pull/16113)
 * feat: make the arguments print themselves with type info [#16232](https://github.com/vitessio/vitess/pull/16232)
 * Group Concat function support for separator [#16237](https://github.com/vitessio/vitess/pull/16237)
 * Prevent Early Ordering Pushdown to Enable Aggregation Pushdown to MySQL [#16278](https://github.com/vitessio/vitess/pull/16278)
 * Add new mysql connection drain [#16298](https://github.com/vitessio/vitess/pull/16298)
 * Improve typing during query planning [#16310](https://github.com/vitessio/vitess/pull/16310)
 * Fail on prepare of reserved connection  [#16316](https://github.com/vitessio/vitess/pull/16316)
 * make sure to add weight_string for the right expression [#16325](https://github.com/vitessio/vitess/pull/16325)
 * flow based tablet load balancer [#16351](https://github.com/vitessio/vitess/pull/16351)
 * feat: remove ORDER BY from inside derived table [#16354](https://github.com/vitessio/vitess/pull/16354)
 * Implement parser support for all select modifiers [#16396](https://github.com/vitessio/vitess/pull/16396)
 * Handle dual queries with LIMIT on the vtgate [#16400](https://github.com/vitessio/vitess/pull/16400)
 * Complex Expression handling for uncorrelated IN and NOT IN subqueries [#16439](https://github.com/vitessio/vitess/pull/16439)
 * Add basic vector support for MySQL 9.x [#16464](https://github.com/vitessio/vitess/pull/16464)
 * allow innodb_lock_wait_timeout as system variable [#16574](https://github.com/vitessio/vitess/pull/16574)
 * Add command to see the unresolved transactions [#16589](https://github.com/vitessio/vitess/pull/16589)
 * Reject TwoPC calls if semi-sync is not enabled [#16608](https://github.com/vitessio/vitess/pull/16608)
 * Additional recursive CTE work [#16616](https://github.com/vitessio/vitess/pull/16616)
 * DDL allowed outside of transaction in vttablet [#16661](https://github.com/vitessio/vitess/pull/16661)
 * `BinlogEvent`: publish `ServerID()` function [#16713](https://github.com/vitessio/vitess/pull/16713)
 * feat: authoritative query timeout for vttablet from vtgate [#16735](https://github.com/vitessio/vitess/pull/16735)
 * Add `vexplain trace` [#16768](https://github.com/vitessio/vitess/pull/16768)
 * Count how many shards were hit by routes when using `vexplain` [#16802](https://github.com/vitessio/vitess/pull/16802)
 * Add VEXPLAIN KEYS for improved sharding key selection [#16830](https://github.com/vitessio/vitess/pull/16830)
 * reject atomic distributed transaction on savepoints and modified system settings [#16835](https://github.com/vitessio/vitess/pull/16835)
 * Making Reshard work smoothly with Atomic Transactions [#16844](https://github.com/vitessio/vitess/pull/16844)
 * Query serving: incorporate mirror query results in log stats, fix mirror query max lag bug [#16879](https://github.com/vitessio/vitess/pull/16879)
 * Enhanced output for vexplain keys [#16892](https://github.com/vitessio/vitess/pull/16892) 
#### Throttler
 * Tablet throttler: multi-metric support [#15988](https://github.com/vitessio/vitess/pull/15988)
 * Throttler: return app name in check result, synthesize "why throttled" explanation from result [#16416](https://github.com/vitessio/vitess/pull/16416)
 * Throttler: `SelfMetric` interface, simplify adding new throttler metrics [#16469](https://github.com/vitessio/vitess/pull/16469)
 * Throttler: `CheckThrottlerResponseCode` to replace HTTP status codes [#16491](https://github.com/vitessio/vitess/pull/16491)
 * Deprecate `UpdateThrottlerConfig`'s `--check-as-check-self` and `--check-as-check-shard` flags [#16507](https://github.com/vitessio/vitess/pull/16507)
 * `mysqld` system metrics, with `TabletManager` rpc [#16850](https://github.com/vitessio/vitess/pull/16850) 
#### VReplication
 * VReplication: Improve handling of vplayer stalls [#15797](https://github.com/vitessio/vitess/pull/15797)
 * VReplication: LookupVindex create use existing artifacts when possible [#16097](https://github.com/vitessio/vitess/pull/16097)
 * VReplication: use new topo named locks and TTL override for workflow coordination [#16260](https://github.com/vitessio/vitess/pull/16260)
 * VReplication: Handle large binlog compressed transactions more efficiently [#16328](https://github.com/vitessio/vitess/pull/16328)
 * VReplication: make flags workflow-specific and dynamically changeable [#16583](https://github.com/vitessio/vitess/pull/16583)
 * VDiff: Improve row diff handling in report [#16618](https://github.com/vitessio/vitess/pull/16618)
 * VDiff: Add control for start/resume and stop [#16654](https://github.com/vitessio/vitess/pull/16654)
 * VReplication: Force flag for traffic switching [#16709](https://github.com/vitessio/vitess/pull/16709)
 * VReplication: Replace most usage of SimulatedNulls [#16734](https://github.com/vitessio/vitess/pull/16734)
 * VReplication: Add VTGate VStreamFlag to include journal events in the stream [#16737](https://github.com/vitessio/vitess/pull/16737)
 * VReplication: Validate min set of user permissions in traffic switch prechecks [#16762](https://github.com/vitessio/vitess/pull/16762)
 * Materialize workflow support for reference tables [#16787](https://github.com/vitessio/vitess/pull/16787)
 * VReplication: Undo vschema changes on LookupVindex workflow creation fail [#16810](https://github.com/vitessio/vitess/pull/16810)
 * VReplication: Support automatically replacing auto_inc cols with sequences [#16860](https://github.com/vitessio/vitess/pull/16860) 
#### VTAdmin
 * VTAdmin: Initiate workflow details tab [#16570](https://github.com/vitessio/vitess/pull/16570)
 * VTAdmin: Workflow status endpoint [#16587](https://github.com/vitessio/vitess/pull/16587)
 * VTAdmin(API): Add workflow start/stop endpoint [#16658](https://github.com/vitessio/vitess/pull/16658)
 * VTAdmin: Distributed transactions list on VTAdmin [#16793](https://github.com/vitessio/vitess/pull/16793)
 * [Release 21.0] [Direct PR] Conclude txn in VTAdmin and `GetUnresolvedTransactions` bug fix [#16949](https://github.com/vitessio/vitess/pull/16949) 
#### VTCombo
 * VTCombo: Ensure VSchema exists when creating keyspace [#16094](https://github.com/vitessio/vitess/pull/16094) 
#### VTGate
 * VStream API: allow keyspace-level heartbeats to be streamed [#16593](https://github.com/vitessio/vitess/pull/16593) 
#### VTTablet
 * Heartbeat writer can always generate on-demand leased heartbeats, even if not at all configured [#16014](https://github.com/vitessio/vitess/pull/16014)
 * Add tablet-tags/`--init_tags` stats [#16695](https://github.com/vitessio/vitess/pull/16695)
 * Add `ChangeTabletTags` RPC to `vtctl`, `ChangeTags` RPC to `vttablet` [#16857](https://github.com/vitessio/vitess/pull/16857) 
#### VTorc
 * Errant GTID Counts metric in VTOrc [#16829](https://github.com/vitessio/vitess/pull/16829) 
#### schema management
 * `schemadiff`: Online DDL support, declarative based [#16462](https://github.com/vitessio/vitess/pull/16462)
 * `shcemadiff`: support `INSTANT` DDL for changing column visibility [#16503](https://github.com/vitessio/vitess/pull/16503)
 * SidecarDB: Don't ignore table charset and collation [#16670](https://github.com/vitessio/vitess/pull/16670)
 * schemadiff: more `INSTANT` algorithm considerations [#16678](https://github.com/vitessio/vitess/pull/16678)
 * schemadiff: reject non-deterministic function in new column's default value [#16684](https://github.com/vitessio/vitess/pull/16684)
 * schemadiff: reject `uuid_short` and `random_bytes` in new column expression [#16692](https://github.com/vitessio/vitess/pull/16692) 
#### vtctldclient
 * Add `vtctldclient` container image [#16318](https://github.com/vitessio/vitess/pull/16318) 
#### web UI
 * VTAdmin(web): Add workflow start/stop actions [#16675](https://github.com/vitessio/vitess/pull/16675)
 * VTAdmin(web): Some tweaks in the workflow details [#16706](https://github.com/vitessio/vitess/pull/16706)
 * VTAdmin(web): Initiate MoveTables workflow create screen [#16707](https://github.com/vitessio/vitess/pull/16707)
### Feature 
#### Backup and Restore
 * adding new mysql shell backup engine [#16295](https://github.com/vitessio/vitess/pull/16295)
 * select backup engine in Backup() and ignore engines in RestoreFromBackup() [#16428](https://github.com/vitessio/vitess/pull/16428) 
#### Query Serving
 * add support for vtgate traffic mirroring (queryserving) [#15992](https://github.com/vitessio/vitess/pull/15992)
 * rpc: retrieve unresolved transactions [#16356](https://github.com/vitessio/vitess/pull/16356)
 * Transaction watcher and notifier [#16363](https://github.com/vitessio/vitess/pull/16363)
 * Distributed Transaction Resolver [#16381](https://github.com/vitessio/vitess/pull/16381)
 * Support recursive CTEs [#16427](https://github.com/vitessio/vitess/pull/16427)
 * Query to read transaction state [#16431](https://github.com/vitessio/vitess/pull/16431)
 * Modify distributed transaction commit flow [#16468](https://github.com/vitessio/vitess/pull/16468)
 * vttablet api distributed transaction changes [#16506](https://github.com/vitessio/vitess/pull/16506)
 * Atomic Transactions correctness with PRS, ERS and MySQL & Vttablet Restarts [#16553](https://github.com/vitessio/vitess/pull/16553)
 * Reject atomic distributed transaction on a network connection [#16584](https://github.com/vitessio/vitess/pull/16584)
 * Atomic Transactions handling with Online DDL [#16585](https://github.com/vitessio/vitess/pull/16585)
 * Multiple changes on Distributed Transaction user apis [#16788](https://github.com/vitessio/vitess/pull/16788)
 * Distributed Transaction: Action on commit prepared or redo prepared failure [#16803](https://github.com/vitessio/vitess/pull/16803) 
#### VTAdmin
 * VTAdmin: Show throttled status in workflows [#16308](https://github.com/vitessio/vitess/pull/16308) 
#### vtctldclient
 * add support for vtgate traffic mirroring [#15945](https://github.com/vitessio/vitess/pull/15945)
 * Add Unresolved Transactions command in vtctld service [#16679](https://github.com/vitessio/vitess/pull/16679)
 * Add conclude transaction command to vtctld service [#16693](https://github.com/vitessio/vitess/pull/16693)
### Internal Cleanup 
#### Backup and Restore
 * builtinbackup: log during restore as restore, not as backup [#16483](https://github.com/vitessio/vitess/pull/16483)
 * Migrate the S3 SDK from v1 to v2 [#16664](https://github.com/vitessio/vitess/pull/16664) 
#### Build/CI
 * Add `release-20.0` to the list of branches to upgrade the Go version on [#16053](https://github.com/vitessio/vitess/pull/16053)
 * Use new way of specifying test files to the vitess-tester [#16233](https://github.com/vitessio/vitess/pull/16233)
 * Remove unnecessary node install step in local/region examples workflow [#16293](https://github.com/vitessio/vitess/pull/16293)
 * fix: fixed the pinned-dependencies [#16612](https://github.com/vitessio/vitess/pull/16612)
 * Fix go.mod files [#16625](https://github.com/vitessio/vitess/pull/16625)
 * Move from 4-cores larger runners to `ubuntu-latest` [#16714](https://github.com/vitessio/vitess/pull/16714) 
#### CLI
 * SetShardTabletControl: Improve flag help output [#16376](https://github.com/vitessio/vitess/pull/16376)
 * Move concurrent connection dial limit out of `healthcheck`. [#16378](https://github.com/vitessio/vitess/pull/16378) 
#### Cluster management
 * Remove unused parameters in reparent code and add CheckLockShard calls [#16312](https://github.com/vitessio/vitess/pull/16312) 
#### Docker
 * Remove unnecessary Docker build workflows [#16196](https://github.com/vitessio/vitess/pull/16196)
 * Remove mysql57/percona57 bootstrap images [#16620](https://github.com/vitessio/vitess/pull/16620) 
#### Documentation
 * Update eol-process.md [#16249](https://github.com/vitessio/vitess/pull/16249)
 * Add a note on `QueryCacheHits` and `QueryCacheMisses` in the release notes [#16299](https://github.com/vitessio/vitess/pull/16299)
 * Add release notes for the new connection drain (+ use dashes instead of underscore) [#16334](https://github.com/vitessio/vitess/pull/16334)
 * Remove specific Kubernetes version instructions and link to the Vites… [#16610](https://github.com/vitessio/vitess/pull/16610) 
#### Examples
 * Remove docker_local from Makefile [#16335](https://github.com/vitessio/vitess/pull/16335) 
#### General
 * Deprecated metrics deletion [#16086](https://github.com/vitessio/vitess/pull/16086)
 * No usage of math/rand [#16264](https://github.com/vitessio/vitess/pull/16264)
 *  [cleanup] Protos: Use UnmarshalPB() when unmarshalling a proto object [#16311](https://github.com/vitessio/vitess/pull/16311)
 * Update golangci-lint to newer faster version [#16636](https://github.com/vitessio/vitess/pull/16636)
 * Remove the vitess-mixin [#16657](https://github.com/vitessio/vitess/pull/16657)
 * bug: fix slice init length [#16674](https://github.com/vitessio/vitess/pull/16674)
 * Fix generated proto [#16702](https://github.com/vitessio/vitess/pull/16702)
 * Update part of codebase to use new for := range syntax [#16738](https://github.com/vitessio/vitess/pull/16738)
 * Remove former maintainer from areas of expertise section [#16801](https://github.com/vitessio/vitess/pull/16801) 
#### Governance
 * Add `timvaillancourt` to some `CODEOWNERS` paths [#16107](https://github.com/vitessio/vitess/pull/16107)
 * Fix the email in `pom.xml` [#16111](https://github.com/vitessio/vitess/pull/16111) 
#### Online DDL
 * Online DDL internal cleanup: using formal statements as opposed to textual SQL [#16230](https://github.com/vitessio/vitess/pull/16230)
 * Online DDL: remove legacy (and ancient) 'REVERT <uuid>' syntax [#16301](https://github.com/vitessio/vitess/pull/16301) 
#### Query Serving
 * refactor: cleaner cloning API [#16079](https://github.com/vitessio/vitess/pull/16079)
 * `schemadiff` small internal refactor: formalizing column charset/collation [#16239](https://github.com/vitessio/vitess/pull/16239)
 * sqlparser: Remove unneeded escaping [#16255](https://github.com/vitessio/vitess/pull/16255)
 * Fix codegen [#16257](https://github.com/vitessio/vitess/pull/16257)
 * deprecate queryserver-enable-settings-pool flag [#16280](https://github.com/vitessio/vitess/pull/16280)
 * Cleanup: Health Stream removed db pool usage [#16336](https://github.com/vitessio/vitess/pull/16336)
 * Remove JSON annotations [#16437](https://github.com/vitessio/vitess/pull/16437)
 * Remove some unused MySQL 5.6 code [#16465](https://github.com/vitessio/vitess/pull/16465)
 * Small internal cleanups [#16467](https://github.com/vitessio/vitess/pull/16467)
 * vindex function: error when keyspace not selected [#16534](https://github.com/vitessio/vitess/pull/16534)
 * Planner cleaning: cleanup and refactor [#16569](https://github.com/vitessio/vitess/pull/16569)
 * Add resultsObserver to ScatterConn [#16638](https://github.com/vitessio/vitess/pull/16638)
 * Fix error code received from vttablets on context expiration [#16685](https://github.com/vitessio/vitess/pull/16685) 
#### Throttler
 * Throttler code: some refactoring and cleanup [#16368](https://github.com/vitessio/vitess/pull/16368) 
#### Topology
 * Topology Server Locking Refactor [#16005](https://github.com/vitessio/vitess/pull/16005) 
#### VReplication
 * VReplication: Improve table plan builder errors [#16588](https://github.com/vitessio/vitess/pull/16588)
 * VReplication: Improve replication plan builder and event application errors [#16596](https://github.com/vitessio/vitess/pull/16596) 
#### VTAdmin
 * Remove unused `formatRelativeTime` import [#16549](https://github.com/vitessio/vitess/pull/16549)
 * VTAdmin: Upgrade deps to address security vulns [#16843](https://github.com/vitessio/vitess/pull/16843) 
#### VTGate
 * Support passing filters to `discovery.NewHealthCheck(...)` [#16170](https://github.com/vitessio/vitess/pull/16170) 
#### VTTablet
 * `txthrottler`: move `ThrottlerInterface` to `go/vt/throttler`, use `slices` pkg, add stats [#16248](https://github.com/vitessio/vitess/pull/16248)
 * Deprecate vttablet metrics `QueryCacheXX` and rename to `QueryPlanCacheXX` [#16289](https://github.com/vitessio/vitess/pull/16289) 
#### schema management
 * `schemadiff`/Online DDL internal refactor [#16767](https://github.com/vitessio/vitess/pull/16767) 
#### vtctldclient
 * VReplication: Add custom logger support to Workflow Server [#16547](https://github.com/vitessio/vitess/pull/16547)
### Performance 
#### Backup and Restore
 * mysqlctl: open backup files with fadvise(2) and FADV_SEQUENTIAL [#16441](https://github.com/vitessio/vitess/pull/16441) 
#### Online DDL
 * Online DDL: avoid SQL's `CONVERT(...)`, convert programmatically if needed [#16597](https://github.com/vitessio/vitess/pull/16597) 
#### Performance
 * go/mysql: use clear builtin for zerofill [#16348](https://github.com/vitessio/vitess/pull/16348)
 * go/mysql: improve GTID encoding for OK packet [#16361](https://github.com/vitessio/vitess/pull/16361) 
#### Query Serving
 * go/mysql: performance optimizations in protocol encoding [#16341](https://github.com/vitessio/vitess/pull/16341)
 * go/vt/sqlparser: improve performance in TrackedBuffer formatting [#16364](https://github.com/vitessio/vitess/pull/16364)
 * `schemadiff`: optimize permutation evaluation [#16435](https://github.com/vitessio/vitess/pull/16435)
 * Performance improvements - Planner [#16631](https://github.com/vitessio/vitess/pull/16631)
 * Optimize Operator Input Handling [#16689](https://github.com/vitessio/vitess/pull/16689)
 * Small fix to decrease memory allocation [#16791](https://github.com/vitessio/vitess/pull/16791)
### Regression 
#### Query Serving
 * fix: order by subquery planning [#16049](https://github.com/vitessio/vitess/pull/16049)
 * feat: add a LIMIT 1 on EXISTS subqueries to limit network overhead [#16153](https://github.com/vitessio/vitess/pull/16153)
 * bugfix: Allow cross-keyspace joins [#16520](https://github.com/vitessio/vitess/pull/16520)
 * [release-21.0] [Direct PR] fix: route engine to handle column truncation for execute after lookup (#16981) [#16986](https://github.com/vitessio/vitess/pull/16986)
 * [release-21.0] Add support for `MultiEqual` opcode for lookup vindexes. (#16975) [#17041](https://github.com/vitessio/vitess/pull/17041)
### Release 
#### Build/CI
 * [release-21.0] Fix the release workflow (#16964) [#17020](https://github.com/vitessio/vitess/pull/17020) 
#### General
 * [main] Copy `v20.0.0-RC1` release notes [#16140](https://github.com/vitessio/vitess/pull/16140)
 * [main] Copy `v20.0.0-RC2` release notes [#16234](https://github.com/vitessio/vitess/pull/16234)
 * [main] Copy `v20.0.0` release notes [#16273](https://github.com/vitessio/vitess/pull/16273)
 * [main] Copy `v18.0.6` release notes [#16453](https://github.com/vitessio/vitess/pull/16453)
 * [main] Copy `v19.0.5` release notes [#16455](https://github.com/vitessio/vitess/pull/16455)
 * [main] Copy `v20.0.1` release notes [#16457](https://github.com/vitessio/vitess/pull/16457)
 * [main] Copy `v18.0.7` release notes [#16746](https://github.com/vitessio/vitess/pull/16746)
 * [main] Copy `v19.0.6` release notes [#16752](https://github.com/vitessio/vitess/pull/16752)
 * [main] Copy `v20.0.2` release notes [#16755](https://github.com/vitessio/vitess/pull/16755)
 * [release-21.0] Code Freeze for `v21.0.0-RC1` [#16912](https://github.com/vitessio/vitess/pull/16912)
 * [release-21.0] Release of `v21.0.0-RC1` [#16950](https://github.com/vitessio/vitess/pull/16950)
 * [release-21.0] Bump to `v21.0.0-SNAPSHOT` after the `v21.0.0-RC1` release [#16955](https://github.com/vitessio/vitess/pull/16955)
 * [release-21.0] Release of `v21.0.0-RC2` [#17022](https://github.com/vitessio/vitess/pull/17022)
 * [release-21.0] Bump to `v21.0.0-SNAPSHOT` after the `v21.0.0-RC2` release [#17049](https://github.com/vitessio/vitess/pull/17049)
 * [release-21.0] Add release notes for known issue in v21.0.0 [#17067](https://github.com/vitessio/vitess/pull/17067)
### Testing 
#### Build/CI
 * Online DDL flow CI: Update golang version to 1.22.4 [#16066](https://github.com/vitessio/vitess/pull/16066)
 * Run the static checks workflow if the file changes [#16659](https://github.com/vitessio/vitess/pull/16659)
 * Fix error contain checks in vtgate package [#16672](https://github.com/vitessio/vitess/pull/16672)
 * Don't show skipped tests in summary action [#16859](https://github.com/vitessio/vitess/pull/16859) 
#### Cluster management
 * Add semi-sync plugin test in main [#16372](https://github.com/vitessio/vitess/pull/16372)
 * [release-21.0] Flaky test fixes (#16940) [#16960](https://github.com/vitessio/vitess/pull/16960) 
#### General
 * CI Summary Addition [#16143](https://github.com/vitessio/vitess/pull/16143)
 * Add Summary in unit-race workflow [#16164](https://github.com/vitessio/vitess/pull/16164)
 * Allow for MySQL 8.4 in endtoend tests [#16885](https://github.com/vitessio/vitess/pull/16885) 
#### Query Serving
 * `schemadiff`: adding a FK dependency unit test [#16069](https://github.com/vitessio/vitess/pull/16069)
 * Adding Distributed Transaction Test [#16114](https://github.com/vitessio/vitess/pull/16114)
 * Vitess tester workflow [#16127](https://github.com/vitessio/vitess/pull/16127)
 * fix flaky test TestQueryTimeoutWithShardTargeting [#16150](https://github.com/vitessio/vitess/pull/16150)
 * test: Remove unreachable test skipping [#16265](https://github.com/vitessio/vitess/pull/16265)
 * Make sure end to end test actually tests the fix [#16277](https://github.com/vitessio/vitess/pull/16277)
 * Skip running vtadmin build for vitess tester workflow [#16459](https://github.com/vitessio/vitess/pull/16459)
 * Fuzzer testing for 2PC transactions [#16476](https://github.com/vitessio/vitess/pull/16476)
 * Move tables with atomic transactions [#16676](https://github.com/vitessio/vitess/pull/16676)
 * Replace ErrorContains checks with Error checks before running upgrade downgrade [#16688](https://github.com/vitessio/vitess/pull/16688)
 * Fix upgrade test in release-20 by skipping tests [#16705](https://github.com/vitessio/vitess/pull/16705)
 * Reduce flakiness in TwoPC testing [#16712](https://github.com/vitessio/vitess/pull/16712)
 * fix distributed transactions disruptions test with move table [#16765](https://github.com/vitessio/vitess/pull/16765)
 * Add a test to verify we respect the overall query timeout [#16800](https://github.com/vitessio/vitess/pull/16800)
 * flaky test fix for query timeout change [#16821](https://github.com/vitessio/vitess/pull/16821)
 * Fix error message check on query timeout [#16827](https://github.com/vitessio/vitess/pull/16827)
 * upgrade vitess-tester to latest version [#16884](https://github.com/vitessio/vitess/pull/16884)
 * Relax vexplain test for upcoming changes [#17035](https://github.com/vitessio/vitess/pull/17035) 
#### Throttler
 * Throttler flaky test: explicitly disabling throttler so as to ensure it does not re-enable [#16369](https://github.com/vitessio/vitess/pull/16369) 
#### VReplication
 * Multi-tenant Movetables: add e2e test for vdiff [#16309](https://github.com/vitessio/vitess/pull/16309)
 * VReplication workflow package: unit tests for StreamMigrator, Mount et al [#16498](https://github.com/vitessio/vitess/pull/16498)
 * CI: Temporarily skip flaky TestMoveTablesUnsharded [#16812](https://github.com/vitessio/vitess/pull/16812) 
#### VTCombo
 * Fix flaky tests that use vtcombo [#16178](https://github.com/vitessio/vitess/pull/16178) 
#### vtexplain
 * Fix flakiness in `vtexplain` unit test case. [#16159](https://github.com/vitessio/vitess/pull/16159) 
#### vttestserver
 * Flaky test fix `TestCanGetKeyspaces` [#16214](https://github.com/vitessio/vitess/pull/16214)

