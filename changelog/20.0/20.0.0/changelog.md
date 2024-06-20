# Changelog of Vitess v20.0.0

### Announcement 
#### Governance
 * add Tim Vaillancourt to maintainers [#15851](https://github.com/vitessio/vitess/pull/15851)
### Bug fixes 
#### Authn/z
 * Load `--grpc_auth_static_client_creds` file once [#15030](https://github.com/vitessio/vitess/pull/15030) 
#### Backup and Restore
 * Mysqld: capture mysqlbinlog std error output [#15278](https://github.com/vitessio/vitess/pull/15278)
 * fix backup goroutine leak [#15410](https://github.com/vitessio/vitess/pull/15410)
 * Ensure that WithParams keeps the transport [#15421](https://github.com/vitessio/vitess/pull/15421)
 * Configurable incremental restore files path [#15451](https://github.com/vitessio/vitess/pull/15451) 
#### Build/CI
 * Use latest go version in update golang version workflow [#15159](https://github.com/vitessio/vitess/pull/15159)
 * Fix `docker_build_images` CI workflow [#15635](https://github.com/vitessio/vitess/pull/15635)
 * Continue building base/k8s when tag version is below v20 [#15638](https://github.com/vitessio/vitess/pull/15638) 
#### CLI
 * Fix some binaries to print the versions [#15306](https://github.com/vitessio/vitess/pull/15306)
 * tablet: remove max-waiters setting [#15323](https://github.com/vitessio/vitess/pull/15323) 
#### Cluster management
 * Fix PromoteReplica call in ERS [#15934](https://github.com/vitessio/vitess/pull/15934)
 * Remove the default for replica-net-timeout [#15938](https://github.com/vitessio/vitess/pull/15938) 
#### Docker
 * Add `mysqlbinlog` and `xtrabackup` to the `vitess/lite` image [#15775](https://github.com/vitessio/vitess/pull/15775) 
#### Evalengine
 * Fix type coercion between the sides of an UNION [#15340](https://github.com/vitessio/vitess/pull/15340)
 * evalengine: Ensure to pass down the precision [#15611](https://github.com/vitessio/vitess/pull/15611)
 * evalengine: Fix additional time type handling [#15614](https://github.com/vitessio/vitess/pull/15614)
 * projection: Return correct collation information [#15801](https://github.com/vitessio/vitess/pull/15801) 
#### General
 * `ExecuteFetch`: error on multiple result sets [#14949](https://github.com/vitessio/vitess/pull/14949)
 * GRPC: Address potential segfault in dedicated connection pooling [#15751](https://github.com/vitessio/vitess/pull/15751)
 * Properly unescape keyspace name in FindAllShardsInKeyspace [#15765](https://github.com/vitessio/vitess/pull/15765) 
#### Online DDL
 * VReplication/OnlineDDL: reordering enum values [#15103](https://github.com/vitessio/vitess/pull/15103)
 * Remove `ALGORITHM=COPY` from Online DDL in MySQL `>= 8.0.32` [#15376](https://github.com/vitessio/vitess/pull/15376)
 * Enum value parsing: do not parse by whitespace [#15493](https://github.com/vitessio/vitess/pull/15493)
 * Flaky test TestOnlineDDLVDiff: add additional check for vreplication workflow to exist [#15695](https://github.com/vitessio/vitess/pull/15695)
 * `schemadiff`: `DROP COLUMN` not eligible for `INSTANT` algorithm if covered by an index [#15714](https://github.com/vitessio/vitess/pull/15714)
 * `schemadiff` INSTANT DDL: impossible changes on tables with `FULLTEXT` index [#15725](https://github.com/vitessio/vitess/pull/15725)
 * SchemaEngine: Ensure GetTableForPos returns table schema for "current" position by default [#15912](https://github.com/vitessio/vitess/pull/15912)
 * [release-20.0-rc] Online DDL shadow table: rename referenced table name in self referencing FK (#16205) [#16208](https://github.com/vitessio/vitess/pull/16208)
 * [release-20.0] Online DDL shadow table: rename referenced table name in self referencing FK (#16205) [#16209](https://github.com/vitessio/vitess/pull/16209) 
#### Query Serving
 * Make connection killing resilient to MySQL hangs [#14500](https://github.com/vitessio/vitess/pull/14500)
 * TxThrottler: dont throttle unless lag [#14789](https://github.com/vitessio/vitess/pull/14789)
 * Planner Bug: Joins inside derived table [#14974](https://github.com/vitessio/vitess/pull/14974)
 * fix: ignore internal tables in schema tracking [#15141](https://github.com/vitessio/vitess/pull/15141)
 * bugfix: wrong field type returned for SUM [#15192](https://github.com/vitessio/vitess/pull/15192)
 * Avoid rewriting unsharded queries and split semantic analysis in two [#15217](https://github.com/vitessio/vitess/pull/15217)
 * Fix Delete with multi-tables related by foreign keys [#15218](https://github.com/vitessio/vitess/pull/15218)
 * sqlparser: use integers instead of literals for Length/Precision  [#15256](https://github.com/vitessio/vitess/pull/15256)
 * Fix Go routine leaks in streaming calls [#15293](https://github.com/vitessio/vitess/pull/15293)
 * Column alias expanding on ORDER BY [#15302](https://github.com/vitessio/vitess/pull/15302)
 * planner: support union statements with ctes [#15312](https://github.com/vitessio/vitess/pull/15312)
 * go/vt/discovery: use protobuf getters for SrvVschema [#15343](https://github.com/vitessio/vitess/pull/15343)
 * Bugfix: GROUP BY/HAVING alias resolution [#15344](https://github.com/vitessio/vitess/pull/15344)
 * SHOW VITESS_REPLICATION_STATUS: Only use replication tracker when it's enabled [#15348](https://github.com/vitessio/vitess/pull/15348)
 * Fixing Column aliases in outer join queries [#15384](https://github.com/vitessio/vitess/pull/15384)
 * Fix view tracking on sharded keyspace [#15436](https://github.com/vitessio/vitess/pull/15436)
 * engine:  fix race in concatenate [#15454](https://github.com/vitessio/vitess/pull/15454)
 * Fix cycle detection for foreign keys [#15458](https://github.com/vitessio/vitess/pull/15458)
 * fail insert when primary vindex cannot be mapped to a shard [#15500](https://github.com/vitessio/vitess/pull/15500)
 * Fix aliasing in routes that have a derived table [#15550](https://github.com/vitessio/vitess/pull/15550)
 * bugfix: handling of ANDed join predicates [#15551](https://github.com/vitessio/vitess/pull/15551)
 * Make sure derived table column names are handled correctly [#15588](https://github.com/vitessio/vitess/pull/15588)
 * Fix TPCH test by providing the correct field information in evalengine [#15623](https://github.com/vitessio/vitess/pull/15623)
 * fix: don't forget DISTINCT for derived tables [#15672](https://github.com/vitessio/vitess/pull/15672)
 * Prevent adding to query details after unserve common has started [#15684](https://github.com/vitessio/vitess/pull/15684)
 * Fix panic in aggregation [#15728](https://github.com/vitessio/vitess/pull/15728)
 * fix: use correct flag field for udf tracking [#15749](https://github.com/vitessio/vitess/pull/15749)
 * Store Decimal precision and size while normalising [#15785](https://github.com/vitessio/vitess/pull/15785)
 * Fix Scale and length handling in `CASE` and JOIN bind variables [#15787](https://github.com/vitessio/vitess/pull/15787)
 * Fix derived table bug [#15831](https://github.com/vitessio/vitess/pull/15831)
 * Fix CTE query by fixing bindvar pushing in unions [#15838](https://github.com/vitessio/vitess/pull/15838)
 * Fix wrong assignment to `sql_id_opt` in the parser [#15862](https://github.com/vitessio/vitess/pull/15862)
 * `schemadiff`: more `INSTANT`  DDL compliance [#15871](https://github.com/vitessio/vitess/pull/15871)
 * fix: handle info_schema routing [#15899](https://github.com/vitessio/vitess/pull/15899)
 * fix: handle table_schema = '' without failing [#15901](https://github.com/vitessio/vitess/pull/15901)
 * Fix aliasing in queries by keeping required projections  [#15943](https://github.com/vitessio/vitess/pull/15943)
 * connpool: Allow time out during shutdown [#15979](https://github.com/vitessio/vitess/pull/15979)
 * `schemadiff`: assume default collation for textual column when collation is undefined [#16000](https://github.com/vitessio/vitess/pull/16000)
 * fix: remove keyspace when merging subqueries [#16019](https://github.com/vitessio/vitess/pull/16019)
 * [release-20.0-rc] fix: rows affected count for multi table update for non-literal column value (#16181) [#16182](https://github.com/vitessio/vitess/pull/16182)
 * [release-20.0] fix: rows affected count for multi table update for non-literal column value (#16181) [#16183](https://github.com/vitessio/vitess/pull/16183)
 * [release-20.0-rc] Handle Nullability for Columns from Outer Tables (#16174) [#16186](https://github.com/vitessio/vitess/pull/16186)
 * [release-20.0] Handle Nullability for Columns from Outer Tables (#16174) [#16187](https://github.com/vitessio/vitess/pull/16187) 
#### TabletManager
 * mysqlctl: Improve handling of the lock file [#15404](https://github.com/vitessio/vitess/pull/15404)
 * Fix possible race in MySQL startup and vttablet in parallel [#15538](https://github.com/vitessio/vitess/pull/15538)
 * mysqlctld: setup a different default for onterm_timeout [#15575](https://github.com/vitessio/vitess/pull/15575)
 * Fix the race condition during vttablet startup [#15731](https://github.com/vitessio/vitess/pull/15731) 
#### Throttler
 * Throttler: fix nil pointer dereference error [#15180](https://github.com/vitessio/vitess/pull/15180)
 * Tablet throttler: starvation fix and consolidation of logic. [#15398](https://github.com/vitessio/vitess/pull/15398)
 * Dedicated poolDialer logic for VTOrc, throttler [#15562](https://github.com/vitessio/vitess/pull/15562)
 * Tablet throttler: recent check diff fix [#16001](https://github.com/vitessio/vitess/pull/16001)
 * `ApplySchema`: reroute `ALTER VITESS_MIGRATION ... THROTTLE ...` via `UpdateThrottlerConfig` [#16030](https://github.com/vitessio/vitess/pull/16030) 
#### Topology
 * discovery: Fix tablets removed from healthcheck when topo server GetTablet call fails [#15633](https://github.com/vitessio/vitess/pull/15633)
 * Fix ZooKeeper Topology connection locks not being cleaned up correctly [#15757](https://github.com/vitessio/vitess/pull/15757) 
#### VReplication
 * VReplication: disable foreign_key_checks for bulk data cleanup [#15261](https://github.com/vitessio/vitess/pull/15261)
 * VReplication: Make Target Sequence Initialization More Robust [#15289](https://github.com/vitessio/vitess/pull/15289)
 * MoveTables Atomic Mode: set FK checks off while deploying schema on targets [#15448](https://github.com/vitessio/vitess/pull/15448)
 * VReplication: Fix workflow filtering in GetWorkflows RPC [#15524](https://github.com/vitessio/vitess/pull/15524)
 * VReplication: Migrate intra-keyspace materialize workflows when Resharding the keyspace [#15536](https://github.com/vitessio/vitess/pull/15536)
 * VReplication: Fix workflow update changed handling [#15621](https://github.com/vitessio/vitess/pull/15621)
 * VReplication: Improve query buffering behavior during MoveTables traffic switching [#15701](https://github.com/vitessio/vitess/pull/15701)
 * VReplication: Take replication lag into account in VStreamManager healthcheck result processing [#15761](https://github.com/vitessio/vitess/pull/15761)
 * VReplication: Improve workflow cancel/delete [#15977](https://github.com/vitessio/vitess/pull/15977)
 * [release-20.0-rc] vtctldclient: Apply (Shard | Keyspace| Table) Routing Rules commands don't work (#16096) [#16125](https://github.com/vitessio/vitess/pull/16125)
 * [release-20.0] vtctldclient: Apply (Shard | Keyspace| Table) Routing Rules commands don't work (#16096) [#16126](https://github.com/vitessio/vitess/pull/16126)
 * [release-20.0-rc] VReplication: Improve workflow cancel/delete (#15977) [#16130](https://github.com/vitessio/vitess/pull/16130)
 * [release-20.0] VReplication: Improve workflow cancel/delete (#15977) [#16131](https://github.com/vitessio/vitess/pull/16131)
 * [release-20.0] CI Bug: Rename shard name back to match existing workflow file for vreplication_migrate_vdiff2_convert_tz (#16148) [#16151](https://github.com/vitessio/vitess/pull/16151)
 * [release-20.0] CI flaky test: Fix flakiness in vreplication_migrate_vdiff2_convert_tz (#16180) [#16189](https://github.com/vitessio/vitess/pull/16189)
 * [release-20.0-rc] VDiff CLI: Fix VDiff `show` bug (#16177) [#16199](https://github.com/vitessio/vitess/pull/16199)
 * [release-20.0] VDiff CLI: Fix VDiff `show` bug (#16177) [#16200](https://github.com/vitessio/vitess/pull/16200)
 * [release-20.0-rc] VReplication: handle escaped identifiers in vschema when initializing sequence tables (#16169) [#16218](https://github.com/vitessio/vitess/pull/16218)
 * [release-20.0] VReplication: handle escaped identifiers in vschema when initializing sequence tables (#16169) [#16219](https://github.com/vitessio/vitess/pull/16219)
 * [release-20.0-rc] VReplication Workflow: set state correctly when restarting workflow streams in the copy phase (#16217) [#16223](https://github.com/vitessio/vitess/pull/16223)
 * [release-20.0] VReplication Workflow: set state correctly when restarting workflow streams in the copy phase (#16217) [#16224](https://github.com/vitessio/vitess/pull/16224) 
#### VTAdmin
 * [VTAdmin API] Fix schema cache flag, add documentation [#15704](https://github.com/vitessio/vitess/pull/15704)
 * [VTAdmin] Remove vtctld web link, improve local example (#15607) [#15824](https://github.com/vitessio/vitess/pull/15824) 
#### VTCombo
 * Correctly set log_dir default in vtcombo [#15153](https://github.com/vitessio/vitess/pull/15153) 
#### VTorc
 * Use the legacy name for timeouts [#15689](https://github.com/vitessio/vitess/pull/15689)
 * Add timeout to all the contexts used for RPC calls in vtorc [#15991](https://github.com/vitessio/vitess/pull/15991) 
#### vtexplain
 * vtexplain: Fix setting up the column information [#15275](https://github.com/vitessio/vitess/pull/15275)
 * vtexplain: Ensure memory topo is set up for throttler [#15279](https://github.com/vitessio/vitess/pull/15279)
 * [release-20.0] Fix `vtexplain` not handling `UNION` queries with `weight_string` results correctly. (#16129) [#16158](https://github.com/vitessio/vitess/pull/16158) 
#### vttestserver
 * Revert unwanted logging change to `vttestserver` [#15148](https://github.com/vitessio/vitess/pull/15148)
 * use proper mysql version in the `vttestserver` images [#15235](https://github.com/vitessio/vitess/pull/15235) 
#### web UI
 * [VTAdmin] Remove vtctld web link, improve local example [#15607](https://github.com/vitessio/vitess/pull/15607)
### CI/Build 
#### Build/CI
 * CI: Use v3 of fossa-action and exclude maven [#15140](https://github.com/vitessio/vitess/pull/15140)
 * mysql: move colldump to its own standalone repository [#15166](https://github.com/vitessio/vitess/pull/15166)
 * Remove concurrency group for check labels workflow [#15197](https://github.com/vitessio/vitess/pull/15197)
 * CI: Use FOSSA push-only token for license scans on PRs [#15222](https://github.com/vitessio/vitess/pull/15222)
 * Update FOSSA license scan links [#15233](https://github.com/vitessio/vitess/pull/15233)
 * Update toolchain version in go.mod [#15245](https://github.com/vitessio/vitess/pull/15245)
 * statsd: Update to datadog-go v5 API [#15247](https://github.com/vitessio/vitess/pull/15247)
 * Ensure to use latest golangci-lint [#15413](https://github.com/vitessio/vitess/pull/15413)
 * Fix go.mod [#15416](https://github.com/vitessio/vitess/pull/15416)
 * bump `github.com/golang/protobuf` to `v1.5.4` [#15426](https://github.com/vitessio/vitess/pull/15426)
 * Update all actions setup to latest versions [#15443](https://github.com/vitessio/vitess/pull/15443)
 * CI: Disable CodeCov GitHub Changed Files Annotations [#15447](https://github.com/vitessio/vitess/pull/15447)
 * Update to latest CodeQL [#15530](https://github.com/vitessio/vitess/pull/15530)
 * Generate vtctldclient RPC client code from vtctlservice protobufs on make proto [#15561](https://github.com/vitessio/vitess/pull/15561)
 * Add @mattlord as CODEOWNER for vtctld[client] related things [#15870](https://github.com/vitessio/vitess/pull/15870)
 * Validate go versions in Static Code Checks CI [#15932](https://github.com/vitessio/vitess/pull/15932)
 * Add CODEOWNERS for tablet throttler and schemadiff [#16036](https://github.com/vitessio/vitess/pull/16036)
 * [release-20.0-rc] Add DCO workflow (#16052) [#16057](https://github.com/vitessio/vitess/pull/16057)
 * [release-20.0] Add DCO workflow (#16052) [#16058](https://github.com/vitessio/vitess/pull/16058)
 * [release-20.0-rc] Remove DCO workaround (#16087) [#16092](https://github.com/vitessio/vitess/pull/16092)
 * [release-20.0] Remove DCO workaround (#16087) [#16093](https://github.com/vitessio/vitess/pull/16093)
 * Revert "[release-20.0-rc] Bump to `v20.0.0-SNAPSHOT` after the `v20.00-RC1` release (#16142)" [#16144](https://github.com/vitessio/vitess/pull/16144) 
#### Docker
 * Docker/vtadmin: Update node version [#16145](https://github.com/vitessio/vitess/pull/16145)
 * [release-20.0] Docker: Update node vtadmin version (#16147) [#16161](https://github.com/vitessio/vitess/pull/16161) 
#### General
 * [main] Upgrade the Golang version to `go1.22.1` [#15405](https://github.com/vitessio/vitess/pull/15405)
 * Upgrade go version to go1.22.2 [#15642](https://github.com/vitessio/vitess/pull/15642)
 * [main] Upgrade the Golang version to `go1.22.3` [#15865](https://github.com/vitessio/vitess/pull/15865)
 * [release-20.0] Upgrade the Golang version to `go1.22.4` [#16060](https://github.com/vitessio/vitess/pull/16060)
 * [release-20.0-rc] [release-20.0] Upgrade the Golang version to `go1.22.4` (#16060) [#16064](https://github.com/vitessio/vitess/pull/16064) 
#### Online DDL
 * `onlineddl_scheduler` test: fix flakiness in artifact cleanup test [#15396](https://github.com/vitessio/vitess/pull/15396)
 * Online DDL: fix flaky `onlineddl_scheduler` CI test [#16011](https://github.com/vitessio/vitess/pull/16011) 
#### Throttler
 * CI upgrade/downgrade tests for Online DDL / throttler / vreplication flow [#16017](https://github.com/vitessio/vitess/pull/16017) 
#### VReplication
 * VReplication: Get workflowFlavorVtctl endtoend testing working properly again [#15636](https://github.com/vitessio/vitess/pull/15636) 
#### VTAdmin
 * Update VTAdmin build script [#15839](https://github.com/vitessio/vitess/pull/15839)
### Dependencies 
#### General
 * Update to the latest x/net [#15680](https://github.com/vitessio/vitess/pull/15680)
 * Revert GRPC context changes [#15780](https://github.com/vitessio/vitess/pull/15780)
 * Upgrade the Golang Dependencies [#15823](https://github.com/vitessio/vitess/pull/15823)
 * Upgrade the Golang Dependencies [#15942](https://github.com/vitessio/vitess/pull/15942) 
#### Observability
 * Bump vitess.io/vitess from 0.16.2 to 0.17.7 in /vitess-mixin [#15918](https://github.com/vitessio/vitess/pull/15918)
 * Update `github.com/Azure/go-autorest/autorest/adal` to fix Dependabot alert [#15986](https://github.com/vitessio/vitess/pull/15986) 
#### VTAdmin
 * Bump vite from 4.5.2 to 4.5.3 in /web/vtadmin [#15634](https://github.com/vitessio/vitess/pull/15634)
 * [release-20.0-rc] Update braces package (#16115) [#16119](https://github.com/vitessio/vitess/pull/16119)
 * [release-20.0] Update braces package (#16115) [#16120](https://github.com/vitessio/vitess/pull/16120) 
#### web UI
 * Remove highcharts dependency pt. 1 [#15970](https://github.com/vitessio/vitess/pull/15970)
### Documentation 
#### Authn/z
 * Add v20 changelog docs for PR #15030 [#15367](https://github.com/vitessio/vitess/pull/15367) 
#### Documentation
 * Fix docs for unmanaged tablets [#15437](https://github.com/vitessio/vitess/pull/15437)
 * [release-20.0-rc] Changelog 20.0: Fix broken links (#16048) [#16075](https://github.com/vitessio/vitess/pull/16075)
 * [release-20.0] Changelog 20.0: Fix broken links (#16048) [#16076](https://github.com/vitessio/vitess/pull/16076) 
#### General
 * Add Shopify to `ADOPTERS.md` [#15853](https://github.com/vitessio/vitess/pull/15853) 
#### Governance
 * amend contributing guide to ban trivial contributions [#15618](https://github.com/vitessio/vitess/pull/15618)
 * remove koz from active maintainer list [#15733](https://github.com/vitessio/vitess/pull/15733) 
#### Topology
 * Add lock shard docs [#15981](https://github.com/vitessio/vitess/pull/15981) 
#### VReplication
 * VDiff CLI: add missing target keyspace in VDiff command examples [#15525](https://github.com/vitessio/vitess/pull/15525) 
#### VTGate
 * Add changelogs for PR #15911 and #15919 [#16044](https://github.com/vitessio/vitess/pull/16044)
### Enhancement 
#### Backup and Restore
 * Point in time recovery and restore: assume (and validate) MySQL56 flavor in position arguments [#15599](https://github.com/vitessio/vitess/pull/15599)
 * mysqlctl: Improve backup restore compatibility check [#15856](https://github.com/vitessio/vitess/pull/15856) 
#### Build/CI
 * [main] Add `release-19.0` to the auto go upgrade (#15157) [#15168](https://github.com/vitessio/vitess/pull/15168)
 * Update paths filter action [#15254](https://github.com/vitessio/vitess/pull/15254)
 * Add memory check for runners for VTOrc tests [#15317](https://github.com/vitessio/vitess/pull/15317)
 * Assign and tag the release team for go update/upgrade auto PRs [#15737](https://github.com/vitessio/vitess/pull/15737) 
#### Cluster management
 * Add unmanaged tablet flag at vttablet level [#14871](https://github.com/vitessio/vitess/pull/14871)
 * Fix error message for planned reparent shard [#15529](https://github.com/vitessio/vitess/pull/15529)
 * Filter tablet map using valid candidates before reparenting to intermediate source [#15540](https://github.com/vitessio/vitess/pull/15540)
 * Add a default for `replica_net_timeout` [#15663](https://github.com/vitessio/vitess/pull/15663)
 * Add `GetServerStatus` RPC to use in PRS [#16022](https://github.com/vitessio/vitess/pull/16022) 
#### Docker
 * Remove MySQL/Percona from the `vitess/lite` Docker image [#15605](https://github.com/vitessio/vitess/pull/15605)
 * Remove `vitess/base` and `vitess/k8s` Docker images [#15620](https://github.com/vitessio/vitess/pull/15620) 
#### Driver
 * Add types to Go SQL driver [#15569](https://github.com/vitessio/vitess/pull/15569) 
#### Evalengine
 * evalEngine: Implement SPACE, REVERSE [#15173](https://github.com/vitessio/vitess/pull/15173)
 * evalengine: Implement LOCATE and friends [#15195](https://github.com/vitessio/vitess/pull/15195)
 * evalEngine: Implement string `INSERT` [#15201](https://github.com/vitessio/vitess/pull/15201)
 * evalengine: Implement BIN, OCT & CHAR functions [#15226](https://github.com/vitessio/vitess/pull/15226)
 * evalEngine: Implement ELT and FIELD [#15249](https://github.com/vitessio/vitess/pull/15249)
 * evalengine: Implement REPLACE [#15274](https://github.com/vitessio/vitess/pull/15274)
 * evalengine: Implement `TO_SECONDS` [#15590](https://github.com/vitessio/vitess/pull/15590)
 * evalengine: Fix temporal cases in `MAKETIME` [#15709](https://github.com/vitessio/vitess/pull/15709)
 * evalengine: Implement `SEC_TO_TIME` [#15755](https://github.com/vitessio/vitess/pull/15755)
 * evalengine: Add support for enum and set [#15783](https://github.com/vitessio/vitess/pull/15783) 
#### Examples
 * Update `operator.yaml` and add schedule backup example [#15969](https://github.com/vitessio/vitess/pull/15969) 
#### General
 * Enable gRPC Server Side Keepalive settings [#14939](https://github.com/vitessio/vitess/pull/14939) 
#### Observability
 * queryserving, observability: instrument vttablet query cache plan hits/misses [#14947](https://github.com/vitessio/vitess/pull/14947)
 * VTGate Warnings: Add `WarnUnshardedOnly` to warnings counter [#15033](https://github.com/vitessio/vitess/pull/15033)
 * VDiff: Add some stats [#15175](https://github.com/vitessio/vitess/pull/15175) 
#### Online DDL
 * DDL strategy flag `--unsafe-allow-foreign-keys` implies setting `FOREIGN_KEY_CHECKS=0` [#15432](https://github.com/vitessio/vitess/pull/15432)
 * `schemadiff`: `SubsequentDiffStrategy`: allow/reject multiple changes on same entity [#15675](https://github.com/vitessio/vitess/pull/15675)
 * Online DDL: unsupporting `gh-ost` DDL strategy [#15693](https://github.com/vitessio/vitess/pull/15693)
 * Online DDL: better support for range partitioning [#15698](https://github.com/vitessio/vitess/pull/15698) 
#### Query Serving
 * Limit concurrent creation of healthcheck gRPC connections [#15053](https://github.com/vitessio/vitess/pull/15053)
 * feat: use collation aware typing for UNION [#15122](https://github.com/vitessio/vitess/pull/15122)
 * Fix evalEngine functions for dates on/before `0000-02-29` [#15124](https://github.com/vitessio/vitess/pull/15124)
 * Subqueries in SET condition of UPDATE statement in presence of foreign keys [#15163](https://github.com/vitessio/vitess/pull/15163)
 * Feature: Adding support for Vindex Hints to allow for greater control over shard routing [#15172](https://github.com/vitessio/vitess/pull/15172)
 * Fix PRS from being blocked because of misbehaving clients [#15339](https://github.com/vitessio/vitess/pull/15339)
 * Filter by keyspace earlier in `tabletgateway`s `WaitForTablets(...)` [#15347](https://github.com/vitessio/vitess/pull/15347)
 * Update Planning for Limits in the presence of foreign keys [#15372](https://github.com/vitessio/vitess/pull/15372)
 * `schemadiff`: supporting textual diff [#15388](https://github.com/vitessio/vitess/pull/15388)
 * Use a throttled logger for exceeded memory warnings [#15424](https://github.com/vitessio/vitess/pull/15424)
 * `schemadiff`: support valid foreign key cycles [#15431](https://github.com/vitessio/vitess/pull/15431)
 * Handle panics during parallel execution [#15450](https://github.com/vitessio/vitess/pull/15450)
 * Optimize with IN Clause for UPDATE/DELETE Statements on Vindexes [#15455](https://github.com/vitessio/vitess/pull/15455)
 * `schemadiff`: remove `ForeignKeyLoopError` and loop detection logic [#15507](https://github.com/vitessio/vitess/pull/15507)
 * Add support for `row_alias` syntax added in MySQL 8.0.19. [#15510](https://github.com/vitessio/vitess/pull/15510)
 * test: failing unit test for type aggregation [#15518](https://github.com/vitessio/vitess/pull/15518)
 * Allow non-reserved-keywords for index names [#15602](https://github.com/vitessio/vitess/pull/15602)
 * feat: support IS UNKNOWN as synonym to IS NULL [#15673](https://github.com/vitessio/vitess/pull/15673)
 * Use Kill Query for Non-Transaction Query Execution and Update Query Timeout / Cancelled Error Message [#15694](https://github.com/vitessio/vitess/pull/15694)
 * Add schema tracking support for UDFs [#15705](https://github.com/vitessio/vitess/pull/15705)
 * Gen4 Planner: support aggregate UDFs [#15710](https://github.com/vitessio/vitess/pull/15710)
 * Prepare schema tracking for all UDFs [#15732](https://github.com/vitessio/vitess/pull/15732)
 * add udfs to vschema on update [#15771](https://github.com/vitessio/vitess/pull/15771)
 * fix: make sure string literals as columns are handled well [#15820](https://github.com/vitessio/vitess/pull/15820)
 * Improve `mcmp` type comparison [#15821](https://github.com/vitessio/vitess/pull/15821)
 * feat: optimise outer joins [#15840](https://github.com/vitessio/vitess/pull/15840)
 * `schemadiff`: atomic diffs for range partition `DROP PARTITION` statement [#15843](https://github.com/vitessio/vitess/pull/15843)
 * Add error transformer to vtgate executor [#15894](https://github.com/vitessio/vitess/pull/15894)
 * allow query timeout hints on shard targeting [#15898](https://github.com/vitessio/vitess/pull/15898)
 * feat: add support for WITH ROLLUP [#15930](https://github.com/vitessio/vitess/pull/15930)
 * `schemadiff`: ALTER TABLE is not INSTANT-able if adding column with default expression value [#16028](https://github.com/vitessio/vitess/pull/16028) 
#### TabletManager
 * Introducing `ExecuteMultiFetchAsDba` gRPC and `vtctldclient ExecuteMultiFetchAsDBA` command [#15506](https://github.com/vitessio/vitess/pull/15506) 
#### Throttler
 * VReplication: Add throttler stats [#15221](https://github.com/vitessio/vitess/pull/15221)
 * Throttler multi-metrics: proto changes [#16040](https://github.com/vitessio/vitess/pull/16040) 
#### Topology
 * Topo: Add version support to GetTopologyPath [#15933](https://github.com/vitessio/vitess/pull/15933) 
#### VReplication
 * VReplication: Enforce consistent order for table copies and diffs [#15152](https://github.com/vitessio/vitess/pull/15152)
 * VReplication: use proper column collations in vstreamer [#15313](https://github.com/vitessio/vitess/pull/15313)
 * VStream: Allow for automatic resume after Reshard across VStreams [#15395](https://github.com/vitessio/vitess/pull/15395)
 * VReplication: Remove auto_increment clauses for MoveTables to a sharded keyspace [#15679](https://github.com/vitessio/vitess/pull/15679)
 * VReplication: Move ENUM and SET mappings from vplayer to vstreamer [#15723](https://github.com/vitessio/vitess/pull/15723)
 * VReplication: Add stream DDL processing stats [#15769](https://github.com/vitessio/vitess/pull/15769)
 * Improve WaitForPos errors, don't include Result struct in message [#15962](https://github.com/vitessio/vitess/pull/15962) 
#### VTCombo
 * [vtcombo] Expose `--tablet_types_to_wait` flag [#14951](https://github.com/vitessio/vitess/pull/14951) 
#### VTGate
 * Add sql text counts stats to `vtcombo`,`vtgate`+`vttablet` [#15897](https://github.com/vitessio/vitess/pull/15897)
 * `vtgate`: support filtering tablets by tablet-tags  [#15911](https://github.com/vitessio/vitess/pull/15911)
 * Add support for sampling rate in `streamlog` [#15919](https://github.com/vitessio/vitess/pull/15919) 
#### VTorc
 * Improve VTOrc startup flow [#15315](https://github.com/vitessio/vitess/pull/15315)
 * Add api end point to print the current database state in VTOrc [#15485](https://github.com/vitessio/vitess/pull/15485)
 * Make `Durabler` interface methods public [#15548](https://github.com/vitessio/vitess/pull/15548)
 * VTOrc: Rework recovery registration [#15591](https://github.com/vitessio/vitess/pull/15591) 
#### vtctldclient
 * `proto`: lexical ordering of `ExecuteMultiFetchAsDBA` [#15558](https://github.com/vitessio/vitess/pull/15558) 
#### vttestserver
 * Add initialize-with-vt-dba-tcp flag to enable TCP/IP connection access to the underlying MySQL instance [#15354](https://github.com/vitessio/vitess/pull/15354)
### Feature Request 
#### Build/CI
 * CI workflows: Split long running vreplication workflows [#15834](https://github.com/vitessio/vitess/pull/15834) 
#### Cluster management
 * [vtctldclient] Add GetShardReplication [#15389](https://github.com/vitessio/vitess/pull/15389) 
#### Query Serving
 * Update with Limit Plan [#15107](https://github.com/vitessio/vitess/pull/15107)
 * Add support for Update Multi Table [#15211](https://github.com/vitessio/vitess/pull/15211)
 * Delete with subquery support [#15219](https://github.com/vitessio/vitess/pull/15219)
 * Multi Target Delete Support [#15294](https://github.com/vitessio/vitess/pull/15294)
 * Feature: Multi Target Update Support [#15402](https://github.com/vitessio/vitess/pull/15402)
 * Foreign Key: Add support for multi target delete [#15504](https://github.com/vitessio/vitess/pull/15504)
 * Foreign Key: Add support for Multi Table and Multi Target Update Statement [#15523](https://github.com/vitessio/vitess/pull/15523)
 * Respect Straight Join in Vitess query planning [#15528](https://github.com/vitessio/vitess/pull/15528)
 * feat: Add support for Insert with row alias [#15790](https://github.com/vitessio/vitess/pull/15790)
 * Add support for multi table update with non literal value [#15980](https://github.com/vitessio/vitess/pull/15980) 
#### Throttler
 * Tablet throttler: adding more stats [#15224](https://github.com/vitessio/vitess/pull/15224) 
#### VReplication
 * Experimental: Multi-tenant import support in Vitess [#15503](https://github.com/vitessio/vitess/pull/15503)
 * VDiff/OnlineDDL: add support for running VDiffs for OnlineDDL migrations [#15546](https://github.com/vitessio/vitess/pull/15546)
 * Multi-tenant MoveTables: Create vreplication streams only on specified shards [#15746](https://github.com/vitessio/vitess/pull/15746)
 * Multi-tenant MoveTables: allow switching replica/rdonly traffic separately before switching primary traffic [#15768](https://github.com/vitessio/vitess/pull/15768)
 * Multi-tenant migrations: add topo locking while updating keyspace routing rules [#15807](https://github.com/vitessio/vitess/pull/15807) 
#### VTorc
 * VTOrc optimize TMC usage [#15356](https://github.com/vitessio/vitess/pull/15356)
 * VTOrc checks and fixes replication misconfiguration issues [#15881](https://github.com/vitessio/vitess/pull/15881)
### Internal Cleanup 
#### Backup and Restore
 * endtoend: Remove usage of deprecated terminology [#15827](https://github.com/vitessio/vitess/pull/15827) 
#### Build/CI
 * [e2e] More vtctldclient updates in tests [#15276](https://github.com/vitessio/vitess/pull/15276)
 * wranger: Clean up leak check and use existing version [#15334](https://github.com/vitessio/vitess/pull/15334)
 * update andrew's email [#15495](https://github.com/vitessio/vitess/pull/15495)
 * Remove self-hosted runners in ci_workflow_gen [#15989](https://github.com/vitessio/vitess/pull/15989)
 * Linkname removal (step 1) [#16016](https://github.com/vitessio/vitess/pull/16016) 
#### Cluster management
 * go/vt/wrangler: pass reparent options structs [#15251](https://github.com/vitessio/vitess/pull/15251)
 * delete TestActionAndTimeout [#15322](https://github.com/vitessio/vitess/pull/15322)
 * Deprecate old reparent metrics and replace with new ones [#16031](https://github.com/vitessio/vitess/pull/16031) 
#### Docker
 * Revert the removal of the MySQL binaries in the `vitess/lite` image [#16042](https://github.com/vitessio/vitess/pull/16042)
 * [release-20.0-rc] Remove unnecessary Docker build workflows (#16196) [#16201](https://github.com/vitessio/vitess/pull/16201)
 * [release-20.0] Remove unnecessary Docker build workflows (#16196) [#16202](https://github.com/vitessio/vitess/pull/16202) 
#### Examples
 * Update env.sh so that is does not error when running on Mac [#15835](https://github.com/vitessio/vitess/pull/15835)
 * Local Examples: Add --binary-as-hex=false flag to mysql alias [#15996](https://github.com/vitessio/vitess/pull/15996) 
#### General
 * New for loops and some assert/require [#15194](https://github.com/vitessio/vitess/pull/15194)
 * Remove loopclosure captures from tests [#15202](https://github.com/vitessio/vitess/pull/15202)
 * Make `--pprof-http` default to false [#15260](https://github.com/vitessio/vitess/pull/15260)
 * discovery: Remove unused code [#15332](https://github.com/vitessio/vitess/pull/15332)
 * chore: remove repetitive words [#15449](https://github.com/vitessio/vitess/pull/15449)
 * Migrate to math/rand/v2 [#15513](https://github.com/vitessio/vitess/pull/15513)
 * Fix misorganized annotations [#15566](https://github.com/vitessio/vitess/pull/15566)
 * changelogs: squash 19.0.2/19.0.3 into just 19.0.3 and remove 19.0.2 [#15665](https://github.com/vitessio/vitess/pull/15665)
 * Cleanup usage of FLUSH PRIVILEGES [#15700](https://github.com/vitessio/vitess/pull/15700)
 * Upgrade the Golang Dependencies [#15743](https://github.com/vitessio/vitess/pull/15743)
 * grpc: Always pass through context for dialer [#15781](https://github.com/vitessio/vitess/pull/15781)
 * Switch to use semisync source / replica plugins [#15791](https://github.com/vitessio/vitess/pull/15791)
 * Use replica queries when available [#15808](https://github.com/vitessio/vitess/pull/15808) 
#### Messaging
 * messager: add consistent log prefix w/ table name [#15973](https://github.com/vitessio/vitess/pull/15973) 
#### Observability
 * Upgrade the golang version used `vitess-mixin` [#15972](https://github.com/vitessio/vitess/pull/15972) 
#### Online DDL
 * New unified internal table names format: part 2, generating new names [#15178](https://github.com/vitessio/vitess/pull/15178) 
#### Query Serving
 * Make tablet collation mismatch warning throttled [#15123](https://github.com/vitessio/vitess/pull/15123)
 * schemadiff: Clean up MySQL version from diff hints [#15210](https://github.com/vitessio/vitess/pull/15210)
 * refactor: change FuncExpr to use Exprs instead of SelectExprs [#15368](https://github.com/vitessio/vitess/pull/15368)
 * refactor: clean up semantics package [#15385](https://github.com/vitessio/vitess/pull/15385)
 * planbuilder: Cleanup unused logic [#15415](https://github.com/vitessio/vitess/pull/15415)
 * cleanup: make sure we use the right Offset [#15576](https://github.com/vitessio/vitess/pull/15576)
 * Add more fields to the marshal output of column [#15622](https://github.com/vitessio/vitess/pull/15622)
 * modify error message when transaction not found in numbered pool [#15760](https://github.com/vitessio/vitess/pull/15760)
 * Delete the deprecated pool size flags [#15844](https://github.com/vitessio/vitess/pull/15844)
 * refactor: introduce helper method to extract logic [#15939](https://github.com/vitessio/vitess/pull/15939)
 * refactor: remove logical plan interface [#16006](https://github.com/vitessio/vitess/pull/16006)
 * Decouple topotools from vschema [#16008](https://github.com/vitessio/vitess/pull/16008) 
#### TabletManager
 * srvtopo: Setup metrics in init() function [#15304](https://github.com/vitessio/vitess/pull/15304)
 * Revert "Skip for-loop alloc in `go/vt/discovery/healthcheck.go`" [#15328](https://github.com/vitessio/vitess/pull/15328)
 * mysqlctld: Remove unneeded resets in init_db.sql [#15832](https://github.com/vitessio/vitess/pull/15832)
 * mysql: Handle more deprecated SQL commands [#15907](https://github.com/vitessio/vitess/pull/15907) 
#### Throttler
 * Throttler: refactor stats variables [#15574](https://github.com/vitessio/vitess/pull/15574)
 * Tablet throttler: remove `LowPriority` logic [#16013](https://github.com/vitessio/vitess/pull/16013) 
#### Topology
 * topo: Clean up unused code [#15515](https://github.com/vitessio/vitess/pull/15515)
 * Etcd2Topo: Use node's ModRevision consistently for in-memory topo.Version value [#15847](https://github.com/vitessio/vitess/pull/15847)
 * Fix documentation for `--lock-timeout` [#16021](https://github.com/vitessio/vitess/pull/16021) 
#### VReplication
 * Remove Usage of VReplicationExec For _vt.vreplication Reads [#14424](https://github.com/vitessio/vitess/pull/14424)
 * VReplication: improve reliability of log management [#15374](https://github.com/vitessio/vitess/pull/15374)
 * delete unused code in vreplication e2e tests [#15378](https://github.com/vitessio/vitess/pull/15378)
 * MoveTables: remove option to specify source keyspace alias for multi-tenant migrations [#15712](https://github.com/vitessio/vitess/pull/15712)
 * Delete the deprecated vreplication tablet type flag [#15857](https://github.com/vitessio/vitess/pull/15857)
 * VReplication: Remove noisy logs [#15987](https://github.com/vitessio/vitess/pull/15987)
 * VReplication: refactor denied tables unit test, add couple more tests [#15995](https://github.com/vitessio/vitess/pull/15995) 
#### VTAdmin
 * Update Node version to current LTS release [#15822](https://github.com/vitessio/vitess/pull/15822) 
#### VTTablet
 * go/cmd: Audit and fix context.Background() usage [#15928](https://github.com/vitessio/vitess/pull/15928) 
#### VTorc
 * Remove unneeded loading of the MySQL driver [#15502](https://github.com/vitessio/vitess/pull/15502)
 * vtorc: Cleanup unused code [#15508](https://github.com/vitessio/vitess/pull/15508)
 * Remove reading emergently instances [#15580](https://github.com/vitessio/vitess/pull/15580)
 * Cleanup unused vtorc code [#15595](https://github.com/vitessio/vitess/pull/15595)
 * VTOrc: Cleanup node registration and unused code [#15617](https://github.com/vitessio/vitess/pull/15617)
 * Remove unused code in VTOrc [#15813](https://github.com/vitessio/vitess/pull/15813)
 * vtorc: Switch to Vitess stats [#15948](https://github.com/vitessio/vitess/pull/15948)
 * Deprecate old metrics in VTOrc and replace with new ones [#15994](https://github.com/vitessio/vitess/pull/15994) 
#### vtctl
 * Remove legacy `EmergencyReparentShard` stats [#15941](https://github.com/vitessio/vitess/pull/15941)
### Other 
#### Other
 * Match MySQL's `LAST_INSERT_ID` behaviour [#15697](https://github.com/vitessio/vitess/pull/15697)
### Performance 
#### General
 * prevent vtctld from creating tons of S3 connections [#15296](https://github.com/vitessio/vitess/pull/15296) 
#### Query Serving
 * Skip for-loop alloc in `go/vt/discovery/healthcheck.go` [#15326](https://github.com/vitessio/vitess/pull/15326)
 * logstats: do not allocate memory while logging [#15539](https://github.com/vitessio/vitess/pull/15539)
 * rewrite shuffleTablets to be clearer and more efficient [#15716](https://github.com/vitessio/vitess/pull/15716) 
#### TabletManager
 * Fix: transition to `math/rand/v2` for Improved Performance and Code Clarity [#15438](https://github.com/vitessio/vitess/pull/15438) 
#### VReplication
 * VReplication Workflows (RowStreamer): explicitly set read only when creating snapshots in the copy phase [#15690](https://github.com/vitessio/vitess/pull/15690) 
#### VTTablet
 * Improve performance for `BaseShowTablesWithSizes` query. [#15713](https://github.com/vitessio/vitess/pull/15713)
 * Do not load table stats when booting `vttablet`. [#15715](https://github.com/vitessio/vitess/pull/15715)
 * [release-20.0] Do not load table stats when booting `vttablet`. (#15715) [#16101](https://github.com/vitessio/vitess/pull/16101)
### Regression 
#### Query Serving
 * Fix routing rule query rewrite [#15253](https://github.com/vitessio/vitess/pull/15253)
 * fix: remove keyspace from column during query builder [#15514](https://github.com/vitessio/vitess/pull/15514)
 * Fix regression where inserts into reference tables with a different name on sharded keyspaces were not routed correctly. [#15796](https://github.com/vitessio/vitess/pull/15796)
 * fix: derived table join column expression to be part of add join predicate on rewrite [#15956](https://github.com/vitessio/vitess/pull/15956)
 * fix: insert on duplicate update to add list argument in the bind variables map [#15961](https://github.com/vitessio/vitess/pull/15961)
 * [release-20.0-rc] fix: order by subquery planning (#16049) [#16133](https://github.com/vitessio/vitess/pull/16133)
 * [release-20.0] fix: order by subquery planning (#16049) [#16134](https://github.com/vitessio/vitess/pull/16134)
 * [release-20.0-rc] feat: add a LIMIT 1 on EXISTS subqueries to limit network overhead (#16153) [#16192](https://github.com/vitessio/vitess/pull/16192)
 * [release-20.0] feat: add a LIMIT 1 on EXISTS subqueries to limit network overhead (#16153) [#16193](https://github.com/vitessio/vitess/pull/16193) 
#### Throttler
 * Enable 'heartbeat_on_demand_duration' in local/examples [#15204](https://github.com/vitessio/vitess/pull/15204) 
#### vttestserver
 * Fix logging issue when running in Docker with the syslog daemon disabled [#15176](https://github.com/vitessio/vitess/pull/15176)
### Release 
#### General
 * Bump to `v20.0.0-SNAPSHOT` after the `v19.0.0-RC1` release [#15138](https://github.com/vitessio/vitess/pull/15138)
 * Copy `v19.0.0-RC1` release notes on `main` [#15164](https://github.com/vitessio/vitess/pull/15164)
 * Copy `v19.0.0` release notes on `main` [#15417](https://github.com/vitessio/vitess/pull/15417)
 * Copy `v17.0.6` release notes on `main` [#15486](https://github.com/vitessio/vitess/pull/15486)
 * Copy `v18.0.3` release notes on `main` [#15488](https://github.com/vitessio/vitess/pull/15488)
 * Copy `v19.0.1` release notes on `main` [#15490](https://github.com/vitessio/vitess/pull/15490)
 * Copy `v19.0.2` release notes on `main` [#15647](https://github.com/vitessio/vitess/pull/15647)
 * Copy `v18.0.4` release notes on `main` [#15659](https://github.com/vitessio/vitess/pull/15659)
 * Copy `v19.0.3` release notes on `main` [#15661](https://github.com/vitessio/vitess/pull/15661)
 * Copy `v18.0.5` release notes on `main` [#15886](https://github.com/vitessio/vitess/pull/15886)
 * Copy `v19.0.4` release notes on `main` [#15887](https://github.com/vitessio/vitess/pull/15887)
 * Copy `v17.0.7` release notes on `main` [#15890](https://github.com/vitessio/vitess/pull/15890)
 * [release-20.0-rc] Code Freeze for `v20.0.0-RC1` [#16046](https://github.com/vitessio/vitess/pull/16046)
 * Bump to `v21.0.0-SNAPSHOT` after the `v20.0.0-RC1` release [#16047](https://github.com/vitessio/vitess/pull/16047)
 * [release-20.0-rc] Release of `v20.0.0-RC1` [#16137](https://github.com/vitessio/vitess/pull/16137)
 * [release-20.0] Copy `v20.0.0-RC1` release notes [#16141](https://github.com/vitessio/vitess/pull/16141)
 * [release-20.0-rc] Bump to `v20.0.0-SNAPSHOT` after the `v20.0.0-RC1` release [#16142](https://github.com/vitessio/vitess/pull/16142)
 * [release-20.0-rc] Bump to `v20.0.0-SNAPSHOT` after the `v20.0.0-RC1` release [#16146](https://github.com/vitessio/vitess/pull/16146)
### Testing 
#### Build/CI
 * Rewrite _many_ tests to use vtctldclient invocations, mostly non-output related stuff [#15270](https://github.com/vitessio/vitess/pull/15270)
 * [e2e] vtctld init tablet and some output-based commands [#15297](https://github.com/vitessio/vitess/pull/15297)
 * CI: Address data races on memorytopo Conn.closed [#15365](https://github.com/vitessio/vitess/pull/15365)
 * Reduce excessing logging in CI  [#15462](https://github.com/vitessio/vitess/pull/15462)
 * Split unit test and unit race into 2 components [#15734](https://github.com/vitessio/vitess/pull/15734)
 * CI: Address data race in TestSchemaVersioning [#15998](https://github.com/vitessio/vitess/pull/15998)
 * test: Replace t.fatalf with testify require [#16038](https://github.com/vitessio/vitess/pull/16038) 
#### CLI
 * Add required tests for `internal/flag` [#15220](https://github.com/vitessio/vitess/pull/15220)
 * test: Add missing tests and refactor existing tests for `go/flagutil` [#15789](https://github.com/vitessio/vitess/pull/15789) 
#### Cluster management
 * Fix Data race in tests introduced in #15934 [#15993](https://github.com/vitessio/vitess/pull/15993) 
#### General
 * Added unit tests for cmd/internal/docgen package [#15019](https://github.com/vitessio/vitess/pull/15019)
 * unit test for go/yaml2/yaml.go [#15027](https://github.com/vitessio/vitess/pull/15027)
 * Added unit tests for `go/cmd/rulesctl/` package [#15028](https://github.com/vitessio/vitess/pull/15028)
 * Added tests for the go/trace package [#15052](https://github.com/vitessio/vitess/pull/15052)
 * Added missing tests for the go/streamlog package [#15064](https://github.com/vitessio/vitess/pull/15064)
 * Added unit tests for vt/grpcclient package [#15072](https://github.com/vitessio/vitess/pull/15072)
 * modernize various tests [#15184](https://github.com/vitessio/vitess/pull/15184)
 * go1.22: remove outdated loopclosure captures in tests [#15227](https://github.com/vitessio/vitess/pull/15227)
 * chore: modernize tests [#15244](https://github.com/vitessio/vitess/pull/15244)
 * Add required tests for `go/netutil` [#15392](https://github.com/vitessio/vitess/pull/15392)
 * Add required tests for `go/stats/opentsdb` [#15394](https://github.com/vitessio/vitess/pull/15394)
 * test: Replace `t.fatalf` with testify `require` in `go/vt/schemamanager` [#15600](https://github.com/vitessio/vitess/pull/15600)
 * Remove mysql 5.7 tests that are no longer required [#15809](https://github.com/vitessio/vitess/pull/15809)
 * Fix unit-test-runner bug [#15815](https://github.com/vitessio/vitess/pull/15815)
 * test: Add tests for `go/ioutil` and refactor existing [#15885](https://github.com/vitessio/vitess/pull/15885)
 * test: Add required tests for `vt/key`, `timer` and `cache/theine/bf` [#15976](https://github.com/vitessio/vitess/pull/15976)
 * [release-20.0] CI Summary Addition [#16172](https://github.com/vitessio/vitess/pull/16172) 
#### Observability
 * VStreamer: add throttled logs when row/result/vstreamers get throttled. [#14936](https://github.com/vitessio/vitess/pull/14936) 
#### Query Serving
 * test: skip should mark the correct *testing.T [#15333](https://github.com/vitessio/vitess/pull/15333)
 * test: Add required tests for `go/mysql/collations/charset` [#15435](https://github.com/vitessio/vitess/pull/15435)
 * test: Add missing tests for `go/mysql/datetime` [#15501](https://github.com/vitessio/vitess/pull/15501)
 * schemadiff: add `EntityDiffByStatement`, a testing friendly utility [#15519](https://github.com/vitessio/vitess/pull/15519)
 * `schemadiff`: better nil check validation [#15526](https://github.com/vitessio/vitess/pull/15526)
 * Add testing for shard scoped foreign keys [#15571](https://github.com/vitessio/vitess/pull/15571)
 * chore: use interface instead of struct for tests [#15581](https://github.com/vitessio/vitess/pull/15581)
 * Fix AVG() sharded planning  [#15626](https://github.com/vitessio/vitess/pull/15626)
 * Run launchable in unit race [#15686](https://github.com/vitessio/vitess/pull/15686)
 * Make upgrade downgrade tests faster by removing redundancy [#15687](https://github.com/vitessio/vitess/pull/15687)
 * Fix Foreign key fuzzer to ignore rows affected [#15841](https://github.com/vitessio/vitess/pull/15841)
 * `schemadiff`: adding charset/collation tests [#15872](https://github.com/vitessio/vitess/pull/15872)
 * test: Add required tests for `go/logstats` [#15893](https://github.com/vitessio/vitess/pull/15893)
 * test: Add missing/required tests for `sqltypes` and `mathstats` [#15920](https://github.com/vitessio/vitess/pull/15920)
 * test: Cleaner plan tests output [#15922](https://github.com/vitessio/vitess/pull/15922) 
#### TabletManager
 * test: Add missing tests for `go/vt/mysqlctl` [#15585](https://github.com/vitessio/vitess/pull/15585)
 * test: Add e2e tests for `replication` [#15671](https://github.com/vitessio/vitess/pull/15671) 
#### Throttler
 * test: Use testify require/assert instead of t.Fatal/Error in `go/vt/throttler` [#15703](https://github.com/vitessio/vitess/pull/15703)
 * v20 backport: CI upgrade/downgrade tests for Online DDL / throttler / vreplication flow [#16065](https://github.com/vitessio/vitess/pull/16065)
 * [release-20.0-rc] v20 backport: CI upgrade/downgrade tests for Online DDL / throttler / vreplication flow (#16065) [#16082](https://github.com/vitessio/vitess/pull/16082) 
#### VReplication
 * VStreamer Unit Tests: framework to remove the need to specify serialized strings in row events for unit tests [#14903](https://github.com/vitessio/vitess/pull/14903)
 * VtctldClient Reshard: add e2e tests to confirm CLI options and fix discovered issues. [#15353](https://github.com/vitessio/vitess/pull/15353)
 * VStreamer unit test: port remaining tests to new framework [#15366](https://github.com/vitessio/vitess/pull/15366)
 * VReplication: Fix vtctldclient SwitchReads related bugs and move the TestBasicV2Workflows e2e test to vtctldclient [#15579](https://github.com/vitessio/vitess/pull/15579)
 * VStreamer unit tests: refactor pending test [#15845](https://github.com/vitessio/vitess/pull/15845) 
#### VTCombo
 * [release-20.0] Fix flaky tests that use vtcombo (#16178) [#16213](https://github.com/vitessio/vitess/pull/16213) 
#### VTorc
 * Add missing tests for `go/vt/vtorc/collection` [#15070](https://github.com/vitessio/vitess/pull/15070) 
#### vtctldclient
 * Add tests for GetTablets partial results [#15829](https://github.com/vitessio/vitess/pull/15829) 
#### vtexplain
 * [release-20.0] Fix flakiness in `vtexplain` unit test case. (#16159) [#16168](https://github.com/vitessio/vitess/pull/16168) 
#### vttestserver
 * Add `no_scatter` flag to vttestserver [#15670](https://github.com/vitessio/vitess/pull/15670)

