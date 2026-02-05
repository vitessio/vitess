# Changelog of Vitess v23.0.0

### Announcement 
#### Governance
 * PR Template: Add AI disclosure [#18638](https://github.com/vitessio/vitess/pull/18638)
### Bug fixes 
#### Backup and Restore
 * fix version issue when using --mysql-shell-speedup-restore=true [#18310](https://github.com/vitessio/vitess/pull/18310) 
#### Build/CI
 * [CI] Use the draft state from the event payload instead of calling `curl`. [#18650](https://github.com/vitessio/vitess/pull/18650) 
#### CLI
 * Normalize flag names at the `pflag` level. [#18642](https://github.com/vitessio/vitess/pull/18642) 
#### Cluster management
 * Add code to clear orphaned files for a deleted keyspace [#18098](https://github.com/vitessio/vitess/pull/18098) 
#### Evalengine
 * fix: Preserve multi-column TupleExpr in tuple simplifier [#18216](https://github.com/vitessio/vitess/pull/18216)
 * Fix evalengine crashes on unexpected types [#18254](https://github.com/vitessio/vitess/pull/18254) 
#### Examples
 * fix topo use in local_example [#18357](https://github.com/vitessio/vitess/pull/18357) 
#### General
 * Fix golang-ci lint file to allow a version without preceding `v` [#18110](https://github.com/vitessio/vitess/pull/18110) 
#### Observability
 * [release-23.0] Improve cgroup metric management (#18791) [#18801](https://github.com/vitessio/vitess/pull/18801) 
#### Online DDL
 * Online DDL: resume vreplication after cut-over/RENAME failure [#18428](https://github.com/vitessio/vitess/pull/18428) 
#### Query Serving
 * bugfix: allow window functions when possible to push down [#18103](https://github.com/vitessio/vitess/pull/18103)
 * Fix parsing to allow `VALUE` in insert and replace statements [#18116](https://github.com/vitessio/vitess/pull/18116)
 * Fix split statement for create procedure to account for definers [#18142](https://github.com/vitessio/vitess/pull/18142)
 * bugfix: INSERT IGNORE not inserting rows [#18151](https://github.com/vitessio/vitess/pull/18151)
 * Fix: Add tablet type to the plan key [#18155](https://github.com/vitessio/vitess/pull/18155)
 * make sure to give MEMBER OF the correct precedence [#18237](https://github.com/vitessio/vitess/pull/18237)
 * Fix subquery merging regression introduced in #11379 [#18260](https://github.com/vitessio/vitess/pull/18260)
 * Fix `SET` and `START TRANSACTION` in create procedure statements [#18279](https://github.com/vitessio/vitess/pull/18279)
 * fix: keep LIMIT/OFFSET even when merging UNION queries [#18361](https://github.com/vitessio/vitess/pull/18361)
 * Fix watcher storm during topo outages [#18434](https://github.com/vitessio/vitess/pull/18434)
 * go/vt/vtgate: handle dual tables in traffic mirroring [#18458](https://github.com/vitessio/vitess/pull/18458)
 * Fix scalar aggregation with literals in empty result sets [#18477](https://github.com/vitessio/vitess/pull/18477)
 * fix: handle aliases in UNIONs better [#18484](https://github.com/vitessio/vitess/pull/18484)
 * Fix for simple projection showing no fields [#18489](https://github.com/vitessio/vitess/pull/18489)
 * bugfix: Plan group by only on top of derived tables correctly [#18505](https://github.com/vitessio/vitess/pull/18505)
 *  Fix foreign key relation with routed tables [#18537](https://github.com/vitessio/vitess/pull/18537)
 * `srvtopo/query.go`: Do not query errors, fall back and refresh [#18589](https://github.com/vitessio/vitess/pull/18589)
 * Introduce aliases for foreign keys verify operations [#18601](https://github.com/vitessio/vitess/pull/18601)
 * fix: remove database qualifier after building query in operator to sql [#18602](https://github.com/vitessio/vitess/pull/18602)
 * CONNPOOL: Fix race condition when waiting for connection [#18713](https://github.com/vitessio/vitess/pull/18713)
 * [release-23.0] Fix handling of tuple bind variables in filtering operations. (#18736) [#18747](https://github.com/vitessio/vitess/pull/18747) 
#### TabletManager
 * Pass envs to mysqld process [#18561](https://github.com/vitessio/vitess/pull/18561) 
#### Throttler
 * Properly handle grpc dial errors in the throttler metric aggregation [#18073](https://github.com/vitessio/vitess/pull/18073)
 * Throttler: keep watching topo even on error [#18223](https://github.com/vitessio/vitess/pull/18223) 
#### Topology
 * Topo: Add NamedLock test for zk2 and consul and get them passing [#18407](https://github.com/vitessio/vitess/pull/18407) 
#### VDiff
 * Fix a panic in VDiff when reconciling extra rows. [#18585](https://github.com/vitessio/vitess/pull/18585) 
#### VReplication
 * VStream API: Reset stopPos in catchup [#18119](https://github.com/vitessio/vitess/pull/18119)
 * VStream API: add additional log lines for debugging frequent eof errors during catchup [#18124](https://github.com/vitessio/vitess/pull/18124)
 * Recover panic in vcopier due to closed channel [#18228](https://github.com/vitessio/vitess/pull/18228)
 * Atomic Copy: Handle error that was ignored while streaming tables and log it [#18313](https://github.com/vitessio/vitess/pull/18313)
 * Perform post copy actions in atomic copy [#18411](https://github.com/vitessio/vitess/pull/18411)
 * Avoid terminating atomic copy workflows on error if they are out of copy phase [#18475](https://github.com/vitessio/vitess/pull/18475)
 * VReplication: Fix bug while reading _vt.vreplication record [#18478](https://github.com/vitessio/vitess/pull/18478)
 * VReplication: Fix some switch writes related issues and logging [#18608](https://github.com/vitessio/vitess/pull/18608)
 * Vstreamer: idempotent gtid set (bugfix) [#18611](https://github.com/vitessio/vitess/pull/18611)
 * VStream: Try new tablet on purged binlog error [#18710](https://github.com/vitessio/vitess/pull/18710)
 * [release-23.0] VReplication: Ensure proper handling of keyspace/database names with dashes (#18762) [#18773](https://github.com/vitessio/vitess/pull/18773)
 * [release-23.0] VReplication: Treat ER_BINLOG_CREATE_ROUTINE_NEED_SUPER as unrecoverable (#18784) [#18820](https://github.com/vitessio/vitess/pull/18820) 
#### VTCombo
 * Flags migration: Make --vschema-ddl-authorized-users dependent on version [#18637](https://github.com/vitessio/vitess/pull/18637) 
#### VTGate
 * Make sure to check if the server is closed in etcd2topo [#18352](https://github.com/vitessio/vitess/pull/18352)
 * fix: ensure callbacks are not called after `VStream` returns [#18689](https://github.com/vitessio/vitess/pull/18689) 
#### VTTablet
 * Fix `vttablet` not being marked as not serving when MySQL stalls [#17883](https://github.com/vitessio/vitess/pull/17883)
 * Check `consultopo` loaded `>=1` credentials w/`-consul_auth_static_file` [#18152](https://github.com/vitessio/vitess/pull/18152)
 * Fix deadlock in semi-sync monitor [#18276](https://github.com/vitessio/vitess/pull/18276)
 * Fix: Deadlock in `Close` and `write` in semi-sync monitor. [#18359](https://github.com/vitessio/vitess/pull/18359)
 * [Bugfix] Broken Heartbeat system in Row Streamer [#18390](https://github.com/vitessio/vitess/pull/18390)
 * Reset in-memory sequence info on vttablet on UpdateSequenceTables request [#18415](https://github.com/vitessio/vitess/pull/18415)
 * Allow empty passwords for unmanaged tablet configuration [#18448](https://github.com/vitessio/vitess/pull/18448)
 * [release-23.0] repltracker: reset replica lag when we are primary (#18800) [#18807](https://github.com/vitessio/vitess/pull/18807)
 * [release-23.0] Fix bug where query consolidator returns empty result without error when the waiter cap exceeded (#18782) [#18833](https://github.com/vitessio/vitess/pull/18833) 
#### vtctl
 * fix: Fix `GenerateShardRanges` returning shard names that don't cover the full range [#18641](https://github.com/vitessio/vitess/pull/18641) 
#### vtctldclient
 * Fix deadlock in `ValidateKeyspace` [#18114](https://github.com/vitessio/vitess/pull/18114)
### CI/Build 
#### Build/CI
 * Add release-22.0 to the golang upgrade workflow [#18089](https://github.com/vitessio/vitess/pull/18089)
 * Update runners to use cncf oracle vms [#18425](https://github.com/vitessio/vitess/pull/18425)
 * Set up better dependency checks [#18508](https://github.com/vitessio/vitess/pull/18508)
 * Try updating the create PR workflow step [#18563](https://github.com/vitessio/vitess/pull/18563)
 * Update MySQL 8.4 to be the default [#18569](https://github.com/vitessio/vitess/pull/18569)
 * Update additional jobs to use 8.4 [#18592](https://github.com/vitessio/vitess/pull/18592)
 * Fix golden tests after disabling proto randomness hack #18325 [#18671](https://github.com/vitessio/vitess/pull/18671)
 * [release-23.0] ci: use the newest mysql apt config package (#18790) [#18794](https://github.com/vitessio/vitess/pull/18794) 
#### Docker
 * docker: add trixie and rm bullseye from build matrix [#18609](https://github.com/vitessio/vitess/pull/18609)
 * build `lite:mysql80` docker image [#18692](https://github.com/vitessio/vitess/pull/18692)
 * Build mysql80 Dockerfile with go1.25.1 [#18694](https://github.com/vitessio/vitess/pull/18694) 
#### General
 * [main] Upgrade the Golang version to `go1.24.2` [#18093](https://github.com/vitessio/vitess/pull/18093)
 * [main] Upgrade the Golang version to `go1.24.3` [#18241](https://github.com/vitessio/vitess/pull/18241)
 * [main] Upgrade the Golang version to `go1.24.4` [#18327](https://github.com/vitessio/vitess/pull/18327)
 * [main] Upgrade the Golang version to `go1.25.0` [#18573](https://github.com/vitessio/vitess/pull/18573)
 * [main] Upgrade the Golang version to `go1.25.1` [#18622](https://github.com/vitessio/vitess/pull/18622) 
#### Java
 * [release-23.0] update java packages to use central instead of ossrh (#18765) [#18767](https://github.com/vitessio/vitess/pull/18767) 
#### VReplication
 * Split workflow with flaky vdiff2 e2e test. Skip flaky Migrate test. [#18300](https://github.com/vitessio/vitess/pull/18300)
### Dependencies 
#### Build/CI
 * Bump subset of Go dependencies [#18577](https://github.com/vitessio/vitess/pull/18577) 
#### General
 * Upgrade the Golang Dependencies [#18082](https://github.com/vitessio/vitess/pull/18082)
 * Bump golang.org/x/net from 0.37.0 to 0.38.0 [#18177](https://github.com/vitessio/vitess/pull/18177)
 * Upgrade the Golang Dependencies [#18266](https://github.com/vitessio/vitess/pull/18266)
 * Bump github.com/go-viper/mapstructure/v2 from 2.2.1 to 2.3.0 [#18404](https://github.com/vitessio/vitess/pull/18404)
 * Main: Bump Go Dependencies [#18686](https://github.com/vitessio/vitess/pull/18686)
 * Upgrade the Golang Dependencies [#18711](https://github.com/vitessio/vitess/pull/18711) 
#### VTAdmin
 * Bump @babel/runtime from 7.26.0 to 7.27.6 in /web/vtadmin [#18467](https://github.com/vitessio/vitess/pull/18467)
 * Bump form-data from 4.0.1 to 4.0.4 in /web/vtadmin [#18473](https://github.com/vitessio/vitess/pull/18473)
 * Bump vite from 4.5.9 to 4.5.14 in /web/vtadmin [#18485](https://github.com/vitessio/vitess/pull/18485)
### Documentation 
#### Documentation
 * copy edit release notes to prepare for v22 GA [#18186](https://github.com/vitessio/vitess/pull/18186)
 * Update MAINTAINERS.md [#18394](https://github.com/vitessio/vitess/pull/18394)
 * tooling: changelog automatic reporting [#18600](https://github.com/vitessio/vitess/pull/18600) 
#### Governance
 * move vmg to emeritus [#18388](https://github.com/vitessio/vitess/pull/18388)
 * Update MAINTAINERS.md and CODEOWNERS [#18462](https://github.com/vitessio/vitess/pull/18462)
 * Update email in Maintainers file [#18472](https://github.com/vitessio/vitess/pull/18472)
 * Remove myself from the maintainer list [#18674](https://github.com/vitessio/vitess/pull/18674)
 * Update list of maintainers. [#18675](https://github.com/vitessio/vitess/pull/18675)
 * Update CODEOWNERS [#18697](https://github.com/vitessio/vitess/pull/18697) 
#### Query Serving
 * Add summary note change for unsharded `create procedure` support [#18148](https://github.com/vitessio/vitess/pull/18148)
### Enhancement 
#### Backup and Restore
 * make xtrabackup ShouldDrainForBackup configurable [#18431](https://github.com/vitessio/vitess/pull/18431) 
#### Build/CI
 * fix: update go-upgrade tool to check patch number (#18252) [#18402](https://github.com/vitessio/vitess/pull/18402)
 * Simplify workflow files. [#18649](https://github.com/vitessio/vitess/pull/18649) 
#### General
 * Refactor grpc flags for standardized naming  [#18009](https://github.com/vitessio/vitess/pull/18009)
 * Refactor flags - Part 3 [#18064](https://github.com/vitessio/vitess/pull/18064)
 * Refactor flags - Part 4 [#18095](https://github.com/vitessio/vitess/pull/18095)
 * Merge flags-refactor branch to main [#18280](https://github.com/vitessio/vitess/pull/18280)
 * Refactor `vtctld` flags to follow standard naming convention [#18294](https://github.com/vitessio/vitess/pull/18294)
 * Flags Refactor - Part 5 [#18296](https://github.com/vitessio/vitess/pull/18296)
 * Refactor `vtcombo` flags - Part 2 [#18297](https://github.com/vitessio/vitess/pull/18297)
 * Refactor `vtcombo` flags - Part 3 [#18298](https://github.com/vitessio/vitess/pull/18298)
 * Add support for querylog-emit-on-any-condition-met flag [#18546](https://github.com/vitessio/vitess/pull/18546)
 * support mysql protocol connection attributes [#18548](https://github.com/vitessio/vitess/pull/18548)
 * Flags migration: Most pending flags migrated for all binaries [#18624](https://github.com/vitessio/vitess/pull/18624) 
#### Observability
 * go/vt/discovery: configurable logger [#17846](https://github.com/vitessio/vitess/pull/17846)
 * Add logging to binlog watcher actions [#18264](https://github.com/vitessio/vitess/pull/18264)
 * Add support for sending grpc server backend metrics via ORCA [#18282](https://github.com/vitessio/vitess/pull/18282)
 * Configure datadog trace debug mode [#18347](https://github.com/vitessio/vitess/pull/18347)
 * Add TabletType to prometheus vttablet_tablet_server_stat… [#18451](https://github.com/vitessio/vitess/pull/18451)
 * log.Info -> log.Infof in tabletserver for reloading ACLs [#18461](https://github.com/vitessio/vitess/pull/18461)
 * Add support for querylog-time-threshold duration flag for query logging [#18520](https://github.com/vitessio/vitess/pull/18520)
 * fix: emit QueryExecutionsByTable for success cases only [#18584](https://github.com/vitessio/vitess/pull/18584) 
#### Online DDL
 * Online DDL: support `alter vitess_migration ... postpone complete` syntax [#18118](https://github.com/vitessio/vitess/pull/18118)
 * Online DDL metrics: `OnlineDDLStaleMigrationMinutes` [#18417](https://github.com/vitessio/vitess/pull/18417)
 * Online DDL cutover enhancements [#18423](https://github.com/vitessio/vitess/pull/18423)
 * Online DDL: set migration's progress to 100% once it's `ready_to_complete` [#18544](https://github.com/vitessio/vitess/pull/18544)
 * OnlineDDL: log changes to `ready_to_complete` [#18557](https://github.com/vitessio/vitess/pull/18557) 
#### Operator
 * Add fetchCredentials to operator.yaml [#18460](https://github.com/vitessio/vitess/pull/18460) 
#### Query Serving
 * Add Deferred Optimization Execution Metric [#18067](https://github.com/vitessio/vitess/pull/18067)
 * feat: support prepare even on plan failure to generate optimized plan on execution with values [#18126](https://github.com/vitessio/vitess/pull/18126)
 * Add TransactionsProcessed metric to track transactions at VTGate [#18171](https://github.com/vitessio/vitess/pull/18171)
 * Allow multi-shard read-only transactions in SINGLE transaction mode [#18173](https://github.com/vitessio/vitess/pull/18173)
 * Improve `UNION` query merging [#18289](https://github.com/vitessio/vitess/pull/18289)
 * Set parsed comments in operator for subqueries [#18369](https://github.com/vitessio/vitess/pull/18369)
 * avoid derived tables for UNION when possible [#18393](https://github.com/vitessio/vitess/pull/18393)
 * Parser: Support CREATE TABLE ... SELECT Statement (#17983) [#18443](https://github.com/vitessio/vitess/pull/18443)
 * bugfix: Fix impossible query for UNION [#18463](https://github.com/vitessio/vitess/pull/18463)
 * Planner: Add comprehensive debug logging for query planner NoRewrite cases [#18476](https://github.com/vitessio/vitess/pull/18476)
 * Fix sqlparser cannot parse 'set names binary' [#18582](https://github.com/vitessio/vitess/pull/18582) 
#### TabletManager
 * Switch to using the default caching sha2 password [#18010](https://github.com/vitessio/vitess/pull/18010)
 * Handle MySQL 9.x as New Flavor in getFlavor() [#18399](https://github.com/vitessio/vitess/pull/18399)
 * Refactor `mysqlctld` flags to follow standardized naming [#18432](https://github.com/vitessio/vitess/pull/18432)
 * Pass SSL config when checking connection to external mysql instance [#18481](https://github.com/vitessio/vitess/pull/18481) 
#### Throttler
 * [QueryThrottler] Implement Query Throttler [#18449](https://github.com/vitessio/vitess/pull/18449)
 * [Query Throttler] Add dry-run mode [#18657](https://github.com/vitessio/vitess/pull/18657) 
#### VReplication
 * LookupVindex: Multiple lookup tables support for `LookupVindexCreate` [#17566](https://github.com/vitessio/vitess/pull/17566)
 * VReplication: Add reference-tables to existing materialize workflow [#17804](https://github.com/vitessio/vitess/pull/17804)
 * Add TMC RPCs for updating sequences for `switchwrites` [#18172](https://github.com/vitessio/vitess/pull/18172)
 * Add a flag to vstream to exclude keyspace from table name [#18274](https://github.com/vitessio/vitess/pull/18274)
 * VStreamer: change in filter logic [#18319](https://github.com/vitessio/vitess/pull/18319)
 * VReplication: Improve permission check logic on external tablets on SwitchTraffic [#18348](https://github.com/vitessio/vitess/pull/18348)
 * VReplication: permission check logic on external tablets -- switch to a more practical solution [#18580](https://github.com/vitessio/vitess/pull/18580)
 * [release-23.0] VReplication: Handle compatible DDL changes (#18739) [#18749](https://github.com/vitessio/vitess/pull/18749) 
#### VTCombo
 * Refactor `vtcombo` flags - Part 1 [#18291](https://github.com/vitessio/vitess/pull/18291)
 * Refactor `vtcombo` flags - Part 4 [#18422](https://github.com/vitessio/vitess/pull/18422) 
#### VTGate
 * vtgateconn minor enhancements [#18551](https://github.com/vitessio/vitess/pull/18551) 
#### VTTablet
 * [vttablet] Emit Constant “userLabelDisabled” When skip-user-metrics is Enabled [#18085](https://github.com/vitessio/vitess/pull/18085)
 * `heartbeatWriter`: wrap error with more info [#18538](https://github.com/vitessio/vitess/pull/18538)
 * `tabletmanager`/`grpctmclient`: use `vtrpcpb`/gRPC error codes [#18565](https://github.com/vitessio/vitess/pull/18565)
 * `RestartReplication` RPC [#18628](https://github.com/vitessio/vitess/pull/18628)
 * connpool: Don't use go internal `sema` functionality [#18719](https://github.com/vitessio/vitess/pull/18719) 
#### VTorc
 * `vtorc`: allow recoveries to be disabled from startup [#18005](https://github.com/vitessio/vitess/pull/18005)
 * Vtorc: Recheck primary health before attempting a failure mitigation [#18234](https://github.com/vitessio/vitess/pull/18234)
 * `vtorc`: add keyspace/shard labels to recoveries stats [#18304](https://github.com/vitessio/vitess/pull/18304)
 * `vtorc`: implement `restartDirectReplicas` as response to `UnreachablePrimary` [#18626](https://github.com/vitessio/vitess/pull/18626) 
#### schema management
 * Query buffering, terminating blocking transactions for `INSTANT` DDL and other "special plans" [#17945](https://github.com/vitessio/vitess/pull/17945)
 * `schemadiff`: `RelatedForeignKeyTables()` [#18195](https://github.com/vitessio/vitess/pull/18195) 
#### vtctl
 * `EmergencyReparentShard`: include SQL thread position in most-advanced candidate selection  [#18531](https://github.com/vitessio/vitess/pull/18531) 
#### vtctldclient
 * `vtorc`: add support for dynamic enable/disable of ERS by keyspace/shard [#17985](https://github.com/vitessio/vitess/pull/17985)
 * Add vtctldclient TLS support to vtadmin GRPC [#18556](https://github.com/vitessio/vitess/pull/18556)
### Feature 
#### CLI
 * feat: Allow specifying the hex width to use when generating shard ranges. [#18633](https://github.com/vitessio/vitess/pull/18633) 
#### Online DDL
 * Feature(onlineddl): Add shard-specific completion to online ddl [#18331](https://github.com/vitessio/vitess/pull/18331) 
#### Query Serving
 * Add a new implementation for handling multiple queries without needing to split them [#18059](https://github.com/vitessio/vitess/pull/18059)
 * Add `transaction_timeout` session variable [#18560](https://github.com/vitessio/vitess/pull/18560) 
#### VReplication
 * VStream: Add flag to support copying only specific tables [#18184](https://github.com/vitessio/vitess/pull/18184)
### Internal Cleanup 
#### Backup and Restore
 * fix log message on mysqlshell backup [#18607](https://github.com/vitessio/vitess/pull/18607) 
#### Build/CI
 * ci: Replace `always()` with `!cancelled()`. [#18659](https://github.com/vitessio/vitess/pull/18659)
 * ci: Bump `actions/setup-go` to `v5.5.0`. [#18660](https://github.com/vitessio/vitess/pull/18660)
 * ci: Disable man-db auto updates. [#18665](https://github.com/vitessio/vitess/pull/18665)
 * Delete invalid comments. [#18666](https://github.com/vitessio/vitess/pull/18666)
 * ci: don't run codecov twice. [#18680](https://github.com/vitessio/vitess/pull/18680)
 * ci: run unit tests from different packages in parallel [#18703](https://github.com/vitessio/vitess/pull/18703) 
#### Cluster management
 * discovery: clarify use of TabletFilter in NewHealthCheck [#18512](https://github.com/vitessio/vitess/pull/18512) 
#### General
 * Flags migration: replace underscore flags in local examples with dashed ones [#18629](https://github.com/vitessio/vitess/pull/18629) 
#### Query Serving
 * Use transaction_isolation vs tx_isolation [#17845](https://github.com/vitessio/vitess/pull/17845)
 * asthelpergen: add design documentation [#18403](https://github.com/vitessio/vitess/pull/18403) 
#### VTGate
 * Remove deprecated methods and metrics [#18149](https://github.com/vitessio/vitess/pull/18149) 
#### VTorc
 * `vtorc`: cleanup `database_instance` location fields [#18339](https://github.com/vitessio/vitess/pull/18339)
 * `vtorc`: rename `isClusterWideRecovery` -> `isShardWideRecovery` [#18351](https://github.com/vitessio/vitess/pull/18351)
 * `vtorc`: remove dupe keyspace/shard in replication analysis [#18395](https://github.com/vitessio/vitess/pull/18395)
 * `vtorc`: move shard primary timestamp to time type [#18401](https://github.com/vitessio/vitess/pull/18401)
 * `vtorc`: update metrics in e2e test `TestAPIEndpoints` [#18406](https://github.com/vitessio/vitess/pull/18406)
 * `vtorc`: rename `ReplicationAnalysis` -> `DetectionAnalysis` [#18615](https://github.com/vitessio/vitess/pull/18615)
 * `vtorc`: cleanup timeout `context.Context` in keyspace/shard refresh [#18643](https://github.com/vitessio/vitess/pull/18643)
 * `vtorc`: add `Reason` and missing cases to `SkippedRecoveries` metric [#18644](https://github.com/vitessio/vitess/pull/18644)
 * `vtorc`: remove aggregated discovery metrics HTTP API, cleanup code [#18672](https://github.com/vitessio/vitess/pull/18672)
 * [release-23.0] `vtorc`: address CodeQL scanning alerts (#18753) [#18756](https://github.com/vitessio/vitess/pull/18756) 
#### schema management
 * tablet manager's `ExecuteFetchAsDba` does not allow multiple queries [#18183](https://github.com/vitessio/vitess/pull/18183) 
#### vtclient
 * Deprecate `github.com/mitchellh/mapstructure` library [#18687](https://github.com/vitessio/vitess/pull/18687)
### Performance 
#### Performance
 * GTID and other `go/mysql` and `vstreamer` optimizations [#18196](https://github.com/vitessio/vitess/pull/18196) 
#### VTTablet
 * Fix: Improve VDiff internal query performance [#18579](https://github.com/vitessio/vitess/pull/18579) 
#### VTorc
 * `vtorc`: use `replication.Mysql56GTIDSet` only for errant detection, delete `inst.OracleGtidSet` [#18627](https://github.com/vitessio/vitess/pull/18627)
### Regression 
#### General
 * Fix regression in v22 around new flag setup [#18507](https://github.com/vitessio/vitess/pull/18507) 
#### Query Serving
 * fix: handle dml query for None opcode [#18326](https://github.com/vitessio/vitess/pull/18326) 
#### Schema Tracker
 * Fix GetSchema RPC to prevent returning view definitions when EnableViews is disabled [#18513](https://github.com/vitessio/vitess/pull/18513)
### Release 
#### Documentation
 * Fix typo in v23 upgrading notes [#18586](https://github.com/vitessio/vitess/pull/18586) 
#### General
 * [main] Copy `v22.0.0-RC1` release notes [#18133](https://github.com/vitessio/vitess/pull/18133)
 * [main] Copy `v21.0.4` release notes [#18144](https://github.com/vitessio/vitess/pull/18144)
 * [main] Copy `v20.0.7` release notes [#18146](https://github.com/vitessio/vitess/pull/18146)
 * [main] Copy `v22.0.0-RC2` release notes [#18169](https://github.com/vitessio/vitess/pull/18169)
 * [main] Copy `v22.0.0-RC3` release notes [#18205](https://github.com/vitessio/vitess/pull/18205)
 * [main] Copy `v22.0.0` release notes [#18224](https://github.com/vitessio/vitess/pull/18224)
 * [main] Copy `v22.0.1` release notes [#18379](https://github.com/vitessio/vitess/pull/18379)
 * [main] Copy `v21.0.5` release notes [#18381](https://github.com/vitessio/vitess/pull/18381)
 * [main] Copy `v20.0.8` release notes [#18383](https://github.com/vitessio/vitess/pull/18383)
 * [release-23.0] Code Freeze for `v23.0.0-RC1` [#18729](https://github.com/vitessio/vitess/pull/18729)
 * [release-23.0] Release of `v23.0.0-RC1` [#18755](https://github.com/vitessio/vitess/pull/18755)
 * [release-23.0] Release of `v23.0.0-RC2` [#18840](https://github.com/vitessio/vitess/pull/18840)
 * [release-23.0] Bump to `v23.0.0-SNAPSHOT` after the `v23.0.0-RC2` release [#18842](https://github.com/vitessio/vitess/pull/18842)
 * update release notes [#18855](https://github.com/vitessio/vitess/pull/18855)
### Security 
#### Backup and Restore
 * [release-23.0] Address dir traversal in file backup storage `GetBackups` RPC (#18814) [#18818](https://github.com/vitessio/vitess/pull/18818) 
#### Java
 * [release-23.0] Resolve `commons-lang` vulnerability in Java driver (#18768) [#18797](https://github.com/vitessio/vitess/pull/18797) 
#### VTAdmin
 * [release-23.0] Address `Moderate` dependabot vulns in VTAdmin (#18744) [#18750](https://github.com/vitessio/vitess/pull/18750)
 * [release-23.0] vtadmin: upgrade vite to the latest (#18803) [#18812](https://github.com/vitessio/vitess/pull/18812) 
#### vtctldclient
 * [release-23.0] Potential fix for code scanning alert no. 2992: Clear-text logging of sensitive information (#18754) [#18760](https://github.com/vitessio/vitess/pull/18760)
 * [release-23.0] `vtctldclient GetPermissions`: hide `authentication_string` from response (#18771) [#18799](https://github.com/vitessio/vitess/pull/18799)
### Testing 
#### Build/CI
 * flaky test fix TestTrackerNoLock and TestCreateLookupVindexMultipleCreate [#18317](https://github.com/vitessio/vitess/pull/18317)
 * Deflake `TestVStreamsMetricsErrors`. [#18679](https://github.com/vitessio/vitess/pull/18679) 
#### Operator
 * Update `operator.yaml` [#18364](https://github.com/vitessio/vitess/pull/18364) 
#### Query Serving
 * test: start mysqld on a random directory [#18096](https://github.com/vitessio/vitess/pull/18096)
 * Change test compatibility [#18201](https://github.com/vitessio/vitess/pull/18201)
 * test: TestQueryTimeoutWithShardTargeting fix flaky test [#18242](https://github.com/vitessio/vitess/pull/18242)
 * json array insert test [#18284](https://github.com/vitessio/vitess/pull/18284)
 * Fix flaky test by being more permissive with the error message in `TestOverallQueryTimeout` [#18299](https://github.com/vitessio/vitess/pull/18299)
 * Fix running foreign key end-to-end tests on MySQL 8.0 [#18617](https://github.com/vitessio/vitess/pull/18617)
 * Simplify error handling assertions in Throttler FileBasedConfigLoader [#18688](https://github.com/vitessio/vitess/pull/18688) 
#### VReplication
 * Fix flakey vstream metrics test [#18287](https://github.com/vitessio/vitess/pull/18287)
 * test: Fix race condition in TestStreamRowsHeartbeat [#18414](https://github.com/vitessio/vitess/pull/18414)
 * CI: Fix `VDiff2` flaky e2e test [#18494](https://github.com/vitessio/vitess/pull/18494) 
#### VTTablet
 * Fix test flakines in semi-sync monitor tests [#18342](https://github.com/vitessio/vitess/pull/18342)
 * connpool: Bump the hang detection timeout to fix flakiness [#18722](https://github.com/vitessio/vitess/pull/18722)

