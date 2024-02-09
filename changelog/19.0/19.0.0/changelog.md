# Changelog of Vitess v19.0.0

### Announcement 
#### General
 * summary: updated summary with 19.0 changes [#15132](https://github.com/vitessio/vitess/pull/15132)
### Bug fixes 
#### Backup and Restore
 * MysqlCtl: implement missing `ReadBinlogFilesTimestamps` function [#14525](https://github.com/vitessio/vitess/pull/14525)
 * Replication: Have the DB flavor process waiting for a pos [#14745](https://github.com/vitessio/vitess/pull/14745) 
#### Build/CI
 * Update create_release.sh [#14492](https://github.com/vitessio/vitess/pull/14492)
 *  Set minimal tokens for auto_approve_pr [#14534](https://github.com/vitessio/vitess/pull/14534)
 * Run Go deps upgrade every week [#14910](https://github.com/vitessio/vitess/pull/14910) 
#### CLI
 * Fix anonymous paths in cobra code-gen [#14185](https://github.com/vitessio/vitess/pull/14185) 
#### Cluster management
 * Fix Panic in PRS due to a missing nil check [#14656](https://github.com/vitessio/vitess/pull/14656)
 * Fix hearbeatWriter Close being stuck if waiting for a semi-sync ACK [#14823](https://github.com/vitessio/vitess/pull/14823)
 * Block replication and query RPC calls until wait for dba grants has completed [#14836](https://github.com/vitessio/vitess/pull/14836)
 * Fix the parser to allow multiple strings one after the other [#15076](https://github.com/vitessio/vitess/pull/15076) 
#### Docker
 * [Docker] Fix VTadmin build  [#14363](https://github.com/vitessio/vitess/pull/14363) 
#### Evalengine
 * evalengine: Misc bugs [#14351](https://github.com/vitessio/vitess/pull/14351)
 * datetime: obey the evalengine's environment time [#14358](https://github.com/vitessio/vitess/pull/14358)
 * Fix nullability checks in evalengine [#14556](https://github.com/vitessio/vitess/pull/14556)
 * evalengine: Handle zero dates correctly [#14610](https://github.com/vitessio/vitess/pull/14610)
 * evalengine: Fix the min / max calculation for decimals [#14614](https://github.com/vitessio/vitess/pull/14614)
 * evalengine: Fix week overflow [#14859](https://github.com/vitessio/vitess/pull/14859)
 * evalengine: Return evalTemporal types for current date / time [#15079](https://github.com/vitessio/vitess/pull/15079) 
#### Examples
 * examples: fix flag syntax for zkctl [#14469](https://github.com/vitessio/vitess/pull/14469) 
#### General
 * viper: register dynamic config with both disk and live [#14453](https://github.com/vitessio/vitess/pull/14453)
 * Protect `ExecuteFetchAsDBA` against multi-statements, excluding a sequence of `CREATE TABLE|VIEW`. [#14954](https://github.com/vitessio/vitess/pull/14954)
 * Use the correct parser for truncation [#14985](https://github.com/vitessio/vitess/pull/14985)
 * Fix log format error in vttls.go [#15035](https://github.com/vitessio/vitess/pull/15035) 
#### Observability
 * Fix #14414:  resilient_server metrics name/prefix logic is inverted, leading to no metrics being recorded [#14415](https://github.com/vitessio/vitess/pull/14415) 
#### Online DDL
 * Online DDL: timeouts for all gRPC calls [#14182](https://github.com/vitessio/vitess/pull/14182)
 * OnlineDDL: fix scenarios where migration hangs instead of directly failing [#14290](https://github.com/vitessio/vitess/pull/14290)
 * schemadiff: fix missing `DROP CONSTRAINT` in duplicate/redundant constraints scenario. [#14387](https://github.com/vitessio/vitess/pull/14387) 
#### Query Serving
 * bugfix: use the proper interface for comment directives [#14267](https://github.com/vitessio/vitess/pull/14267)
 * evalengine: Use the right unknown type to initialize [#14313](https://github.com/vitessio/vitess/pull/14313)
 * engine: fix race when reading fields in Concatenate [#14324](https://github.com/vitessio/vitess/pull/14324)
 * tabletserver: do not consolidate streams on primary tablet when consolidator mode is `notOnPrimary` [#14332](https://github.com/vitessio/vitess/pull/14332)
 * Planner bugfix [#14365](https://github.com/vitessio/vitess/pull/14365)
 * semantics: Fix missing union pop from scoper [#14401](https://github.com/vitessio/vitess/pull/14401)
 * fix: mismatch in column count and value count [#14417](https://github.com/vitessio/vitess/pull/14417)
 * Make column resolution closer to MySQL [#14426](https://github.com/vitessio/vitess/pull/14426)
 * vtgate/engine: Fix race condition in join logic [#14435](https://github.com/vitessio/vitess/pull/14435)
 * Bug fix: Use target tablet from health stats cache when checking replication status [#14436](https://github.com/vitessio/vitess/pull/14436)
 * Ensure hexval and int don't share BindVar after Normalization [#14451](https://github.com/vitessio/vitess/pull/14451)
 * planbuilder bugfix: expose columns through derived tables [#14501](https://github.com/vitessio/vitess/pull/14501)
 * Fix missing query serving error code [#14520](https://github.com/vitessio/vitess/pull/14520)
 * Fix type coercion in cascading non-literal updates [#14524](https://github.com/vitessio/vitess/pull/14524)
 * Type Cast all update expressions in verify queries [#14555](https://github.com/vitessio/vitess/pull/14555)
 * expression rewriting: enable more rewrites and limit CNF rewrites [#14560](https://github.com/vitessio/vitess/pull/14560)
 * bug fix: stop all kinds of expressions from cnf-exploding [#14585](https://github.com/vitessio/vitess/pull/14585)
 * fix concurrency on stream execute engine primitives [#14586](https://github.com/vitessio/vitess/pull/14586)
 * bugfix: do not rewrite an expression twice [#14641](https://github.com/vitessio/vitess/pull/14641)
 * txserializer: change log message based on dry run [#14651](https://github.com/vitessio/vitess/pull/14651)
 * Vindexes: Pass context in consistent lookup handleDup [#14653](https://github.com/vitessio/vitess/pull/14653)
 * Fail correlated subquery in planning phase instead of a runtime error [#14701](https://github.com/vitessio/vitess/pull/14701)
 * bugfix: use the original expression and not the alias [#14704](https://github.com/vitessio/vitess/pull/14704)
 * Fix RegisterNotifier to use a copy of the tables to prevent data races [#14716](https://github.com/vitessio/vitess/pull/14716)
 * fix: flush tables with read lock to run only with reserved connection [#14720](https://github.com/vitessio/vitess/pull/14720)
 * TabletServer: Handle nil targets properly everywhere [#14734](https://github.com/vitessio/vitess/pull/14734)
 * bugfix: don't panic when missing schema information [#14787](https://github.com/vitessio/vitess/pull/14787)
 * schemadiff: allow char->varchar FK reference type matching [#14849](https://github.com/vitessio/vitess/pull/14849)
 * sqlparser: FORCE_CUTOVER is a non-reserved keyword [#14885](https://github.com/vitessio/vitess/pull/14885)
 * Improve err extraction logic [#14887](https://github.com/vitessio/vitess/pull/14887)
 * Add nil check to prevent panics [#14902](https://github.com/vitessio/vitess/pull/14902)
 * evalengine bugfix: handle nil evals correctly when coercing values [#14906](https://github.com/vitessio/vitess/pull/14906)
 * Vttablet panic in requests Wait [#14924](https://github.com/vitessio/vitess/pull/14924)
 * `schemadiff`: fix diffing of textual columns with implicit charsets [#14930](https://github.com/vitessio/vitess/pull/14930)
 * bugfix: Columns alias expanding [#14935](https://github.com/vitessio/vitess/pull/14935)
 * Fix panic for unknown columns in foreign key managed mode [#15025](https://github.com/vitessio/vitess/pull/15025)
 * Fix subquery cloning and dependencies [#15039](https://github.com/vitessio/vitess/pull/15039)
 * Fix `buffer_drain_concurrency` not doing anything [#15042](https://github.com/vitessio/vitess/pull/15042)
 * Copy expression types to avoid weight_strings and derived tables [#15069](https://github.com/vitessio/vitess/pull/15069)
 * Improve efficiency and accuracy of mysqld.GetVersionString [#15096](https://github.com/vitessio/vitess/pull/15096)
 * mysql: Ensure we set up the initial collation correctly [#15115](https://github.com/vitessio/vitess/pull/15115) 
#### Schema Tracker
 * discovery: fix crash with nil server vschema [#15086](https://github.com/vitessio/vitess/pull/15086) 
#### TabletManager
 * mysqlctl: Cleanup stale socket lockfile [#14553](https://github.com/vitessio/vitess/pull/14553)
 * mysqlctl: Fix cleaning up the stale lock file [#14600](https://github.com/vitessio/vitess/pull/14600)
 * tabletserver: Skip wait for DBA grants for external tablets [#14629](https://github.com/vitessio/vitess/pull/14629)
 * mysqlctl: Error out on stale socket [#14650](https://github.com/vitessio/vitess/pull/14650) 
#### Throttler
 * Throttler: set timeouts on gRPC communication and on topo communication [#14165](https://github.com/vitessio/vitess/pull/14165)
 * examples: rm heartbeat flags [#14980](https://github.com/vitessio/vitess/pull/14980) 
#### Topology
 * Ignore non-Shard keys in FindAllShardsInKeyspace List impl [#15117](https://github.com/vitessio/vitess/pull/15117) 
#### VReplication
 * VReplication: error on vtctldclient commands w/o tablet types [#14294](https://github.com/vitessio/vitess/pull/14294)
 * VDiff: wait for shard streams of one table diff to complete for before starting that of the next table [#14345](https://github.com/vitessio/vitess/pull/14345)
 * Vtctld SwitchReads: fix bug where writes were also being switched as part of switching reads when all traffic was switched using SwitchTraffic [#14360](https://github.com/vitessio/vitess/pull/14360)
 * VDiff tablet selection: pick non-serving tablets in Reshard workflows [#14413](https://github.com/vitessio/vitess/pull/14413)
 * VDiff: "show all" should only report vdiffs for the specified keyspace and workflow [#14442](https://github.com/vitessio/vitess/pull/14442)
 * VReplication: Properly Handle FK Constraints When Deferring Secondary Keys [#14543](https://github.com/vitessio/vitess/pull/14543)
 * Materializer: normalize schema via schemadiff on --atomic-copy [#14636](https://github.com/vitessio/vitess/pull/14636)
 * VReplication TableStreamer: Only stream tables in tablestreamer (ignore views) [#14646](https://github.com/vitessio/vitess/pull/14646)
 * VDiff: Fix vtctldclient limit bug [#14778](https://github.com/vitessio/vitess/pull/14778)
 * VReplication: Guard against unsafe _vt.vreplication writes [#14797](https://github.com/vitessio/vitess/pull/14797)
 * VReplication SwitchWrites: Properly return errors in SwitchWrites [#14800](https://github.com/vitessio/vitess/pull/14800)
 * VReplication: Update singular workflow in traffic switcher [#14826](https://github.com/vitessio/vitess/pull/14826)
 * Flakes: Fix flaky vtctl unit test TestMoveTables [#14886](https://github.com/vitessio/vitess/pull/14886)
 * VReplication: send unique key name to `rowstreamer`, which can then use with `FORCE INDEX` [#14916](https://github.com/vitessio/vitess/pull/14916)
 * VDiff: Make max diff duration upgrade/downgrade safe [#14995](https://github.com/vitessio/vitess/pull/14995) 
#### vtctl
 * VReplication: Add missing info to vtctldclient workflow SHOW output [#14225](https://github.com/vitessio/vitess/pull/14225) 
#### vtctldclient
 * vtctldclient: Apply tablet type filtering for keyspace+shard in GetTablets [#14467](https://github.com/vitessio/vitess/pull/14467)
### CI/Build 
#### Backup and Restore
 * Incremental backup: fix race condition in reading 'mysqlbinlog' output [#14330](https://github.com/vitessio/vitess/pull/14330) 
#### Build/CI
 * Enhance PR template + CI workflow for backport labels. [#14779](https://github.com/vitessio/vitess/pull/14779)
 * Update MySQL apt package and GPG signature [#14785](https://github.com/vitessio/vitess/pull/14785)
 * fix: build on delete operator [#14833](https://github.com/vitessio/vitess/pull/14833)
 * CI: Adjust FOSSA API secret name [#14918](https://github.com/vitessio/vitess/pull/14918)
 * CI: Tweak our code coverage profile behavior [#14967](https://github.com/vitessio/vitess/pull/14967)
 * Fix relevant files listing for `endtoend` CI [#15104](https://github.com/vitessio/vitess/pull/15104) 
#### Docker
 * Vitess MySQL Docker Image [#14158](https://github.com/vitessio/vitess/pull/14158)
 * Build and push Docker `vitess/vttestserver` DockerHub from GitHub Actions [#14314](https://github.com/vitessio/vitess/pull/14314)
 * Add `vtexplain` and `vtbackup` to base docker auto-build [#14318](https://github.com/vitessio/vitess/pull/14318) 
#### Evalengine
 * Fix codegen command with the right type [#14376](https://github.com/vitessio/vitess/pull/14376) 
#### General
 * [main] Upgrade the Golang version to `go1.21.2` [#14193](https://github.com/vitessio/vitess/pull/14193)
 * [main] Upgrade the Golang version to `go1.21.3` [#14231](https://github.com/vitessio/vitess/pull/14231)
 * [main] Upgrade the Golang version to `go1.21.4` [#14488](https://github.com/vitessio/vitess/pull/14488)
 * [main] Upgrade the Golang version to `go1.21.5` [#14689](https://github.com/vitessio/vitess/pull/14689)
 * connpool: fix racy test [#14731](https://github.com/vitessio/vitess/pull/14731) 
#### Online DDL
 * onlineddl_vrepl_stress: fix flakiness caused by timeouts [#14295](https://github.com/vitessio/vitess/pull/14295)
 * OnlineDDL: reduce vrepl_stress workload in forks [#14302](https://github.com/vitessio/vitess/pull/14302)
 * Online DDL: fix endtoend test dropping foreign key [#14522](https://github.com/vitessio/vitess/pull/14522)
 * VTGate/foreign keys stress test: add tests for 'standard' replica [#14747](https://github.com/vitessio/vitess/pull/14747) 
#### VTAdmin
 * Update vtadmin dependencies [#14336](https://github.com/vitessio/vitess/pull/14336)
 * Fix stray vtadmin package-lock.json content [#14350](https://github.com/vitessio/vitess/pull/14350) 
#### VTorc
 * docker: add dedicated vtorc container [#14126](https://github.com/vitessio/vitess/pull/14126)
### Dependabot 
#### General
 * Bump github.com/cyphar/filepath-securejoin from 0.2.3 to 0.2.4 [#14239](https://github.com/vitessio/vitess/pull/14239)
 * Bump golang.org/x/net from 0.14.0 to 0.17.0 [#14260](https://github.com/vitessio/vitess/pull/14260)
 * Bump google.golang.org/grpc from 1.55.0-dev to 1.59.0 [#14364](https://github.com/vitessio/vitess/pull/14364)
 * build(deps): bump golang.org/x/crypto from 0.16.0 to 0.17.0 [#14814](https://github.com/vitessio/vitess/pull/14814) 
#### Java
 * build(deps): bump com.google.guava:guava from 30.1.1-jre to 32.0.0-jre in /java [#14759](https://github.com/vitessio/vitess/pull/14759)
 * build(deps): bump io.netty:netty-handler from 4.1.93.Final to 4.1.94.Final in /java [#14863](https://github.com/vitessio/vitess/pull/14863) 
#### VTAdmin
 * Bump @cypress/request and cypress in /vitess-mixin/e2e [#14038](https://github.com/vitessio/vitess/pull/14038)
 * Bump postcss from 8.4.21 to 8.4.31 in /web/vtadmin [#14173](https://github.com/vitessio/vitess/pull/14173)
 * Bump @babel/traverse from 7.21.4 to 7.23.2 in /web/vtadmin [#14304](https://github.com/vitessio/vitess/pull/14304)
 * Bump @adobe/css-tools from 4.3.1 to 4.3.2 in /web/vtadmin [#14654](https://github.com/vitessio/vitess/pull/14654)
 * build(deps-dev): bump vite from 4.2.3 to 4.5.2 in /web/vtadmin [#15001](https://github.com/vitessio/vitess/pull/15001)
### Documentation 
#### CLI
 * Bypass cobra completion commands so they still function [#14217](https://github.com/vitessio/vitess/pull/14217) 
#### Documentation
 * release notes: edit summary for consistency [#14319](https://github.com/vitessio/vitess/pull/14319)
 * release notes: add FK import to summary [#14518](https://github.com/vitessio/vitess/pull/14518)
 * 19.0 release notes: ExecuteFetchAsDBA breaking change [#15021](https://github.com/vitessio/vitess/pull/15021) 
#### General
 * Add summary changes for recent PRs [#14598](https://github.com/vitessio/vitess/pull/14598)
 * Add summary changes to indicate MySQL 5.7 is EOL and Vitess is dropping support for it in v19 [#14663](https://github.com/vitessio/vitess/pull/14663)
### Enhancement 
#### Backup and Restore
 * increase vtctlclient backupShard command success rate [#14604](https://github.com/vitessio/vitess/pull/14604)
 * Backup: `--incremental-from-pos` supports backup name [#14923](https://github.com/vitessio/vitess/pull/14923)
 * Incremental backup: do not error on empty backup [#15022](https://github.com/vitessio/vitess/pull/15022) 
#### Build/CI
 * CI: Re-enable FOSSA scan and add Codecov [#14333](https://github.com/vitessio/vitess/pull/14333)
 * Automatic approval of `vitess-bot` clean backports [#14352](https://github.com/vitessio/vitess/pull/14352)
 * Tell shellcheck to follow sourced files [#14377](https://github.com/vitessio/vitess/pull/14377)
 * Add step to static check to ensure consistency of GHA workflows [#14724](https://github.com/vitessio/vitess/pull/14724) 
#### CLI
 * VReplication: Add traffic state to vtctldclient workflow status output [#14280](https://github.com/vitessio/vitess/pull/14280)
 * vtctldclient,grpcvtctldserver ApplySchema: return unknown params from grpcvtctldserver.ApplySchema, log them in vtctldclient.ApplySchema [#14672](https://github.com/vitessio/vitess/pull/14672) 
#### Cluster management
 * Add HealthCheck's `healthy` map to the VTGate UI [#14521](https://github.com/vitessio/vitess/pull/14521)
 * Make vttablet wait for vt_dba user to be granted privileges [#14565](https://github.com/vitessio/vitess/pull/14565)
 * Add wait for reading mycnf to prevent race [#14626](https://github.com/vitessio/vitess/pull/14626)
 * Add log for error to help debug [#14632](https://github.com/vitessio/vitess/pull/14632)
 * Take replication lag into account while selecting primary candidate [#14634](https://github.com/vitessio/vitess/pull/14634)
 * Postpone waiting for dba grants after restore has succeeded [#14680](https://github.com/vitessio/vitess/pull/14680)
 * vtctldclient: --strict rejects unknown vindex params in ApplyVSchema [#14862](https://github.com/vitessio/vitess/pull/14862)
 * Respect tolerable replication lag even when the new primary has been provided in PRS [#15090](https://github.com/vitessio/vitess/pull/15090) 
#### Docker
 * Build and push Docker `vitess/lite` to DockerHub from GitHub Actions [#14243](https://github.com/vitessio/vitess/pull/14243)
 * Build and push Docker `vitess/base` and component images to DockerHub from GitHub Actions [#14271](https://github.com/vitessio/vitess/pull/14271)
 * Be more explicit in release notes regarding the deprecation of certain `vitess/lite` tags [#15040](https://github.com/vitessio/vitess/pull/15040) 
#### Evalengine
 * evalengine: Improve the typing situation for functions [#14533](https://github.com/vitessio/vitess/pull/14533)
 * evalengine: Implement SUBSTRING [#14899](https://github.com/vitessio/vitess/pull/14899)
 * evalengine: Implement FROM_DAYS [#15058](https://github.com/vitessio/vitess/pull/15058)
 * evalengine: Implement TO_DAYS [#15065](https://github.com/vitessio/vitess/pull/15065)
 * evalengine: Add MID alias [#15066](https://github.com/vitessio/vitess/pull/15066)
 * evalEngine: Implement TIME_TO_SEC [#15094](https://github.com/vitessio/vitess/pull/15094) 
#### Examples
 * Tools: Remove dependencies installed by `make tools` [#14309](https://github.com/vitessio/vitess/pull/14309)
 * Deprecate `mysqld` in `vitess/lite` and use `mysql:8.0.30` image for the operator [#14990](https://github.com/vitessio/vitess/pull/14990) 
#### General
 * build: Allow compilation on Windows [#14718](https://github.com/vitessio/vitess/pull/14718) 
#### Observability
 * Debug vars: Expose build version in `/debug/vars` [#14713](https://github.com/vitessio/vitess/pull/14713)
 * [servenv] optional pprof endpoints [#14796](https://github.com/vitessio/vitess/pull/14796)
 * vtgate: increment vtgate_warnings counter for non atomic commits [#15010](https://github.com/vitessio/vitess/pull/15010)
 * query_executor: Record `WaitingForConnection` stat in all cases [#15073](https://github.com/vitessio/vitess/pull/15073) 
#### Online DDL
 * Online DDL: support DROP FOREIGN KEY statement [#14338](https://github.com/vitessio/vitess/pull/14338)
 * Online DDL: revert considerations for migrations with foreign key constraints [#14368](https://github.com/vitessio/vitess/pull/14368)
 * Enable Online DDL foreign key support (also in vtgate stress tests) when backing MySQL includes appropriate patch [#14370](https://github.com/vitessio/vitess/pull/14370)
 * Online DDL: lint DDL strategy flags [#14373](https://github.com/vitessio/vitess/pull/14373)
 * schemadiff: remove table name from auto-generated FK constraint name [#14385](https://github.com/vitessio/vitess/pull/14385)
 * TableGC: speed up GC process via `RequestChecks()`. Utilized by Online DDL for artifact cleanup [#14431](https://github.com/vitessio/vitess/pull/14431)
 * Support `fast_analyze_table` variable, introduced in public MySQL fork [#14494](https://github.com/vitessio/vitess/pull/14494)
 * Online DDL: edit CONSTRAINT names in CREATE TABLE [#14517](https://github.com/vitessio/vitess/pull/14517)
 * Online DDL: support migration cut-over backoff and forced cut-over [#14546](https://github.com/vitessio/vitess/pull/14546)
 * ApplySchema: log selected flags [#14798](https://github.com/vitessio/vitess/pull/14798)
 * schemadiff: using MySQL capabilities to analyze a SchemaDiff and whether changes are applicable instantly/immediately. [#14878](https://github.com/vitessio/vitess/pull/14878)
 * OnlineDDL to use schemadiff version capabilities; refactor some `flavor` code. [#14883](https://github.com/vitessio/vitess/pull/14883)
 * `schemadiff`: formalize `InstantDDLCapability` [#14900](https://github.com/vitessio/vitess/pull/14900) 
#### Query Serving
 * add option for warming reads to mirror primary read queries onto replicas from vtgates to warm bufferpools [#13206](https://github.com/vitessio/vitess/pull/13206)
 * Add support for new lock syntax in MySQL8 [#13965](https://github.com/vitessio/vitess/pull/13965)
 * gen4: Support explicit column aliases on derived tables [#14129](https://github.com/vitessio/vitess/pull/14129)
 * Gracefully shutdown VTGate instances [#14219](https://github.com/vitessio/vitess/pull/14219)
 * Mark non-unique lookup vindex as backfill to ignore vindex selection [#14227](https://github.com/vitessio/vitess/pull/14227)
 * UNION column type coercion [#14245](https://github.com/vitessio/vitess/pull/14245)
 * Add support for common table expressions [#14321](https://github.com/vitessio/vitess/pull/14321)
 * Add cycle detection for foreign keys [#14339](https://github.com/vitessio/vitess/pull/14339)
 * feat: support invisible columns [#14366](https://github.com/vitessio/vitess/pull/14366)
 * Add support for more queries [#14369](https://github.com/vitessio/vitess/pull/14369)
 * schemadiff: identify a FK sequential execution scenario, and more [#14397](https://github.com/vitessio/vitess/pull/14397)
 * Add support for AVG on sharded queries [#14419](https://github.com/vitessio/vitess/pull/14419)
 * Use hash joins when nested loop joins are not feasible [#14448](https://github.com/vitessio/vitess/pull/14448)
 * Make `Foreign_key_checks` a Vitess Aware variable [#14484](https://github.com/vitessio/vitess/pull/14484)
 * Add `SHOW VSCHEMA KEYSPACES` query [#14505](https://github.com/vitessio/vitess/pull/14505)
 * Support unlimited number of ORs in `ExtractINFromOR` [#14566](https://github.com/vitessio/vitess/pull/14566)
 * planbuilder: push down ordering through filter [#14583](https://github.com/vitessio/vitess/pull/14583)
 * refactor the INSERT engine primitive [#14606](https://github.com/vitessio/vitess/pull/14606)
 * Optimise hash joins [#14644](https://github.com/vitessio/vitess/pull/14644)
 * schemadiff: granular foreign key reference errors [#14682](https://github.com/vitessio/vitess/pull/14682)
 * schemadiff: pursue foreign key errors and proceed to build schema [#14705](https://github.com/vitessio/vitess/pull/14705)
 * schemadiff: additional FK column type matching rules [#14751](https://github.com/vitessio/vitess/pull/14751)
 * Fix order by and group by normalization [#14764](https://github.com/vitessio/vitess/pull/14764)
 * reduce NOWAIT usage to tables with unique keys for foreign key plans [#14772](https://github.com/vitessio/vitess/pull/14772)
 * vtgate: record warning for partially successful cross-shard commits [#14848](https://github.com/vitessio/vitess/pull/14848)
 * Added support for group_concat and count distinct with multiple expressions [#14851](https://github.com/vitessio/vitess/pull/14851)
 * Multi Table Delete Planner Support [#14855](https://github.com/vitessio/vitess/pull/14855)
 * Improve sharded query routing for tuple list [#14892](https://github.com/vitessio/vitess/pull/14892)
 * Make Schema Tracking case-sensitive [#14904](https://github.com/vitessio/vitess/pull/14904)
 * Explain Statement plan improvement [#14928](https://github.com/vitessio/vitess/pull/14928)
 * planner: support cross shard DELETE with LIMIT/ORDER BY [#14959](https://github.com/vitessio/vitess/pull/14959)
 * `transaction_mode` variable to return flag default if unset [#15032](https://github.com/vitessio/vitess/pull/15032)
 * evalengine: Implement LAST_DAY [#15038](https://github.com/vitessio/vitess/pull/15038)
 * `schemadiff`: analyze and report foreign key loops/cycles [#15062](https://github.com/vitessio/vitess/pull/15062)
 * Add support for multi table deletes with foreign keys [#15081](https://github.com/vitessio/vitess/pull/15081)
 * Add support for delete planning with limits in presence of foreign keys [#15097](https://github.com/vitessio/vitess/pull/15097) 
#### TabletManager
 * Allow for passing in the MySQL shutdown timeout [#14568](https://github.com/vitessio/vitess/pull/14568) 
#### Throttler
 * Tablet throttler: post 18 refactoring, race condition fixes, unit & race testing, deprecation of HTTP checks [#14181](https://github.com/vitessio/vitess/pull/14181) 
#### VReplication
 * vreplication timeout query optimizer hints [#13840](https://github.com/vitessio/vitess/pull/13840)
 * VReplication: Ensure that RowStreamer uses optimal index when possible [#13893](https://github.com/vitessio/vitess/pull/13893)
 * go/vt/wrangler: add len(qr.Rows) check to no streams found log msg [#14062](https://github.com/vitessio/vitess/pull/14062)
 * Migrate CreateLookupVindex and ExternalizeVindex to vtctldclient [#14086](https://github.com/vitessio/vitess/pull/14086)
 * set vreplication net read and net write timeout session vars to high values [#14203](https://github.com/vitessio/vitess/pull/14203)
 * allow tablet picker to exclude specified tablets from its candidate list [#14224](https://github.com/vitessio/vitess/pull/14224)
 * VReplication: Add --all-cells flag to create sub-commands [#14341](https://github.com/vitessio/vitess/pull/14341)
 * go/vt/wrangler: reduce VReplicationExec calls when getting copy state [#14375](https://github.com/vitessio/vitess/pull/14375)
 * VStream: Skip vindex keyrange filtering when we can [#14384](https://github.com/vitessio/vitess/pull/14384)
 * implement `--max-report-sample-rows` for VDiff [#14437](https://github.com/vitessio/vitess/pull/14437)
 * VReplication VPlayer: support statement and transaction batching [#14502](https://github.com/vitessio/vitess/pull/14502)
 * Snapshot connection: revert to explicit table locks when `FTWRL` is unavailable [#14578](https://github.com/vitessio/vitess/pull/14578)
 * VReplication: Improve replication plan error messages [#14752](https://github.com/vitessio/vitess/pull/14752)
 * VDiff: Support a max diff time for tables [#14786](https://github.com/vitessio/vitess/pull/14786)
 * VDiff: Support diffing tables without a defined Primary Key [#14794](https://github.com/vitessio/vitess/pull/14794) 
#### VTAdmin
 * Optimize the GetWorkflows RPC [#14212](https://github.com/vitessio/vitess/pull/14212) 
#### vtctldclient
 * Support cluster bootstrapping in vtctldclient [#14315](https://github.com/vitessio/vitess/pull/14315) 
#### vttestserver
 * Make vttestserver docker image work with vtctldclient [#14665](https://github.com/vitessio/vitess/pull/14665)
### Feature Request 
#### Build/CI
 * Automatically update the Golang dependencies using a CRON [#14891](https://github.com/vitessio/vitess/pull/14891) 
#### Evalengine
 * evalengine: implement AggregateEvalTypes [#15085](https://github.com/vitessio/vitess/pull/15085) 
#### Query Serving
 * Cache stream query plans in vttablet [#13264](https://github.com/vitessio/vitess/pull/13264)
 * Foreign key on update action with non literal values [#14278](https://github.com/vitessio/vitess/pull/14278)
 * `Replace into` statement plan with foreign keys : unsharded [#14396](https://github.com/vitessio/vitess/pull/14396)
 * Enable REPLACE INTO engine and Fix Foreign key locking issue [#14532](https://github.com/vitessio/vitess/pull/14532)
 * Add foreign key support for insert on duplicate key update [#14638](https://github.com/vitessio/vitess/pull/14638)
 * Multi Table Delete Support: join with reference table [#14784](https://github.com/vitessio/vitess/pull/14784)
 * `schemadiff`: `EnumReorderStrategy`, checking if enum or set values change ordinal [#15106](https://github.com/vitessio/vitess/pull/15106) 
#### VReplication
 * Provide subset of shards for certain VReplication Commands [#14873](https://github.com/vitessio/vitess/pull/14873)
### Internal Cleanup 
#### Backup and Restore
 * vtbackup: Fix copy pasta typo in option description [#14664](https://github.com/vitessio/vitess/pull/14664) 
#### Build/CI
 * Typo fix and remove unsupported branch for go version upgrade matrix [#14896](https://github.com/vitessio/vitess/pull/14896)
 * Reduce the frequency of the golang dependency upgrade CRON [#15008](https://github.com/vitessio/vitess/pull/15008)
 * Remove codebeat badge [#15116](https://github.com/vitessio/vitess/pull/15116) 
#### CLI
 * Make vtctldclient mount command more standard [#14281](https://github.com/vitessio/vitess/pull/14281)
 * remove deprecated flags from the codebase [#14544](https://github.com/vitessio/vitess/pull/14544)
 * cleanup deprecated flag types in tabletenv [#14733](https://github.com/vitessio/vitess/pull/14733) 
#### Cluster management
 * Enable verbose logging for some more RPCs [#14770](https://github.com/vitessio/vitess/pull/14770)
 * go/vt/topo: add error value to GetTablet logs [#14846](https://github.com/vitessio/vitess/pull/14846) 
#### Docker
 * Remove `MYSQL_FLAVOR` from all Docker images [#14159](https://github.com/vitessio/vitess/pull/14159) 
#### Documentation
 * Fix broken link in docker readme [#14222](https://github.com/vitessio/vitess/pull/14222)
 * Mention roadmap planning/modification in the release process [#14254](https://github.com/vitessio/vitess/pull/14254) 
#### Evalengine
 * refactor: introduce evalengine type and use it [#14292](https://github.com/vitessio/vitess/pull/14292)
 * sqlparser: export all Expr interfaces [#14371](https://github.com/vitessio/vitess/pull/14371)
 * evalengine: Proper support for bit literals [#14374](https://github.com/vitessio/vitess/pull/14374)
 * evalengine: fix numeric coercibility [#14473](https://github.com/vitessio/vitess/pull/14473)
 * evalengine: Internal cleanup and consistency fixes [#14854](https://github.com/vitessio/vitess/pull/14854) 
#### General
 * chore: unnecessary use of fmt.Sprintf [#14328](https://github.com/vitessio/vitess/pull/14328)
 * Miscellaneous typo fixes to comments  [#14472](https://github.com/vitessio/vitess/pull/14472)
 * Refactor: use NonEmpty() instead of !IsEmpty() [#14499](https://github.com/vitessio/vitess/pull/14499)
 * Fix license header typo [#14630](https://github.com/vitessio/vitess/pull/14630)
 * go/vt/vtgate: fix nilness issues [#14685](https://github.com/vitessio/vitess/pull/14685)
 * go/vt/vttablet: fix nilness issues [#14686](https://github.com/vitessio/vitess/pull/14686)
 * go/vt/vtadmin: fix nilness issues [#14687](https://github.com/vitessio/vitess/pull/14687)
 * go/cache: fix nilness issues and unused code [#14688](https://github.com/vitessio/vitess/pull/14688)
 * Keyspace ServedFrom: remove this deprecated attribute and related code [#14694](https://github.com/vitessio/vitess/pull/14694)
 * go/vt/topo: fix nilness issues and unused variables [#14709](https://github.com/vitessio/vitess/pull/14709)
 * go/vt/wrangler: fix nilness issues and unused variable [#14710](https://github.com/vitessio/vitess/pull/14710)
 * go/vt/vtctl: fix nilness issues and error scopes [#14711](https://github.com/vitessio/vitess/pull/14711)
 * mysql: Refactor out usage of servenv [#14732](https://github.com/vitessio/vitess/pull/14732)
 * Remove servenv usage and config flags from collations [#14781](https://github.com/vitessio/vitess/pull/14781)
 * Remove unused EventStreamer [#14783](https://github.com/vitessio/vitess/pull/14783)
 * Cleanup of dead code [#14799](https://github.com/vitessio/vitess/pull/14799)
 * go: resolve various nilness issues [#14803](https://github.com/vitessio/vitess/pull/14803)
 * sqlparser: Refactor out servenv and inject everywhere [#14822](https://github.com/vitessio/vitess/pull/14822)
 * Allow for building 32 bit libraries for subparts [#14841](https://github.com/vitessio/vitess/pull/14841)
 * Improve links in README [#14867](https://github.com/vitessio/vitess/pull/14867)
 * Use one canonical style for unlimited queries [#14870](https://github.com/vitessio/vitess/pull/14870)
 * Fix a number of CodeQL warnings [#14882](https://github.com/vitessio/vitess/pull/14882)
 * Update Go dependencies [#14888](https://github.com/vitessio/vitess/pull/14888)
 * Modify the release instructions to properly clone Vitess when using the vtop examples [#14889](https://github.com/vitessio/vitess/pull/14889)
 * Dead code cleanup [#14894](https://github.com/vitessio/vitess/pull/14894)
 * Refactor out more usage of servenv for mysql version [#14938](https://github.com/vitessio/vitess/pull/14938)
 * refac: deprecate `vitess/go/maps2` for `golang.org/x/exp/maps` [#14960](https://github.com/vitessio/vitess/pull/14960)
 * vtenv: Introduce vtenv for passing in collation & parser information [#14994](https://github.com/vitessio/vitess/pull/14994) 
#### Observability
 * Remove some logs that are logging excessively on large clusters [#14825](https://github.com/vitessio/vitess/pull/14825)
 * vstreamer: rm excessive logging [#14856](https://github.com/vitessio/vitess/pull/14856) 
#### Online DDL
 * New unified internal table names format, part 1: identifying and accepting new format tables [#14613](https://github.com/vitessio/vitess/pull/14613) 
#### Query Serving
 * Use panic instead of errors inside the operator package [#14085](https://github.com/vitessio/vitess/pull/14085)
 * sqlparser: normalize IndexInfo [#14177](https://github.com/vitessio/vitess/pull/14177)
 * refactor plan test cases [#14192](https://github.com/vitessio/vitess/pull/14192)
 * Rename `BinaryIsAtVersion` to `BinaryIsAtLeastAtVersion` [#14269](https://github.com/vitessio/vitess/pull/14269)
 * Refactor: foreign key in semantic analysis phase [#14273](https://github.com/vitessio/vitess/pull/14273)
 * Rename Foreign Key enum values in VSchema  and drop `FK_` prefix [#14274](https://github.com/vitessio/vitess/pull/14274)
 * Refactor: New operator InsertionSelection to adhere to the operator model [#14286](https://github.com/vitessio/vitess/pull/14286)
 * refactor: move more code from logical plans to ops [#14287](https://github.com/vitessio/vitess/pull/14287)
 * evalengine: serialize to SQL [#14337](https://github.com/vitessio/vitess/pull/14337)
 * vindexes: Efficient unicode hashing [#14395](https://github.com/vitessio/vitess/pull/14395)
 * tx throttler: remove unused topology watchers [#14412](https://github.com/vitessio/vitess/pull/14412)
 * sqlparser: Use KEY instead of INDEX for normalized form [#14416](https://github.com/vitessio/vitess/pull/14416)
 * tx_throttler: delete topo watcher metric instead of deprecating [#14445](https://github.com/vitessio/vitess/pull/14445)
 * Remove excessive VTGate logging of default planner selection [#14554](https://github.com/vitessio/vitess/pull/14554)
 * refactor: minor cleanups in planner code [#14642](https://github.com/vitessio/vitess/pull/14642)
 * planbuilder: clean up code [#14657](https://github.com/vitessio/vitess/pull/14657)
 * Pass on vindex errors with wrap than overriding them [#14737](https://github.com/vitessio/vitess/pull/14737)
 * refactor: remove more errors from operator planning [#14767](https://github.com/vitessio/vitess/pull/14767)
 * Change variable name for better readability [#14771](https://github.com/vitessio/vitess/pull/14771)
 * go/cache: use generics and remove unused API [#14850](https://github.com/vitessio/vitess/pull/14850)
 * Export `convertMySQLVersionToCommentVersion` to use it in vitess-operator [#14988](https://github.com/vitessio/vitess/pull/14988) 
#### TabletManager
 * logging: log time taken for tablet initialization only once [#14597](https://github.com/vitessio/vitess/pull/14597)
 * Replace use of `WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS` with `WAIT_FOR_EXECUTED_GTID_SET` [#14612](https://github.com/vitessio/vitess/pull/14612) 
#### Throttler
 * MaxReplicationLagModule.recalculateRate no longer fills the log [#14875](https://github.com/vitessio/vitess/pull/14875) 
#### VReplication
 * VReplication: VTTablet flag cleanup [#14297](https://github.com/vitessio/vitess/pull/14297) 
#### vtctl
 * Move all examples to vtctldclient [#14226](https://github.com/vitessio/vitess/pull/14226) 
#### vtctldclient
 * Fix typo for `--cells` flag help description in `ApplyRoutingRules` [#14721](https://github.com/vitessio/vitess/pull/14721)
### Performance 
#### Evalengine
 * Tiny Weights [#14402](https://github.com/vitessio/vitess/pull/14402) 
#### General
 * Replace usages of bytes.Buffer with strings.Builder [#14539](https://github.com/vitessio/vitess/pull/14539) 
#### Query Serving
 * Improved Connection Pooling [#14034](https://github.com/vitessio/vitess/pull/14034)
 * schemadiff: improved heuristic for dependent migration permutation evaluation time [#14249](https://github.com/vitessio/vitess/pull/14249)
 * mysql/conn: do not allocate during writes [#14482](https://github.com/vitessio/vitess/pull/14482)
 * Use GetTabletsByCell in healthcheck [#14693](https://github.com/vitessio/vitess/pull/14693)
 * mysql: do not allocate in parseOKPacket [#15067](https://github.com/vitessio/vitess/pull/15067)
 * mysql: remove more allocations from parseOKPacket [#15082](https://github.com/vitessio/vitess/pull/15082) 
#### Throttler
 * Throttler: Use tmclient pool for CheckThrottler tabletmanager RPC [#14979](https://github.com/vitessio/vitess/pull/14979) 
#### Topology
 * go/vt/topo: enable concurrency for FindAllShardsInKeyspace [#14670](https://github.com/vitessio/vitess/pull/14670)
 * Improve TopoServer Performance and Efficiency For Keyspace Shards [#15047](https://github.com/vitessio/vitess/pull/15047)
### Regression 
#### Query Serving
 * use aggregation engine over distinct engine when overlapping order by [#14359](https://github.com/vitessio/vitess/pull/14359)
 * Performance Fixes for Vitess 18 [#14383](https://github.com/vitessio/vitess/pull/14383)
 * tuple: serialized form [#14392](https://github.com/vitessio/vitess/pull/14392)
 * planbuilder: use OR for not in comparisons [#14607](https://github.com/vitessio/vitess/pull/14607)
 * add foreign key as part of set statement when reserved connection is needed [#14696](https://github.com/vitessio/vitess/pull/14696)
 * fix: insert on duplicate key update missing BindVars [#14728](https://github.com/vitessio/vitess/pull/14728)
 * Subquery inside aggregration function [#14844](https://github.com/vitessio/vitess/pull/14844)
### Release 
#### CLI
 * [main] Add vtctldclient info to the 18.0 summary (#14259) [#14265](https://github.com/vitessio/vitess/pull/14265) 
#### Documentation
 * Add release instructions for Milestones [#14175](https://github.com/vitessio/vitess/pull/14175)
 * Update v17 Release notes with a Breaking Change [#14215](https://github.com/vitessio/vitess/pull/14215) 
#### General
 * add release-18.0 to golang upgrade [#14133](https://github.com/vitessio/vitess/pull/14133)
 * moving main to 19.0 snapshot [#14137](https://github.com/vitessio/vitess/pull/14137)
 * update release notes on `main` after releases [#14171](https://github.com/vitessio/vitess/pull/14171)
 * tooling: don't add bots to authors list [#14411](https://github.com/vitessio/vitess/pull/14411)
 * move release notes of 18 to main [#14474](https://github.com/vitessio/vitess/pull/14474)
 * v18.0.1 release notes to main [#14579](https://github.com/vitessio/vitess/pull/14579)
 * port release notes of v18.0.2, v17.0.5 and v16.0.7 to main [#14840](https://github.com/vitessio/vitess/pull/14840)
 * [release-19.0] Code Freeze for `v19.0.0-RC1` [#15137](https://github.com/vitessio/vitess/pull/15137)
### Testing 
#### Backup and Restore
 * Add a retry to remove the vttablet directory during upgrade/downgrade backup tests [#14753](https://github.com/vitessio/vitess/pull/14753) 
#### Build/CI
 * Flakes: Shutdown vttablet before mysqld in backup tests [#14647](https://github.com/vitessio/vitess/pull/14647)
 * Tests: Fix vdiff test breakage from concurrent merge [#14865](https://github.com/vitessio/vitess/pull/14865)
 * Flakes: De-flake TestGatewayBufferingWhenPrimarySwitchesServingState [#14968](https://github.com/vitessio/vitess/pull/14968)
 * Remove usage of additional test package [#15007](https://github.com/vitessio/vitess/pull/15007)
 * Revert "exclude test from race" [#15014](https://github.com/vitessio/vitess/pull/15014)
 * Added missing tests for the sqltypes package [#15056](https://github.com/vitessio/vitess/pull/15056) 
#### CLI
 * Fixed bug in flagutil package and added tests [#15046](https://github.com/vitessio/vitess/pull/15046) 
#### General
 * Reduce wait time in test helpers [#14476](https://github.com/vitessio/vitess/pull/14476)
 * go/vt/tlstest: fix nilness issues [#14812](https://github.com/vitessio/vitess/pull/14812)
 * Add logging for failing tests in CI [#14821](https://github.com/vitessio/vitess/pull/14821)
 * bytes2: Add tests for StringUnsafe and Reset methods [#14940](https://github.com/vitessio/vitess/pull/14940)
 * Tests: Add test in syslogger for `LOG_EMERG` level [#14942](https://github.com/vitessio/vitess/pull/14942)
 * Add required tests for `go/acl` [#14943](https://github.com/vitessio/vitess/pull/14943)
 * Add missing test for `go/bucketpool` [#14944](https://github.com/vitessio/vitess/pull/14944)
 * test: adds test for acl [#14956](https://github.com/vitessio/vitess/pull/14956)
 * tests: improve coverage for `go/bytes2/buffer.go` [#14958](https://github.com/vitessio/vitess/pull/14958)
 * tests: add tests for `go/list` [#14962](https://github.com/vitessio/vitess/pull/14962)
 * tests: add tests for `go/json2` [#14964](https://github.com/vitessio/vitess/pull/14964)
 * tests: add tests for `go/protoutil/duration` [#14965](https://github.com/vitessio/vitess/pull/14965)
 * tests: add tests to `go/mathutil` [#14969](https://github.com/vitessio/vitess/pull/14969)
 * tests: add tests for `vitess/go/cmd/zk/internal/zkfilepath` [#14970](https://github.com/vitessio/vitess/pull/14970)
 * unit test for go/sets/set.go [#14973](https://github.com/vitessio/vitess/pull/14973)
 * Add required tests for `go/mysql/hex` [#14976](https://github.com/vitessio/vitess/pull/14976)
 * Remove `AppendFloat` from `go/mysql/format` and add required tests [#14986](https://github.com/vitessio/vitess/pull/14986)
 * tests: add tests for `go/sqlescape` [#14987](https://github.com/vitessio/vitess/pull/14987)
 * tests: add tests for `go/slice` [#14989](https://github.com/vitessio/vitess/pull/14989)
 * tests: Add tests for `go/textutil` [#14991](https://github.com/vitessio/vitess/pull/14991)
 * tests: increase coverage for multiple files in `vitess/go/stats` to 100% [#14997](https://github.com/vitessio/vitess/pull/14997)
 * tests: add tests for `zkfs` utilities [#15002](https://github.com/vitessio/vitess/pull/15002)
 * Add missing tests for `go/event/syslogger` [#15005](https://github.com/vitessio/vitess/pull/15005)
 * fix: `Unescape(Escape(str))` now returns the original string [#15009](https://github.com/vitessio/vitess/pull/15009)
 * tests: add tests for `go/vt/hook` [#15015](https://github.com/vitessio/vitess/pull/15015)
 * Added unit tests for the tools/codegen package [#15016](https://github.com/vitessio/vitess/pull/15016)
 * Added unit tests for the tools/releases package [#15017](https://github.com/vitessio/vitess/pull/15017)
 * tests: Add tests for `go/vt/external` [#15023](https://github.com/vitessio/vitess/pull/15023)
 * Move test files to regular names again [#15037](https://github.com/vitessio/vitess/pull/15037)
 * Add required tests for `go/unicode2` [#15051](https://github.com/vitessio/vitess/pull/15051)
 * tests: add tests for `go/mathstats` [#15054](https://github.com/vitessio/vitess/pull/15054)
 * Added tests for the go/vt/callinfo package [#15059](https://github.com/vitessio/vitess/pull/15059)
 * Added tests for the vt/logz package [#15060](https://github.com/vitessio/vitess/pull/15060)
 * Add required tests for `go/tb` [#15063](https://github.com/vitessio/vitess/pull/15063) 
#### Query Serving
 * Fix data race in `TestWarmingReads` [#14187](https://github.com/vitessio/vitess/pull/14187)
 * vtgate: Allow more errors for the warning check [#14421](https://github.com/vitessio/vitess/pull/14421)
 * vtgate: Allow additional errors in warnings test [#14461](https://github.com/vitessio/vitess/pull/14461)
 * Foreign Key Fuzzer Benchmark [#14542](https://github.com/vitessio/vitess/pull/14542)
 * test: enable test in downgrade testing [#14625](https://github.com/vitessio/vitess/pull/14625)
 * Refactor Upgrade downgrade tests [#14782](https://github.com/vitessio/vitess/pull/14782)
 * Add check to avoid runtime error and add tests for `go/mysql/fastparse` [#15000](https://github.com/vitessio/vitess/pull/15000)
 * tests: add tests for `vt/vtgate/engine/opcode` [#15045](https://github.com/vitessio/vitess/pull/15045)
 * tests: add tests to `go/vt/vtgate/semantics/bitset` [#15049](https://github.com/vitessio/vitess/pull/15049)
 * Added test for AnalyzeStrict [#15126](https://github.com/vitessio/vitess/pull/15126) 
#### Throttler
 * Throttler: refactor global configuration setting as throttler member [#14853](https://github.com/vitessio/vitess/pull/14853)
 * Throttler: fix race conditions in Operate() termination and in tests [#14971](https://github.com/vitessio/vitess/pull/14971) 
#### Topology
 * FlakyFix: `TestZk2Topo` [#14162](https://github.com/vitessio/vitess/pull/14162) 
#### VReplication
 * VReplication: extended e2e test for workflows with tables containing foreign key constraints [#14327](https://github.com/vitessio/vitess/pull/14327)
 * TestStreamMigrateMainflow: fix panic in test [#14420](https://github.com/vitessio/vitess/pull/14420)
 * Flaky TestFKExtWorkflow: fix Foreign Key stress test flakiness [#14714](https://github.com/vitessio/vitess/pull/14714)
 * Some VReplication e2e Refactoring [#14735](https://github.com/vitessio/vitess/pull/14735)
 * Test: Take test host/runner specs into account for VDiff diff duration test [#14868](https://github.com/vitessio/vitess/pull/14868)
 * vtctldclient CLI validation: Add e2e test to check that options to the vtctldclient commands are supported [#14957](https://github.com/vitessio/vitess/pull/14957) 
#### vtctl
 * Reduce flakiness in TestShardReplicationPositions [#14708](https://github.com/vitessio/vitess/pull/14708)

