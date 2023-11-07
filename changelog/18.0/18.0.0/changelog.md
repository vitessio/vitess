# Changelog of Vitess v18.0.0

### Bug fixes 
#### Backup and Restore
 * vtctldclient: Add missing new backup option [#13543](https://github.com/vitessio/vitess/pull/13543)
 * Backup: safe compressor/decompressor closure [#13668](https://github.com/vitessio/vitess/pull/13668)
 * Address vttablet memory usage with backups to Azure Blob Service [#13770](https://github.com/vitessio/vitess/pull/13770)
 * Do not drain tablet in incremental backup [#13773](https://github.com/vitessio/vitess/pull/13773)
 * go/cmd/vtbackup: wait for plugins to finish initializing [#14113](https://github.com/vitessio/vitess/pull/14113) 
#### Build/CI
 * Remove `os.Exit` in release-notes generation [#13310](https://github.com/vitessio/vitess/pull/13310)
 * CI: Fix make build related issues [#13583](https://github.com/vitessio/vitess/pull/13583)
 * Enable failures in `tools/e2e_test_race.sh` and fix races [#13654](https://github.com/vitessio/vitess/pull/13654)
 * Fix regular expression issue in Golang Upgrade and remove `release-14.0` from target branch [#13846](https://github.com/vitessio/vitess/pull/13846)
 * Make `Static Code Checks Etc` fail if the `./changelog` folder is out-of-date [#14003](https://github.com/vitessio/vitess/pull/14003) 
#### CLI
 * viperutil: Remove potential cross site reflecting issue [#13483](https://github.com/vitessio/vitess/pull/13483)
 * [vtctldclient] flags need to be defined to be deprecated [#13681](https://github.com/vitessio/vitess/pull/13681)
 * Fix missing deprecated flags in `vttablet` and `vtgate` [#13975](https://github.com/vitessio/vitess/pull/13975)
 * [release-18.0] Fix anonymous paths in cobra code-gen (#14185) [#14238](https://github.com/vitessio/vitess/pull/14238)
 * servenv: Remove double close() logic [#14457](https://github.com/vitessio/vitess/pull/14457)
 * [release-18.0] servenv: Remove double close() logic (#14457) [#14459](https://github.com/vitessio/vitess/pull/14459) 
#### Cluster management
 * Prevent resetting replication every time we set replication source [#13377](https://github.com/vitessio/vitess/pull/13377)
 * Don't run any reparent commands if the host is empty [#13396](https://github.com/vitessio/vitess/pull/13396)
 * [main] Upgrade-Downgrade Fix: Schema-initialization stuck on semi-sync ACKs while upgrading (#13411) [#13440](https://github.com/vitessio/vitess/pull/13440)
 * fix: error.as method usage to send pointer to the reference type expected [#13496](https://github.com/vitessio/vitess/pull/13496)
 * check keyspace snapshot time if none specified for backup restores [#13557](https://github.com/vitessio/vitess/pull/13557)
 * Flaky tests: Fix race in memory topo [#13559](https://github.com/vitessio/vitess/pull/13559)
 * ignore all error for views in engine reload [#13590](https://github.com/vitessio/vitess/pull/13590)
 * Fix `BackupShard` to get its options from its own flags [#13813](https://github.com/vitessio/vitess/pull/13813) 
#### Docker
 * Fix ubi8.arm64.mysql80 build package mirrorserver error [#13431](https://github.com/vitessio/vitess/pull/13431)
 * Fix dependencies in docker build script [#13520](https://github.com/vitessio/vitess/pull/13520)
 * Use NodeJS v18 in VTAdmin Dockerfile [#13751](https://github.com/vitessio/vitess/pull/13751)
 * [release-18.0] [Docker] Fix VTadmin build  (#14363) [#14378](https://github.com/vitessio/vitess/pull/14378) 
#### Evalengine
 * Fix a number of encoding issues when evaluating expressions with the evalengine [#13509](https://github.com/vitessio/vitess/pull/13509)
 * Fix type comparisons for Nullsafe* functions [#13605](https://github.com/vitessio/vitess/pull/13605)
 * fastparse: Fix bug in overflow detection [#13702](https://github.com/vitessio/vitess/pull/13702)
 * evalengine: Mark UUID() function as non-constant [#14051](https://github.com/vitessio/vitess/pull/14051)
 * [release-18.0] evalengine: Misc bugs (#14351) [#14354](https://github.com/vitessio/vitess/pull/14354) 
#### Examples
 * Use $hostname in vtadmin script as all other scripts do [#13231](https://github.com/vitessio/vitess/pull/13231)
 * Local example 101: idempotent on existing clusters [#13373](https://github.com/vitessio/vitess/pull/13373)
 * Examples: only terminate vtadmin if it was started [#13433](https://github.com/vitessio/vitess/pull/13433)
 * `examples/compose`: fix `consul:latest` error w/`docker-compose up -d` [#13468](https://github.com/vitessio/vitess/pull/13468) 
#### General
 * Flakes: Synchronize access to logErrStacks in vterrors [#13827](https://github.com/vitessio/vitess/pull/13827)
 * [release-18.0] viper: register dynamic config with both disk and live (#14453) [#14455](https://github.com/vitessio/vitess/pull/14455) 
#### Online DDL
 * Solve RevertMigration.Comment read/write concurrency issue [#13700](https://github.com/vitessio/vitess/pull/13700)
 * Fix closed channel `panic` in Online DDL cutover [#13729](https://github.com/vitessio/vitess/pull/13729)
 * OnlineDDL: fix nil 'completed_timestamp' for cancelled migrations [#13928](https://github.com/vitessio/vitess/pull/13928)
 * Fix `ApplySchema --batch-size` with ` --allow-zero-in-date` [#13951](https://github.com/vitessio/vitess/pull/13951)
 * TableGC: support DROP VIEW [#14020](https://github.com/vitessio/vitess/pull/14020)
 * OnlineDDL: cleanup cancelled migration artifacts; support `--retain-artifacts=<duration>` DDL strategy flag [#14029](https://github.com/vitessio/vitess/pull/14029)
 * bugfix: change column name and type to json [#14093](https://github.com/vitessio/vitess/pull/14093)
 * [Release 18.0]: Online DDL: timeouts for all gRPC calls (#14182) [#14189](https://github.com/vitessio/vitess/pull/14189) 
#### Query Serving
 * fix: GetField to use existing session for query [#13219](https://github.com/vitessio/vitess/pull/13219)
 * VReplication Workflows: make sequence tables follow routing rules [#13238](https://github.com/vitessio/vitess/pull/13238)
 * Adding random query generation for endtoend testing of the Gen4 planner [#13260](https://github.com/vitessio/vitess/pull/13260)
 * Bug fix: SQL queries erroring with message `unknown aggregation random` [#13330](https://github.com/vitessio/vitess/pull/13330)
 * bugfixes: collection of fixes to bugs found while fuzzing [#13332](https://github.com/vitessio/vitess/pull/13332)
 * bug: don't always wrap aggregation in coalesce [#13348](https://github.com/vitessio/vitess/pull/13348)
 * Random selection of keyspace based on available tablet [#13359](https://github.com/vitessio/vitess/pull/13359)
 * Enable Tcp keep alive and provide keep alive period setting [#13434](https://github.com/vitessio/vitess/pull/13434)
 * Improving random query generation for endtoend testing [#13460](https://github.com/vitessio/vitess/pull/13460)
 * ignore ongoing backfill vindex from routing selection [#13505](https://github.com/vitessio/vitess/pull/13505)
 * [release-17.0] ignore ongoing backfill vindex from routing selection [#13523](https://github.com/vitessio/vitess/pull/13523)
 * Fix flaky vtgate test TestInconsistentStateDetectedBuffering [#13560](https://github.com/vitessio/vitess/pull/13560)
 * Fix show character set [#13565](https://github.com/vitessio/vitess/pull/13565)
 * vtgate: fix race condition iterating tables and views from schema tracker [#13673](https://github.com/vitessio/vitess/pull/13673)
 * sqlparser: Track if original default value is a literal [#13730](https://github.com/vitessio/vitess/pull/13730)
 * Fix for "text type with an unknown/unsupported collation cannot be hashed" error [#13852](https://github.com/vitessio/vitess/pull/13852)
 * VTGate Buffering: Use a more accurate heuristic for determining if we're doing a reshard [#13856](https://github.com/vitessio/vitess/pull/13856)
 * sqlparser: Tablespace option is case sensitive [#13884](https://github.com/vitessio/vitess/pull/13884)
 * Rewrite `USING` to `ON` condition for joins [#13931](https://github.com/vitessio/vitess/pull/13931)
 * handle large number of predicates without timing out [#13979](https://github.com/vitessio/vitess/pull/13979)
 * Fix `NOT IN` expression used in the SET NULL for a child table on an update  [#13988](https://github.com/vitessio/vitess/pull/13988)
 * Fix the `SELECT` query we run on the child table to verify that update is allowed on a RESTRICT constraint [#13991](https://github.com/vitessio/vitess/pull/13991)
 * fix data race in join engine primitive olap streaming mode execution [#14012](https://github.com/vitessio/vitess/pull/14012)
 * fix: cost to include subshard opcode [#14023](https://github.com/vitessio/vitess/pull/14023)
 * Add session flag for stream execute grpc api [#14046](https://github.com/vitessio/vitess/pull/14046)
 * Fix cascading Delete failure while using Prepared statements [#14048](https://github.com/vitessio/vitess/pull/14048)
 * Fix Fk verification and update queries to accommodate for bindVariables being NULL [#14061](https://github.com/vitessio/vitess/pull/14061)
 * DDL execution to commit open transaction [#14110](https://github.com/vitessio/vitess/pull/14110)
 * fix: analyze statement parsing and planning [#14268](https://github.com/vitessio/vitess/pull/14268)
 * [release-18.0] fix: analyze statement parsing and planning (#14268) [#14275](https://github.com/vitessio/vitess/pull/14275)
 * [release-18.0] schemadiff: fix missing `DROP CONSTRAINT` in duplicate/redundant constraints scenario. (#14387) [#14391](https://github.com/vitessio/vitess/pull/14391)
 * [release-18.0] vtgate/engine: Fix race condition in join logic (#14435) [#14441](https://github.com/vitessio/vitess/pull/14441) 
#### Schema Tracker
 * Vttablet schema tracking: Fix _vt.schema_version corruption [#13045](https://github.com/vitessio/vitess/pull/13045)
 * Ignore error while reading table data in Schema.Engine reload [#13421](https://github.com/vitessio/vitess/pull/13421)
 * schema.Reload(): ignore column reading errors for views only, error for tables [#13442](https://github.com/vitessio/vitess/pull/13442) 
#### TabletManager
 * mysqlctl: Correctly encode database and table names [#13312](https://github.com/vitessio/vitess/pull/13312)
 * Fix remote VersionString API [#13484](https://github.com/vitessio/vitess/pull/13484)
 * mysqlctl: Remove noisy log line [#13599](https://github.com/vitessio/vitess/pull/13599)
 * GetSchema: limit concurrent operations [#13617](https://github.com/vitessio/vitess/pull/13617)
 * mysqlctl: Reduce logging for running commands [#13659](https://github.com/vitessio/vitess/pull/13659) 
#### Throttler
 * Tablet throttler: only start watching SrvKeyspace once it's confirmed to exist [#13384](https://github.com/vitessio/vitess/pull/13384)
 * Throttler: reintroduce deprecated flags so that deprecation actually works [#13597](https://github.com/vitessio/vitess/pull/13597)
 * Silence 'CheckThrottler' gRPC calls [#13925](https://github.com/vitessio/vitess/pull/13925)
 * Tablet throttler: empty list of probes on non-leader [#13926](https://github.com/vitessio/vitess/pull/13926)
 * [release-18.0] Throttler: set timeouts on gRPC communication and on topo communication (#14165) [#14167](https://github.com/vitessio/vitess/pull/14167)
 * Tablet throttler: fix race condition by removing goroutine call [#14179](https://github.com/vitessio/vitess/pull/14179)
 * [release-18.0] Tablet throttler: fix race condition by removing goroutine call (#14179) [#14198](https://github.com/vitessio/vitess/pull/14198) 
#### VReplication
 * VReplication: Fix VDiff2 DeleteByUUID Query [#13255](https://github.com/vitessio/vitess/pull/13255)
 * Better handling of vreplication setState() failure [#13488](https://github.com/vitessio/vitess/pull/13488)
 * VReplication: Ignore unrelated shards in partial MoveTables traffic state [#13515](https://github.com/vitessio/vitess/pull/13515)
 * VReplication: Ensure ROW events are sent within a transaction [#13547](https://github.com/vitessio/vitess/pull/13547)
 * VReplication: Make Source Tablet Selection More Robust [#13582](https://github.com/vitessio/vitess/pull/13582)
 * vtgate tablet gateway buffering: don't shutdown if not initialized [#13695](https://github.com/vitessio/vitess/pull/13695)
 * VReplication: Improve MoveTables Create Error Handling [#13737](https://github.com/vitessio/vitess/pull/13737)
 * Minor --initialize-target-sequences followups [#13758](https://github.com/vitessio/vitess/pull/13758)
 * Flakes: skip flaky check that ETA for a VReplication VDiff2 Progress command is in the future. [#13804](https://github.com/vitessio/vitess/pull/13804)
 * Flakes: VReplication unit tests: reduce goroutine leakage [#13824](https://github.com/vitessio/vitess/pull/13824)
 * Properly support ignore_nulls in CreateLookupVindex [#13913](https://github.com/vitessio/vitess/pull/13913)
 * VReplication: Handle SQL NULL and JSON 'null' correctly for JSON columns [#13944](https://github.com/vitessio/vitess/pull/13944)
 * copy over existing vreplication rows copied to local counter if resuming from another tablet [#13949](https://github.com/vitessio/vitess/pull/13949)
 * VDiff: correct handling of default source and target cells [#13969](https://github.com/vitessio/vitess/pull/13969)
 * MoveTables Cancel: drop denied tables on target when dropping source/target tables [#14008](https://github.com/vitessio/vitess/pull/14008)
 * VReplication VPlayer: set foreign_key_checks on initialization [#14013](https://github.com/vitessio/vitess/pull/14013)
 * json: Fix quoting JSON keys [#14066](https://github.com/vitessio/vitess/pull/14066)
 * VDiff: properly split cell values in record when using TabletPicker [#14099](https://github.com/vitessio/vitess/pull/14099)
 * VDiff: Cleanup the controller for a VDiff before deleting it [#14107](https://github.com/vitessio/vitess/pull/14107)
 * [release-18.0] VReplication: error on vtctldclient commands w/o tablet types (#14294) [#14298](https://github.com/vitessio/vitess/pull/14298)
 * [release-18.0] Vtctld SwitchReads: fix bug where writes were also being switched as part of switching reads when all traffic was switched using SwitchTraffic (#14360) [#14379](https://github.com/vitessio/vitess/pull/14379)
 * [release-18.0] VDiff: wait for shard streams of one table diff to complete for before starting that of the next table (#14345) [#14382](https://github.com/vitessio/vitess/pull/14382)
 * [release-18.0] VDiff tablet selection: pick non-serving tablets in Reshard workflows (#14413) [#14418](https://github.com/vitessio/vitess/pull/14418)
 * VReplication: Handle multiple streams in UpdateVReplicationWorkflow RPC [#14447](https://github.com/vitessio/vitess/pull/14447)
 * [release-18.0] VDiff: "show all" should only report vdiffs for the specified keyspace and workflow (#14442) [#14466](https://github.com/vitessio/vitess/pull/14466)
 * [release-18.0] VReplication: Handle multiple streams in UpdateVReplicationWorkflow RPC (#14447) [#14468](https://github.com/vitessio/vitess/pull/14468) 
#### VTAdmin
 * Unset the PREFIX environment variable when building VTAdmin [#13554](https://github.com/vitessio/vitess/pull/13554) 
#### VTCombo
 * Fix vtcombo DBDDL plugin race condition [#13117](https://github.com/vitessio/vitess/pull/13117) 
#### VTorc
 * Ensure to call `servenv.Init` when needed [#13638](https://github.com/vitessio/vitess/pull/13638) 
#### vtctl
 * [release-18.0] VReplication: Add missing info to vtctldclient workflow SHOW output (#14225) [#14240](https://github.com/vitessio/vitess/pull/14240)
### CI/Build 
#### Backup and Restore
 * Refactor `backup_pitr` into two distinct CI tests: builtin vs Xtrabackup [#13395](https://github.com/vitessio/vitess/pull/13395)
 * Fixing `backup_pitr` flaky tests via wait-for loop on topo reads [#13781](https://github.com/vitessio/vitess/pull/13781)
 * [release-18.0] Incremental backup: fix race condition in reading 'mysqlbinlog' output (#14330) [#14335](https://github.com/vitessio/vitess/pull/14335) 
#### Build/CI
 * Update a number of dependencies [#13031](https://github.com/vitessio/vitess/pull/13031)
 * Cleanup unused Dockerfile entries [#13327](https://github.com/vitessio/vitess/pull/13327)
 * flags: Remove hardcoded runner paths [#13482](https://github.com/vitessio/vitess/pull/13482)
 * added no-commit-collection option to launchable record build command [#13490](https://github.com/vitessio/vitess/pull/13490)
 * Replace deprecated `github.com/golang/mock` with `go.uber.org/mock` [#13512](https://github.com/vitessio/vitess/pull/13512)
 * [viper WatchConfig] platform-specific write to ensure callback fires exactly once [#13627](https://github.com/vitessio/vitess/pull/13627)
 * build: Allow passing in custom -ldflags [#13748](https://github.com/vitessio/vitess/pull/13748)
 * Run auto golang upgrade only on vitessio/vitess [#13766](https://github.com/vitessio/vitess/pull/13766)
 * collations: implement collation dumping as a docker image [#13879](https://github.com/vitessio/vitess/pull/13879) 
#### Docker
 * docker/k8s: add bookworm builds [#13436](https://github.com/vitessio/vitess/pull/13436)
 * Bump docker images to `bullseye` [#13664](https://github.com/vitessio/vitess/pull/13664) 
#### Documentation
 * fix docgen for subcommands [#13518](https://github.com/vitessio/vitess/pull/13518)
 * update docgen to embed commit ID in autogenerated doc frontmatter [#14056](https://github.com/vitessio/vitess/pull/14056) 
#### General
 * go/mysql: switch to new API for x/exp/slices.SortFunc [#13644](https://github.com/vitessio/vitess/pull/13644)
 * [main] Upgrade the Golang version to `go1.21.1` [#13933](https://github.com/vitessio/vitess/pull/13933)
 * [release-18.0] Upgrade the Golang version to `go1.21.2` [#14195](https://github.com/vitessio/vitess/pull/14195)
 * [release-18.0] Upgrade the Golang version to `go1.21.3` [#14230](https://github.com/vitessio/vitess/pull/14230) 
#### Online DDL
 * CI: fix onlineddl_scheduler flakiness [#13754](https://github.com/vitessio/vitess/pull/13754)
 * [release-18.0] OnlineDDL: reduce vrepl_stress workload in forks (#14302) [#14349](https://github.com/vitessio/vitess/pull/14349) 
#### Query Serving
 * Endtoend: stress tests for VTGate FOREIGN KEY support [#13799](https://github.com/vitessio/vitess/pull/13799)
 * ci: pool-related test flakyness [#14076](https://github.com/vitessio/vitess/pull/14076) 
#### Throttler
 * Deprecating and removing tablet throttler CLI flags and tests [#13246](https://github.com/vitessio/vitess/pull/13246)
 * Throttler: verify deprecated flags are still allowed [#13615](https://github.com/vitessio/vitess/pull/13615) 
#### VReplication
 * Flakes: Remove CI endtoend test for VReplication Copy Phase Throttling [#13343](https://github.com/vitessio/vitess/pull/13343)
 * Flakes: Improve reliability of vreplication_copy_parallel test [#13857](https://github.com/vitessio/vitess/pull/13857) 
#### VTAdmin
 * Improve time taken to run the examples by optimizing `vtadmin` build [#13262](https://github.com/vitessio/vitess/pull/13262) 
#### VTorc
 * [release-18.0] docker: add dedicated vtorc container (#14126) [#14148](https://github.com/vitessio/vitess/pull/14148)
### Dependabot 
#### General
 * Bump word-wrap from 1.2.3 to 1.2.4 in /web/vtadmin [#13569](https://github.com/vitessio/vitess/pull/13569)
 * Bump tough-cookie from 4.1.2 to 4.1.3 in /web/vtadmin [#13767](https://github.com/vitessio/vitess/pull/13767)
 * [release-18.0] Bump github.com/cyphar/filepath-securejoin from 0.2.3 to 0.2.4 (#14239) [#14253](https://github.com/vitessio/vitess/pull/14253)
 * [release-18.0] Bump golang.org/x/net from 0.14.0 to 0.17.0 (#14260) [#14264](https://github.com/vitessio/vitess/pull/14264) 
#### Java
 * java: update to latest dependencies for grpc and protobuf [#13996](https://github.com/vitessio/vitess/pull/13996) 
#### Observability
 * Bump tough-cookie and @cypress/request in /vitess-mixin/e2e [#13768](https://github.com/vitessio/vitess/pull/13768) 
#### VTAdmin
 * build(deps-dev): bump vite from 4.2.1 to 4.2.3 in /web/vtadmin [#13240](https://github.com/vitessio/vitess/pull/13240)
 * Bump protobufjs from 7.2.3 to 7.2.5 in /web/vtadmin [#13833](https://github.com/vitessio/vitess/pull/13833)
 * [release-18.0] Bump postcss from 8.4.21 to 8.4.31 in /web/vtadmin (#14173) [#14258](https://github.com/vitessio/vitess/pull/14258)
 * [release-18.0] Bump @babel/traverse from 7.21.4 to 7.23.2 in /web/vtadmin (#14304) [#14308](https://github.com/vitessio/vitess/pull/14308)
### Documentation 
#### CLI
 * gentler warning message on config-not-found [#13215](https://github.com/vitessio/vitess/pull/13215)
 * switch casing in onlineddl subcommand help text [#14091](https://github.com/vitessio/vitess/pull/14091)
 * [release-18.0] Bypass cobra completion commands so they still function (#14217) [#14234](https://github.com/vitessio/vitess/pull/14234) 
#### Documentation
 * Add security audit report [#13221](https://github.com/vitessio/vitess/pull/13221)
 * update link for reparenting guide [#13350](https://github.com/vitessio/vitess/pull/13350)
 * anonymize homedirs in generated docs [#14101](https://github.com/vitessio/vitess/pull/14101)
 * Summary changes for foreign keys [#14112](https://github.com/vitessio/vitess/pull/14112)
 * fix bad copy-paste in zkctld docgen [#14123](https://github.com/vitessio/vitess/pull/14123)
 * [release-18.0] release notes: edit summary for consistency (#14319) [#14320](https://github.com/vitessio/vitess/pull/14320) 
#### General
 * Improve release process documentation [#14000](https://github.com/vitessio/vitess/pull/14000) 
#### Governance
 * governance doc clean up  [#13337](https://github.com/vitessio/vitess/pull/13337)
### Enhancement 
#### Backup and Restore
 * go/vt/mysqlctl: instrument s3 upload time [#12500](https://github.com/vitessio/vitess/pull/12500)
 * metrics: change vtbackup_duration_by_phase to binary-valued vtbackup_phase [#12973](https://github.com/vitessio/vitess/pull/12973)
 * Incremental backup & recovery: restore-to-timestamp [#13270](https://github.com/vitessio/vitess/pull/13270)
 * backup: Allow for upgrade safe backups [#13449](https://github.com/vitessio/vitess/pull/13449)
 * Incremental backup: accept GTID position without 'MySQL56/' flavor prefix [#13474](https://github.com/vitessio/vitess/pull/13474)
 * Backup & Restore: vtctldclient to support PITR flags [#13513](https://github.com/vitessio/vitess/pull/13513)
 * BackupShard: support incremental backup [#13522](https://github.com/vitessio/vitess/pull/13522)
 * Point in time recovery: fix cross-tablet GTID evaluation [#13555](https://github.com/vitessio/vitess/pull/13555)
 * Backup/restore: provision and restore a tablet with point-in-time recovery flags [#13964](https://github.com/vitessio/vitess/pull/13964)
 * go/cmd/vtbackup: report replication status metrics during catch-up phase [#13995](https://github.com/vitessio/vitess/pull/13995) 
#### Build/CI
 * Set the number of threads for release notes generation with a flag [#13273](https://github.com/vitessio/vitess/pull/13273)
 * Optimize `make build` in `test.go` and in CI [#13567](https://github.com/vitessio/vitess/pull/13567)
 * Skip VTAdmin build in more places [#13588](https://github.com/vitessio/vitess/pull/13588)
 * Skip VTAdmin build in Docker tests [#13836](https://github.com/vitessio/vitess/pull/13836)
 * Migrates most workflows to 4 and 16 cores Large GitHub-Hosted-Runners [#13845](https://github.com/vitessio/vitess/pull/13845)
 * Skip launchable if the Pull Request is marked as a Draft [#13886](https://github.com/vitessio/vitess/pull/13886)
 * [release-18.0] Automatic approval of `vitess-bot` clean backports (#14352) [#14357](https://github.com/vitessio/vitess/pull/14357) 
#### CLI
 * Vtctldclient MoveTables [#13015](https://github.com/vitessio/vitess/pull/13015)
 * migrate vtorc to use cobra commands [#13917](https://github.com/vitessio/vitess/pull/13917) 
#### Cluster management
 * increase length of reparent_journal columns [#13287](https://github.com/vitessio/vitess/pull/13287)
 * Improvements to PRS [#13623](https://github.com/vitessio/vitess/pull/13623)
 * Add 2 more durability policies that allow RDONLY tablets to send semi-sync ACKs [#13698](https://github.com/vitessio/vitess/pull/13698)
 * `vtctld`/`vtorc`: improve reparenting stats [#13723](https://github.com/vitessio/vitess/pull/13723) 
#### Documentation
 * consolidate docs [#13959](https://github.com/vitessio/vitess/pull/13959) 
#### Evalengine
 * evalengine: implement date/time math [#13274](https://github.com/vitessio/vitess/pull/13274)
 * sqlparser: Add support for TIMESTAMPADD [#13314](https://github.com/vitessio/vitess/pull/13314)
 * mysql: introduce icuregex package [#13391](https://github.com/vitessio/vitess/pull/13391)
 * icuregex: Lazy load ICU data into memory [#13640](https://github.com/vitessio/vitess/pull/13640)
 * evalengine: Improve weight string support [#13658](https://github.com/vitessio/vitess/pull/13658)
 * evalengine: Fix JSON weight string computation [#13669](https://github.com/vitessio/vitess/pull/13669) 
#### Examples
 * Misc Local Install improvements. [#13446](https://github.com/vitessio/vitess/pull/13446) 
#### General
 * Refactor code to remove `evalengine` as a dependency of `VTOrc` [#13642](https://github.com/vitessio/vitess/pull/13642) 
#### Observability
 * vtorc: add detected_problems counter [#13967](https://github.com/vitessio/vitess/pull/13967) 
#### Online DDL
 * `vtctl OnlineDDL`: complete command set [#12963](https://github.com/vitessio/vitess/pull/12963)
 * Online DDL: improved row estimation via ANALYE TABLE with --analyze-table strategy flag [#13352](https://github.com/vitessio/vitess/pull/13352)
 * OnlineDDL: support @@migration_context in vtgate session. Use if non-empty [#13675](https://github.com/vitessio/vitess/pull/13675)
 * Vtgate: pass 'SHOW VITESS_MIGRATIONS' to tablet's query executor [#13726](https://github.com/vitessio/vitess/pull/13726)
 * vtctldclient OnlineDDL CANCEL [#13860](https://github.com/vitessio/vitess/pull/13860)
 * vtctldclient: support OnlineDDL `complete`, `launch` commands  [#13896](https://github.com/vitessio/vitess/pull/13896)
 * [release-18.0] Online DDL: lint DDL strategy flags (#14373) [#14399](https://github.com/vitessio/vitess/pull/14399) 
#### Query Serving
 * vindexes: return unknown params [#12951](https://github.com/vitessio/vitess/pull/12951)
 * Fix and Make aggregation planner handle aggregation functions better [#13228](https://github.com/vitessio/vitess/pull/13228)
 * vtgate planner: HAVING in the new operator horizon planner [#13289](https://github.com/vitessio/vitess/pull/13289)
 * Support complex aggregation in Gen4's Operators [#13326](https://github.com/vitessio/vitess/pull/13326)
 * Adds support for ANY_VALUE [#13342](https://github.com/vitessio/vitess/pull/13342)
 * Aggregation engine refactor [#13378](https://github.com/vitessio/vitess/pull/13378)
 * Move more horizon planning to the operators [#13412](https://github.com/vitessio/vitess/pull/13412)
 * Move UNION planning to the operators [#13450](https://github.com/vitessio/vitess/pull/13450)
 * Improve and Fix Distinct Aggregation planner [#13466](https://github.com/vitessio/vitess/pull/13466)
 * Enhancing VTGate buffering for MoveTables and Shard by Shard Migration [#13507](https://github.com/vitessio/vitess/pull/13507)
 * Add 2 new metrics with tablet type labels [#13521](https://github.com/vitessio/vitess/pull/13521)
 * vtgate table schema tracking to use GetSchema rpc [#13544](https://github.com/vitessio/vitess/pull/13544)
 * Add a `keyspace` configuration in the `vschema` for foreign key mode [#13553](https://github.com/vitessio/vitess/pull/13553)
 * Reduce usages of old horizon planning fallback [#13595](https://github.com/vitessio/vitess/pull/13595)
 * Add dry-run/monitoring-only mode for TxThrottler [#13604](https://github.com/vitessio/vitess/pull/13604)
 * go/vt/vitessdriver: implement driver.{Connector,DriverContext} [#13704](https://github.com/vitessio/vitess/pull/13704)
 * More union merging [#13743](https://github.com/vitessio/vitess/pull/13743)
 * Move subqueries to use the operator model [#13750](https://github.com/vitessio/vitess/pull/13750)
 * Add support for tuple as value type [#13800](https://github.com/vitessio/vitess/pull/13800)
 * icuregex: Update to ICU 73 [#13912](https://github.com/vitessio/vitess/pull/13912)
 * Change internal vindex type recommendation for integrals to xxhash [#13956](https://github.com/vitessio/vitess/pull/13956)
 * Foreign key cascade: retain "for update" lock on select query plans [#13985](https://github.com/vitessio/vitess/pull/13985)
 * Improve the rewriter to simplify more queries [#14059](https://github.com/vitessio/vitess/pull/14059)
 * [release-18.0] gen4: Support explicit column aliases on derived tables (#14129) [#14156](https://github.com/vitessio/vitess/pull/14156) 
#### Schema Tracker
 * vttablet: do not notify `vtgate` about internal tables [#13897](https://github.com/vitessio/vitess/pull/13897) 
#### TabletManager
 * Tablet throttler: throttled app configuration via `vtctl UpdateThrottlerConfig` [#13351](https://github.com/vitessio/vitess/pull/13351) 
#### Throttler
 * txthrottler: verify config at vttablet startup, consolidate funcs [#13115](https://github.com/vitessio/vitess/pull/13115)
 * txthrottler: add metrics for topoWatcher and healthCheckStreamer [#13153](https://github.com/vitessio/vitess/pull/13153)
 * `UpdateThrottlerConfig --unthrottle-app ...` [#13494](https://github.com/vitessio/vitess/pull/13494)
 * Reroute 'ALTER VITESS_MIGRATION ... THROTTLE ...' through topo [#13511](https://github.com/vitessio/vitess/pull/13511)
 * Tablet throttler: inter-checks via gRPC  [#13514](https://github.com/vitessio/vitess/pull/13514)
 * Per workload TxThrottler metrics [#13526](https://github.com/vitessio/vitess/pull/13526)
 * Throttler: exempt apps via `UpdateThrottlerConfig --throttle-app-exempt` [#13666](https://github.com/vitessio/vitess/pull/13666) 
#### Topology
 * Support arbitrary ZooKeeper config lines [#13829](https://github.com/vitessio/vitess/pull/13829) 
#### VReplication
 * MoveTables:  allow copying all tables in a single atomic copy phase cycle [#13137](https://github.com/vitessio/vitess/pull/13137)
 * VReplication: More intelligently manage vschema table entries on unsharded targets [#13220](https://github.com/vitessio/vitess/pull/13220)
 * MoveTables sequence e2e tests: change terminology to use basic vs simple everywhere for partial movetables workflows [#13435](https://github.com/vitessio/vitess/pull/13435)
 * wrangler,workflow/workflow: materialize from intersecting source shards based on primary vindexes [#13782](https://github.com/vitessio/vitess/pull/13782)
 * Implement Reshard in vtctldclient [#13792](https://github.com/vitessio/vitess/pull/13792)
 * VDiff: Migrate client command to vtctldclient [#13976](https://github.com/vitessio/vitess/pull/13976)
 * Migrate vreplication commands to vtctldclient: Mount and Migrate [#14174](https://github.com/vitessio/vitess/pull/14174)
 * [release-18.0] Migrate CreateLookupVindex and ExternalizeVindex to vtctldclient (#14086) [#14183](https://github.com/vitessio/vitess/pull/14183)
 * Migrate Materialize command to vtctldclient [#14184](https://github.com/vitessio/vitess/pull/14184)
 * [Release 18.0] Backport  of #17174 [#14210](https://github.com/vitessio/vitess/pull/14210)
 * [release-18.0] Migrate Materialize command to vtctldclient (#14184) [#14214](https://github.com/vitessio/vitess/pull/14214)
 * [release-18.0] VReplication: Add traffic state to vtctldclient workflow status output (#14280) [#14282](https://github.com/vitessio/vitess/pull/14282)
 * [release-18.0] VReplication: Add --all-cells flag to create sub-commands (#14341) [#14343](https://github.com/vitessio/vitess/pull/14343) 
#### VTAdmin
 * [release-18.0] Optimize the GetWorkflows RPC (#14212) [#14233](https://github.com/vitessio/vitess/pull/14233) 
#### VTCombo
 * `vttestserver`: persist vschema changes in `--persistent_mode` [#13065](https://github.com/vitessio/vitess/pull/13065) 
#### VTorc
 * Improve VTOrc failure detection to be able to better handle dead primary failures [#13190](https://github.com/vitessio/vitess/pull/13190)
 * Add flag to VTOrc to enable/disable its ability to run ERS [#13259](https://github.com/vitessio/vitess/pull/13259)
 * Add metric for showing the errant GTIDs in VTOrc [#13281](https://github.com/vitessio/vitess/pull/13281)
 * Add timestamp to vtorc debug page [#13379](https://github.com/vitessio/vitess/pull/13379)
 * Augment VTOrc to also store the shard records and use it to better judge Primary recoveries [#13587](https://github.com/vitessio/vitess/pull/13587)
 * Fix a couple of logs in VTOrc [#13667](https://github.com/vitessio/vitess/pull/13667)
 * Errant GTID Metrics Refactor [#13670](https://github.com/vitessio/vitess/pull/13670)
 * VTOrc converts a tablet to DRAINED type if it detects errant GTIDs on it [#13873](https://github.com/vitessio/vitess/pull/13873) 
#### vtctl
 * vtctl,vindexes: logs warnings and export stat for unknown vindex params [#13322](https://github.com/vitessio/vitess/pull/13322)
 * vtctldclient OnlineDDL: support `throttle`, `unthrottle` [#13916](https://github.com/vitessio/vitess/pull/13916) 
#### web UI
 * Add vtsql flags to vtadmin [#13674](https://github.com/vitessio/vitess/pull/13674)
### Feature Request 
#### CLI
 * [vtctld] more cobra binaries [#13930](https://github.com/vitessio/vitess/pull/13930)
 * [cobra] vtgate and vttablet [#13943](https://github.com/vitessio/vitess/pull/13943)
 * [cli] migrate mysqlctl and mysqlctld to cobra [#13946](https://github.com/vitessio/vitess/pull/13946)
 * [CLI] cobra lots of things [#14007](https://github.com/vitessio/vitess/pull/14007)
 * miscellaneous cobras [#14069](https://github.com/vitessio/vitess/pull/14069)
 * [cli] cobra zookeeper [#14094](https://github.com/vitessio/vitess/pull/14094) 
#### Online DDL
 * Add OnlineDDL show support [#13738](https://github.com/vitessio/vitess/pull/13738)
 * [onlineddl] retry and cleanup [#13830](https://github.com/vitessio/vitess/pull/13830) 
#### Query Serving
 * Add group_concat aggregation support [#13331](https://github.com/vitessio/vitess/pull/13331)
 * Add support for kill statement [#13371](https://github.com/vitessio/vitess/pull/13371)
 * Build foreign key definition in schema tracker [#13657](https://github.com/vitessio/vitess/pull/13657)
 * Foreign Keys: `INSERT` planning [#13676](https://github.com/vitessio/vitess/pull/13676)
 * Foreign Keys: `DELETE` planning [#13746](https://github.com/vitessio/vitess/pull/13746)
 * Foreign Keys: `UPDATE` planning [#13762](https://github.com/vitessio/vitess/pull/13762)
 * Add Foreign key Cascade engine primitive [#13802](https://github.com/vitessio/vitess/pull/13802)
 * Foreign key cascade planning for DELETE and UPDATE queries [#13823](https://github.com/vitessio/vitess/pull/13823)
 * Add Foreign key verify constraint engine primitive [#13848](https://github.com/vitessio/vitess/pull/13848)
 * Add VSchema DDL support for dropping sequence and auto increment [#13882](https://github.com/vitessio/vitess/pull/13882)
 * Update Cascade Planning leading to Foreign key constraint verification [#13902](https://github.com/vitessio/vitess/pull/13902)
 * Disallow Insert with Duplicate key update and Replace Into queries on foreign key column, set locks on fk queries [#13953](https://github.com/vitessio/vitess/pull/13953) 
#### VReplication
 * VReplication: Initialize Sequence Tables Used By Tables Being Moved [#13656](https://github.com/vitessio/vitess/pull/13656)
 * MoveTables: add flag to specify that routing rules should not be created when a movetables workflow is created [#13895](https://github.com/vitessio/vitess/pull/13895)
### Internal Cleanup 
#### Build/CI
 * docker/k8s: Cleanup done TODO [#13347](https://github.com/vitessio/vitess/pull/13347)
 * Remove unused chromedriver [#13573](https://github.com/vitessio/vitess/pull/13573)
 * docker/bootstrap: remove --no-cache flag [#13785](https://github.com/vitessio/vitess/pull/13785) 
#### CLI
 * remove query_analyzer binary and release [#14055](https://github.com/vitessio/vitess/pull/14055)
 * [release-18.0] Make vtctldclient mount command more standard (#14281) [#14283](https://github.com/vitessio/vitess/pull/14283) 
#### Cluster management
 * Fix logging by omitting the host and port in `SetReadOnly` [#13470](https://github.com/vitessio/vitess/pull/13470)
 * Improve logging and renaming PrimaryTermStartTimestamp in vttablets [#13625](https://github.com/vitessio/vitess/pull/13625) 
#### Evalengine
 * collations: Refactor to separate basic collation information from data [#13868](https://github.com/vitessio/vitess/pull/13868) 
#### Examples
 * docker/mini: remove refs to orc configs [#13495](https://github.com/vitessio/vitess/pull/13495) 
#### General
 * servenv: Allow for explicit bind address [#13188](https://github.com/vitessio/vitess/pull/13188)
 * Remove `out.txt` and add `release-17.0` to go upgrade automation [#13261](https://github.com/vitessio/vitess/pull/13261)
 * Deprecate VTGR [#13301](https://github.com/vitessio/vitess/pull/13301)
 * mysql: Refactor dependencies [#13688](https://github.com/vitessio/vitess/pull/13688)
 * Remove explicit usage of etcd v2 (api and storage) [#13791](https://github.com/vitessio/vitess/pull/13791)
 * Go 1.21 cleanups [#13862](https://github.com/vitessio/vitess/pull/13862)
 * [wrangler] cleanup unused functions [#13867](https://github.com/vitessio/vitess/pull/13867)
 * [misc] Delete more unused functions, tidy up dupe imports [#13878](https://github.com/vitessio/vitess/pull/13878)
 * Clean up deprecated slice header usage and unused code [#13880](https://github.com/vitessio/vitess/pull/13880)
 * [misc] tidy imports [#13885](https://github.com/vitessio/vitess/pull/13885)
 * [staticcheck] miscellaneous tidying [#13892](https://github.com/vitessio/vitess/pull/13892)
 * [staticcheck] Cleanup deprecations [#13898](https://github.com/vitessio/vitess/pull/13898)
 * Consolidate helper functions for working with proto3 time messages [#13905](https://github.com/vitessio/vitess/pull/13905)
 * [staticcheck] Last few staticchecks! [#13909](https://github.com/vitessio/vitess/pull/13909)
 * Remove deprecated flags before `v18.0.0` [#14071](https://github.com/vitessio/vitess/pull/14071) 
#### Observability
 * stats: use *time.Ticker instead of time.After() [#13492](https://github.com/vitessio/vitess/pull/13492) 
#### Query Serving
 * Operator planner refactor [#13294](https://github.com/vitessio/vitess/pull/13294)
 * Refactor and add a comment to schema initialisation code [#13309](https://github.com/vitessio/vitess/pull/13309)
 * vtgate v3 planner removal [#13458](https://github.com/vitessio/vitess/pull/13458)
 * vtgate buffering logic: remove the deprecated healthcheck based implementation [#13584](https://github.com/vitessio/vitess/pull/13584)
 * Refactor Expression and Statement Simplifier [#13636](https://github.com/vitessio/vitess/pull/13636)
 * Remove duplicate ACL check in tabletserver handleHTTPConsolidations [#13876](https://github.com/vitessio/vitess/pull/13876)
 * inputs method to return additional information about the input primitive [#13883](https://github.com/vitessio/vitess/pull/13883)
 * refactor: move DML logic to sql_builder.go [#13920](https://github.com/vitessio/vitess/pull/13920)
 * Fix `TestLeftJoinUsingUnsharded` and remove instability when running E2E locally [#13973](https://github.com/vitessio/vitess/pull/13973)
 * Remove excessive logging in transactions [#14021](https://github.com/vitessio/vitess/pull/14021)
 * moved timeout test to different package [#14028](https://github.com/vitessio/vitess/pull/14028)
 * [release-18.0] Rename Foreign Key enum values in VSchema  and drop `FK_` prefix (#14274) [#14299](https://github.com/vitessio/vitess/pull/14299)
 * tx_throttler: remove topo watchers metric [#14444](https://github.com/vitessio/vitess/pull/14444) 
#### TabletManager
 * mysqlctl: Use DBA connection for schema operations [#13178](https://github.com/vitessio/vitess/pull/13178)
 * k8stopo: Include deprecation warning [#13299](https://github.com/vitessio/vitess/pull/13299)
 * k8stopo: Remove the deprecated Kubernetes topo [#13303](https://github.com/vitessio/vitess/pull/13303)
 * vtgr: Remove deprecated vtgr [#13308](https://github.com/vitessio/vitess/pull/13308)
 * mysqlctl: Move more to use built in MySQL client [#13338](https://github.com/vitessio/vitess/pull/13338) 
#### Throttler
 * `txthrottler`: remove `txThrottlerConfig` struct, rely on `tabletenv` [#13624](https://github.com/vitessio/vitess/pull/13624) 
#### VReplication
 * Use sqlparser for all dynamic query building in VDiff2 [#13319](https://github.com/vitessio/vitess/pull/13319)
 * vreplication: Move to use collations package [#13566](https://github.com/vitessio/vitess/pull/13566) 
#### VTAdmin
 * [VTAdmin] Upgrade to use node 18.16.0 [#13288](https://github.com/vitessio/vitess/pull/13288) 
#### VTorc
 * VTOrc: Update the primary key for all the tables from `hostname, port` to `alias` [#13243](https://github.com/vitessio/vitess/pull/13243)
 * vtorc: Cleanup more unused code [#13354](https://github.com/vitessio/vitess/pull/13354)
 * Improve lock action string [#13355](https://github.com/vitessio/vitess/pull/13355)
 * Improve VTOrc logging statements, now that we have alias as a field [#13428](https://github.com/vitessio/vitess/pull/13428)
 * Remove excessive logging in VTOrc APIs [#13459](https://github.com/vitessio/vitess/pull/13459)
 * [release-16.0] Remove excessive logging in VTOrc APIs (#13459) [#13462](https://github.com/vitessio/vitess/pull/13462) 
#### vtctl
 * [release-18.0] Move all examples to vtctldclient (#14226) [#14241](https://github.com/vitessio/vitess/pull/14241) 
#### vtexplain
 * vtexplain: Fix passing through context for cleanup [#13900](https://github.com/vitessio/vitess/pull/13900)
### Performance 
#### General
 * proto: Faster clone [#13914](https://github.com/vitessio/vitess/pull/13914) 
#### Query Serving
 * Cache info schema table info [#13724](https://github.com/vitessio/vitess/pull/13724)
 * gen4: Fast aggregations [#13904](https://github.com/vitessio/vitess/pull/13904)
 * Cache v3 [#13939](https://github.com/vitessio/vitess/pull/13939)
 * Reduce network pressure on multi row insert [#14064](https://github.com/vitessio/vitess/pull/14064)
 * VTGate FK stress tests suite: improvements [#14098](https://github.com/vitessio/vitess/pull/14098) 
#### TabletManager
 * BaseShowTablesWithSizes: optimize MySQL 8.0 query [#13375](https://github.com/vitessio/vitess/pull/13375)
 * Support views in BaseShowTablesWithSizes for MySQL 8.0 [#13394](https://github.com/vitessio/vitess/pull/13394) 
#### vtctl
 * `ApplySchema`: support `--batch-size` flag in 'direct' strategy [#13693](https://github.com/vitessio/vitess/pull/13693)
### Regression 
#### Backup and Restore
 * Fix backup on s3 like storage [#14311](https://github.com/vitessio/vitess/pull/14311)
 * [release-18.0] Fix backup on s3 like storage (#14311) [#14362](https://github.com/vitessio/vitess/pull/14362) 
#### Query Serving
 * fix: ShardedRouting clone to clone slice of reference correctly [#13265](https://github.com/vitessio/vitess/pull/13265)
 * Handle inconsistent state error in query buffering [#13333](https://github.com/vitessio/vitess/pull/13333)
 * fix:  insert with negative value [#14244](https://github.com/vitessio/vitess/pull/14244)
 * [release-18.0] fix:  insert with negative value (#14244) [#14247](https://github.com/vitessio/vitess/pull/14247)
 * [release-18.0] use aggregation engine over distinct engine when overlapping order by (#14359) [#14361](https://github.com/vitessio/vitess/pull/14361)
 * [release-18.0] Performance Fixes for Vitess 18 (#14383) [#14393](https://github.com/vitessio/vitess/pull/14393)
 * [release-18.0] tuple: serialized form (#14392) [#14394](https://github.com/vitessio/vitess/pull/14394)
### Release 
#### Build/CI
 * Fix incorrect output in release scripts [#13385](https://github.com/vitessio/vitess/pull/13385)
 * Optimize release notes generation to use GitHub Milestones [#13398](https://github.com/vitessio/vitess/pull/13398) 
#### CLI
 * Add vtctldclient info to the 18.0 summary [#14259](https://github.com/vitessio/vitess/pull/14259) 
#### Documentation
 * Add end-of-life documentation + re-organize internal documentation [#13401](https://github.com/vitessio/vitess/pull/13401)
 * Update known issues in `v16.x` and `v17.0.0` [#13618](https://github.com/vitessio/vitess/pull/13618) 
#### General
 * Copy v17.0.0-rc changelog to main [#13248](https://github.com/vitessio/vitess/pull/13248)
 * Update release notes for 17.0.0-rc2 [#13306](https://github.com/vitessio/vitess/pull/13306)
 * Forward port of release notes changes from v17.0.0 GA [#13370](https://github.com/vitessio/vitess/pull/13370)
 * Add v15.0.4, v16.0.3, and v17.0.1 changelogs [#13661](https://github.com/vitessio/vitess/pull/13661)
 * Copy release notes for v17.0.2 and v16.0.4 [#13811](https://github.com/vitessio/vitess/pull/13811)
 * Code freeze of release-18.0 [#14131](https://github.com/vitessio/vitess/pull/14131)
 * Release of v18.0.0-rc1 [#14136](https://github.com/vitessio/vitess/pull/14136)
 * Back to dev mode after `v18.0.0-rc1` release [#14169](https://github.com/vitessio/vitess/pull/14169)
 * Code freeze of release-18.0 [#14405](https://github.com/vitessio/vitess/pull/14405)
### Testing 
#### Build/CI
 * Flakes: Address TestMigrate Failures [#12866](https://github.com/vitessio/vitess/pull/12866)
 * [vipersync] skip flaky test [#13501](https://github.com/vitessio/vitess/pull/13501)
 * [vipersync] deflake TestWatchConfig [#13545](https://github.com/vitessio/vitess/pull/13545)
 * Fix bug in `fileNameFromPosition` test helper [#13778](https://github.com/vitessio/vitess/pull/13778)
 * Flakes: Delete VTDATAROOT files in reparent test teardown within CI [#13793](https://github.com/vitessio/vitess/pull/13793)
 * CI: Misc test improvements to limit failures with various runners [#13825](https://github.com/vitessio/vitess/pull/13825)
 * actually test vtcombo [#14095](https://github.com/vitessio/vitess/pull/14095)
 * Remove FOSSA Test from CI until we can do it in a secure way [#14119](https://github.com/vitessio/vitess/pull/14119) 
#### Cluster management
 * Fix `Fakemysqldaemon` to store the host and port after `SetReplicationSource` call [#13439](https://github.com/vitessio/vitess/pull/13439)
 * Deflake `TestPlannedReparentShardPromoteReplicaFail` [#13548](https://github.com/vitessio/vitess/pull/13548)
 * Flaky tests: Fix wrangler tests [#13568](https://github.com/vitessio/vitess/pull/13568) 
#### General
 * [CI] deflake viper sync tests [#13185](https://github.com/vitessio/vitess/pull/13185)
 * Remove `--disable_active_reparents` flag in vttablet-up.sh [#13504](https://github.com/vitessio/vitess/pull/13504)
 * Add leak checking for vtgate tests [#13835](https://github.com/vitessio/vitess/pull/13835) 
#### Online DDL
 * Fix potential panics due to "Fail in goroutine after test completed" [#13596](https://github.com/vitessio/vitess/pull/13596)
 * [OnlineDDL] add label so break works as intended [#13691](https://github.com/vitessio/vitess/pull/13691) 
#### Query Serving
 * Deflake `TestQueryTimeoutWithDual` test [#13405](https://github.com/vitessio/vitess/pull/13405)
 * Fix `TestGatewayBufferingWhileReparenting` flakiness [#13469](https://github.com/vitessio/vitess/pull/13469)
 * fix TestQueryTimeoutWithTables flaky test [#13579](https://github.com/vitessio/vitess/pull/13579)
 * schemadiff: add time measure test for massive schema load and diff [#13697](https://github.com/vitessio/vitess/pull/13697)
 * End to end testing suite for foreign keys [#13870](https://github.com/vitessio/vitess/pull/13870)
 * Fix setup order to avoid races [#13871](https://github.com/vitessio/vitess/pull/13871)
 * Use correct syntax in test [#13907](https://github.com/vitessio/vitess/pull/13907)
 * test: added test to check binlogs to contain the cascade events [#13970](https://github.com/vitessio/vitess/pull/13970)
 * E2E Fuzzing testing for foreign keys [#13980](https://github.com/vitessio/vitess/pull/13980)
 * Fix foreign key plan tests expectation [#13997](https://github.com/vitessio/vitess/pull/13997)
 * [release-18.0] vtgate: Allow more errors for the warning check (#14421) [#14423](https://github.com/vitessio/vitess/pull/14423) 
#### VReplication
 * Flakes: remove non-determinism from vtctldclient MoveTables unit test [#13765](https://github.com/vitessio/vitess/pull/13765)
 * Flakes: empty vtdataroot before starting a new vreplication e2e test [#13803](https://github.com/vitessio/vitess/pull/13803)
 * Flakes: Add recently added 'select rows_copied' query to ignore list  [#13993](https://github.com/vitessio/vitess/pull/13993)
 * [release-18.0] TestStreamMigrateMainflow: fix panic in test [#14425](https://github.com/vitessio/vitess/pull/14425) 
#### VTorc
 * Fix flakiness in `TestDeadPrimaryRecoversImmediately` [#13232](https://github.com/vitessio/vitess/pull/13232)
 * Fix flakiness in VTOrc tests [#13489](https://github.com/vitessio/vitess/pull/13489)
 * Skip flaky test `TestReadOutdatedInstanceKeys` [#13561](https://github.com/vitessio/vitess/pull/13561)
 * Reintroduce `TestReadOutdatedInstanceKeys` with debugging information [#13562](https://github.com/vitessio/vitess/pull/13562) 
#### vtctl
 * Fix merge conflict with new tests [#13869](https://github.com/vitessio/vitess/pull/13869)

