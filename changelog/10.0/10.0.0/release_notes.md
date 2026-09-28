This release complies with VEP-3 which removes the upgrade order requirement. Components can be upgraded in any order. It is recommended that the upgrade order should still be followed if possible, except to canary test the new version of VTGate before upgrading the rest of the components.

## Known Issues
* Running binaries with `--version` or running `select @@version` from a MySQL client still shows `10.0.0-RC1`
* Online DDL [cannot be used](https://github.com/vitessio/vitess/pull/7873#issuecomment-822798180) if you are using the keyspace filtering feature of VTGate
* VReplication errors when a fixed-length binary column is used as the sharding key #8080

* A critical vulnerability [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228) in the Apache Log4j logging library was disclosed on Dec 9 2021.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and additional CVEs
  [CVE-2021-45046](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45046) and [CVE-2021-44832](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44832) followed.
  These have been fixed in release `2.17.1`. This release of Vitess, `v10.0.0`, uses a version of Log4j below `2.17.1`, for this reason, we encourage you to use version `v10.0.5` instead, to benefit from the vulnerability patches.

* An issue where the value of the `-force` flag is used instead of `-keep_data` flag's value in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174.

## Bugs Fixed

### VTGate / MySQL compatibility
* Remove printing of ENFORCED word so that statements remain compatible with mysql 5.7 #7458
* Allow any ordering of generic options in column definitions #7459
* Corrects the comment handling in vitess #7581
* Fix regression - should be able to plan subquery on top of subquery #7682
* Nullable Timestamp Column Fix #7740
* VTGate: Fix the error messages in drop, create and alter database commands #7397
* VTGate: Fix information_schema query with system schema in table_schema filter #7430
* VTGate: Fix Set Statement in Tablet when executed with bindvars #7431
* VTGate: Fix for Query Serving when Toposerver is Down #7484
* VTGate: Add necessary bindvars when preparing queries #7493
* VTGate: Show anywhere plan fix to consider default keyspace #7531
* VTGate: Fix table parsing on VSchema generation #7511
* VTGate: Use the emulated MySQL version for special comments #7510
* VTGate: Reset Session for Reserved Connection when the connection id is not found #7539
* VTGate: Healthcheck: update healthy tablets correctly when a stream returns an error or timeout #7732
* VTGate: Fix for reserved connection usage with transaction #7646
* VTGate: Fix MySQL Workbench failure on login with `select current_user()` #7705
* VTGate: Constraint names and database names with spaces. #7745
* VTGate: Fix dual table query when system schema is selected database #7734

### Other
* VTTablet: Correctly initialize statsTabletTypeCounts during startup #7390
* Backup/Restore: Respect -disable_active_reparents in backup/restore #7576
* Backup/Restore: check disable_active_reparents properly before waiting for position update #7703


## Functionality Added or Changed

### VTGate / MySQL compatibility / Query Serving

* VTGate: Gen4 Planner: AxB vs BxA #7274
* VTGAte: Gen4 fallback planning #7370
* VTGate: Support for CALL procedures #7287
* VTGate: Set default for @@workload to OLTP #7288
* VTGate: Change @@version and @@version_comment #7337
* VTGate: Fix VitessAware system variables of type boolean return NULL when MySQL is not involved #7353
* VTGate: Add stats for RowsAffected similar to RowsReturned #7380
* VTGate: Added information_schema_stats_expiry to allowed list of set vars #7435
* VTGate: LFU Cache Implementation #7439
* VTGate: Describe table to route based on table name and qualifier #7445
* VTGate: Olap error message fix #7448
* VTGate: Temporary Table support in unsharded keyspace #7411
* VTGate: Publish table size on schema #7444
* VTGate: Support for caching_sha2_password plugin in mysql/client #6716
* VTGate: Moving Show plan from executor to planbuilder #7475
* VTGate: Adds another case to merge routes for information_schema queries #7504
* VTGate: Add innodb_read_rows as vttablet metric #7520
* VTGate: Adds support for Show variables #7547
* VTGate: gen4: fail unsupported queries #7409
* VTGate: Fix Metadata in SHOW queries #7540
* VTGate: Update AST helper generation #7558
* VTGate: Avoiding addition of redundant unary operators #7579
* VTGate: Optimise AST rewriting #7583
* VTGate: Add Show Status query to vtexplain and make asthelpergen/sizegen quiet #7590
* VTGate: Add support for SELECT ALL #7593
* VTGate: Empty statement error code change in sql parsing #7618
* VTGate: Socket system variable to return vitess mysql socket #7637
* VTGate: Make DROP/CREATE DATABASE pluggable #7381
* VTGate: Allow Select with lock to pass through in vttablet #7584
* VTGate: Fix ordering in SELECT INTO and printing of strings #7655
* VTGate: AST Equals code generator #7672
* VTGate:  [tabletserver] More resilient wait for schema changes #7684
* VTGate: Fix flush statement planner #7695
* VTGate: Produce query warnings for using features not supported when sharded #7538
* VTGate: Support for ALTER VITESS_MIGRATION statements #7663
* VTGate: Solve I_S queries using CNF rewriting #7677
* VTGate: System schema queries #7685
* VTGate: Make the AST visitor faster #7701
* VTGate: COM_PREPARE - Single TCP response packet with all MySQL Packets #7713
* VTGate: Replace the database name in result fields only if needed #7714
* VTGate: Split ast_helper into individual gen files #7727
* VTGate: Adds support for ordering on character fields for sharded keyspace queries #7678
* VTGate: Show columns query on system schema #7729
* VTGate: Disallow foreign key constraint on ddl #7780
* VTGate: VTGate: support -enable_online_ddl flag #7694
* VTGate: Default to false for system settings to be changed per session at the database connection level #7921
* VTGate: vtctl: return error on invalid ddl_strategy #7924
* VTGate: [10.0] Squashed backport of #7903 #7927
* VTGate: [10.0] Fix bug with reserved connections to stale tablets #7935
* VTGate: [10.0] Fix for keyspaces_to_watch regression #7936
* VTGate: [10.0] Update healthy tablets correctly for primary down #7937
* VTGate: [10.0] Allow modification of tablet unhealthy_threshold via debugEnv #7938

### Testing 
* Fuzzing: Add vtctl fuzzer #7605
* Fuzzing: Add more fuzzers #7622
* Fuzzing: Add 3 fuzzers for mysql endpoints #7639
* Fuzzing: Add oss-fuzz build script #7591
* Fuzzing: Add requirement for minimum length of input data #7722
* Fuzzing: Add new mysql fuzzer #7660
* Fuzzing: Add [grpcvtgateconn]  fuzzer #7689
* Fuzzing: Make mysql fuzzer more calls during each iteration #7766

### Performance
* VTGate: [perf] zero-copy tokenizer #7619
* VTGate: [perf: sqlparser faster formatting #7710
* VTGate :[perf] Cache reserved bind variables in queries #7698
* VTGate: [perf] sqlparser yacc codegen #7669
* VTGate: Making fast AST rewriter faster #7726
* VTGate: Cached Size Implementation #7387
* VTGate: Plan remove mutexes #7468
* LFU Cache Bug Fixes #7479
* [cache] Handle all possible initialization cases #7556
* VTGate: [servenv] provide a global flag for profiling #7496
* VTGate: [vttablet] Benchmarks and two small optimizations #7560
* [pprof]: allow stopping profiling early with a signal #7594
* perf: RPC Serialization #7519
* perf: keyword lookups in the tokenizer #7606

### Cluster Management
* [vtctld] Migrate topo management RPCs #7395
* [vtctldclient] Set `SilenceErrors` on the root command, so we don't double-log #7404
* [vtctldclient] Command line flags: dashes and underscores synonyms #7406
* Extract the `maxReplPosSearch` struct out to `topotools` #7420
* Add protoutil package, refactor ISP to use it #7421
* Add `ErrorGroup` to package concurrency, use in `waitOnNMinusOneTablets` #7429
* [vtctld / wrangler] Extract some reparent methods out to functions for shared use between wrangler and VtctldServer #7434
* [vtctld/wrangler] Extract `EmergencyReparentShard` logic to dedicated struct and add unit tests #7464
* Provide named function for squashing usage errors; start using it #7451
* [concurrency] Add guard against potentially blocking forever in ErrorGroup.Wait() when NumGoroutines is 0 #7463
* Add hook for statsd integration #7417
* [concurrency] Add guard against potentially blocking forever in ErrorGroup.Wait() when NumGoroutines is 0 #7463
* Resilient rebuild keyspace graph check, tablet controls not in `RebuildKeyspaceGraph` command  #7442
* [reparentutil / ERS] confirm at least one replica succeeded to `SetMaster`, or fail #7486
* [reparentutil / wrangler] Extract PlannedReparentShard logic from wrangler to PlannedReparenter struct #7501
* Add backup/restore duration stats #7512
* Refresh replicas and rdonly after MigrateServedTypes except on skipRefreshState. #7327
* [eparentutil] ERS should not attempt to WaitForRelayLogsToApply on primary tablets that were not running replication #7523
* [orchestrator] prevent XSS attack via 'orchestrator-msg' params #7526
* [vtctld] Add remaining reparent commands to VtctldServer #7536
* [reparentutil] ERS should not attempt to WaitForRelayLogsToApply on primary tablets that were not running replication #7523
* Shutdown vttablet gracefully if tablet record disappears #7563
* ApplySchema: -skip_preflight #7587
* Table GC: disable binary logging on best effort basis #7588
* Addition of waitSig pprof argument to start recording on USR1 #7616
* Add combine TLS certs feature #7609
* Check error response before attempting to access InitShardPrimary response #7658
* [vtctld] Migrate GetSrvKeyspace as GetSrvKeyspaces in VtctldServer #7680
* [vtctld] Migrate ShardReplicationPositions #7690
* [reparentutil | ERS] Bind status variable to each goroutine in WaitForRelayLogsToApply #7692
* [servenv] Fix var shadowing caused by short variable declaration #7702
* [vtctl|vtctldserver] List/Get Tablets timeouts #7715
* vtctl ApplySchema supports '-request_context' flag #7777

### VReplication

* VReplication: vstreamer to throttle on source endpoint #7324
* VReplication: Throttle on target tablet #7364
* VReplication: Throttler: fix to client usage in vreplication and table GC #7422
* VReplication: MoveTables/Reshard add flags to start workflows in a stopped state and to stop workflow once copy is completed #7449
* VReplication: Support for caching_sha2_password plugin in mysql/client #6716
* VReplication: Validate SrvKeyspace during Reshard/SwitchReads #7481
* VReplication: [MoveTables] Refresh SrvVSchema (for Routing Rules) and source tablets (for Blacklisted Tables) on completion #7505
* VReplication : Data migration from another Vitess cluster #7546
* VReplication : [materialize] Add cells and tablet_types parameters #7562
* VReplication:  JSON Columns: fix bug where vreplication of update statements were failing #7640
* VReplication: Make the frequency at which heartbeats update the _vt.vreplication table configurable #7659
* VReplication: Error out if binlog compression is turned on #7670
* VReplication: Tablet throttler: support for custom query & threshold #7541
* VStream API: allow aligning streams from different shards to minimize skews across the streams #7626
* VReplication: Backport 7809: Update rowlog for the API change made for the vstream skew alignment feature #7890

### OnlineDDL

* OnlineDDL: update gh-ost binary to v1.1.1 #7394
* Online DDL via VReplication #7419
* Online DDL: VReplicatoin based mini stress test CI #7492
* OnlineDDL: Revert for VReplication based migrations #7478
* Online DDL: Internal support for eta_seconds #7630
* Online DDL: Support 'SHOW VITESS_MIGRATIONS' queries #7638
* Online DDL: Support for REVERT VITESS_MIGRATION statement #7656
* Online DDL: Declarative Online DDL #7725

### VTAdmin

Vitess 10.0 introduces a highly-experimental multi-cluster admin API and web UI, called VTAdmin. Deploying the vtadmin-api and vtadmin-web components is completely opt-in. If you're interested in trying it out and providing early feedback, come find us in #feat-vtadmin in the Vitess slack. Note that VTAdmin relies on the new VtctldServer API, so you must be running the new grpc-vtctld service on your vtctlds in order to use it.

* VTAdmin: Add vtadmin-web build flag for configuring fetch credentials #7414
* VTAdmin: Add `cluster` field to vtadmin-api's /api/gates response #7425
* VTAdmin: Add /api/clusters endpoint to vtadmin-api #7426
* VTAdmin: Add /api/schemas endpoint to vtadmin-api #7432
* VTAdmin: [vtadmin-web] Add routes and simple tables for all entities #7440
* VTAdmin: [vtadmin-web] Set document.title from route components #7450
* VTAdmin: [vtadmin-web] Add DataTable component with URL pagination #7487
* VTAdmin: [vtadmin-api] Add shards to /api/keyspaces response #7453
* VTAdmin: [vtadmin-web] Add replaceQuery + pushQuery to useURLQuery hook #7507
* VTAdmin: [vtadmin-web] An initial pass for tablet filters #7515
* VTAdmin: [vtadmin-web] Add a Select component #7524
* VTAdmin: [vtadmin-api] Add /vtexplain endpoint #7528
* VTAdmin: [vtadmin-api] Reorganize tablet-related functions into vtadmin/cluster/cluster.go #7553
* VTAdmin: Three small bugfixes in Tablets table around stable sort order, display type lookup, and filtering by type #7568
* VTAdmin: [vtadmin] Add GetSchema endpoint #7596
* VTAdmin: [vtadmin/testutil] Add testutil helper to manage the complexity of recursively calling WithTestServer #7601
* VTAdmin: [vtadmin] Add FindSchema route #7610
* VTAdmin: [vtadmin-web] Add simple /schema view with table definition #7615
* VTAdmin: [vtadmin] vschemas api endpoints #7625
* VTAdmin: [vtadmin] Add support for additional service healthchecks in grpcserver #7635
* VTAdmin: [vtadmin] test refactors #7641
* VTAdmin: [vtadmin] propagate error contexts #7642
* VTAdmin: [vtadmin] tracing refactor #7649
* VTAdmin: [vtadmin] GetWorkflow(s) endpoints #7662
* VTAdmin: [vitessdriver|vtadmin] Support Ping in vitessdriver, use in vtadmin to healthcheck connections during Dial #7709
* VTAdmin: [vtadmin]  Add to local example #7699
* VTAdmin: [vtexplain] lock #7724
* VTAdmin: [vtadmin] Aggregate schema sizes #7751
* VTAdmin: [vtadmin-web] Add comments + 'options' parameter to API hooks #7754
* VTAdmin: [vtadmin-web] Add common max-width to infrastructure table views #7760
* VTAdmin: [vtadmin-web] Add hooks + skeleton view for workflows #7762
* VTAdmin: [vtadmin-web] Add a hasty filter input to the /schemas view #7779

### Other / Tools

* [rulesctl] Implements CLI tool for rule management #7712

## Examples / Tutorials

* Source correct shell script in README #7749

## Documentation

* Add Severity Labels document #7542
* Remove Google Groups references #7664
* Move some commas around in README.md :) #7671
* Add Andrew Mason to Maintainers List #7757

## Build/CI Environment Changes

* Update java build versions to vitess 10.0.0 #7383
* CI: check run analysis to post JSON from file #7386
* Fix Dockerfiles for vtexplain and vtctlclient #7418
* CI: Add descriptive names to vrep shards. Update test generator script #7454
* CI: adding 'go mod tidy' test #7461
* Docker builds vitess/vtctlclient to install curl #7466
* Add VT_BASE_VER to vtexplain/Dockerfile #7467
* Enable -mysql_server_version in vttestserver, and utilize it in vttestserver container images #7474
* [vtctld | tests only] testtmclient refactor #7518
* CI: skip some tests on forked repos #7527
* Workflow to check make sizegen #7535
* Add mysqlctl docker image #7557
* Restore CI workflow shard 26, accidentally dropped #7569
* Update CODEOWNERS #7586
* CI: ci-workflow-gen  turn string to array to reduce conflicts #7582
* Add percona-toolkit (for pt-osc/pt-online-schema-change) to the docker/lite images #7603
* CI: Use ubuntu-18.04 in tests #7614
* [vttestserver] Fix to work with sharded keyspaces #7617
* Add tools.go #7517
* Make vttestserver compatible with persistent data directories #7718
* Add vtorc binary for rpm,deb builds #7750
* Fixes bug that prevents creation of logs directory #7761
* [Java] Guava update to 31.1.1 #7764
* make: build vitess as static binaries by default #7795 ← Potentially breaking change
* make: build vitess as static binaries by default (10.0 backport) #7808
* java: prepare java version for release 10.0 #7922

## Functionality Neutral Changes
* VTGate: Remove unused key.Destination.IsUnique() #7565
* VTGate: Add information_schema query on prepare statement #7746
* VTGate: Tests for numeric_precision and numeric_scale columns in information_schema #7763
* Disable flaky test until it can be fixed #7623
* Tests: reset stat at the beginning of test #7644
* Cleanup mysql server_test #7645
* vttablet: fix flaky tests #7543
* Removed unused tests for Wordpress installation #7516
* Fix unit test fail after merge #7550
* Add test with NULL input values for vindexes that did not have any. #7552


## VtctldServer
As part of an ongoing effort to transition from the VtctlServer gRPC API to the newer VtctldServer gRPC API, we have updated the local example to use the corresponding new vtctldclient to perform InitShardPrimary (formerly, InitShardMaster) operations.

To enable the new VtctldServer in your vtctld components, update the -service_map flag to include grpc-vtctld. You may specify both grpc-vtctl,grpc-vtctld to gracefully transition.

The migration is still underway, but you may begin to transition to the new client for migrated commands. For a full listing, refer either to proto/vtctlservice.proto or consult vtctldclient --help.


