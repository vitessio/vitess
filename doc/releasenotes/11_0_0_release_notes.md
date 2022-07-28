This release complies with VEP-3 which removes the upgrade order requirement. Components can be upgraded in any order. It is recommended that the upgrade order should still be followed if possible, except to canary test the new version of VTGate before upgrading the rest of the components.

## Known Issues

- A critical vulnerability [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228) in the Apache Log4j logging library was disclosed on Dec 9 2021.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and additional CVEs
  [CVE-2021-45046](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45046) and [CVE-2021-44832](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44832) followed.
  These have been fixed in release `2.17.1`. This release of Vitess, `v11.0.0`, uses a version of Log4j below `2.17.1`, for this reason, we encourage you to use version `v11.0.4` instead, to benefit from the vulnerability patches.

- An issue where the value of the `-force` flag is used instead of `-keep_data` flag's value in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174.

## Bug fixes
### Build/CI
* update moby/term to fix darwin build issue #7787
* Removing Linux/amd64 specific subdependencies #7796
* CI: run some tests inside docker to workaround GH Actions issue #7868
* Fix flaky race condition in vtexplain #7930
* Fixing flaky endtoend/tablegc tests #7947
* Add libwww-perl to allow pt-osc wrapper to work in vitess/lite #8141
### Cluster management
* Add missing return on a failed `GetShard` call in `FindAllShardsInKeyspace` #7992
* Increase the default srv_topo_timeout #8011
* parser: support index name in FOREIGN KEY clause; Online DDL to reject FK clauses #8058
* Correctly parse cell names with dashes in tablet aliases #8167
* /healthz should not report ok when vttablet is not connected to mysql #8238
* [topo] Refactor `ExpandCells` to not error on valid aliases #8291
* tm state: don't populate metadata in updateLocked  #8362
* [grpcvtctldserver] Fix backup detail limit math #8402
### Query Serving
* fix select star, col1 orderby col1 bug. #7743
* VTExplain: Add support for multi table join query #7829
* Routing of Information Schema Queries #7841
* Fix + integration test for `keyspaces_to_watch` routing regression [fixes #7882] #7873
* Revert "[tablet, queryrules] Extend query rules to check comments" #7897
* vtctl: return error on invalid ddl_strategy #7923
* Panic when EOF after @ symbol #7925
* Fixes encoding of sql strings #8033
* Make TwoPC engine initialization async. #8048
* Fix for issue with information_schema queries with both table name and schema name predicates #8087
* Fix for transactions not allowed to finish during PlannedReparentShard #8089
* Set fully parsed to false when ignoring errors #8094
* Fix for query and sub query with limits #8097
* Fix buffering when using reserved connections #8102
* Parse generated columns in DDL #8117
* More explicit message on online DDL parsing error #8118
* healthcheck: attempt to update primary only if the current tablet is serving #8121
* PRIMARY in index hint list for master #8160
* Signed int parse #8189
* Delete table reference alias support #8393
* Fix for function calls in DEFAULT value of CREATE TABLE statement in release-11.0 #8476
* Backport: Fixing multiple issues related to onlineddl/lifecycle #8517
* boolean values should not be parenthesised in default clause - release 11 #8531


### VReplication
* VDiff: Use byte compare if weight_string() returns null for either source or target #7696
* Rowlog: Update rowlog for the API change made for the vstream skew alignment feature #7809
* Pad binlog values for binary() columns to match the value returned by mysql selects #7969
* Change vreplication error metric name to start with a string to pass prometheus validations #7983
* Pass the provided keyspace through to `UpdateDisableQueryService` rather than hard-coding the sourceKeyspace #8020
* Fix vreplication timing metrics #8024
* Switchwrites: error if no tablets available on target for reverse replication #8142
* Remove noisy vexec logs #8144
* VReplicationExec: don't create new stats objects on a select #8166
* Binlog JSON Parser: handle inline types correctly for large documents #8187
* Schema Tracking Flaky Test: Ignore unrelated gtid progress events #8283
* Adds padding to keyrange comparison #8296
* VReplication Reverse Workflows: add keyspace scope to vindex while creating reverse vreplication streams #8385
* OnlineDDL/Vreplication stress test: investigating failures #8390
* Ignore SBR statements from pt-table-checksum #8396
* Return from throttler goroutine if context is cancelled to prevent goroutine leaks #8489
* VDiff: Add BIT datatype to list of byte comparable types #8401
* Fix excessive VReplication logging to file and db #8521

### VTAdmin
* Add missing return in `vtctld-*` DSN case, and log any flag that gets ignored #7872
* [vtadmin-web] Do not parse numbers/booleans in URL query parameters by default #8100
* [vtadmin-web] Small bugfix where stream source displayed instead of target #8311
## CI/Build
### Build/CI
* Planbuilder: Fix fuzzer #7952
* java: Bump SNAPSHOT version to 11.0.0-SNAPSHOT after Vitess release v10 #7968
* Add optional TLS feature to gRPC servers #8049
* Add image and base image arguments to build.sh #8064
* [fuzzing] Add report #8128
* CI: fail PR if it does not have required labels #8147
* trigger pr-labels workflow when labels added/removed from PR #8157
* Add vmg as a maintainer #8186
* Refactor vtorc endtoend tests #8215
* Use shared testing helper which guards against races in tmclient construction #8300
* moved multiple vtgate and tabletgateway to individual shards #8305
* include squashed PRs in release notes #8326
* pr-labels workflow: only check actual PRs #8327
* Heuristic fix for a flaky test: allow for some noise to pass through #8339
* ci: upgrade to Go 1.16 #8274
* mod: upgrade etcd to stable version #8357
* Resolve go.mod issue with direct dependency on planetscale/tengo #8383
* Initial GitHub Docker Build Setup #8399

### Cluster management
* make: build vitess as static binaries by default #7795
* Fix TableGC flaky test by reducing check interval #8270
### Java
* Bump commons-io from 2.6 to 2.7 in /java #7953
### Other
* Bump MySQL version for docker builds to 8.0.23 #7811
### Query Serving
* Online DDL Vreplication test suite: adding tests #8213
* Online DDL/VReplication: more passing tests (no UK/PK) #8225
* Online DDL: reject ALTER TABLE...RENAME statements #8227
* Online DDL/VReplication: fail on existence of FOREIGN KEY #8228
* Online DDL/VReplication: test PK column case change #8336
* Online DDL/VReplication: add PK DATETIME->TIMESTAMP test #8338
* Bugfix: assign cexpr.references for column CONVERTed to utf8mb4 #8355


### VReplication
* Towards a VReplication/OnlineDDL testing suite #8181
* Online DDL/Vreplication: column type awareness #8239
* remove duplicate ReadMigrations, fixing build #8258
* VReplication (and by product, Online DDL): support GENERATED column as part of PRIMARY KEY #8335
### vttestserver
* docker/vttestserver:  Set max_connections in default-fast.cnf at container build time #7810
## Documentation
### Cluster management
* Enhance k8stopo flag documentation #8458
### Build/CI
* Update version for latest snapshot #7801
* v10 GA Release Notes #7964
* Updates and Corrections to v9 Release Notes #8295
### Query Serving
* Update link to 'Why FK not supported in Online DDL' blog post #8372
### Other
* correct several wrong words #7822
* MAINTAINERS.md: Update enisoc's email. #7909
* Modification of the Markdown list format in the release notes template #7962
* Update vreplicator docs #8014
* Release notes: add Known Issues #8027
## Enhancement
### Observability
* [trace] Add logging support to opentracing plugins #8289


### Build/CI
* Makefile: add cross-build target for cross-compiling client binaries #7806
* git: improve signoff detection #7852
* Release notes generation #7932
* vitess/lite docker build fails for mysql80 #7943
* Fix a gofmt warning #7959
* docker/vttestserver/run.sh:  Add $CHARSET environment variable #7970
* docker/lite/install_dependencies.sh:  If the dependency install loop reaches its max number of retries, consider that a failure; exit the script nonzero so the build halts. #7976
* Add commit count and authors to the release notes #7982
* Automate version naming and release tagging #8034
* Make sure to only allow codegen when the rest of the code compiles #8169
* Add options to vttestserver to pass -foreign_key_mode, -enable_online_ddl, and -enable_direct_ddl through to vtcombo #8177
### Cluster management
* Dynamic throttle metric threshold #7742
* Bump Bootstrap version, per CVE-2018-14040 (orchestrator/vtorc) #7824
* Check tablet alias before removing after error stream #7915
* vttablet/tabletmanager - add additional test for tmstate.Open() #7993
* Add `vttablet_restore_done` hook #8007
* Added ValidateVSchema #8012
* Add RemoteOperationTimeout to both legacy and grpc `ChangeTabletType` implementations. #8052
* [vtctldserver] Add guard against self-reparent, plus misc updates #8084
* [tm_state] updateLocked should re-populate local metadata tables to reflect promotion rule changes #8107
* [vtctld] Migrate `ApplyVSchema` to `VtctldServer` #8113
* vtctl.generateShardRanges -> key.GenerateShardRanges #8134
* etcd: add grpc.WithBlock to client config #8205
* [workflow] Add tracing to `GetWorkflows` endpoint #8266
* [vtctldclient] Add legacy shim #8284
* [vtctldserver] Add tracing #8285
* [vtctldserver] Add additional backup info fields #8321
* [workflow] Call `scanWorkflow` concurrently #8272
### Query Serving
* [Gen4] Implemented table alias functionality in the semantic analysis #7629
* Improved error messages (tabletserver) #7747
* Allow modification of tablet unhealthy_threshold via debugEnv #7753
* [Gen4] Initial Horizon Planning #7775
* [tablet, queryrules] Extend query rules to check comments #7784
* Add support for showing global gtid executed per shard #7856
* Minor cleanups around errors on the vtgate #7864
* log unsupported queries #7865
* Show databases like #7912
* Add rank as reserved keyword #7944
* Online DDL: introducing ddl_strategy `-singleton-context` flag #7946
* Ignore the error and log as warn if not able to validate the current system setting for check and ignore case #8004
* Detect and signal schema changes on vttablets #8005
* DDL bypass plan #8013
* Online DDL: progress & ETA for Vreplication migrations #8015
* Scatter errors as warning in olap query #8018
* livequeryz: livequeryz/terminate link should be relative #8025
* Handle online DDL user creation if we do not have SUPER privs (e.g. AWS Aurora) #8038
* Update the schema copy with minimal changes #8067
* Gen4: Support for order by column not present in projection #8070
* Schema tracking in vtgate #8074
* protobuf: upgrade #8075
* added mysql 8.0 reserved keywords #8086
* Fix ghost/pt-osc in the external DB case where MySQL might be reporting #8115
* Support for vtgate -enable_direct_ddl flag #8116
* add transaction ID to query log for acquisition and analysis #8133
* Inline reference #8136
* Improve ScatterErrorsAsWarnings functionality #8139
* Gen4: planning Select IN #8155
* Primary key name #8188
* Online DDL: read and publish gh-ost FATAL message where possible #8192
* Expose inner net.Conn to be able to write better unit tests #8217
* vtgate: validate important flags during startup #8218
* Online DDL/VReplication: AUTO_INCREMENT support and tests #8223
* [Gen4] some renaming from v4 to Gen4  #8234
* Schema Tracking: Optimize and Bug Fix #8243
* gen4: minor refactoring to improve readability #8245
* gen4: plan more opcodes #8254
* Online DDL: report rows_copied for migration (initial support in gh-ost) #8255
* Schema tracking: new tables in sharded keyspace #8256
* [parser] use table_alias for ENGINE option in CREATE TABLE stmt #8307
* Add /debug/env for vtgate #8292
* gen4: outer joins #8312
* Gen4: expand star in projection list #8325
* gen4: Fail all queries not handled well by gen4 #8359
* Gen4 fail more2 #8382
* SHOW VITESS_MIGRATION '...' LOGS, retain logs for 24 hours #8532
* [11.0] query serving to continue when topo server restarts #8533
* [11.0] Disable allowing set statements on system settings by default #8540
### VReplication
* Change local example to use v2 vreplication flows and make v2 flows as the default #8527
* Use Dba user when Vexec is runAsAdmin #7731
* Add table for logging stream errors, state changes and key workflow steps #7831
* Fix some static check warning #7960
* VSchema Validation on ReshardWorkflow Creation #7977
* Tracking `rows_copied` #7980
* Vdiff formatting improvements #8079
* Ignore generated columns in workflows #8129
* VReplication Copy Phase: Increase default replica lag tolerance. Also make it and copy timeout modifiable via flags #8130
* Materialize: Add additional comparison operators in Materialize and fix bug where they not applied for sharded keyspaces #8247
* Copy Phase: turn on OptimizeInserts by default #8248
* Tracker/VStreamer: only reload schema for tables in current database and not for internal table artifacts #8257
* Online DDL/Vreplication suite: support ENUM->VARCHAR/TEXT type change #8275
* Added TableName to DiffReport struct. #8279
* New VReplication lag metric  #8306
* VStream API: add heartbeat for idle streams #8244
* Online DDL/VReplication: support non-UTF8 character sets #8322
* Online DDL/Vreplication suite: fix test for no shared UK #8334
* Online DDL/VReplication: support DROP+ADD column of same name #8337
* Online DDL/VReplication test suite: support ENUM as part of PRIMARY KEY #8345
* Change local example to use v2 vreplication flows #8527
* Tablet Picker: add metric to record lack of available tablets #8403
### VTAdmin
* [vtadmin-web] Add useSyncedURLParam hook to persist filter parameter in the URL #7857
* [vtadmin-web] Display more data on /gates view and add filtering #7876
* [vtadmin] Promote ErrNoSchema to a TypedError which returns http 404 #7885
* [vtadmin] gate/tablet/vtctld FQDNs #7886
* [vtadmin-web] Display vindex data on Schema view #7917
* [vtadmin-web] Add filtering and source/target shards to Workflows view #7948
* [vtadmin-web] Add filtering + shard counts/status to Keyspaces view #7991
* [vtadmin-web] Display shard state on Tablets view + extract tablet utilities  #7999
* [vtadmin] experimental tabletdebug #8003
* [vtadmin-web] Display timestamps + stream counts on Workflows view #8009
* [vtadmin-web] Updates to table styling + other CSS #8072
* [vtadmin-web] Add Tab components #8119
* [vtadmin-api] Update GetTablet to use alias instead of hostname #8163
* [vtadmin-api] Rename flag 'http-tablet-fqdn-tmpl' to 'http-tablet-url-tmpl' + update vtadmin flags for local example #8164
* Add `--tracer` flag to vtadmin and actually start tracing #8165
* [vtadmin-web] The hastiest Tablet view (+ experimental debug vars) #8170
* [vtadmin-web] Add tabs to Workflow view #8203
* [vtctld] Add GetSrvVSchemas command #8221
* [vtadmin-api] Add HTTP endpoints for /api/srvvschemas and /api/srvvschema/{cluster}/{cell} #8226
* [vtadmin-web] Add QPS and VReplicationQPS charts to Tablet view #8263
* [vtadmin-web] Add client-side error handling interface + Bugsnag implementation #8287
* [vtadmin-web] Add chart for stream vreplication lag across all streams in a workflow #8331
* [vtctldproxy] Add more annotations to vtctld Dial calls #8346


### vttestserver
* Vttest create db #7989
* Adds an environment variable to set the MySQL max connections limit in vttestserver docker image #8210
## Feature Request
### Build/CI
* [trace] Add optional flags to support flexible jaeger configs #8199
### Cluster management
* Add VtctldServer to vtcombo #7896
* [vtctldserver] Migrate routing rules RPCs, and also `RebuildVSchemaGraph` #8197
* [vtctldserver] Migrate `CellInfo`, `CellAlias` rw RPCs #8219
* [vtctldserver] Add RefreshState RPCs #8232
### Query Serving
* VTGate grpc implementation of Prepare and CloseSession #8211
* Schema tracking: One schema load at a time per keyspace #8224
### VTAdmin
* [vtadmin-web] Add DataFilter + Workspace layout components #8032
* [vtadmin-web] Add Tooltip + HelpTooltip components #8076
* [vtadmin-web] Add initial Stream view, render streams on Workflow view #8091
* [vtadmin-web] The hastiest-ever VTExplain UI #8092
* [vtadmin-web] Add source-map-explorer util #8093
* [vtadmin-web] Add Keyspace detail view #8111
* [vtadmin-api] Add GetKeyspace endpoint #8125
* [workflow] Add vreplication_log data to workflow protos, and `VtctldServer.GetWorkflows` method #8261
* [vtadmin] Add debug endpoints #8268
## Internal Cleanup
### Build/CI
* Makefile: fix cross-build comments #8246
* remove unused hooks with refs to master branch #8250
* [vtctldserver] Update tests to prevent races during tmclient init #8320
* sks-keyservers.net seems to be finally dead, replace with #8363
* endtoend: change log level of etcd start from error to info #8370
### Cluster management
* Online DDL: code cleanup #7589
* [wrangler|topotools] Migrate `UpdateShardRecords`, `RefreshTabletsByShard`, and `{Get,Save}RoutingRules` #7965
* Tear down old stream_migrater shim, now that we're fully in `package workflow` #8073
* [mysqlctl] Restructure `MetadataManager` to reduce public API surface area #8152
* naming: master to primary #8251
* vtorc: code cleanup #8269
### Query Serving
* Plan StreamExecute Queries #7941
### VReplication
* [workflow] extract migration targets from wrangler #7934
* [wrangler|workflow] Extract vrStream type to workflow.VReplicationStream #7966
* [wrangler|workflow] Extract `workflowState` and `workflowType` out to `package workflow` #7967
* [wrangler|workflow] extract `*wrangler.streamMigrater` to `workflow.StreamMigrator` #8008
* [workflow] Migrate `getCellsWith{Shard,Table}ReadsSwitched`, `TrafficSwitchDirection` and `TableRemovalType` to package workflow #8190
* [workflow] Cleanup wrangler wrappers, migrate `checkIfJournalExistsOnTablet` to package workflow #8193
* Backports of #8403 #8483 #8489 #8401 #8521 #8396 from main into release 11.0 #8536
### VTAdmin
* [vtadmin-api] Replace magic numbers with `net/http` constants #8127
* [vtadmin-web] Move single-entity view components into subfolders #8202
* [vtadmin] Ensure we log any errors when closing the tracer #8262
## Other
### Build/CI
* add vtctldclient to binary directory #7889
### Cluster management
* vttablet/tabletmanager: add isInSRVKeyspace/isShardServing #7929
### Other
* Change VitessInputFormat key type #199
* Online DDL plan via Send; "singleton" migrations on tablets #7785
* flaky onlineddl tests: reduce -online_ddl_check_interval #7847
* Looking into flaky endtoend upgrade test #7900
* fixing flaky upgrade test #7901
### Query Serving
* Introduce Concatenated Fixed-width Composite or CFC vindex #7537
* Add common tags for stats backends that support it #7651
* Add support for showing global vgtid executed #7797
* perf: vttablet/mysql optimizations #7800
* Fix bug with reserved connections to stale tablets #7879
* Memory Sort to close the goroutines when callback returns error  #7903
* OnlineDDL: more migration check ticks upon migration start #7961
* Update gh-ost binary to v1.1.3 #8021
* VReplication Online DDL: fix classification of virtual columns #8043
### VReplication
* VReplicationErrors metric: use . as delimiter instead of _ to behave well with Prometheus #7807
### VTAdmin
* Update `GetSchema` filtering to exclude shards where `IsMasterServing` but no `MasterAlias` #7805
* [vtadmin-api] Reintroduce include_non_serving_shards opt to GetSchema #7814
* [vtadmin-web] Add DataCell component #7817
* Rewrite useTableDefinitions hook as getTableDefinitions util #7821
* [vtadmin-web] Display (approximate) table sizes + row count on /schemas view #7826
* [vtadmin-web] Add Pip + TabletServingPip components #7827
## Performance
### Query Serving
* vttablet: stream consolidation #7752
* perf: optimize bind var generation #7828
### VReplication
* Optimize the catchup phases by filtering out rows which not within range of copied pks during inserts #7708
* Ability to compress gtid when stored in _vt.vreplication's pos column #7877
* Performance & benchmarks (table copying) #7881
* Dynamic packet sizing #7933
* perf: vreplication client CPU usage #7951
* VDIff: fix performance regression introduced by progress logging  #8016
* proto: Generate faster code using vtprotobuf #8173
* proto: enable pooling for vreplication #8273
## Testing
### Build/CI
* Attempt to fix TLS Server Flaky test #7842
* TestMakeCommonTags Flaky Test: match elements  #7843
* upgrade tests: test against v9.0.0 #7848
* FOSSA scan added #7862
* ci: fix all racy tests #7904
* CI: Revert docker change for unit tests from #7868 #7940
* Add online ddl on start queries to schema list in vtexplain tablet #8397
### Java
* [Java] JDBC mysql driver test #8154
### Other
* Fuzzing: Fixup oss-fuzz build script #7782
* Wrangler tests: Return a fake tablet in the wrangler test dialer to avoid tablet picker errors spamming the test logs #7863
### Query Serving
* clean up test #7816
* tabletserver: fix flaky test #7851
* Planbuilder: Add fuzzer #7902
* mysql: Small adjustments to fuzzer #7907
* vtgate/engine: Add fuzzer #7914
* Adding Fuzzer Test Cases #8106
* Addition of fuzzer issues #8195
### VReplication
* vstreamer: Add fuzzer #7918
### vttestserver
* Speedup new vttestserver tests #8229
* Vttestserver docker test #8253
### Cluster management
* Make timestamp authoritative for master information #8381


The release includes 1080 commits (excluding merges)

Thanks to all our contributors: @AdamKorcz, @GuptaManan100, @Hellcatlk, @Johnny-Three, @acharisshopify, @ajm188, @alexrs, @aquarapid, @askdba, @deepthi, @doeg, @dyv, @enisoc, @frouioui, @gedgar, @guidoiaquinti, @harshit-gangal, @hkdsun, @idvoretskyi, @jmoldow, @kirs, @mcronce, @narcsfz, @noxiouz, @rafael, @rohit-nayak-ps, @setassociative, @shlomi-noach, @systay, @tokikanno, @vmg, @wangmeng99, @yangxuanjia, @zhangshj-inspur