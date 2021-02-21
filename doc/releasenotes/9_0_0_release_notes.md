This release complies with VEP-3 which removes the upgrade order requirement. Components can be upgraded in any order. It is recommended that the upgrade order should still be followed if possible, except to canary test the new version of VTGate before upgrading the rest of the components.

## Incompatible Changes

The following PRs made changes to behaviors that clients might rely on. They should be reviewed carefully so that client code can be changed in concert with a Vitess release deployment.
* The update to golang 1.15 (#7204) might break systems that use TLS certificates with a common name. A fix is documented here (https://github.com/golang/go/issues/40748#issuecomment-673612108)

Vitess 9.0 is not compatible with the previous release of the Vitess Kubernetes Operator (2.2.0). A new version of the Operator (2.3.0) is available that is compatible.

## Bugs Fixed

### VTGate / MySQL compatibility
* Set Global #6957
* Set udv allow more expressions #6964
* Bug which caused the connection to not close in case of error writing an error packet #6977
* Bug fix #7048 for SelectNone in StreamExecute route engine #7050

### Other
* Binary PK: fix bug where padding of binary columns was being done incorrectly #6963
* Pad non-fractional part of DECIMAL type #6967
* Bug fix regression in /healthz #7090
* Fix metadata related operation hangs when zk down #7228
* Fix accidentally-broken legacy vtctl output format #7285

## Functionality Added or Changed

### VTGate / MySQL compatibility

* VTGate: Allow INSERT with all defaults #6969
* VTGate: Allow YEAR column type with length specified #6975
* VTGate: Retry Execute when reserved connection is lost #6983
* VTGate: Use ephemeral buffer when reading rows #6990
* VTGate: Allow time_zone in reserved connection #6998
* VTGate: Improved support for UNION #7007
* VTGate: Add DDL parser support for FULLTEXT indexes. #7001
* VTGate: Extend comparability of EvalResult to support hash codes #7016
* VTGate: Use proto equal method #7017
* VTGate: Fix COM_STMT_EXECUTE packet decode #7020
* VTGate: Adds Planning and Parsing Support for Create Index of MySQL 5.7 #7024
* VTGate: UNION DISTINCT support on vtgate #7029
* VTGate: Improve database ddl plan #7034
* VTGate: Support for hex & shard in vindex query #7044
* VTGate: Use distinct primitive to solve more queries #7047
* VTGate: Route using vindex for composite IN clause #7049
* VTGate: Optimise struct field layout #7052
* VTGate: Refactoring of plan building #7054
* VTGate: Rewrite joins written with the USING construct #6660
* VTGate: Add option to GetSchema to only send the row count and data length over the wire 
#6985
* VTGate: Adds Planning and Parsing Support for Create Database of MySQL 5.7 #7068
* VTGate: Make sure to check all GROUP BY columns #7080
* VTGate: Separate sub-query and derived table into different structs #7081
* VTGate: Adds Planning and Parsing Support for Alter Database of MySQL 5.7 #7086
* VTGate: Convert usages of DDL struct to DDLStatement interface #7096
* VTGate: Adds Planning and Parsing Support for Drop Database of MySQL 5.7 #7098
* VTGate: Restore SHOW SCHEMAS support; fixes #7100 #7102
* VTGate: Refactor Code to create a separate struct for CREATE TABLE #7116
* VTGate: Allows for vttestserver and vtcombo to respond to VtGateExecute. #7121
* VTGate: Support for lock and unlock tables #7139
* VTGate: Merge SelectDBA routes when possible #7140
* VTGate: Adds support for all the rails queries using information_schema #7143
* VTGate: Add support for unary expression in order by column #7163
* VTGate: Skip query rewriting for dual table #7164
* VTGate: Refactor Code to create a separate struct for ALTER VSCHEMA #7173
* VTGate: Refactor Show plans #7185
* VTGate: Show privilege support #7194
* VTGate: Planning and Parsing Support for Drop Table, Drop View and Alter View #7178
* VTGate: Cache only dml and select plans #7196
* VTGate: Planning and Parsing Support for Alter Table #7199
* VTGate: Add FindAllShardsInKeyspace to vtctldserver #7201
* VTGate: improve-log: FAILED_PRECONDITION #7215
* VTGate: Planner refactoring #7103
* VTGate: Migrate `vtctlclient InitShardMaster` => `vtctldclient InitShardPrimary` #7220
* VTGate: Add Planning and Parsing Support for Truncate, Rename, Drop Index and Flush #7242
* VTGate: Fix create table format function to include if not exists #7250
* VTGate: Added default databases when calling 'show databases' #7256
* VTGate : Add Update.AddWhere to mirror Select.AddWhere #7277
* VTGate :Rremoved resolver usage from StreamExecute #7281
* VTGate: Adding a MySQL connection at Vtgate to run queries on it directly in case of testing mode #7291
* VTGate: Added vitess_version as variable #7295
* VTGate: Default to false for system settings to be changed per session at the database connection level #7299
* VTGate: Gen4: Add Limit clause support #7312
* VTGate: Gen4: Handling subquery in query graph #7313
* VTGate: Addition of @@enable_system_settings #7300
* VTGate: Route INFORMATION_SCHEMA queries #6932
* VTGate: Adds Planning and Parsing Support for Create Index of MySQL 5.7 #7024
* VTGate: Log sql which exceeds max memory rows #7055
* VTExplain: Add sequence table support for vtexplain #7186
* VSchema: Support back-quoted names #7073
* Healthcheck: healthy list should be recomputed when a tablet is removed #7176

### Set Statement Support 

Set statement support has been added in Vitess. There are [some system variables](https://github.com/vitessio/vitess/blob/master/go/vt/sysvars/sysvars.go#L147,L190) which are disabled by default and can be enabled using flag `-enable_system_settings` on VTGate. These system variables are set on the mysql server. Because they change the mysql session, using them leads to the Vitess connection no longer using the connection pool and forcing dedicated connections.


### VReplication

* VReplication: refactored and enhanced support for JSON columns #6829
* VReplication: Don't update tx timestamp on heartbeat #6930
* VReplication E2E Tests: Refactored tests for readability and attempting to fix flakiness #6991
* VRepl/Tablet Picker: improve observability of selected tablet #6999
* VReplication: Handle comment statement type in vstreamer #7092
* VReplication e2e: Fine tuned test to reduce flakiness and added more logging to debug future flakiness #7138
* VReplication: Make relay log size & rows configurable. #6992
* VReplication: New workflows cli UX. Allow reads/writes to be switched independently #7071
* VReplication: DropSources: change table rename logic #7230
* VReplication: MoveTables: delete routing rules and update vschema on Complete and Abort #7234
* VReplication: V2 Workflow Start: wait for streams to start and report errors if any while starting a workflow #7248
* VReplication: Ignore temp tables created by onlineddl #7159
* VReplication V2 Workflows: rename Abort to Cancel #7276
* VReplication DryRun: Report current dry run results for v2 commands #7255
* VReplication: Miscellaneous improvements #7275
* VReplication: Tablet throttle support "/throttle/check-self" available on all tablets #7319
* VStreamer Events: remove preceding zeroes from decimals in Row Events #7297
* Workflow Show: use timeUpdated to calculate vreplication lag #7342
* vtctl: Add missing err checks for VReplication v2 #7361
* VStreamer Field Event: add allowed values for set/enum #6981
* VDiff: lock keyspace while snapshoting, restart target in case of errors #7012
* [vtctld]: fix error state in Workflow Show #6970
* [vtctld] Workflow command: minor fixes #7008
* [vtctl] Add missing err checks for VReplication v2 #7361

### VTTablet

* VTTablet: fast and reliable state transitions #7011
* VTTablet: don't shutdown on too many connections #7039
* VTTablet: debug/env page to change variables in real-time #7189
* VTTablet: Adds better errors when there are timeouts in resource pools #7002
* VTTablet: Return to re-using server IDs for binlog connections #6941
* VTTablet: Correctly initialize the TabletType stats #6989
* Backup: Use provided xtrabackup_root_path to find xbstream #7359
* Backup: Use pargzip instead of pgzip for compression. #7037
* Backup: Add s3 server-side encryption and decryption with customer provided key #7088

### OnlineDDL

* Online DDL: follow ups in multiple trajectories #6901
* Online DDL: cancel running migrations executed by another tablet #7006
* OnlineDDL: Adding `ddl_strategy` session variable #7042
* Online DDL: ddl_strategy session variable and vtctl command line argument #7045
* Online DDL:  Removing online ddl query hint from ALTER TABLE #7069
* Online DDL: vtgate -ddl-strategy flag renamed to -ddl_strategy #7074
Automatically retry migration that was interrupted during master failover
Automatically terminate migrations run by a failed tablet
* Online DDL:request_context/migration_context #7082 
* Online DDL: Support CREATE, DROP statements in ApplySchema and online DDL #7083
* Online DDL: ddl_type column #7097
* OnlineDDL: "cancel-all" command to cancel all pending migrations in keyspace #7099
* OnlineDDL: Support `vtctl OnlineDDL <keyspace> show <context>` #7145
* OnlineDDL: Normalizing Online-DDL queries #7153
* Online DDL: ddl_strategy=direct #7172
* Online DDL: Executor database pool size increase #7206
* Online DDL: DROP TABLE translated to RENAME TABLE statement #7221
* Online DDL: Adding @@session_uuid to vtgate; used as 'context' #7263
* Online DDL: ignore errors if extracted gh-ost binary is identical to installed binary #6928
* Online DDL: Table lifecycle: skip time hint for unspecified states #7151


### VTadmin

* VTadmin: Initial vtadmin-api, clusters, and service discovery #7187
* VTadmin: The tiniest possible first implementation of vtadmin-web #7218
* VTadmin: Add cluster protos to discovery and vtsql package constructors #7224
* VTadmin: Add static file service discovery implementation #7229
* VTadmin: Query vtadmin-api from vtadmin-web with fetch + react-query #7239
* VTadmin: Add vtctld proxy to vtadmin API, add GetKeyspaces endpoint #7266
* VTadmin: [vtctld] Expose vtctld gRPC port in local Docker example + update VTAdmin README #7306
* VTadmin: Add CSS variables + fonts to VTAdmin #7309
* VTadmin: Add React Router + a skeleton /debug page to VTAdmin #7310
* VTadmin: Add NavRail component #7316
* VTadmin: Add Button + Icon components #7350
* [vtctld]:  vtctldclient generator #7238
* [vtctld] Migrate cell getters #7302
* [vtctld] Migrate tablet getters #7311
* [vtctld] Migrate GetSchema #7346
* [vtctld] vtctldclient command pkg #7321
* [vtctld] Add GetSrvVSchema command #7334
* [vtctld] Migrate ListBackups as GetBackups in new vtctld server #7352
 Merged
* [vtctld] Migrate GetVSchema to VtctldServer #7360

### Other

* Fix comment typo #6974
* Fix all occurrences of `fmt.Sprint(x)` where x is `int` #7244
* Fix incorrect comments #7257
* Fix comment for IDPool #7212
* IsInternalOperationTableName: see if a table is used internally by vitess #7104

## Examples / Tutorials

* Update demo #7205
* Delete select_commerce_data.sql #7245
* Docker/vttestserver: Add MYSQL_BIND_HOST env #7293
* Examples/operator: fix tags and add vtorc example #7358
* local docker: copy examples/common into /vt/common to match MoveTables user guide #7252
* Update docker-compose examples to take advantage of improvements in Vitess #7009

## Documentation

* Vitess Slack Guidelines v1.0 #6961
* Do vschema_customer_sharded.json before create_customer_sharded.sql #7210
* Added readme for the demo example #7226
* Pull Request template: link to contribution guide #7314

## Build Environment Changes

* Clean up plan building test cases #7057
* Fix unit test error #6953, #6993
* Fixing the 5.6 builds of vitess/lite #6960
* Pin mariadb to use mariadb-server-10.2 #6966
* Replace vitess:base with vitess:lite images for docker-compose services #7004
* Fix flakey TestParallelRunnerApprovalFirstRunningSecondRunning test #7014
* Allow custom image tags in compose #7043
* Support statsd for vitess #7072
* Add vtctl to make install-local #7125
* Updating Java unit tests for JDK9+ compatibility #7144
* Add Go Version to Bootstrap Image #7182
* Update Vitess v8.0 images #7174
* Fix broken package ref in UBI docker build #7183
* Convert CentOS extra packages installation to yum instead of downloading #7188
* Make docker_local: fix missing mysql_server package #7213
* Add unit test case to improve test coverage for go/sqltypes/result.go #7227
* Update Golang to 1.15 #7204
* Add linter configuration #7247
* Tracking failed check runs #7026
* Github Actions CI Builds: convert matrix strategy for unit and cluster tests to individual tests #7258
* Add Update.AddWhere to mirror Select.AddWhere #7277
* Descriptive names for CI checks #7289
* Testing upgrade path from / downgrade path to v8.0.0 #7294
* Add mysqlctl to docker images #7326

## Functionality Neutral Changes

* Healthcheck: add unit test for multi-cell replica configurations #6978
* Adds timeout to checking for tablets. #7106
* Remove deprecated vtctl commands, flags and vttablet rpcs #7115
* Fixes comment to mention the existence of reference tables. #7122
* Updated pull request template to add more clarity #7193
* Redact password #7198
* action_repository: no need for http.Request #7124
* Testing version upgrade/downgrade path from/to 8.0 #7323
* Use `context` from Go's standard library #7235

