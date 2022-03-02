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
* Healthcheck: use isIncluded correctly to fix replica/rdonly routing bug #6904

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
* VTGate: Initial implementation of vtctld service #7128
* VTGate: improve-log: FAILED_PRECONDITION #7215
* VTGate: Default to false for system settings to be changed per session at the database connection level #7299
* VTGate: Route INFORMATION_SCHEMA queries #6932
* VTGate: Adds Planning and Parsing Support for Create Index of MySQL 5.7 #7024
* VTGate: Log sql which exceeds max memory rows #7055
* VTGate: Enable Client Session Tracking feature in mysql protocol #6783
* VTGate: Show columns from table_name targeted like select queries #6825
* VTGate: This PR adds logic to simplify subquery expressions that are simple to
* VTGate: Adding MySQL Check Constraints #6865
* VTGate: Manage read your own writes system settings #6871
* VTGate: Allow table_schema comparisons #6887
* VTGate: Additional options support for SELECT INTO and LOAD DATA #6872
* VTGate: Fixes vtgate which throws an error in case of empty statements #6947
* VTGate: [Forward Port] #6940 - Fix error handling in olap mode #6949
* VTGate: Adds Planning and Parsing Support for Create View of MySQL 5.7 #7060
* VTGate: fix error: cannot run Select on table "dual" #7118
* VTGate: Allow system table to be set as default database #7150
* VTGate: Move auto_increment from reserved to non reserved keyword #7162
* VTGate: Add only expr of aliasedExpr to weightstring function #7165
* VTGate: [9.0] don't try to compare varchars in vtgate #7271
* VTGate: Load Data From S3 #6823
* VTGate: Unnest simple subqueries #6831
* VTGate: Adding MySQL Check Constraints #6869
* VTExplain: Add sequence table support for vtexplain #7186
* VSchema: Support back-quoted names #7073
* Healthcheck: healthy list should be recomputed when a tablet is removed #7176
* Healthcheck: Hellcatlk wants to merge 1 commit into master from master #6953

### Set Statement Support 

Set statement support has been added in Vitess. There are [some system variables](https://github.com/vitessio/vitess/blob/main/go/vt/sysvars/sysvars.go#L147,L190) which are disabled by default and can be enabled using flag `-enable_system_settings` on VTGate. These system variables are set on the mysql server. Because they change the mysql session, using them leads to the Vitess connection no longer using the connection pool and forcing dedicated connections.


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
* VReplication: Set time zone to UTC while streaming rows #6845
* VReplication: Materialization and character sets: Add test to verify/demo a workaround for charset issues while using string functions in filters #6847
* VReplication: Tool to diagnose vreplication issues in production #6892
* VReplication: Allow multiple blacklists for master #6816
* VStreamer Field Event: add allowed values for set/enum #6981
* VDiff: lock keyspace while snapshoting, restart target in case of errors #7012
* VDiff: make enums comparable #6880
* VDiff: add ability to limit number of rows to compare #6890
* VDiff/Tablet Picker: fix issue where vdiff sleeps occasionally for tablet picker retry interval #6944
* [vtctld]: fix error state in Workflow Show #6970
* [vtctld] Workflow command: minor fixes #7008
* MoveTables: validate that source tables exist, move all tables #7018
* SwitchWrites bug: reverse replication workflows can have wrong start positions #7169

### VTTablet

* VTTablet: fast and reliable state transitions #7011
* VTTablet: don't shutdown on too many connections #7039
* VTTablet: debug/env page to change variables in real-time #7189
* VTTablet: Adds better errors when there are timeouts in resource pools #7002
* VTTablet: Return to re-using server IDs for binlog connections #6941
* VTTablet: Correctly initialize the TabletType stats #6989
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
* Online DDL: Migration uses low priority throttling #6830
* Online DDL: Fix parsing of online-ddl command line options #6900
* OnlineDDL bugfix: make sure schema is applied on tablet #6910
* OnlineDDL: request_context/migration_context #7082
* OnlineDDL: Fix missed rename in onlineddl_test #7148
* OnlineDDL: Online DDL endtoend tests to support MacOS #7168

### VTadmin

* VTadmin: Initial vtadmin-api, clusters, and service discovery #7187
* VTadmin: The tiniest possible first implementation of vtadmin-web #7218
* VTadmin: Add cluster protos to discovery and vtsql package constructors #7224
* VTadmin: Add static file service discovery implementation #7229
* VTadmin: Query vtadmin-api from vtadmin-web with fetch + react-query #7239
* VTadmin: Move allow_alias option in MySqlFlag enum to precede the aliased IDs #7166
* [vtctld]:  vtctldclient generator #7238

### Other

* Fix comment typo #6974
* Fix all occurrences of `fmt.Sprint(x)` where x is `int` #7244
* Fix incorrect comments #7257
* Fix comment for IDPool #7212
* IsInternalOperationTableName: see if a table is used internally by vitess #7104
* Add timeout for mysqld_shutdown #6849
* Should receive healthcheck updates from all tablets in cells_to_watch #6852
* Workflow listall with no workflows was missing newline #6853
* Allow incomplete SNAPSHOT keyspaces #6863

## Examples / Tutorials

* Examples/operator: fix tags and add vtorc example #7358
* local docker: copy examples/common into /vt/common to match MoveTables user guide #7252
* Update docker-compose examples to take advantage of improvements in Vitess #7009

## Documentation

* Vitess Slack Guidelines v1.0 #6961
* Do vschema_customer_sharded.json before create_customer_sharded.sql #7210
* Added readme for the demo example #7226
* Adding @shlomi-noach to CODEOWNERS #6855
* Add Rohit Nayak to maintainers #6903
* 7.0.3 Release Notes #6902
* 8_0_0 Release Notes #6958
* Update maintainers of Vitess #7093
* Updating Email Address #7095
* Update morgo changes #7105
* Move PR template to .github directory #7126
* Fix trivial typo #7179
* Add @ajm188 + @doeg to CODEOWNERS for vtctld service files #7202
* Add @ajm188 + @doeg as vtadmin codeowners #7223:w


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
* Modify targets to restore behavior of make install #6842
* Download zookeeper 3.4.14 from archive site #6865
* Bump junit from 4.12 to 4.13.1 in /java #6870
* Fix ListBackups for gcp and az to work with root directory #6873
* Pulling bootstrap resources from vitess-resources #6875
* [Java] Bump SNAPSHOT version to 9.0 after Vitess release 8.0 #6907
* Change dependencies for lite builds #6933
* Truncate logged query in dbconn.go. #6959
* [GO] go mod tidy #7137
* goimport proto files correctly #7264
* Cherry pick version of #7233 for release-9.0 #7265
* Update Java version to 9.0 #7369
* Adding curl as dependency #6965

## Functionality Neutral Changes

* Healthcheck: add unit test for multi-cell replica configurations #6978
* Healthcheck: Correct Health Check for Non-Serving Types #6908
* Adds timeout to checking for tablets. #7106
* Remove deprecated vtctl commands, flags and vttablet rpcs #7115
* Fixes comment to mention the existence of reference tables. #7122
* Updated pull request template to add more clarity #7193
* Redact password #7198
* action_repository: no need for http.Request #7124
* Testing version upgrade/downgrade path from/to 8.0 #7323
* Use `context` from Go's standard library #7235
* Update `operator.yaml` backup engine description #6832
* Docker - upgrade to Debian Buster #6833
* Updating azblob to remove directory after removing backup #6836
* Fixing some flaky tests #6874
* Flaky test: attempt to fix TestConnection in go/test/endtoend/messaging #6879
* Stabilize test #6882
* Tablet streaming health fix: never silently skip health state changes #6885
* Add owners to /go/mysql #6886
* Fixes a bug in Load From statement #6911
* Query consolidator: fix to ignore leading margin comments #6917
* Updates to Contacts section as Reporting #7023
* Create pull_request_template #7027
* Fixed pull request template path #7062

## Backport
* Backport: [vtctld] Fix accidentally-broken legacy vtctl output format #7292
* Backport #7276: Vreplication V2 Workflows: rename Abort to Cancel #7339
* Backport #7297: VStreamer Events: remove preceding zeroes from decimals in Row Events
* Backport #7255: VReplication DryRun: Report current dry run results for v2 commands #7345
* Backport #7275: VReplication: Miscellaneous improvements #7349
* Backport 7342: Workflow Show: use timeUpdated to calculate vreplication lag #7354
* Backport 7361: vtctl: Add missing err checks for VReplication v2 #7363
* Backport 7297: VStreamer Events: remove preceding zeroes from decimals in Row Events #7340
