This release complies with VEP-3 which removes the upgrade order requirement. Components can be upgraded in any order. It is recommended that the upgrade order should still be followed if possible, except to canary test the new version of VTGate before upgrading the rest of the components.

## Incompatible Changes

The following PRs made changes to behavior that clients might be relying on. They should be reviewed carefully so that client code can be changed in concert with a Vitess release deployment.
* Change error code from 1203 to 1153 for three specific error conditions. #6630
  The three errors being changed:
When a GRPC message coming back to the vtgate exceeds the configured size limit (configured via grpc_max_message_size)
If a write is attempting to make a change with a payload that is larger than the configured allowable size
If a scatter query is attempting to collect and process too many rows in memory within the vtgate to construct a response
* Zero auto-increment mode: This enables inserting a 0 value (and not just NULL) into an auto-increment column to generate sequence values. This matches the default behavior of MySQL. #6749
* Turn off schema tracker by default #6712

## Bugs Fixed

### VTGate / MySQL compatibility
* Fix where clause in information schema with correct database name #6599
* Fix DDL execution on reserved connection without in active transaction #6514
* Fix allow passed table_schema in information_schema queries to vttablet if the keyspace is not found #6653
* Fix information schema for non-existent schema #6667
* Fix reserved connection in autocommit mode on DML #6748
* Fix dbname that needs escaping that we broke in fce4cfd894 #6794
* MySQL CLI, mysql --ssl does not work in MySQL 8.0 #5556
* OperationalError: (_mysql_exceptions.OperationalError) (2013, 'Lost connection to MySQL server during query') #6208
* Fix support for `show tables where tables_in_db` to use keyspace name, not underlying db name #6446
* Common empty result set query on MySQL returns a row in Vitess #6663
* Operator precedence issue #6734
* Table names out-of-order between replica and primary #6738
* Close Idle reserved connections and Rollback Idle transactions #6552
* Minor Vindex fixes: #6526
* VTgate does not report autocommit system variable and SERVER_STATUS_IN_TRANS flag correctly #5825
* MariaDB JDBC Connector fails to connect to vtgate with default options #5851
* VTgate panics if there are no vttablets and vtctlds #6117
* Remove shard session held in vtgate for reserved connection on connection failure #6522
* Session variable does not work as expected in 7.0 #6559
* Retry should not happen when in dedicated connection #6523
* Correctly report AUTOCOMMIT status in network packets #6551
* Close Idle reserved connections and Rollback Idle transactions #6552
* Reset Session for Reserved Connection Query Failure #6673
* Errors in multi-statement queries not fatal to statement sequence #6747 #6808
* Lock wait timeout on DELETE from table using lookup index #6698
* Application couldn't connect to external db giving error tabletserver.go:1522] Code: INVALID_ARGUMENT syntax error at position 33 near ':vtg2' (CallerID: gnocchi) : Sql: "select convert(:vtg1, CHAR(:vtg2)) as anon_1 from dual", #6466
* The client got the wrong database name #6753
* DBAPIError exception wrapped from (pymysql.err.InternalError) (1105, u'vtgate: http://gnocchi-gnocchizone-vtgate:15000/: reserved connections are not supported on old gen gateway') #6577
* Provide support for “SELECT INTO OUTFILE S3”  #6811
* Fix error around breaking of multi statements #6824
* Support SHOW TABLE STATUS FROM db type queries in vtgate #6354
* SHOW FULL FIELDS doesn't follow routing rules #6803
* VDiff/Tablet Picker: fix picker retry logic to handle canceled context #6954
* Healthcheck: Use isIncluded correctly to fix replica/rdonly routing bug #6922

### Other
* VReplication tablet_picker should keep trying to find a tablet until context expires #6546
* VReplication improve reverse workflow: update cells/tablet_types #6556
* VDiff: fix panic for tables with a unicode_loose_md5 vindex #6640
* Vtctld UI: Add mysqld port display back to vtctld web UI #6598 #6597 
* Vtctld: Fix workflow CopyState fetching (and thus printing). #6657
* Fix unsaved event regression #6710
* Change tracker to use allprivs instead of dba #6720
* Fixes long wait filter keyspace #6721
* s3 ListBackups doesn’t work from root directory #6732
* Vtctld: ExecuteFetchAsDba and ExecuteFetchAsAllPrivs do not escape db names. #6545
* Can't switch on restore_from_backup on existing shard with no tables #4896
* Refactor conn.go #6818


## Functionality Added or Changed

### VTGate / MySQL compatibility

* Add support for DELETE IGNORE #6722
* Only take on simple dual queries in the vtgate #6666
* Support mycli access to vtgate #4365
* Add support for unicode_loose_xxhash #6457
* Provide virtual information_schema #5394
* information_schema.tables_schema comparison with dynamic value no longer works #6827
* Mysqldump: Allow setting multiple variables in one statement #5401
* Block lock function usage other than dual table #6470
* Lock Session Support #6517
* Make sure lookup vindexes are queryable inside transaction #6499
* Add unicode_loose_xxhash Vindex type #6549
* Support vindex update or delete when destination is provided by client in dml query #6554
* Make sure to handle EXPLAIN on tablets #6579
* Rewrite SHOW TABLES #6615
* Make offset work in OLAP mode #6655
* Support sql_calc_found_rows with limit in sharded keyspace #6680
* Lock shard session heartbeat #6683
* Mysql -ssl tag is deprecated in the later versions of mysql #6682
* Use statement support in olap mode #6692
* Delete row using consistent lookup vindex in where clause #6700
* Add vtexplain support for non-evenly sharded keyspaces #6705
* Make sure dual queries set found_rows correctly #6718
* Add if not exists support to database ddl in parsing #6724
* Allow Update of same vindex using same vindex lookup on consistent lookup vindex #6736
* Update Vindex only on changes #6737
* Add support for `SHOW (DATABASES|VITESS_SHARDS|VITESS_TABLETS) LIKE` #6750
* 'CASCADE' support #6743
* Mask database name #6791
* Make sure to backtick schema names properly #6550
* Transaction Limiter: fix quota leak if Begin() errors or if the tx killer runs #6731
* Allow Load Data From S3 #6823

### OLAP Functioanlity 
* Allow switching between OLAP and OLTP #6691
* Use statement support in olap mode #6692
* Fix error handling in olap mode #6940


### Set Statement Support
Set statement support is added in Vitess. There are [some system variables](https://github.com/vitessio/vitess/blob/master/go/vt/sysvars/sysvars.go#L147,L190) which are disabled by default and can be enabled using flag `-enable_system_settings` on VTGate.These system variables are set on the backing MySQL instance, and will force the connection to be dedicated instead of part of the connection pool.

* Disabled passthrough system variables by default. #6859
* Allow switching workload between OLAP and OLTP #4086 #6691
* Support for multiple session set statement syntax #6508 #6495 #6494
* Allow enumeration in set system variables #6493
* Handle boolean settings better #6501
* Set statements refactored to use plan building #6487
* Add more system settings #6486
* Evaluate system variables #6708



### VReplication

* VReplication: _vt.vreplication source column VARBINARY->BLOB, allowing more tables to participate in a workflow #6421
* VReplication and vstreamer performance and error metrics #6519, #6132
* VReplication: Find collations for char primary keys and cast comparison during catchup #6568
* Varchar primary keys with collations can cause vreplication to miss rows #6622
* VReplication: Timestamp w/ Heartbeat #6635
* VReplication: Improve few error/log messages #6669
* VReplication: handle SET statements in the binlog stream #6772
* VReplication: e2e Tests added to CI #6730
* VDiff logs: additional logging around vdiff/vstreamer for observability #6684
* VStream: experimental POC of mysql protocol #6670
* Materialize: Support rollups using a literal value as a column #6733
* Materialization: Ignore non-participating tables that are renamed/deleted #6778
* Materialize: add ability for maintaining accurate table-level aggregates #6767
* Schema Tracker: Turn off schema tracker by default #6712 (breaking change)
* Workflow: Add List-All Command #6533
* Workflow: List -> Show, and Expand Metadata #6544
* Refactor: Cleanup Noisy Workflow Logging #6744
* Schema tracker: Change tracker to use the allprivs user instead of dba #6720
* vtctl: Adding VExec and Workflow commands #6410
* vtctl Workflow cmd: don't expect all shards to have workflow streams #6576
* Reshard: Sort table definitions before comparing them #6765 #6738
* Expose a shard range calculator as a vtctl command GenerateShardRanges #6751 #6752
* VStream: Field event now has all column attributes #6525
* VStream events do not populate field "Flags" on FIELD events #5914
* RFC: VReplication based SplitClone #4604
* Experimental: Mysql Protocol support for VStream #6675
* VReplication: Add metrics around vstreamer and vreplication metrics #6787
* Fix issues with missing tables in vstreams #6789
* Add validations and logs to fix common issues faced during MoveTables/Reshard #6814
* Allow multiple blacklists for master #6816

### VTtablet
* VTtablet throttling #6668
https://vitess.io/docs/reference/features/tablet-throttler/
* TabletManager: publish displayState #6648
* TabletManager: call setReadOnly inside ChangeTabletType #6762
* TabletManager: change how SetReadOnly is called to avoid errors from externally managed tablets #6786
* TabletManager: call SetReadOnly inside ChangeTabletType #6804
* vttablet: Open and Close healthStreamer #6515
* vttablet: create database if not present #6490
* Restore: do not change tablet type to RESTORE if not actually performing a restore #6679
* Restore: checkNoDB should not require tables to be present #6695
* Heartbeat: Additional metrics/stats (lag histogram) #6528 #6634
* VTtablet throttler feature flag: -enable-lag-throttler #6815
* VTtablet two-phase commit design doc #6498

### VTorc - Orchestrator
Following PRs are experimental version of Vitess-native Orchestrator 'vtorc' is ready for users to try out.

* Orchestrator: initial import of Orchestrator #6582
* Orchestrator: vitess mode #6613
* Orchestrator: more changes #6714
* Orchestrator: use contexts with timeout for remote operations #6780
* Orchestrator: fixed orchestrator govet errors #6781
* Orchestrator: Add clusters_to_watch flag to. Defaults to all clusters. #6821

### Point in Time Recovery

* PITR: Fix deadlock in ff code #6774
* Add flags to allow PITR binlog client TLS support #6775

### Other
* Add realtime health stats to vtctld /api/keyspace/ks/tablets API #6569
* vtctld UI: Fix logic for displaying vindexes #6603
* Online schema changes [#6547](https://vitess.io/docs/user-guides/schema-changes/)
* Table lifecycle management #6719, [docs](https://vitess.io/docs/reference/features/table-lifecycle/)
* Online-DDL: migration uses low priority throttling #6837
* Emergency Reparent Refactor #6449
  * Make emergency reparents more robust. #6206
* Reparent test failures #6706
* Update user-facing terminology in the vtctld2 UI #6481
* GetSchema: Batch/parallel access to underlying mysqld for lower latency #6491
* Prometheus interface: vtgate_buffer_* metrics cardinality #6326
* Show more vindex details in vtctld /app/schema UI #6450
* GRPC: update enforcement policy on server to match the one from the client #6629

## Examples / Tutorials

* region_sharding: working resharding example #6565
* vtcompose/docker-compose: fix InitShardMaster #6584
* [docker] fix tablet hostname #6509

## Documentation

* Replace more uses of slave with replica/replication #6520
* Update some option docstrings for accuracy. #6590
* Minor addition to Materialize help text. #6627
* Remove auto-generation comment #6728
* add skip flag that can skip comparing source & destination schema when run splitdiff #6477
* Fix and clarify some help options #6817

## Build Environment Changes

* Address #6631 by reducing optimization level #6632
* Add front-end build tooling + fix build error #6473
* Replace gogo proto with golang #6571
* Improve make proto #6580
* Remove chromium hard-coding #6636
* Docker - allow BUILD_NUMBER variable override #6628 
* Docker/lite: Fix package URLs for ubi images. #6704
* Helm & docker/k8s update #6723
* Fix build errors in ./go/vt/vtgate for go1.15 #6800
* Fix build error on vcursor_impl tests on newer versions of go #6799
* [java] bump java version to 8.0.0-SNAPSHOT for next release #6478
* Helm/docker: fix Docker builds + tag 7.0.2 #6773
* Docker: Upgrade to Debian Buster (release-8.0) #6888
* Docker: Pin mysql 5.7 and 8.0 versions for xtrabackup compatibility
* Docker: Revert "Docker - upgrade to Debian Buster (release-8.0)" #6929
* Zookeper: Download zookeeper 3.4.14 from archive site #6867

## Functionality Neutral Changes

* Routing cleanup: remove routeOptions #6531
* PITR:  testcase #6594
* Replaced Error with a warning in case parsing of VT_SPAN_CONTEXT fails #6766
* Reparent tests refactoring: setup/teardown for each test, reduce cut/paste, improve readability #6726
* Decider: endtoend test infrastructure + tests #6770
* Orchestrator: add more test cases #6801
* Orchestrator: don't set DbName for tests that don't need it. #6812
* Add unit test case to improve test coverage for go/sqltypes/named_result.go #6672
* End to end: deflake sharding tests #6715
* Workflow test: fix flaky test. remove obsolete code #6694
* Add unit test for func Proto3ValuesEqual. #6649
* Ensure tests for discoverygateway #6536
* Convert syslog events to normal log events #6511
* Add diagnostic logging to healthcheck. #6512
* Healthcheck should receive healthcheck updates from all tablets in cells_to_watch #6852 
* Named columns and convenience SQL query interface #6543
* Check http response code in vtctld API tests #6570
* Reverting package-lock.json to the one pre-PR #6603 #6611
* Removed sqlparser.preview to set logstats stmtType and use plan.type #6637
* AST struct name rewording #6642
* Fix: Remove SetMaster Query Expectation from ERS Test #6617
* Fix path for vttablet query log in end to end test #6656
* Fixes error log #6496
* Add diagnostic logging to healthcheck. #6535
* Try to install apt keys from list of different keyservers #6674
* Trivial copy-pasta comment fixup #6592
* Remove long-unused memcache and cacheservice #6596
* Fix MoveTables docstring; was not valid JSON. #6606
* Tablet gateway: unit tests #6608
* Tablet gateway doesn't see all tablets in cells from cells_to_watch #6846
* Add Sleep to ERS Unit Test #6610
* Strings to enum #6729
* Remove syslog dependency #3563
* Operator precedence must take associativity into consideration #6758
* Add optional ignore to delete statement #6802
* Invalid trace payload causes vitess to fail the request and return an error #6759
* Vttablet: minor logging fix #6564
* Vttablet: add more logging to checkMastership #6618
