## Incompatible Changes

*This release includes the following changes which may result in incompatibilities when upgrading from a previous release*. *It is important that Vitess components are* _[upgraded in the recommended order](https://vitess.io/docs/user-guides/upgrading/#upgrade-order)_. *This will change in the next release as documented in* *[VEP-3](https://github.com/vitessio/enhancements/blob/master/veps/vep-3.md).*

* VTGate: The default gateway (-gateway_implementation flag value) is now "tabletgateway". The old default was “discoverygateway”. To use the old gateway, specify the flag in the command line. Anyone already using the flag will see no change; to use the new gateway, remove the -gateway_implementation flag. The new gateway is simpler and uses a new healthcheck module with no loss of functionality. #6155

* VTGate: VTGate incorrectly returned Warning Code field name as Type. This has now been fixed. Code or scripts that depend on the incorrect behavior might break.  #6437

* VReplication: The "-cell" parameter to MoveTables has been renamed to “-cells”, since it now accepts multiple comma-separated cells or cellsAliases. The parameter specifies which cells can provide tablets as the source for vreplication streams. #6456

* VTGate: The possibility of sending OpenTracing trace spans through query comments has been changed. The old way of doing it was severely broken, so our assumption is that no-one has successfully been using this feature. Still, this is an API change, and we are reporting it as a breaking change. #6463

* VTTablet has been refactored to be generally more responsive to state changes and its environment: #6131 #6139 #6241 #6263 #6311 #6345 #6348 #6384 #6396 #6432 #6461

    * Some initialization parameters are now mandatory: init_keyspace, init_shard and init_tablet_type. The flow where you could issue a vtctlclient InitTablet and then invoke vttablet without those init parameters is no longer supported.

    * Consequently vtctl's InitTablet and UpdateTabletAddrs are deprecated.

    * demote_master_type is deprecated: Now that init_tablet_type is mandatory, that value can act as the demote_master_type. There is no need for a separate flag.

    * VTTablet will publish the discovered MySQL port on startup, and will not update it during the life of the process. There is no known use case where a MySQL port is expected to change. So, there is no need to keep polling MySQL for its port.

    * Decouple enable_replication_watcher from disable_active_reparents. Currently, vttablet attempts to fix replication only if enable_replication_watcher is true and disable_active_reparents is false. This behavior has changed to depend only on the disable_active_reparents flag.

    * Transitioning from a serving type to SPARE used to go through a brief lameduck period specified by the serving_state_grace_period (default 0). This has been removed. The transition will now happen immediately even if a grace period is specified.

    * It was possible to enable health reporting and heartbeat at the same time. Performing the two at the same time is not useful. As of this release, enabling heartbeat automatically disables the polling mode of checking for replica health.

    * The ACL rules used to be reported on the vttablet status page. The design was using an incorrect framework. This has been removed.

* Build: make targets "build_web" and “embed_static” have been removed. Instead there are new targets “web_bootstrap”, “web_build” and “web_start” #6473

## Deprecations

* The following vtctl commands have been deprecated. Deprecated commands will continue to work until the next release (8.0).  #6428 #6345 #6461

    * StartSlave -> Replaced with StartReplication

    * StopSlave -> Replaced with StopReplication

    * ChangeSlaveType -> Replaced with ChangeTabletType

    * InitTablet

    * UpdateTabletAddrs

* Various vtctl commands had a flag called "-wait_slave_timeout". It has been replaced with “-wait_replicas_timeout”. Scripts using the old flag will continue to work until the next major release (8.0).  #6428

* Helm charts: The current helm charts have not been keeping up with the forward progress in vitess, and there has been no maintainer for it. Additionally, the operator has pulled ahead of what the helm charts could do. The operator can also be functionally more versatile than the helm charts because custom code can be written to manage complex workflows. To maintain focus and velocity in the project, we are deprecating the helm charts. #6439

* Consul: The usage of Consul has been problematic for vitess users. The main difficulty comes from the fact that it's non-trivial for a multi-cell deployment. Due to this, we want to discourage the use of consul as a topo server, and are deprecating support for it. #6440

## Bugs Fixed

### VTGate / MySQL compatibility

* Handle non-keyword tokens FORMAT, TREE and VITESS. (This was broken by #6110) #6445

* When using user-defined variables that have not been initialized first, a non-fatal panic occurs and the connection is dropped. #6294 #6308

* Setting system variable panics vtgate. #6162 #6163

* regression: USE does not work from MySQL client. #6153 #6157

* USE does not return an error if a keyspace doesn't exist. #3060 #6157

* Dotted table names are not escaped properly in lookup vindex flow. #6142 #6145

* Produce correct error code (1105 instead of 1062) when inserting duplicates into consistent_lookup vindex. #6144

* Rewriting expressions fail when used as function parameters. #6281

* Rollback after error clobbers updates. #6285

* Queries should not be wrapped into transactions if **autocommit=1** is set. #6264

* Prepared statements with bind variables in column specification produce ‘missing bind var’ error. #6287 #6298

* Prepared statement queries returning NULL results are not  parsed. #6240 #6249

* DEFAULT function not working with status column. #6221 #6224

* VTGate panic kills connection when selecting database() when using OLAP workload. #6200 #6205

* Wrong column value can be inserted due to parameterization variable confusion. #6266 #6270

* Fix owned table name formatting and duplicate error code in consistent lookup. #6145

* Support for case-insensitive user defined variables. #6225

* Fix performance regression on VTGate. #6203

* Return NULL for non-existent user-defined variables. #6308

* MySQL connection closing should not inflate query counts. #6226

* show vitess_shards intermittently returns empty list. #5038 #5189

* Protect insert lookup queries from bind variable name clashes #6328

* CAST/CONVERT should work correctly #6304

* Allow charset introducers for vindex columns #6301

* Fix panic from EXPLAIN #6204

* Handle special comments that start with ! #6186

* Fix panic with uppercase types #6140

* Allow ‘$’ in identifiers #6136

### Other

* VTTablet: Etcd query timeout because auto-committable transactions are not properly rolled back. #6258 #6299

* VTTablet: Fixed race in schema engine during notification of schema changes. #6268

* VTTablet: Reloading ACL config was throwing misleading error messages.his has now been fixed and the error messages improved. #6309

* VTTablet: Panic during backup caused an error in the pgzip library has been fixed by contributing a fix to pgzip and upgrading to a new release that includes the fix. #6189

* VTTablet: Improvement of error handling by retrying while uploading to S3. #6182 #6395 #6405

* VTTablet: builtinbackupengine was uploading a manifest despite errors from S3. Error collection during backup has been fixed to address this. #6349 #6350

* VTTablet: Fix dba connection leak in LockTables. #6424

* VTTablet: Fix connection pool leak in schema engine. #6426

* VTTablet: Fix regression where a heartbeat interval flag setting of 1s would have resulted in an internal value of 1000s #6356 #6357

* VTTablet: Fix panic from binlog_streamer when ‘-watch_replication_stream’ is set., #6253

* VTTablet: Add context to MysqlDaemon schema functions. This lets us handle disconnected RPCs instead of the client hanging forever. #6243

* VTTablet: fix panic from txlogz #6238

* VTTablet: Add back support for millisecond values for flags that was dropped when yaml config was introduced #6250

* Vtctld UI: Change all external links in vitess to use https #6170 #6169

* Ensure SrvVSchema gets rebuilt for all cells #6276

* Fix MariadbGTIDSet multi-domain support #6184

* mysqlctl: Fix connection leak during killConnection() #6245

* Fix release script to install examples in the correct directory #6159

* examples/operator: fix port-forward command #6418

* VStream client: should send current position events immediately #6391 #6397

## Functionality Added or Changed

### VTGate/ MySQL compatibility

* Transactions on read-only replicas are now supported. Start a transaction by first choosing the desired tablet_type ("use keyspace@replica"), then issuing a “BEGIN” statement. Internally, this will issue a “START TRANSACTION READ ONLY” on the underlying database. This is only available if using the new gateway_implementation of tabletgateway (which is now the default).  #6166 #6244

* Create better plans for NULL comparisons. Instead of using a SelectScatter, now vtgate will produce empty results when the predicate is a comparison against literal NULLs. #6133 #6425

* Support IN, NOT IN and equality (=) with null values #6152 #6146

* Add EXPLAIN support for vtgate plans. View the produced vtgate query plan by using explain format=vitess #6110

* Allow users to perform savepoint related constructs. #6355 #6404 #6412 #6414

* Added support for user-level advisory lock functions like get_lock and release_lock. This is considered experimental and will be improved in future releases based on user feedback. #6367 #6370 #6470

* Allow users to change system variable settings at session level using set statements. This is a major change that was merged later in the development cycle. It is considered experimental and there may be edge cases. #6107 #6459 #6472 #6488

* "ignore_nulls" option to lookup vindexes #6147 #6222:  There are situations where the "from" columns of a lookup vindex can be null. Such columns cannot be inserted in the lookup due to the uniqueness constraints of a lookup. The new “ignore_nulls” option of the lookup vindex allows you to accommodate null values in rows. Such rows can still be found through other means, like the primary vindex.

* Add support for row_count() function. #6174 #6179

* XOR Operator is now supported. #6369 #6371

* Allow 'binary' for character_set_results. It is equivalent to null. #6237

* Improve logging by adding keyspace, table, and tablet_type. #6282

* VTGate now takes in optional flags for ‘max_payload_size’ and ‘warn_payload_size’. A payload size above the warning threshold will only produce a warning. A payload size that exceeds ‘max_payload_size’ will result in an error. The default behavior is not to check payload size, which is backwards-compatible. #6143 #6375

* A new comment directive (‘/*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */) has been added to provide the ability to override the payload size checking per query. #6143

* A new comment directive (‘/*vt+ IGNORE_MAX_MEMORY_ROWS=1 */) has been added to provide the ability to ignore the "-max_memory_rows" setting per query. #6430

* Support ‘select * from information_schema.processlist’. #6310

* Support batch lookup queries for integer and binary types. Other types will continue to require one query per row. #6420

* Support system settings at session level. #6149

* Prepare for future features by adding reserved connections. This is the ability for a connected client to have a dedicated connection instead of using the connection pool. #6303 #6313 #6402 #6441 #6451

* Allow charset introducers with vindex column values. #6236 #6314

* Support Union All between different shards. #6351

* Exclude KeyspaceId(s) from plan cache key.This improves the cache hit rate. #6233

* Reload TLS config on SIGHUP. #6215

* Drop vindex DDL. #6151

* show vitess_tablets now reports master_term_start_time. We ensure that this is reported correctly after reparents. #6135 #6292 #6293

* Allow parenthesized selects #6342

* Support DO statements #6239

* Support calling database() inside other functions #6291

* Support IN, NOT IN and equality (=) with null values #6152 #6146

### VReplication

* VStream Copy extends the VStream API to allow streaming of the contents of databases or tables. Earlier you had to specify a position to start streaming events from. #6277

* VReplication workflows now accept "-cells" as an option. This can be a comma-separated list of cells or cellsAliases. The source tablet for vreplication will be chosen randomly among the tablets that belong to the specified cells/cellsAliases. If the option is not provided, we default to the cell that is local to the vreplication target. #6442, #6456 #6465

* Schema Versioning. Tablets now have a schema tracker that can be enabled to store schema versions as DDLs are applied. The _vt.schema_version table contains the schema with the corresponding ddl and gtid position when the ddl was applied. The tracked schemas are used by the new Historian service to provide vstreams with the correct schema while streaming old events. #6300 #6164 #6278 #6399 #6411 #6435 #6448

* Support for specifying multiple external sources to facilitate migration into vitess.

* Foreign Keys are now supported on the target database during vreplication. In the copy phase we disable FK checks and re-enable them once the replication phase starts. #6284

* Materialize: added a drop_constraint option during table copy to strip constraints while creating target tables #6422

* Materialize: optimizations while creating target tables resulting in a significant improvement in performance when moving a large number of tables from an external MySQL instance #6207, #6251

* DropSources: Added ‘-rename_tables’ option to rename the source tables rather than dropping them entirely. #6383

* Savepoints: Vreplication streams are not able to handle savepoint events in the binlog. #6438

* Add keyspace, shard and workflow labels to VReplicationSecondsBehindMaster metrics #6279

* Correct command line help for MigrateServedTypes, MigrateServedFrom and SwitchWrites #6187

* VExec and Workflow commands have been added to vtctld as a layer above VReplicationExec. They allow listing/stopping/starting/deleting workflows and ability to run custom queries on the _vt.vreplication table for all shards in a workflow. #6410

### Point-in-time Recovery

In the 4.0 release we introduced functionality to recover a keyspace into a snapshot based on a specified time. That feature only allowed you to restore a backup, but did not have the ability to automatically replay binlogs up to the specified time. The feature has now been completed so that true point-in-time recovery is possible. This is currently only supported for MySQL compatible installations, not MariaDB. #4857 #6267 #6408

### Healthcheck

* VTGate can now use a new healthcheck that is logically simpler while providing the same functionality. This is the default. The older healthcheck has been deprecated but is still available to use if desired by setting "-gateway_implementation=discoverygateway" #5750 #6155

* VTCombo has been migrated to use the new healthcheck #6302

* VTExplain has been migrated to use new healthcheck #6460

### MiniVitess

A docker container that can be used to bring up a self-contained vitess cluster, which can automatically resolve and self-configure to meet an external MySQL cluster. While this might sound very similar to VTCombo and vttestserver, it is not quite the same. 

MiniVitess spins up in docker. Given a MySQL server hostname, schema name & credentials, it runs an automated topology analysis via orchestrator, identifying the cluster members. It proceeds to bring up distinct tablet for each server and opens up vtgate connections. #6377

### Examples / Tutorials

* Vitess operator example #6154

* Region sharding example #6209 #6275

* Change local example to use new command DropSources #6178

* Are-you-alive example now makes it easier to test multiple endpoints #6202

* Local docker tutorial added to website

* Simplified helm chart for examples (however, helm is now deprecated) #6168

### Other

* vtctlclient ListAllTablets now shows the MasterTermStartTime. This is shown as "<null>" for read-only tablet types. In case of an old master that has not yet updated the topo record, TabletType will be reported as “unknown” #6135

* New debug handlers have been added to enable/disable golang block and mutex profiling. #6137

* Vitessdriver: Allow overriding driver name. Default stays as "vitess" #6138

* VTCombo: add options to allow starting MySQL within VTCombo. Note that vttestserver provides similar functionality. #6265

* Disable foreign key checks during preflight schema change tests #4696

* Fix labels for opentsdb #6289

* Allow empty shards when running ValidateSchemaKeyspace behind flag ‘-skip-no-master’ #6216

* Death by SIGPIPE on stdout/stderr can be avoided by setting the new flag ‘catch-sigpipe’ #6297

* Prepared Statement support added when using special vitess construct of use `<keyspace>:<shard>` by adding GetField support in engine plan execute #6132

* GTIDSet Union #6180

* Add relay log position and file based positions to ReplicationStatus #6217

* Function that can find errant GTIDs #6296

* VTTablet: Add StopReplicationMode to StopReplicationAndGetStatus RPC #6335

* VTTablet: DemoteMaster RPC returns full status instead of string representation of current position #6365

* VTTablet: WaitForPosition RPC can now accept either file-based or GTID-based positions #6374

## Documentation

* Fix links to local example and sample client in vitessdriver godoc #6254

* Flag documentation has been improved #6261

* All flags are now documented in the [program reference](https://vitess.io/docs/reference/programs/) on the website.

## Build Environment Changes

* golangci-lint now runs as a pre-commit hook and CI check #6385 #6406

* CI test for region_sharding example #6275

* Removed staticcheck as a pre-commit hook because it is included in golangci-lint #6416

* Faster unit tests #6415

* Consul-api version upgraded to 1.8.0 #6358

* Upgrade pgzip to v1.2.4 #6189

* Upgrade log4j2 version to 2.13.3 #6403

* Upgrade netty version to 4.1.48.Final #6317

* Upgrade tcnative version to 2.0.30.Final #6317 

* Add docker_local target to Makefile and a docker/local/run.sh script #6393

* Only check for k3s on Linux #6353

* UI build tooling was broken, it has now been fixed. #6473

## Functionality Neutral Changes

* Refactor table filtering logic #6242 #6259

* VTGate: Replace Sprintf with string concatenation in normalizer, this reduces CPU usage on benchmarks #6127

* VTTablet: Reduce the number of packets sent from vttablet to MySQL #6130

* Fixed flakiness in External Connector Test: fixed race #6201

* Fixed flakiness in Message Test: test had not been updated after jitter functionality was added #6283

* Fix flakiness in TestConnectTimeout #6188

* Fix TLS test for Go 1.13+ #6185

* Fixed linter errors #6416 #6364 #6307

* More dollar-sign tests #6363 

* VTExplain: use DiscoveryGateway (and old healthcheck) until migration can be completed #6248

* Deprecated RPCs from 6.0 have been deleted #6380

* Update references to orchestrator repo to point to openark instead of github #6360

* Terminology: deprecate or replace references to "slave" with “replica” except where referring to actual sql commands or fields returned from MySQL. #6428 #6392 #6379

* Terminology: replace user-facing references to "slave" with “replica” or “replication” in UI #6481

* Fix log format - use Infof instead of Info #6413

* Cleanup temp files used in tests #6400

* Add unit test for func stringMapToString #6280

* Check errors in etcd2topo unit test #6128

* Unit test for atomic.go #6120

* Unit test for ResolveIPv4Addrs #6230

* Enabled set statement tests #6167

* Replaced VTTablet ExecuteBatch api call with Execute for autocommit query. #6407 #6431

* Fixed flaky TestQueryPlanCache #6454

* Fix test compile errors #6434

* Scatter_conn unit test using new healthcheck #6458

* Test that vtexplain works with healthcheck #6252

* Tests: Removed error check that caused spurious test failures #6462

* More test cases for IGNORE_MAX_PAYLOAD_SIZE #6467

* VSCopy Test: Mods/bug fixes/test logging to make test repeatable/debuggable, fix flakiness #6341 #6235

* Cleanup scatter conn #6257

* Simplify vtgate executor #6256

* Add tests for SHOW COLUMNS #6192

* Added fuzzer #6175

* Simplified TxPool and ConnPool code #6150

