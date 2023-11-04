# Release of Vitess v13.0.0
## Major Changes

### Vitess now has native support for MySQL collations

When using the gen4 planner, Vitess is now capable of performing collation-aware string comparisons in the vtgates. This
improves the performance and reliability of several query plans that were previously relying on a debug-only SQL API in
MySQL to perform these comparisons remotely. It also enables new query plans that were previously not possible.

A full list of the supported collations can be
found [in the Vitess documentation](https://vitess.io/docs/13.0/user-guides/configuration-basic/collations/).

### The native Evaluation engine in vtgate has been greatly improved

The SQL evaluation engine that runs inside the vtgates has been rewritten mostly from scratch to more closely match
MySQL's behavior. This allows Vitess to execute more parts of the query plans locally, and increases the complexity and
semantics of the SQL expressions that can be used to perform cross-shard queries.

### vttablet -use_super_read_only flag now defaults to true

The default value used to be false. What this means is that during a failover, we will set `super_read_only` on database
flavors that support them (MySQL 5.7+ and Percona 5.7+). In addition, all Vitess-managed databases will be started
with `super-read-only` in the cnf file. It is expected that this change is safe and backwards-compatible. Anyone who is
relying on the current behavior should pass `-use_super_read_only=false` on the vttablet command line, and make sure
they are using a custom my.cnf instead of the one provided as the default by Vitess.

### vtgate -buffer_implementation now defaults to keyspace_events

The default value used to be `healthcheck`. The new `keyspace_events` implementation has been tested in production with
good results and shows more consistent buffering behavior during PlannedReparentShard operations. The `keyspace_events`
implementation utilizes heuristics to detect additional cluster states where buffering is safe to perform, including
cases where the primary may be down. If there is a need to revert back to the previous buffer implementation, ensure
buffering is enabled in vtgate and pass the flag `-buffer_implementation=healthcheck`.

### ddl_strategy: -postpone-completion flag

`ddl_strategy` (either `@@ddl_strategy` in VtGate or `-ddl_strategy` in `vtctlclient ApplySchema`) supports the
flag `-postpone-completion`

This flag indicates that the migration should not auto-complete. This applies for:

- any `CREATE TABLE`
- any `DROP TABLE`
- `ALTER` table in `online` strategy
- `ALTER` table in `gh-ost` strategy

Note that this flag is not supported for `pt-osc` strategy.

Behavior of migrations with this flag:

- an `ALTER` table begins, runs, but does not cut-over.
- `CREATE` or `DROP` migrations are silently not even scheduled

### alter vitess_migration ... cleanup

A new query is supported:

```sql
alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' cleanup
```

This query tells Vitess that a migration's artifacts are good to be cleaned up asap. This allows Vitess to free disk
resources sooner. As a reminder, once a migration's artifacts are cleaned up, the migration is no longer revertible.

### alter vitess_migration ... complete

A new query is supported:

```sql
alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' complete
```

This command indicates that a migration executed with `-postpone-completion` is good to complete. Behavior:

- For running `ALTER`s (`online` and `gh-ost`) which are ready to cut-over: cut-over imminently (though not immediately
    - cut-over depends on polling interval, replication lag, etc)
- For running `ALTER`s (`online` and `gh-ost`) which are only partly through the migration: they will cut-over
  automatically when they complete their work, as if `-postpone-completion` wasn't indicated
- For queued `CREATE` and `DROP` migrations: "unblock" them from being scheduled. They'll be scheduled at the scheduler'
  s discretion. there is no guarantee that they will be scheduled to run immediately.

### vtctl/vtctlclient ApplySchema: ALTER VITESS_MIGRATION

`vtctl ApplySchema` now supports `ALTER VITESS_MIGRATION ...` statements. Example:

```shell
$ vtctl ApplySchema -skip_preflight -sql "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' complete" commerce
```

### vtctl/vtctlclient ApplySchema: allow zero in date

`vtctl/vtctlclient ApplySchema` now respects `-allow-zero-in-date` for `direct` strategy. For example, the following
statement is now accepted:

```shell
vtctlclient ApplySchema -skip_preflight -ddl_strategy='direct -allow-zero-in-date' -sql "create table if not exists t2(id int primary key, dt datetime default '0000-00-00 00:00:00')" commerce
```

### vtctl/vtctlclient ApplySchema -uuid_list

`vtctlient ApplySchema` now support a new optional `-uuid_list` flag. It is possible for the user to explicitly specify
the UUIDs for given migration(s). UUIDs must be in a specific format. If given, number of UUIDs must match the number of
DDL statements. Example:

```shell
vtctlclient OnlineDDL ApplySchema -sql "drop table t1, drop table t2" -uuid_list "d08f0000_51c9_11ec_9cf2_0a43f95f28a3,d08f0001_51c9_11ec_9cf2_0a43f95f28a3" commerce
```

Vitess will assign each migration with given UUID in order of appearance. It is the user's responsibility to ensure
given UUIDs are globally unique. If the user submits a migration with an already existing UUID, that migration never
gets scheduled nor executed.

### vtctl/vtctlclient ApplySchema -migration_context

`-migration_context` flag is synonymous to `-request_context`. Either will work. We will encourage use
of `-migration_context` as it is more consistent with output of `SHOW VITESS_MIGRATIONS ...` which includes
the `migration_context` column.

### vtctl/vtctlclient ApplySchema -caller_id
`-caller_id` flag sets the Effective Caller ID of the ApplySchema operation so that the operation can succeed with a database that 
is enforcing strict ACL checking.

### vtctl/vtctlclient OnlineDDL ... complete

Complementing the `alter vitess_migration ... complete` query, a migration can also be completed via `vtctl`
or `vtctlclient`:

```shell
vtctlclient OnlineDDL <keyspace> complete <uuid>
```

For example:

```shell
vtctlclient OnlineDDL commerce complete d08ffe6b_51c9_11ec_9cf2_0a43f95f28a3
```

### vtctl/vtctlclient OnlineDDL -json

The command now accepts an optional `-json` flag. With this flag, the output is a valid JSON listing all columns and
rows.

### vtadmin-web updated to node v16.13.0 (LTS)

Building vtadmin-web now requires node >= v16.13.0 (LTS). Upgrade notes are given
in https://github.com/vitessio/vitess/pull/9136.

### PlannedReparentShard for cluster initialization
For setting up the cluster and electing a primary for the first time, `PlannedReparentShard` should be used
instead of `InitShardPrimary`.

`InitShardPrimary` is a forceful command and copies over the executed gtid set from the new primary to all the other replicas. So, if the user
isn't careful, it can lead to some replicas not being setup correctly and lead to errors in replication and recovery later.
`PlannedReparentShard` is a safer alternative and does not change the executed gtid set on the replicas forcefully. It is the preferred alternate to initialize
the cluster.

If using a custom `init_db.sql` that omits `SET sql_log_bin = 0`, then `InitShardPrimary` should still be used instead of `PlannedReparentShard`.

### Durability Policy flag
A new flag has been added to vtctl, vtctld and vtworker binaries which allows the users to set the durability policies.

If semi-sync is not being used then `-durability_policy` should be set to `none`. This is also the default option.

If semi-sync is being used then `-durability_policy` should be set to `semi_sync` and `-enable_semi_sync` should be set in vttablets.

## Incompatible Changes
### Error message change when vttablet row limit exceeded:
* In previous Vitess versions, if the vttablet row limit (-queryserver-config-max-result-size) was exceeded, an error like:
  ```shell
  ERROR 10001 (HY000): target: unsharded.0.master: vttablet: rpc error: code = ResourceExhausted desc = Row count exceeded 10000 (errno 10001) (sqlstate HY000) ...
  would be reported to the client.
  ```
  To avoid confusion, the error mapping has been changed to report the error as similar to:
  ```shell
  ERROR 10001 (HY000): target: unsharded.0.primary: vttablet: rpc error: code = Aborted desc = Row count exceeded 10000 (errno 10001) (sqlstate HY000) ...
  instead
  ```
* Because of this error code change, the vttablet metric:
  `vttablet_errors{error_code="ABORTED"}`
  will be incremented upon this type of error, instead of the previous metric:
  `vttablet_errors{error_code="RESOURCE_EXHAUSTED"}`
* In addition, the vttablet error message logged is now different.
  Previously, a (misleading;  due to the PoolFull) error was logged:
  ```shell
  E0112 09:48:25.420641  278125 tabletserver.go:1368] PoolFull: Row count exceeded 10000 (errno 10001) (sqlstate HY000) ...
  ```
  Post-change, a more accurate warning is logged instead:
  ```shell
  W0112 09:38:59.169264   35943 tabletserver.go:1503] Row count exceeded 10000 (errno 10001) (sqlstate HY000) ...
  ```
* If you were using -queryserver-config-terse-errors to redact some log messages containing bind vars in 13.0-SNAPSHOT, you should now instead enable -sanitize_log_messages which sanitizes all log messages containing sensitive info

### Column types for textual queries now match MySQL's behavior

The column types for certain queries performed on a `vtgate` (most notably, those that `SELECT` system variables)
have been changed to match the types that would be returned if querying a MySQL instance directly: textual fields that
were previously returned as `VARBINARY` will now appear as `VARCHAR`.

This change should not have an impact on MySQL clients/connectors for statically typed programming languages, as these
clients coerce the returned rows into whatever types the user has requested, but clients for dynamic programming
languages may now start returning as "string" values that were previously returned as "bytes".

## Deprecations

### vtgate `-gateway_implementation` flag is deprecated (and ignored)

Support for `discoverygateway` is being dropped. `tabletgateway` is now the only supported implementation. Scripts using
this flag should be updated to remove the flag as it will be deleted in the next release.

### web/vtctld2 is deprecated and can optionally be turned off

The vtctld2 web interface is no longer maintained and is planned for removal in Vitess 16. Motivation for this change and a roadmap to removing [the web/vtctld2 codebase](https://github.com/vitessio/vitess/tree/main/web/vtctld2) is given in https://github.com/vitessio/vitess/issues/9686.

Vitess operators can optionally disable the vtctld2 web interface ahead of time by calling the `vtctld` process with the flag `enable_vtctld_ui=false`. For more details on this flag, see https://github.com/vitessio/vitess/pull/9614.

------------
## Changelog

### Bug fixes
#### Backup and Restore
* vtctl: Check ShardReplicationStatuses error and make sure they are well returned #8966
* Skip replication config in restore when active reparents are disabled #9688
* Fix semi-sync for Backup and RestoreFromBackup commands by calling SetReplicationSource RPC #9730
#### Cluster management
* Fix vtctlclient `DeleteCellInfo(force=true)` command with downed local topo #9081
* Fix race condition when rapidly starting/stopping health check #9152
* Topo write error in change tablet type #9157
* Default build info tablet tags to off #9187
* Replication manager should not start health ticks after calling Promote Replica #9274
* Fix error checking in ERS #9330
* Fix for context cancellation in ERS #9503
#### General
* Avoid some pollution of CLI argument namespace across vtgate and vttablet #8931
* Update to the latest Tengo version #9110
#### Orchestrator
* Set orc maintenance mode on tablet that is being promoted by PRS #8859
#### Query Serving
* Fix for schema tracker not recognizing RENAME for schema reloading #8963
* Handle `sql_mode` differently for new value change #9014
* Fix savepoint support with reserved connections #9030
* Use a DBA user pool for the heartbeat writer #9033
* Handle single column vindex correctly with multi-columns in Gen4 #9060
* Remove tablet healthcheck cache record on error #9106
* Fix request buffering while reparenting #9112
* Ensure that the hex query predicates are normalized for planner cache #9118
* Fix table/sizes query, to allow vttablet schema engine to see partitioned tables #9188
* Remove keyspace from the query before sending it to the shards #9190
* Make sure to copy bindvars when using them concurrently #9191
* Fix owned vindex query to keep alias in table expression #9244
* Use decoded hex string when calculating the keyspace ID #9277
* Fix show statement in prepare call #9280
* Avoid parenthesis around default null in BLOB, TEXT, GEOMETRY, and JSON column #9301
* Allow unowned `lookup_unique` vindex columns to be `null` #9302
* Estimate replica lag when the number of seconds behind from mysqld is unknown #9308
* Clear existing keyspace schema in vtgate before [re]loading it #9437
* Fix AST copy issue in the Gen4Fallback planner #9487
* Fix a bug in the Gen4 planner for join query pushing output column on the left-hand side #9521
* Extract collation data to enable distinct aggregation #9645
* Use the gen4 planner for queries with outer joins #9671
* Gen4: make sure not to merge unsharded tables from different keyspaces #9676
* Fixed missing order/group by, limit, having in derived #9707
#### TabletManager
* Prevent Race Conditions Between Tablet Deletes and Updates #9237
#### VReplication
* Fix boolean parameter order in DropSources call for v2 flows #9175
* VReplication Workflow Create: Check for Copying state while waiting for streams to start #9206
* MoveTables: update vschema while moving tables with autoincrement from sharded to unsharded #9288
* Schema Engine now accountable for Online DDL swapped tables  #9324
* Take MySQL Column Type Into Account in VStreamer #9331
* Allow source columns to be specified multiple times in a vreplication rule filter #9335
* Fixed case-sensitive TabletType in SwitchTraffic #9440
* Use LOCK TABLES during SwitchWrites to prevent lost writes #9481
* VReplication Workflow: manage SQL_MODE as MySQL does in mysqldump #9505
* Routing Rules: Improved escaping of reserved and invalid names for tables and keyspaces  #9522
* Ignore internal tables in MoveTables and Reshard Workflows #8992,#9578
#### VTCombo
* vtcombo: `CREATE DATABASE` creates shards as specified by the topology #9130
### CI/Build
#### Build/CI
* Remove Percona5.6 Unit Tests for Vitess 13.0 #9067
* Addition of upgrade downgrade testing to the test suite #9300, #9473, #9501
* Re-enable default Make behavior of failing target if any command fails #9319
* ubuntu-latest now uses MySQL 8.0.26, let us override it with latest 8.0.x #9368
* Add vtadmin binary to release and Docker images #9405
#### VTAdmin
* Enforce node + npm versions with .npmrc #9336
#### vtexplain
* Correct handling of column types for queries involving multiple tables #9260
### Dependabot
#### Build/CI
* Upgrade `go-proxyproto` to `v0.6.1` to fix vulnerabilities #9425
#### Java
* Bump log4j-core from 2.15.0 to 2.16.0 in /java #9387
* Bump log4j-api from 2.16.0 to 2.17.1 in /java #9423
* Updates all major Java dependencies to latest #9599
#### VTAdmin
* Bump tmpl from 1.0.4 to 1.0.5 in /web/vtadmin #9278
### Enhancement
#### Authn/z
* Add support for TLS certification revocation list (CRL) files #8807
#### Backup and Restore
* Set super_read_only off during restore (achieves same as PR #8929) #9240
#### Cluster management
* Support -allow-zero-in-date in ApplySchema 'direct' strategy #9273
* Remove legacy OnlineDDL code deprecated in earlier version #8971
* Add version-info to tablet tags in the topo #8973
* Add default collation to VTGate and VTTablet #9097
* Use relay log position in PRS while choosing the new primary #9270
* vtctl OnlineDDL supports 'complete' command #9298
* vtctl ApplySchema accepts 'ALTER VITESS_MIGRATION...' statements #9303
* Make the heartbeat writer use 2 pools #9320
* New vtctl ApplySchema flag `-uuid_list` #9325
* Fix ERS to be used for initialization as well #9511
* Fix ERS to work when the primary candidate's replication is stopped #9512
* Add 'enable_vtctld_ui' flag to vtctld to explicitly enable (or disable) the vtctld2 UI (backport) #9713
#### Examples
* Update operator.yaml to v2.5.0 #9016
#### General
* vitessdriver: add support for DistributedTx #9451
#### Observability
* Modify `terse` errors to also redact errors in logs #9094
#### Query Serving
* Initial implementation of partitions (Create statement) #8691
* Add parsing for common table expressions using WITH clause #8918
* Support SQL SELECT LIMIT  #8944
* Support for union in subquery in Gen4 #8948
* Support for ordering using derived table columns in Gen4 #8961
* Add planner-version flag to vtexplain #8979
* Rewrite NOT expressions for cleaner plans #8987
* Minimize logging of errors when loading keyspace with disabled schema tracking #8989
* Support for ordering on vindex function and straight_join queries to be planned by ignoring the hint as warning in Gen4 #8990
* Improve internal subquery dependencies update in Gen4 #8998
* Added tokenizer support for NCHAR_STRING #9006
* Query timeout in comment directive in Gen4 #9008
* Added brackets in default value if column is of type TEXT, BLOB, GEOMETRY or JSON #9011
* Addition of the filter primitive and planning in Gen4 #9017
* Initial integration of the collation module in Gen4 #9018
* Support extract function in the SQL parser #9029
* Add collation name to the type definition in Gen4 #9038
* Support semi-join in Gen4 #9039
* Changed Collations and Expression grammar #9075
* Support transactions with OLAP workload #9115
* Use of advisory lock in prepare queries #9129
* Add hash join primitive and planning in Gen4 #9140
* Respect `--allow-zero-in-date` in CREATE and in declarative migrations #9142
* Improve merging of DBA queries in Gen4 #9183
* Evaluate REVERTibility of a migration in OnlineDDL #9232
* Improve group by queries planning when they use a sharding key #9243
* Make sure VTGate and VTTablet collations are matching #9248
* Support filtering on derived union system table with star projection #9263
* MySQL: Better handling of query failures within a transaction to match the MySQL behavior #9269
* Add Tuples and InOp implementation to EvalEngine #9281
* Use evalengine for Vindex lookups in route primitive #9291
* Adds parsing support for string introducers #9309
* Multi-column vindex support for DML #9338
* Add list (`IN()`) support to vindex functions #9426
* Fix parsing of table names that start with a number #9456
* Make the MaxWaiters setting configurable for the OLAP pool #9484
* Add support for health service in gRPC server #9528
* Filter after left join #9531
* Add Information_Schema to evalengine #9536
* Faster and safer unsharded query planning #9542
* Add support for PLANNER SQL comment #9545
* MySQL: use UTF8MB4 consistently as the connection charset #9558
* Do not auto retry after "server lost" errors (errno 2013) in vttablet #9009
* Online DDL: migration with same context and SQL now considered duplicate and silently marked as complete #9107
* Support ALTER VITESS_MIGRATION ... CLEANUP statement #9160
* OnlineDDL: implementing -postpone-completion, ALTER VITESS_MIGRATION ... COMPLETE #9171
* OnlineDDL: support concurrent REVERT migrations #9192
* VTGate: change default query buffer implementation (used to avoid client errors during state transitions such as PlannedReparentShard) #9359 #9360
* Fix misleading error mapping for row limit error #9448
* Add -sanitize_log_messages tablet flag to sanitize all log messages which may contain sensitive info and remove -queryserver-config-terse-errors impact on log messages #9636
#### TabletManager
* Have vttablet use super_read_only by default when a tablet becomes a replica #9312
* MySQL: Add ConnectionReady callback to Handler interface #9496
* MySQL: Add UnimplementedHandler struct #9526
#### VReplication
* Support VStream with `keyspaces_to_watch` #8988
* VEvent: Add Keyspace/Shard properties to proto #9386
* Initial refactoring of the 'vstream * from' functionality in vtgate #9392
* Add flag to keep routing rules in v2 vrepl workflows #9441
* SwitchTraffic: check vreplication lag before switching #9538
#### VTAdmin
* [vtadmin] viper config support #9154
* [VTAdmin] Update react-scripts, postcss #9493
#### VTorc
* Have a common EmergencyReparentShard (ERS) implementation for both VtOrc and Vtctl #8492
* Use PlannedReparentShard (PRS) in VtOrc while doing Graceful Primary Takeover #9258
* Make PRS use Durability Rules #9259
* Use PRS in Vtorc for electing new primary #9409
#### vtctl
* ListAllTablets: improve efficiency within vitess (fewer topo calls) and for caller (keyspace and tablet type filters) #9560
* vtctl OnlineDDL show: export all columns, support -json #9568
* vtctl ApplySchema: introduce -migration_context flag, synonym to -request_context #9572
#### vttestserver
* Add option to run vtcombo and vttest local cluster with a real topo server #9176
### Feature Request
#### Query Serving
* Add support for SET statements in OLAP mode #9253
* Multi-column vindex support in select statements #9326
* Multi-column vindex support in update and delete queries #9467
* Multicolumn partial vindex selection #9390
* Subsharding vindex support #9428
* Non-unique vindex routing in update and delete query #9554
#### VTAdmin
* [vtadmin] Add create+delete shards functionality #9012
* Add experimental `whoami` api endpoint #9233
* [vtadmin] Add ping feature, Dropdown and Dialog components #9318
* [VTAdmin] Dynamic Cluster support #9544
#### VTorc
* Merge InitShardPrimary functionality into PlannedReparentShard #9276
### Internal Cleanup
#### Backup and Restore
* Inclusive naming: use new RPCs in vtctl/vtctld/wrangler etc. #9181
#### Cluster management
* reparent_journal: add backwards compatible alter statement #9439
* [vtctl] command deprecations #8967
* [wrangler] Cleanup duplicate wrangler methods #9015
* Migrate k8s topo CRD to v1 api #9045
* [schematools] Move more functions out of `wrangler` to package importable by `grpcvtctldserver` #9123
* [vtctld] Migrate `ReloadSchema*` rpcs #8832
* [vtctldserver] migrate ExecuteHook #9024
* [vtctldclient-codegen] Add support in codegen for streaming RPCs #9064
#### General
* Delete unused `go/cmd/automation_{client,server}` binaries #9234
* Inclusive naming: replace master with primary #9427,#9430,#9438,#9454,#9103,#9182
* naming: delete old code that was needed for version compatibility #9516
* MySQL: Pass mysql.Conn through {Hash,PlainText,Caching} storage interfaces #9264
#### Java
* build(deps): bump log4j-api and log4j-core from 2.13.3 to 2.15.0 in /java #9348,#9349
#### Observability
* Changed master to primary everywhere #9506
#### Query Serving
* Deprecate and ignore `gateway_implementation` flag to vtgate #9482
* Deprecate vtctl command `IgnoreHealthError` #9594
#### VReplication
* Fixed legacy vreplication data that may be using master tablet types #9497
#### VTAdmin
* [vtadmin-web] Upgrade to node v16.13.0 LTS #9136
#### VTorc
* Fix linting issues in Vtorc #9449
* Pass Semi sync information from durability policy and use it to log discrepancies #9533
#### vtctl
* Inclusive naming: removing vtctl flags and commands with `master` in them that were deprecated in 12.0 #9179
* vtctl: delete deprecated `blacklisted_tables` flag #9406
#### web UI
* Migrate jQuery (pt. 1) #9239
* Migrate jQuery (pt. 2/3)  #9256
### Performance
#### Query Serving
* vttablet: Better use of vtprotobuf memory pool #9365
### Testing
#### Query Serving
* Fuzzing: Add more fuzzers #9249


The release includes 2020 commits (excluding merges)

Thanks to all our contributors: @AdamKorcz, @FancyFane, @GuptaManan100, @Phanatic, @Thirumalai-Shaktivel, @ajm188, @aquarapid, @arthurschreiber, @arvind-murty, @askdba, @carsonoid, @chapsuk, @choo-stripe, @dasl-, @dbussink, @dctrwatson, @deepthi, @dependabot[bot], @derekperkins, @doeg, @frouioui, @hallaroo, @harshit-gangal, @hkdsun, @king-11, @mattlord, @mattrobenolt, @mvh-stripe, @notfelineit, @oscerd, @pH14, @ritwizsinha, @rohit-nayak-ps, @shichao-an, @shlomi-noach, @systay, @utk9, @vmg, @y5w, @zhongr3n