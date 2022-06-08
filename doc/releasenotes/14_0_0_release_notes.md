# Release of Vitess v14.0.0-RC1

## Major Changes

### Command-line syntax deprecations

Vitess has begun a transition to a new library for CLI flag parsing.
In order to facilitate a smooth transition, certain syntaxes that will not be supported in the future now issue deprecation warnings when used.

The messages you will likely see, along with explanations and migrations, are:

#### "Use of single-dash long flags is deprecated"

Single-dash usage will be only possible for short flags (e.g. `-v` is okay, but `-verbose` is not).

To migrate, update your CLI scripts from:

```
$ vttablet -tablet_alias zone1-100 -init_keyspace mykeyspace ... # old way
```

To:

```
$ vttablet --tablet_alias zone1-100 --init_keyspace mykeyspace ... # new way
```

#### "Detected a dashed argument after a position argument."

As the full deprecation text goes on to (attempt to) explain, mixing flags and positional arguments will change in a future version that will break scripts.

Currently, when invoking a binary like

```
$ vtctl --topo_implementation etcd2 AddCellInfo --root "/vitess/global"
```

everything after the `AddCellInfo` is treated by `package flag` as a positional argument, and we then use a sub FlagSet to parse flags specific to the subcommand.
So, at the top-level, `flag.Args()` returns `["AddCellInfo", "--root", "/vitess/global"]`.

The library we are transitioning to is more flexible, allowing flags and positional arguments to be interwoven on the command-line.
For the above example, this means that we would attempt to parse `--root` as a top-level flag for the `vtctl` binary.
This will cause the program to exit on error, because that flag is only defined on the `AddCellInfo` subcommand.

In order to transition, a standalone double-dash (literally, `--`) will cause the new flag library to treat everything following that as a positional argument, and also works with the current flag parsing code we use.

So, to transition the above example without breakage, update the command to:

```shell
$ vtctl --topo_implementation etcd2 AddCellInfo -- --root "/vitess/global"
$ # the following will also work
$ vtctl --topo_implementation etcd2 -- AddCellInfo --root "/vitess/global"
$ # the following will NOT work, because --topo_implementation is a top-level flag, not a sub-command flag
$ vtctl -- --topo_implementation etcd2 AddCellInfo --root "/vitess/global"
```

### New command line flags and behavior

#### vttablet --heartbeat_on_demand_duration

`--heartbeat_on_demand_duration` joins the already existing heartbeat flags `--heartbeat_enable` and `--heartbeat_interval` and adds new behavior to heartbeat writes.

`--heartbeat_on_demand_duration` takes a duration value, such as `5s`.

The default value for `--heartbeat_on_demand_duration` is zero, which means the flag is not set and there is no change in behavior.

When `--heartbeat_on_demand_duration` has a positive value, then heartbeats are only injected on demand, per internal requests. For example, when `--heartbeat_on_demand_duration=5s`, the tablet starts without injecting heartbeats. An internal module, like the lag throttle, may request the heartbeat writer for heartbeats. Starting at that point in time, and for the duration (a lease) of `5s` in our example, the tablet will write heartbeats. If no other requests come in during that duration, then the tablet then ceases to write heartbeats. If more requests for heartbeats come while heartbeats are being written, then the tablet extends the lease for the next `5s` following up each request. Thus, it stops writing heartbeats `5s` after the last request is received.

The heartbeats are generated according to `--heartbeat_interval`.

#### Deprecation of --online_ddl_check_interval

The flag `--online_ddl_check_interval` is deprecated and will be removed in `v15`. It has been unused in `v13`.

#### Deprecation of --planner-version for vtexplain

The flag `--planner-version` is deprecated and will be removed in `v15`. Instead, please use `--planer_version`.

### Online DDL changes

#### Online DDL is generally available

Online DDL is no longer experimental (with the exception of `pt-osc` strategy). Specifically:

- Managed schema changes, the scheduler, the backing tables
- Supporting SQL syntax
- `vitess` strategy (online DDL via VReplication)
- `gh-ost` strategy (online DDL via 3rd party `gh-ost`)
- Recoverable migrations
- Revertible migrations
- Declarative migrations
- Postponed migrations
- and all other functionality

Are all considered production-ready.

`pt-osc` strategy (online DDL via 3rd party `pt-online-schema-change`) remains experimental.

#### Throttling

See new SQL syntax for controlling/viewing throttling for Online DDL, down below.

#### ddl_strategy: 'vitess'

`ddl_strategy` now takes the value of `vitess` to indicate VReplication-based migrations. It is a synonym to `online` and uses the exact same functionality. In the future, the `online` term will phase out, and `vitess` will remain the term of preference.

Example:

```shell
vtctlclient ApplySchema -skip_preflight -ddl_strategy='vitess' -sql "alter table my_table add column my_val int not null default 0" commerce
```

#### --singleton-context and REVERT migrations

It is now possible to submit a migration with `--singleton-context` strategy flag, while there's a pending (queued or running) `REVERT` migration that does not have a `--singleton-context` flag.

#### Support for CHECK constraints

Online DDL operations are more aware of `CHECK` constraints, and properly handle the limitation where a `CHECK`'s name has to be unique in the schema. As opposed to letting MySQL choose arbitrary names for shadow table's `CHECK` consraints, Online DDL now generates unique yet deterministic names, such that all shards converge onto same names.

Online DDL attempts to preserve the original check's name as a suffix to the generated name, where possible (names are limited to `64` characters).

#### Behavior changes

- `vtctl ApplySchema --uuid_list='...'` now rejects a migration if an existing migration has the same UUID but with different `migration_context`.

### Table lifecycle

#### Views

Table lifecycle now supports views. It ensures to not purge rows from views, and does not keep views in `EVAC` state (they are immediately transitioned to `DROP` state).

#### Fast drops

On Mysql `8.0.23` or later, the states `PURGE` and `EVAC` are automatically skipped, thanks to `8.0.23` improvement to `DROP TABLE` speed of operation.

### Tablet throttler

#### API changes

Added `/throttler/throttled-apps` endpoint, which reports back all current throttling instructions. Note, this only reports explicit throttling requests (sych as ones submitted by `/throtler/throttle-app?app=...`). It does not list incidental rejections based on throttle thresholds.

API endpoint `/throttler/throttle-app` now accepts a `ratio` query argument, a floating point in the range `[0..1]`, where:

- `0` means "do not throttle at all"
- `1` means "always throttle"
- any numbr in between is allowd. For example, `0.3` means "throttle in 0.3 probability", ie on a per request and based on a dice roll, there's a `30%` change a request is denied. Overall we can expect about `30%` of requests to be denied. Example: `/throttler/throttle-app?app=vreplication&ratio=0.25`

See new SQL syntax for controlling/viewing throttling, down below.

### New Syntax

#### Control and view Online DDL throttling

We introduce the following syntax, to:

- Start/stop throttling for all Online DDL migrations, in general
- Start/stop throttling for a particular Online DDL migration
- View throttler state


```sql
ALTER VITESS_MIGRATION '<uuid>' THROTTLE [EXPIRE '<duration>'] [RATIO <ratio>];
ALTER VITESS_MIGRATION THROTTLE ALL [EXPIRE '<duration>'] [RATIO <ratio>];
ALTER VITESS_MIGRATION '<uuid>' UNTHROTTLE;
ALTER VITESS_MIGRATION UNTHROTTLE ALL;
SHOW VITESS_THROTTLED_APPS;
```

default `duration` is "infinite" (set as 100 years)

- allowed units are (s)ec, (m)in, (h)our
  ratio is in the range `[0..1]`.
- `1` means full throttle - the app will not make any progress
- `0` means no throttling at all
- `0.8` means on 8 out of 10 checks the app makes, it gets refused

The syntax `SHOW VITESS_THROTTLED_APPS` is a generic call to the throttler, and returns information about all throttled apps, not specific to migrations

`SHOW VITESS_MIGRATIONS ...` output now includes `user_throttle_ratio`

This column is updated "once in a while", while a migration is running. Normally this is once a minute, but can be more frequent. The migration reports back what was the throttling instruction set by the user while it was/is running.
This column does not indicate any actual lag-based throttling that takes place per production state. It only reports the explicit throttling value set by the user.

### Heartbeat

The throttler now checks in with the heartbeat writer to request heartbeats, any time it (the throttler) is asked for a check.

When `--heartbeat_on_demand_duration` is not set, there is no change in behavior.

When `--heartbeat_on_demand_duration` is set to a positive value, then the throttler ensures that the heartbeat writer generated heartbeats for at least the following duration. This also means at the first throttler check, it's possible that heartbeats are idle, and so the first check will fail. As heartbeats start running, followup checks will get a more accurate lag evaluation and will respond accordingly. In a sense, it's a "cold engine" scenario, where the engine takes time to start up, and then runs smoothly.

### VDiff2

We introduced a new version of VDiff -- currently marked as Experimental -- that executes the VDiff on tablets rather than in vtctld. While this is experimental we encourage you to try it out and provide feedback! This input will be invaluable as we improve this feature on the march toward a production-ready version. You can try it out by adding the `--v2` flag to your VDiff command. Here's an example:
```
$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer
VDiff bf9dfc5f-e5e6-11ec-823d-0aa62e50dd24 scheduled on target shards, use show to view progress

$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer show last

VDiff Summary for customer.commerce2customer (bf9dfc5f-e5e6-11ec-823d-0aa62e50dd24)
State: completed
HasMismatch: false

Use "--format=json" for more detailed output.

$ vtctlclient --server=localhost:15999 VDiff -- --v2 --format=json customer.commerce2customer show last
{
        "Workflow": "commerce2customer",
        "Keyspace": "customer",
        "State": "completed",
        "UUID": "bf9dfc5f-e5e6-11ec-823d-0aa62e50dd24",
        "HasMismatch": false,
        "Shards": "0"
}
```

:information_source:  NOTE: even before it's marked as production-ready (feature complete and tested widely in 1+ releases), it should be safe to use and is likely to provide much better results for very large tables.

For additional details, please see the [RFC](https://github.com/vitessio/vitess/issues/10134) and the [README](https://github.com/vitessio/vitess/tree/main/go/vt/vttablet/tabletmanager/vdiff/README.md).

### Durability Policy

#### Deprecation of durability_policy Flag
The durability policy for a keyspace is now stored in the keyspace record in the topo server.
The `durability_policy` flag used by vtctl, vtctld, and vtworker binaries has been deprecated.

#### New and Augmented Commands
The vtctld command `CreateKeyspace` has been augmented to take in an additional argument called `durability-policy` which will
allow users to set the desired durability policy for a keyspace at creation time.

For existing keyspaces, a new command `SetKeyspaceDurabilityPolicy` has been added, which allows users to change the
durability policy of an existing keyspace.

If semi-sync is not being used then durability policy should be set to `none` for the keyspace. This is also the default option.

If semi-sync is being used then durability policy should be set to `semi_sync` for the keyspace and `--enable_semi_sync` should be set on vttablets.

### Deprecation of Durability Configuration
The `Durability` configuration is deprecated and removed from VTOrc. Instead VTOrc will find the durability policy of the keyspace from
the topo server. This allows VTOrc to monitor and repair multiple keyspaces which have different durability policies in use.

**VTOrc will ignore the keyspaces which have no durability policy specified in the keyspace record. So on upgrading to v14, users must run
the command `SetKeyspaceDurabilityPolicy` specified above, to ensure VTOrc continues to work as desired. The recommended upgrade
path is to upgrade vtctld, run `SetKeyspaceDurabilityPolicy` and then upgrade VTOrc.**

------------
## Changelog

### Announcement
#### General
* v14 release notes: Online DDL is production ready #10310
### Bug fixes
#### Backup and Restore
* Skip repl config in restore when active reparents disabled #9675
#### Cluster management
* [grpcshim] Fix select race and move to internal package #9116
* Use AllPrivs user for table GC transitions #9690
* [topo/helpers] Make CopyShardReplications idempotent #9849
* Fix the race between PromoteReplica and replication manager tick #9859
* Propagate error from 'updateLocked' to client #10181
* Revert semi_sync change to reintroduce enable_semi_sync and remove durability_policy #10201
* The flag --online_ddl_check_interval belongs in vtctld #10320
* vt/wrangler: fix deleting primary tablet if `Shard` does not exist #10373
* topo: cache CellInfo along with conn #10408
#### General
* Sanitize tx serializer log & error messages #9802
* Changes for make tools #10117
* Sanitize log messages #10367
#### Query Serving
* Extract collation data to enable distinct aggregation #9639
* Add QueryRowsAffected and QueryRowsReturned to vttablet metrics and deprecate QueryRowCounts #9656
* Use the gen4 planner for queries with outer joins #9657
* Gen4: make sure to not merge unsharded tables from different keyspaces #9665
* Parser fix for CREATE VIEW statement #9693
* Add collation parsing to generated columns in Create Table #9694
* Fixed missing order/group by, limit, having in derived #9701
* Online DDL: resubmitting a migration with same UUID retries it in case it was cancelled or failed. #9704
* Query plan/rules: apply for multi-table statement #9747
* Stop rewriting `JoinCondition` with `USING` #9767
* Manage MySQL Replication Status States Properly #9853
* Fix `__sq_has_values1` error with `PulloutSubquery` #9855
* OnlineDDL executor: route all VReplicationExec through single function #9861
* fix: planner panic on derived tables sorting in query builder  #9869
* feat: add weightstring for distinct #9874
* Fix: Sequence query to ignore reserved and transaction #9968
* fix: dual query with exists clause having system table query in it #9969
* fix: make concatenate and limit concurrent safe #9979
* ApplySchema: allow-zero-in-date embedded as query comment in call to ExecuteFetchAsDba #9998
* Fix: reserved connection retry logic when vttablet or mysql is down #10005
* gen4: Fix sub query planning when the outer query is a dual query #10007
* Fix parsing of bind variables in a few places #10015
* Route explain tab plan to the proper Keyspace #10027
* OnlineDDL: double statement validation upon submission #10065
* Fix Gen4 only_full_group_by regression #10069
* Fix Gen4 group/order by with derived table #10074
* Only start SQL thread temporarily to WaitForPosition if needed #10104
* Vitess online ddl: modify a column from textual to non-textual, ignore charset #10116
* Fix StreamExecute in Gen4CompareV3 #10122
* Do not mutate replication state in WaitSourcePos and ignore tablets with SQL_Thread stopped in ERS #10148
* Fix for empty results when no shards can be found to route to #10152
* fix: handle reserved connection reset when tx killer has locked the connection #10153
* Do not send field query when using reserved connection #10163
* Backwards compatible replication status to state transition #10167
* sqlparser: Handle case sensitive AST option for table options #10191
* Emit the ENGINE field for table options as case sensitive #10197
* check for connectionID before adding to querylist #10212
* Fix handling of unsigned and zerofill in parser and normalization #10220
* Fix parsing of the foreign key constraint actions in different order #10224
* Fix handling of VISIBLE or INVISIBLE keyword on indexes #10243
* Fix formatting for function expressions and booleans #10255
* fix: concatenate engine primitive #10257
* Move all schemadiff comparisons to canonical form #10261
* Do not cache plans that are invalid because of `--no_scatter` #10279
* Fix parsing the special case convert charset logic #10288
* Deprecate flag --online_ddl_check_interval #10308
* Fix schema tracking issue when `PRIMARY` tablet changes #10335
* Remove normalization of integral length types #10336
* Fix failure when Unowned lookup IS NULL #10346
* Fix for empty results when no shards can be found to route to [v3] #10360
* Revert super_read_only config file changes #10366
* OnlineDDL/vitess fix: convert data type to JSON  #10390
* OnlineDDL/vrepl: more error codes, spatial types #10394
* Online DDL fix: instant is possible for VIRTUAL, not STORED #10411
* schemadiff: column name check in CHECK constraint is case insensitive #10413
* making log sanitization more precise #10417
* v3: Fix issue when no routes are found from a lookup #10422
* Tablet throttler: dynamic ThrottleMetricThreshold #10439
* Change use_super_read_only default back to false #10448
#### VReplication
* Support VDiff across DB versions #9679
* Externalize Lookup VIndexes properly when not stopping after copy #9771
* Add TEXT field comparison support to evalengine (for VDiff) #9790
* Add CHAR field comparison support to evalengine (for VDiff) #9800
* Check for nil vschema.Table in StreamMigrator #9828
* VPlayer: use stored/binlogged ENUM index value in WHERE clauses #9868
* vstreamer: flatten savepoint events #9892
* vstreamer: do not forward savepoint events #9907
* VReplication: use strict sql_mode for Online DDL #9963
* VStreamer: recompute table plan if a new table is encountered for the same id #9978
* Increase max vrepl workflow source definition size from 64KiB to 16MiB #10018
* VReplication: maintain original column case for pkCols #10033
* Avoid deadlocks related to 0 receiver behavior #10132
* SwitchTraffic should switch everything when no tablet_types provided #10434
#### VTAdmin
* [vtadmin] threadsafe dynamic clusters #10044
#### VTorc
* BugFix: Resolve Recoveries at the end #10286
#### vtexplain
* Case-insensitive planner-version flag in VTExplain #10086
* fix: check that all keyspaces loaded successfully before using them #10396
### CI/Build
#### Build/CI
* Track full git sha1 string instead of the short version #9970
* Skip CI workflows based on the list of modified files #10031
* CI: fix flaky test via extended timeout in vrepl_stress_suite #10231
* Fix `tabletmanager_throttler` flakyness: increase wait time #10271
* remove references to MySQL and Percona 5.6 #10295
#### Cluster management
* fix endtoend onlineddl flakyness caused by --heartbeat_on_demand_duration #10236
#### General
* Upgrade main to `go1.18` #9893
* Upgrade main to `go1.18.1` #10101
* Upgrade main to `go1.18.3` #10447
#### Query Serving
* More testing for vreplication unique key violation protection #9988
* Fix to --heartbeat_on_demand_duration race condition #10247
* OnlineDDL: adding an endtoend test to validate partial shard REVERT #10338
### Dependabot
#### Examples
* build(deps): bump gopkg.in/yaml.v2 from 2.2.5 to 2.2.8 in /examples/are-you-alive #9429
### Documentation
#### Documentation
* Update release notes with web/vtctld2 deprecation announcement and flag. #9735
* Add help text for LegacyVtctlCommand and VtctldCommand #9837
#### Examples
* Fix commands in readme files to use double dashes for arguments #10389
### Enhancement
#### Build/CI
* Check for proto drift in the CI #9644
* make generate_ci_workflows: mysql80 workflows for selected tests #9740
* Add vitess/lite ubi8 images for mysql80, including for arm64 #9830
#### CLI
* [vtctldclient] Add entrypoint for GetVersion rpc #9994
#### Cluster management
* Change semi-sync information to use the parameter passed and deprecate enable_semi_sync #9725
* Filter candidates which cannot make forward progress after ERS #9765
* [vtctldserver] Add locking checks to Delete{Keyspace,Shard} #9777
* [topo] ShardReplicationFix typed responses #9876
* tablet lag throttler: small API improvements #10045
* OnlineDDL: skip GetSchema where possible #10107
* Refresh ephemeral information before cluster operations in VTOrc #10115
* On demand heartbeats via `--heartbeat_on_demand_duration`, used by the tablet throttler #10198
* Adds DurabilityPolicy to the KeyspaceInfo in the topo and the associated RPCs #10221
* Refactor Durability Policy implementation and usage to read the durability policy from the keyspace #10375
* Use Durability Policy in the topo server in VTOrc and deprecate Durability config #10423
#### Examples
* Add consul-topo for local example #9806
* Add vtadmin to local example by default #10430
* Halve request and double limit for RAM in the example cluster #10450
#### General
* Binlog event parsing: better analysis ; support for semi-sync #9596
* Support views in Online DDL #9606
* [VEP-4, phase 1] Flag Deprecation Warnings #9733
* Table lifecycle: support fast DROP TABLE introduced in MySQL 8.0.23 #9778
* Flavor capabilities: MySQL GR and more #10451
#### Query Serving
* Harmonize error codes for pool timeouts expiring across all pools #9483
* Store innodbRowsRead in a Counter instead of a Gauge #9609
* Improve performance of information schema query. #9632
* OnlineDDL: reject duplicate UUID with different migration_context #9637
* Reduce the number of reserved connections when setting system variables #9641
* OnlineDDL: 'vitess' strategy, synonym for 'online' #9642
* Gen4: Rework how aggregation is planned #9643
* Support for ALTER TABLE ... PARTITION BY ... #9683
* OnlineDDL: force (once) vreplication's WithDDL to run before running a vitess migration #9702
* Push projection to union #9703
* Make gen4 the default planner for release 14 #9710
* Cleanup of parsing of Partitions and additional parsing support #9712
* Introducing schemadiff, a declarative diff for table/view CREATE statements #9719
* Online DDL vitess migration's cut-over: query buffering #9755
* Online DDL declarative migrations now use schemadiff, removing tengo #9772
* Online DDL: ready_to_complete hint column #9813
* Add parsing support for prepare statements #9818
* Online DDL: vitess migrations cut-over to have zero race conditions #9832
* Add parsing support for Trim grammar function #9834
* Add parsing support for LATERAL keyword #9843
* Add parsing support for JSON Utility Functions #9850
* Remove the `Gateway` interface #9852
* Gen4: Add UPDATE planning #9871
* Add parsing support for JSON_TABLE() #9879
* Add parsing support for JSON value creators functions #9880
* fix: allow multiple distinct columns on single shards #9940
* Add parsing support for Json schema validation functions #9971
* Add parsing support for JSON Search Functions #9990
* deps: upgrade grpc and protobuf #10024
* feat: allow more complex expressions to be used in outer queries #10034
* schemadiff: load, normalize, validate and compare schemas #10048
* Add parsing support for JSON value modification and attribute functions #10062
* Always reset reserved shard session on CODE_UNAVAILABLE #10063
* Adds parsing for NOW in DEFAULT clause #10085
* Gen4 feature: optimize OR queries to IN #10090
* Relax singleton-context constraint for pending non-singleton-context reverts #10114
* Add parsing support for Partition Definitions #10127
* feat gen4: allow pushing aggregations inside derived tables #10128
* schemadiff: support for RangeRotationStrategy diff hint #10131
* Avoid enabling reserved connection on show query #10138
* Add support for non aggregated columns in `OrderedAggregate` #10139
* schemadiff: CanonicalStatementString(), utilize sqlparser.CanonicalString() #10142
* feat: make gen4 plan subsharding queries better #10151
* schemadiff: EntityDiff has a 'Entities() (from Entity, to Entity)' function #10161
* Introducing a SQL syntax to control and view Online DDL throttling #10162
* feat: optimize EXISTS queries through AST rewriting #10174
* schemadiff: logically Apply() a diff to a CreateTable, CreateView or to a Schema #10183
* Using LCS to achieve shortest column reordering list #10184
* enhancement: handle advisory locks  #10186
* schemadiff: normalize() table. Set names to all keys #10188
* schemadiff: validate() table structure at the end of apply() #10189
* schemadiff: schema is immutable through Apply() #10190
* VReplication: use PK equivalent (PKE) if no PK found #10192
* Add parsing support for Window Functions #10199
* schemadiff: normalize table options case #10200
* Allow updating unowned lookup vindex columns. #10207
* partial dml execution logic ehancement #10223
* gen4: JOIN USING planning #10226
* schemadiff: partitions validations #10229
* Add parsing support for Subpartition Definitions #10232
* schemadiff: analyze and apply ADD PARTITION and DROP PARTITION statements #10234
* Add convenience functions for working with schemadiff objects #10238
* Add parsing support for expressions in index definitions #10240
* Update the parsing support for JSON_VALUE #10242
* schemadiff: support functional indexes #10244
* Fix formatting of character set annotation on string literals #10245
* schemadiff: use ParseStrictDDL whenever expecting a DDL statement #10246
* Fix parsing of encryption attribute for create database #10249
* Add support for parsing MATCH in a foreign key definition #10250
* Add parsing support for additional index and table options #10251
* Handle additional DDL in column definitions #10253
* evalengine: CASE expressions #10262
* vt/sqlparser: support SRID #10267
* Improve dealing with check constraints #10268
* Add parsing and schemadiff support for ALTER TABLE ALTER CHECK #10269
* Add unary utf8mb3 charset marker #10274
* Allow updating the subsequent diff #10289
* Improve the formatting for partitioning #10291
* Add additional normalization rules for schemadiff #10296
* schemadiff: RangeRotationDistinctStatements partitioning hint #10309
* Remove internal savepoint post query execution #10311
* feat: use column offsets when HAVING predicate has aggregation #10312
* Improve parser and schemadiff support for ALTER TABLE #10313
* Online DDL: fast (and correct) RANGE PARTITION operations, 1st iteration #10315
* Add support for additional encoding configurations #10321
* Add parsing support for Regular Expressions #10322
* schemadiff: validate columns referenced by generated columns #10328
* schemadiff: case insensitive col/index/partition name comparison #10330
* [gen4] More aggregation support #10332
* Add better int normalization #10340
* Additional schemadiff improvements for indexes #10345
* Structured schemadiff errors #10356
* Add validation of check constraints #10357
* schemadiff: validate columns referenced by FOREIGN KEY #10359
* Aggregation on top of LIMIT #10361
* Add support for parsing ARRAY in cast / convert operations #10362
* Online DDL: support for CHECK CONSTRAINTS #10369
* schemadiff: constraint name diff hints strategies #10380
* Replace 'CREATE TABLE LIKE' with programmatic copy #10381
* Streaming implementation of Projection engine primitive #10384
* feat: do not use lookup vindexes for IS NULL predicates #10388
* Online DDL: support INSTANT DDL special plan, flag protected (flag undocumented for now) #10402
* feat: make v3 not plan IS NULL queries against a lookup vindex #10412
* Online DDL: support Instant DDL for change of column's DEFAULT #10414
* Parsing Support for XML functions #10438
* Formalize MySQL capabilities by flavor/version #10445
#### TabletManager
* messaging: support vt_message_cols to limit cols #9670
* Table lifecycle: support views #9776
#### VReplication
* OnlineDDL: delete _vt.vreplication entry as part of CLEANUP operation #9638
* Tablet server/VReplication: use information_schema.columns to get the list of columns instead of "select * from table"  #9794
* Add `--drop_constraints` to MoveTables #9904
* Support throttling vstreamer copy table work on source tablets #9923
* Improve escape handling for vindex materializer #9929
* VReplication: fail on unrecoverable errors #9973
* Prefer using REPLICA tablets when selecting vreplication sources #10040
* Improve Tablet Refresh Behavior in VReplication Traffic Switch Handling #10058
* VReplication: use db_filtered user for vstreams #10080
* VStream API: allow cells to be specified for picking source tablets to stream from #10294
* MoveTables: adjust datetimes when importing from non-UTC sources into UTC targets #10102
* Vdiff2: initial release #10382
#### VTAdmin
* [vtadmin-web] Add REACT_APP_READONLY_MODE flag #9731
* [vtadmin] custom discovery resolver #9977
* [vtadmin] grpc healthserver + channelz #10038
* [Dynamic Clusters] No Duplicates #10043
* [vtadmin] full dynamic cluster config #10071
* [vtadmin] non-blocking resolver #10205
* [vtadmin] tablet topopools + other cleanup #10325
* [vtadmin] cluster Closer implementation + dynamic Cluster bugfix #10355
#### VTCombo
* Add flag to vtcombo to load a json-encoded test topology file #9633
#### VTorc
* Revocation phase using durability policies #9659
* VTOrc: Refactor and reload of ephemeral information for remaining recovery functions #10150
* vtorc: more auditing in GracefulPrimaryTakeover #10285
#### vttestserver
* vtcombo: Add the ability to specify routing rules in test topology #9695
#### web UI
* [vtctld] Add 'enable_vtctld_ui' flag to vtctld to explicitly enable (or disable) the vtctld2 UI #9614
### Feature Request
#### Cluster management
* [vtctldserver] Migrate `Backup` RPC to vtctldserver/client #9798
* [vtctldclient] Add entrypoint for `ApplySchema` #9829
* [vtctldserver] Migrate `BackupShard` RPC #9857
* [vtctldserver] Migrate `RemoveBackup` RPC #9865
* [vtctldserver] Migrate `RestoreFromBackup` RPC #9873
* [vtctldserver] Migrate `ShardReplicationFix` RPC #9878
* [vtctldserver] Migrate `SourceShard{Add,Delete}` RPCs #9886
* [vtctldserver] ExecuteFetchAs{App,DBA} #9890
* [vtctldserver] Migrate `ShardReplication{Add,Remove}` RPCs #9899
* [vtctldserver] Migrate GetPermissions PR#9903 #10013
#### Observability
* [vtadmin] Add prom metrics endpoint support #10334
#### Query Serving
* mysql: allow parsing the 'info' field of MySQL packets #9651
* Add support for insert using select #9748
* Add parsing support for Multi INDEX HINTS #9811
#### VTAdmin
* [VTAdmin] Tablet Actions (vtctld2 Parity) #9601
* [VTAdmin][vtctldserver] Keyspace Functions #9667
* Allow --no-rbac flag that allows users to not pass rbac config #9972
* [vtadmin] grpc dynamic clusters #10050
* [vtadmin] schema cache #10120
* [vtadmin] cell apis #10227
* [vtadmin] reload schema #10300
* [vtadmin] reparenting actions #10351
### Internal Cleanup
#### Build/CI
* Remove usage of additional uuid package #10307
#### CLI
* [vtctl] Deprecate query commands #9934
* [vtctl] Deprecate Throttler RPCs #9962
* [vtadmin] Remove deprecated dynamic cluster flag #10067
* Update vtctl help output to use double dashes for long flag names #10405
#### Cluster management
* Move deprecation warning to just before we actually invoke the legacy command path #9787
* Online DDL: cleanup old vtctld+topo flow #9816
* Rename ApplySchemaRequest.request_context=>migration_context #9824
* rename master_alias to primary_alias in reparent_journal table #10098
* Use the `HealthCheckImpl` in `vtctld` instead of the `LegacyHealthCheck` #10254
* Cleanup: removed legacy, unused code (online DDL paths in topo) #10379
#### Examples
* Examples: fix warnings #9875
* Migrate remaining vtctldclient commands in local example #9894
#### General
* Support sanitizing potentially sensitive information in log messages #9550
* [VEP-4, phase 1] Flag cleanup in `go/cmd` #9896
#### Query Serving
* Delete discovery gateway #9500
* Remove queryserver-config-terse-errors impact on log messages #9634
* Split aggregation in scalar and ordered aggregation #9810
* Minor fix requested by linter #9989
* remove set_var session field from proto #10135
#### TabletManager
* delete RPCs that were deprecated in 13.0 #10099
#### VReplication
* remove changeMasterToPrimary, we no longer need it #10100
* Ensure all legacy sharding commands are clearly deprecated #10281
#### VTAdmin
* [vtadmin] threadsafe vtsql #10000
* [vtsql] Remove vtsql.PingContext check from dial calls #10037
* [vtadmin] Refactor api.getSchemas => cluster.GetSchemas #10108
* [vtadmin] Remove explicit Dial-ing for vtctldclient proxy #10233
* [vtadmin] Removing explicit Dial-ing for vtsql proxy #10237
* [vtadmin] proper alias types for tablet RPCs #10290
* [vtadmin] Remove mutex, now that vtexplain has no global state #10387
* [vtadmin] Rename reparent-related RPCs, endpoints, methods, RBAC actions #10404
#### vtctl
* Fixes #8277: vtctld logs leak pii http headers #9669
#### vtexplain
* replace planner-version with planner_version in vtexplain #10420
### Performance
#### Schema Tracker
* Improve performance of the `DetectSchemaChange` query #10416
### Testing
#### Build/CI
* More aggressive tests for vitess migration cut-over #9956
* fix tablegc flaky test #10230
#### CLI
* [VEP-4, part 1] Flag cleanup in `go/mysql` and some endtoend tests #9910
* [VEP4, phase 1] Update flags in `endtoend/recovery` #9911
* [VEP4, phase 1] Update flags in `endtoend/schemadiff` #9912
* [VEP4, phase 1] Update flags and imports for `endtoend/{sharded,sharding}` #9913
* [VEP 4, phase 1] Cleanup flags in `endtoend/vault` (and some comments in `endtoend/tabletmanager`) #9916
* [VEP4, phase 1] fix flags in tests `endtoend/versionupgrade` #9917
* [VEP4, phase 1] [endtoend/vreplication] update flags in tests #9918
* [VEP4, phase 1] [endtoend/vtcombo] fix flags in tests #9919
* [VEP4, phase1] Cleanup flags in `endtoend/vtgate` and `endtoend/vtorc` #9930
* [VEP4, phase1] fix flags in tests in `endtoend/worker` and strings in `go/trace` #9931
#### General
* test: use `T.TempDir` to create temporary test directory #10433
#### Query Serving
* vtexplain: added multicol test to show subshard routing #9697
* Compare Vitess against MySQL in E2E tests #9965
* Make the tests explicit for the SET variables for MySQL 5.7 #10263


The release includes 1065 commits (excluding merges)

Thanks to all our contributors: @FancyFane, @GuptaManan100, @Juneezee, @K-Kumar-01, @Phanatic, @ajm188, @akenneth, @aquarapid, @arthurschreiber, @brendar, @cuishuang, @dasl-, @dbussink, @deepthi, @dependabot[bot], @derekperkins, @doeg, @fatih, @frouioui, @harshit-gangal, @malpani, @matthiasr, @mattlord, @mattrobenolt, @notfelineit, @pjambet, @rohit-nayak-ps, @rsajwani, @shlomi-noach, @simon-engledew, @systay, @utk9, @vmg, @vmogilev, @y5w, @yields