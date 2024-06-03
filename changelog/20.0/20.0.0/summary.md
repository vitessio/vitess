
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deletions](#deletions)** 
    - [`--vreplication_tablet_type` flag](#vreplication-tablet-type-deletion)
    - [Pool Capacity Flags](#pool-flags-deletion)
    - [MySQL binaries in the vitess/lite Docker images](#vitess-lite)
    - [vitess/base and vitess/k8s Docker images](#base-k8s-images)
    - [`gh-ost` binary and endtoend tests](#gh-ost-binary-tests-removal)
    - [Legacy `EmergencyReparentShard` stats](#legacy-emergencyshardreparent-stats)
  - **[Breaking changes](#breaking-changes)**
    - [Metric Name Changes in VTOrc](#metric-change-vtorc)
    - [ENUM and SET column handling in VTGate VStream API](#enum-set-vstream)
    - [`shutdown_grace_period` Default Change](#shutdown-grace-period-default)
    - [New `unmanaged` Flag and `disable_active_reparents` deprecation](#unmanaged-flag)
    - [`recovery-period-block-duration` Flag deprecation](#recovery-block-deprecation)
    - [`mysqlctld` `onterm-timeout` Default Change](#mysqlctld-onterm-timeout)
    - [`MoveTables` now removes `auto_increment` clauses by default when moving tables from an unsharded keyspace to a sharded one](#move-tables-auto-increment)
    - [`Durabler` interface method renaming](#durabler-interface-method-renaming)
  - **[Query Compatibility](#query-compatibility)**
    - [Vindex Hints](#vindex-hints)
    - [Update with Limit Support](#update-limit)
    - [Update with Multi Table Support](#multi-table-update)
    - [Update with Multi Target Support](#update-multi-target)
    - [Delete with Subquery Support](#delete-subquery)
    - [Delete with Multi Target Support](#delete-multi-target)
    - [User Defined Functions Support](#udf-support)
    - [Insert Row Alias Support](#insert-row-alias-support)
  - **[Query Timeout](#query-timeout)**
  - **[Flag changes](#flag-changes)**
    - [`pprof-http` default change](#pprof-http-default)
    - [New `healthcheck-dial-concurrency` flag](#healthcheck-dial-concurrency-flag)
    - [New minimum for `--buffer_min_time_between_failovers`](#buffer_min_time_between_failovers-flag)
    - [New `track-udfs` vtgate flag](#vtgate-track-udfs-flag)
    - [Help text fix for `--lock-timeout`](#documentation-lock-timeout)
- **[Minor Changes](#minor-changes)**
  - **[New Stats](#new-stats)**
    - [VTTablet Query Cache Hits and Misses](#vttablet-query-cache-hits-and-misses)
  - **[`SIGHUP` reload of gRPC client static auth creds](#sighup-reload-of-grpc-client-auth-creds)**
  - **[VTAdmin](#vtadmin)**
    - [Updated to node v20.12.2](#updated-node)
    - [Replaced highcharts with d3](#replaced-highcharts)

## <a id="major-changes"/>Major Changes

### <a id="deletions"/>Deletion

#### <a id="vreplication-tablet-type-deletion"/>`--vreplication_tablet_type` flag

The previously deprecated flag `--vreplication_tablet_type` has been deleted.

#### <a id="pool-flags-deletion"/>Pool Capacity Flags

The previously deprecated flags `--queryserver-config-query-pool-waiter-cap`, `--queryserver-config-stream-pool-waiter-cap` and `--queryserver-config-txpool-waiter-cap` have been deleted.

#### <a id="vitess-lite"/>MySQL binaries in the `vitess/lite` Docker images

In `v19.0.0` we had deprecated the `mysqld` binary in the `vitess/lite` Docker image.
Making MySQL/Percona version specific image tags also deprecated.

Starting in `v20.0.0` we no longer build the MySQL/Percona version specific image tags.
Moreover, the `mysqld` binary is no longer present on the `vitess/lite` image.

Here are the images we will no longer build and push:

| Image                           | Available | 
|---------------------------------|-----------|
| `vitess/lite:v20.0.0`           | YES       |
| `vitess/lite:v20.0.0-mysql57`   | NO        |
| `vitess/lite:v20.0.0-mysql80`   | NO        |
| `vitess/lite:v20.0.0-percona57` | NO        |
| `vitess/lite:v20.0.0-percona80` | NO        |


If you have not done it yet, you can use an official MySQL Docker image for your `mysqld` container now such as: `mysql:8.0.30`.
Below is an example of a kubernetes yaml file before and after upgrading to an official MySQL image:

```yaml
# before:

# you are still on v19 and are looking to upgrade to v20
# the image used here includes MySQL 8.0.30 and its binaries

    mysqld:
      mysql80Compatible: vitess/lite:v19.0.0-mysql80
```
```yaml
# after:

# if we still want to use MySQL 8.0.30, we now have to use the
# official MySQL image with the 8.0.30 tag as shown below 

    mysqld:
      mysql80Compatible: mysql:8.0.30 # or even mysql:8.0.34 for instance
```

#### <a id="base-k8s-images"/>`vitess/base` and `vitess/k8s` Docker images

Since we have deleted MySQL from our `vitess/lite` image, we are removing the `vitess/base` and `vitess/k8s` images.

These images are no longer useful since we can use `vitess/lite` as the base of many other Docker images (`vitess/vtgate`, `vitess/vtgate`, ...).

#### <a id="gh-ost-binary-tests-removal"/>`gh-ost` binary and endtoend tests

Vitess 20.0 drops support for `gh-ost` DDL strategy.

`vttablet` binary no longer embeds a `gh-ost` binary. Users of `gh-ost` DDL strategy will need to supply a `gh-ost` binary on the `vttablet` host or pod. Vitess will look for the `gh-ost` binary in the system `PATH`; otherwise the user should supply `vttablet --gh-ost-path`.

Vitess' endtoend tests no longer use nor test `gh-ost` migrations.

#### <a id="legacy-emergencyshardreparent-stats"/>Legacy `EmergencyReparentShard` stats

The following `EmergencyReparentShard` stats were deprecated in Vitess 18.0 and are removed in Vitess 20.0:
- `ers_counter`
- `ers_success_counter`
- `ers_failure_counter`

These counters are replaced by the following stats _(introduced in Vitess 18.0)_:
- `emergency_reparent_counts` - Number of times `EmergencyReparentShard` has been run. It is further subdivided by the keyspace, shard and the result of the operation.
- `planned_reparent_counts` - Number of times `PlannedReparentShard` has been run. It is further subdivided by the keyspace, shard and the result of the operation.

Also, the `reparent_shard_operation_timings` stat was added to provide per-operation timings of reparent operations.

### <a id="breaking-changes"/>Breaking Changes

#### <a id="metric-change-vtorc"/>Metric Name Changes in VTOrc

The following metric names have been changed in VTOrc. The old metrics are still available in `/debug/vars` for this release, but will be removed in later releases. The new metric names and the deprecated metric names resolve to the same metric name on prometheus, so there is no change there.

|               Old Metric Name                |             New Metric Name              |                 Name in Prometheus                 |
|:--------------------------------------------:|:----------------------------------------:|:--------------------------------------------------:|
|           `analysis.change.write`            |          `AnalysisChangeWrite`           |           `vtorc_analysis_change_write`            |  
|                `audit.write`                 |               `AuditWrite`               |                `vtorc_audit_write`                 |  
|            `discoveries.attempt`             |           `DiscoveriesAttempt`           |            `vtorc_discoveries_attempt`             |  
|              `discoveries.fail`              |            `DiscoveriesFail`             |              `vtorc_discoveries_fail`              |  
| `discoveries.instance_poll_seconds_exceeded` | `DiscoveriesInstancePollSecondsExceeded` | `vtorc_discoveries_instance_poll_seconds_exceeded` |  
|          `discoveries.queue_length`          |         `DiscoveriesQueueLength`         |          `vtorc_discoveries_queue_length`          |  
|          `discoveries.recent_count`          |         `DiscoveriesRecentCount`         |          `vtorc_discoveries_recent_count`          |  
|               `instance.read`                |              `InstanceRead`              |               `vtorc_instance_read`                |  
|           `instance.read_topology`           |          `InstanceReadTopology`          |           `vtorc_instance_read_topology`           |
|         `emergency_reparent_counts`          |        `EmergencyReparentCounts`         |         `vtorc_emergency_reparent_counts`          |
|          `planned_reparent_counts`           |         `PlannedReparentCounts`          |          `vtorc_planned_reparent_counts`           |
|      `reparent_shard_operation_timings`      |     `ReparentShardOperationTimings`      |  `vtorc_reparent_shard_operation_timings_bucket`   |
		
		

#### <a id="enum-set-vstream"/>ENUM and SET column handling in VTGate VStream API

The [VTGate VStream API](https://vitess.io/docs/reference/vreplication/vstream/) now returns [`ENUM`](https://dev.mysql.com/doc/refman/en/enum.html) and [`SET`](https://dev.mysql.com/doc/refman/en/set.html) column type values in [`VEvent`](https://pkg.go.dev/vitess.io/vitess/go/vt/proto/binlogdata#VEvent) messages (in the embedded [`RowChange`](https://pkg.go.dev/vitess.io/vitess/go/vt/proto/binlogdata#RowChange) messages) as their string values instead of the integer based ones — in both the copy/snapshot phase and the streaming phase. This change was done to make the `VStream` API more user-friendly, intuitive, and to align the behavior across both phases. Before [this change](https://github.com/vitessio/vitess/pull/15723) the values for [`ENUM`](https://dev.mysql.com/doc/refman/en/enum.html) and [`SET`](https://dev.mysql.com/doc/refman/en/set.html) columns were string values in the copy phase but integer values (which only have an internal meaning to MySQL) in the streaming phase. This inconsistency led to various [challenges and issues](https://github.com/vitessio/vitess/issues/15750) for each `VStream` client/consumer (e.g. the [`Debezium` Vitess connector](https://debezium.io/documentation/reference/stable/connectors/vitess.html) failed to properly perform a snapshot for tables containing these column types). Now the behavior is intuitive — clients need the string values as the eventual sink is often not MySQL so each consumer needed to perform the mappings themselves — and consistent. While this is a (potentially) breaking change, a new boolean field has been added to the [`FieldEvent`](https://pkg.go.dev/vitess.io/vitess/go/vt/proto/binlogdata#FieldEvent) message called `EnumSetStringValues`. When that field is `false` (in Vitess v19 and older) then the consumer will need to perform the mappings during streaming phase, but not during copy phase. When this field is `true`, then no mapping is required. This will help to ensure a smooth transition for all consumers over time. To demonstrate, let's look at the textual output (printing the received `VEvents` as strings) when streaming a single `enum_set_test` table from the unsharded `commerce` keyspace so that we can see what the VStream looks like before and after when we start a new VStream in copy/snapshot mode and then transition to streaming mode for the following table:

```sql
CREATE TABLE `enum_set_test` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(120) DEFAULT NULL,
  `shirt_size` enum('small','medium','large','xlarge','xxlarge') DEFAULT NULL,
  `hobbies` set('knitting','cooking','pickleball','biking','hiking','motorcycle','video games','reading') DEFAULT NULL,
  PRIMARY KEY (`id`)
)
```

And with the table having this data when we start our `VStream` and begin the copy/snapshot phase:

```sql
mysql> select * from enum_set_test;
+----+-----------+------------+-------------------------+
| id | name      | shirt_size | hobbies                 |
+----+-----------+------------+-------------------------+
|  1 | Billy Bob | xlarge     | cooking,reading         |
|  2 | Sally Mae | medium     | knitting,cooking,hiking |
+----+-----------+------------+-------------------------+
2 rows in set (0.00 sec)
```

And finally we will perform the following inserts and updates to the table during the streaming phase:

```sql
insert into enum_set_test values (3, "Matt Lord", 'medium', 'pickleball,biking,hiking,motorcycle,video games,reading');
insert into enum_set_test values (4, "Jerry Badyellow", 'large', '');
update enum_set_test set shirt_size = 'small', hobbies = 'knitting,cooking,hiking,reading' where id = 2;
```

Vitess v19 and older:

```text
[type:BEGIN keyspace:"commerce" shard:"0" type:FIELD field_event:{table_name:"commerce.enum_set_test" fields:{name:"id" type:INT32 table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"id" column_length:11 charset:63 flags:49667 column_type:"int"} fields:{name:"name" type:VARCHAR table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"name" column_length:480 charset:255 column_type:"varchar(120)"} fields:{name:"shirt_size" type:ENUM table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"shirt_size" column_length:28 charset:255 flags:256 column_type:"enum('small','medium','large','xlarge','xxlarge')"} fields:{name:"hobbies" type:SET table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"hobbies" column_length:288 charset:255 flags:2048 column_type:"set('knitting','cooking','pickleball','biking','hiking','motorcycle','video games','reading')"} keyspace:"commerce" shard:"0"} keyspace:"commerce" shard:"0"]
[type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/ce357206-0d49-11ef-8fd1-a74564279579:1-35"}} keyspace:"commerce" shard:"0"]
[type:ROW row_event:{table_name:"commerce.enum_set_test" row_changes:{after:{lengths:1 lengths:9 lengths:6 lengths:15 values:"1Billy Bobxlargecooking,reading"}} keyspace:"commerce" shard:"0"} keyspace:"commerce" shard:"0" type:ROW row_event:{table_name:"commerce.enum_set_test" row_changes:{after:{lengths:1 lengths:9 lengths:6 lengths:23 values:"2Sally Maemediumknitting,cooking,hiking"}} keyspace:"commerce" shard:"0"} keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/ce357206-0d49-11ef-8fd1-a74564279579:1-35" table_p_ks:{table_name:"enum_set_test" lastpk:{fields:{name:"id" type:INT32 charset:63 flags:49667} rows:{lengths:1 values:"2"}}}}} keyspace:"commerce" shard:"0" type:COMMIT keyspace:"commerce" shard:"0"]
[type:BEGIN keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/ce357206-0d49-11ef-8fd1-a74564279579:1-35"}} keyspace:"commerce" shard:"0" type:COMMIT keyspace:"commerce" shard:"0"]
[type:COPY_COMPLETED keyspace:"commerce" shard:"0" type:COPY_COMPLETED]
[type:BEGIN timestamp:1715179728 current_time:1715179728532658000 keyspace:"commerce" shard:"0" type:FIELD timestamp:1715179728 field_event:{table_name:"commerce.enum_set_test" fields:{name:"id" type:INT32 table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"id" column_length:11 charset:63 flags:49667 column_type:"int"} fields:{name:"name" type:VARCHAR table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"name" column_length:480 charset:255 column_type:"varchar(120)"} fields:{name:"shirt_size" type:ENUM table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"shirt_size" column_length:28 charset:255 flags:256 column_type:"enum('small','medium','large','xlarge','xxlarge')"} fields:{name:"hobbies" type:SET table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"hobbies" column_length:288 charset:255 flags:2048 column_type:"set('knitting','cooking','pickleball','biking','hiking','motorcycle','video games','reading')"} keyspace:"commerce" shard:"0"} current_time:1715179728535652000 keyspace:"commerce" shard:"0" type:ROW timestamp:1715179728 row_event:{table_name:"commerce.enum_set_test" row_changes:{after:{lengths:1 lengths:9 lengths:1 lengths:3 values:"3Matt Lord2252"}} keyspace:"commerce" shard:"0" flags:1} current_time:1715179728535739000 keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/ce357206-0d49-11ef-8fd1-a74564279579:1-36"}} keyspace:"commerce" shard:"0" type:COMMIT timestamp:1715179728 current_time:1715179728535754000 keyspace:"commerce" shard:"0"]
[type:BEGIN timestamp:1715179735 current_time:1715179735538607000 keyspace:"commerce" shard:"0" type:ROW timestamp:1715179735 row_event:{table_name:"commerce.enum_set_test" row_changes:{after:{lengths:1 lengths:15 lengths:1 lengths:1 values:"4Jerry Badyellow30"}} keyspace:"commerce" shard:"0" flags:1} current_time:1715179735538659000 keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/ce357206-0d49-11ef-8fd1-a74564279579:1-37"}} keyspace:"commerce" shard:"0" type:COMMIT timestamp:1715179735 current_time:1715179735538672000 keyspace:"commerce" shard:"0"]
[type:BEGIN timestamp:1715179741 current_time:1715179741728690000 keyspace:"commerce" shard:"0" type:ROW timestamp:1715179741 row_event:{table_name:"commerce.enum_set_test" row_changes:{before:{lengths:1 lengths:9 lengths:1 lengths:2 values:"2Sally Mae219"} after:{lengths:1 lengths:9 lengths:1 lengths:3 values:"2Sally Mae1147"}} keyspace:"commerce" shard:"0" flags:1} current_time:1715179741728730000 keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/ce357206-0d49-11ef-8fd1-a74564279579:1-38"}} keyspace:"commerce" shard:"0" type:COMMIT timestamp:1715179741 current_time:1715179741728744000 keyspace:"commerce" shard:"0"]
```

Vitess v20 and newer:

```text
[type:BEGIN keyspace:"commerce" shard:"0" type:FIELD field_event:{table_name:"commerce.enum_set_test" fields:{name:"id" type:INT32 table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"id" column_length:11 charset:63 flags:49667 column_type:"int"} fields:{name:"name" type:VARCHAR table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"name" column_length:480 charset:255 column_type:"varchar(120)"} fields:{name:"shirt_size" type:ENUM table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"shirt_size" column_length:28 charset:255 flags:256 column_type:"enum('small','medium','large','xlarge','xxlarge')"} fields:{name:"hobbies" type:SET table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"hobbies" column_length:288 charset:255 flags:2048 column_type:"set('knitting','cooking','pickleball','biking','hiking','motorcycle','video games','reading')"} keyspace:"commerce" shard:"0" enum_set_string_values:true} keyspace:"commerce" shard:"0"]
[type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/156f702a-0d47-11ef-8723-653d045ab990:1-50"}} keyspace:"commerce" shard:"0"]
[type:ROW row_event:{table_name:"commerce.enum_set_test" row_changes:{after:{lengths:1 lengths:9 lengths:6 lengths:15 values:"1Billy Bobxlargecooking,reading"}} keyspace:"commerce" shard:"0"} keyspace:"commerce" shard:"0" type:ROW row_event:{table_name:"commerce.enum_set_test" row_changes:{after:{lengths:1 lengths:9 lengths:6 lengths:23 values:"2Sally Maemediumknitting,cooking,hiking"}} keyspace:"commerce" shard:"0"} keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/156f702a-0d47-11ef-8723-653d045ab990:1-50" table_p_ks:{table_name:"enum_set_test" lastpk:{fields:{name:"id" type:INT32 charset:63 flags:49667} rows:{lengths:1 values:"2"}}}}} keyspace:"commerce" shard:"0" type:COMMIT keyspace:"commerce" shard:"0"]
[type:BEGIN keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/156f702a-0d47-11ef-8723-653d045ab990:1-50"}} keyspace:"commerce" shard:"0" type:COMMIT keyspace:"commerce" shard:"0"]
[type:COPY_COMPLETED keyspace:"commerce" shard:"0" type:COPY_COMPLETED]
[type:BEGIN timestamp:1715179399 current_time:1715179399817221000 keyspace:"commerce" shard:"0" type:FIELD timestamp:1715179399 field_event:{table_name:"commerce.enum_set_test" fields:{name:"id" type:INT32 table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"id" column_length:11 charset:63 flags:49667 column_type:"int"} fields:{name:"name" type:VARCHAR table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"name" column_length:480 charset:255 column_type:"varchar(120)"} fields:{name:"shirt_size" type:ENUM table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"shirt_size" column_length:28 charset:255 flags:256 column_type:"enum('small','medium','large','xlarge','xxlarge')"} fields:{name:"hobbies" type:SET table:"enum_set_test" org_table:"enum_set_test" database:"vt_commerce" org_name:"hobbies" column_length:288 charset:255 flags:2048 column_type:"set('knitting','cooking','pickleball','biking','hiking','motorcycle','video games','reading')"} keyspace:"commerce" shard:"0" enum_set_string_values:true} current_time:1715179399821735000 keyspace:"commerce" shard:"0" type:ROW timestamp:1715179399 row_event:{table_name:"commerce.enum_set_test" row_changes:{after:{lengths:1 lengths:9 lengths:6 lengths:55 values:"3Matt Lordmediumpickleball,biking,hiking,motorcycle,video games,reading"}} keyspace:"commerce" shard:"0" flags:1} current_time:1715179399821762000 keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/156f702a-0d47-11ef-8723-653d045ab990:1-51"}} keyspace:"commerce" shard:"0" type:COMMIT timestamp:1715179399 current_time:1715179399821801000 keyspace:"commerce" shard:"0"]
[type:BEGIN timestamp:1715179399 current_time:1715179399822310000 keyspace:"commerce" shard:"0" type:ROW timestamp:1715179399 row_event:{table_name:"commerce.enum_set_test" row_changes:{after:{lengths:1 lengths:15 lengths:5 lengths:0 values:"4Jerry Badyellowlarge"}} keyspace:"commerce" shard:"0" flags:1} current_time:1715179399822355000 keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/156f702a-0d47-11ef-8723-653d045ab990:1-52"}} keyspace:"commerce" shard:"0" type:COMMIT timestamp:1715179399 current_time:1715179399822360000 keyspace:"commerce" shard:"0"]
[type:BEGIN timestamp:1715179400 current_time:1715179400512056000 keyspace:"commerce" shard:"0" type:ROW timestamp:1715179400 row_event:{table_name:"commerce.enum_set_test" row_changes:{before:{lengths:1 lengths:9 lengths:6 lengths:23 values:"2Sally Maemediumknitting,cooking,hiking"} after:{lengths:1 lengths:9 lengths:5 lengths:31 values:"2Sally Maesmallknitting,cooking,hiking,reading"}} keyspace:"commerce" shard:"0" flags:1} current_time:1715179400512094000 keyspace:"commerce" shard:"0" type:VGTID vgtid:{shard_gtids:{keyspace:"commerce" shard:"0" gtid:"MySQL56/156f702a-0d47-11ef-8723-653d045ab990:1-53"}} keyspace:"commerce" shard:"0" type:COMMIT timestamp:1715179400 current_time:1715179400512108000 keyspace:"commerce" shard:"0"]
```

An example key difference there being that `after:{lengths:1 lengths:9 lengths:1 lengths:3 values:"2Sally Mae1147"}` from Vitess v19 and older becomes `after:{lengths:1 lengths:9 lengths:5 lengths:31 values:"2Sally Maesmallknitting,cooking,hiking,reading"}` from Vitess v20 and newer. So `1` -> `small` and `147` -> `knitting,cooking,hiking,reading` for the `ENUM` and `SET` column values respectively. This also demonstrates why this mapping is necessary in consumers/clients, as `147` has no logical meaning/value for this column outside of MySQL internals.

If you're using the [`Debezium` Vitess connector](https://debezium.io/documentation/reference/stable/connectors/vitess.html), you should upgrade your connector to 2.7 (the next release) — which should contain [the relevant necessary changes](https://issues.redhat.com/browse/DBZ-7792) — *prior to upgrading Vitess* to v20.0.1 or later. If you're using any of the PlanetScale connectors ([`AirByte`](https://github.com/planetscale/airbyte-source/), [`FiveTran`](https://github.com/planetscale/fivetran-source), or [`singer-tap`](https://github.com/planetscale/singer-tap)) then no actions are required.

If you're using a custom `VStream` client/consumer, then you will need to build a new client with the updated v20 [binlogdata protos](https://pkg.go.dev/vitess.io/vitess/go/vt/proto/binlogdata) ([source](https://github.com/vitessio/vitess/blob/main/proto/binlogdata.proto) for which would be in `main` or the `release-20.0` branch) before needing to support Vitess v20.0.1 or later. Your client will then be able to handle old and new messages, with older messages always having this new field set to `false`.

#### <a id="shutdown-grace-period-default"/>`shutdown_grace_period` Default Change

The `--shutdown_grace_period` flag, which was introduced in v2 with a default of `0 seconds`, has now been changed to default to `3 seconds`.
This makes reparenting in Vitess resilient to client errors, and prevents PlannedReparentShard from timing out.

In order to preserve the old behaviour, the users can set the flag back to `0 seconds` causing open transactions to never be shutdown, but in that case, they run the risk of PlannedReparentShard calls timing out.

#### <a id="unmanaged-tablet"/> New `unmanaged` Flag and `disable_active_reparents` deprecation

New flag `--unmanaged` has been introduced in this release to make it easier to flag unmanaged tablets. It also runs validations to make sure the unmanaged tablets are configured properly. `--disable_active_reparents` flag has been deprecated for `vttablet`, `vtcombo` and `vttestserver` binaries and will be removed in future releases. Specifying the `--unmanaged` flag will also block replication commands and replication repairs.

Starting this release, all unmanaged tablets should specify this flag.


#### <a id="recovery-block-deprecation"/> `recovery-period-block-duration` Flag deprecation

The flag `--recovery-period-block-duration` has been deprecated in VTOrc from this release. Its value is now ignored and the flag will be removed in later releases.
VTOrc no longer blocks recoveries for a certain duration after a previous recovery has completed. Since VTOrc refreshes the required information after
acquiring a shard lock, blocking of recoveries is not required.

#### <a id="mysqlctld-onterm-timeout"/>`mysqlctld` `onterm_timeout` Default Change

The `--onterm_timeout` flag default value has changed for `mysqlctld`. It now is by default long enough to be able to wait for the default `--shutdown-wait-time` when shutting down on a `TERM` signal. 

This is necessary since otherwise MySQL would never shut down cleanly with the old defaults, since `mysqlctld` would shut down already after 10 seconds by default.

#### <a id="move-tables-auto-increment"/>`MoveTables` now removes `auto_increment` clauses by default when moving tables from an unsharded keyspace to a sharded one

A new `--remove-sharded-auto-increment` flag has been added to the [`MoveTables` create sub-command](https://vitess.io/docs/20.0/reference/programs/vtctldclient/vtctldclient_movetables/vtctldclient_movetables_create/) and it is set to `true` by default. This flag controls whether any [MySQL `auto_increment`](https://dev.mysql.com/doc/refman/en/example-auto-increment.html) clauses should be removed from the table definitions when moving tables from an unsharded keyspace to a sharded one. This is now done by default as `auto_increment` clauses should not typically be used with sharded tables and you should instead rely on externally generated values such as a form of universally/globally unique identifiers or use [Vitess sequences](https://vitess.io/docs/reference/features/vitess-sequences/) in order to ensure that each row has a unique identifier (Primary Key value) across all shards. If for some reason you want to retain them you can set this new flag to `false` when creating the workflow.

#### <a id="durabler-interface-method-renaming"/>`Durabler` interface method renaming

The methods of [the `Durabler` interface](https://github.com/vitessio/vitess/blob/main/go/vt/vtctl/reparentutil/durability.go#L70-L79) in `go/vt/vtctl/reparentutil` were renamed to be public _(capitalized)_ methods to make it easier to integrate custom Durability Policies from external packages. See [RFC for details](https://github.com/vitessio/vitess/issues/15544).

Users of custom Durability Policies must rename private `Durabler` methods.

Changes:
- The `promotionRule` method was renamed to `PromotionRule`
- The `semiSyncAckers` method was renamed to `SemiSyncAckers`
- The `isReplicaSemiSync` method was renamed to `IsReplicaSemiSync`

### <a id="query-compatibility"/>Query Compatibility

#### <a id="vindex-hints"/> Vindex Hints

Vitess now supports Vindex hints that provide a way for users to influence the shard routing of queries in Vitess by specifying, which vindexes should be considered or ignored by the query planner. This feature enhances the control over query execution, allowing for potentially more efficient data access patterns in sharded databases.

Example:
 ```sql
    SELECT * FROM user USE VINDEX (hash_user_id, secondary_vindex) WHERE user_id = 123;
    SELECT * FROM order IGNORE VINDEX (range_order_id) WHERE order_date = '2021-01-01';
 ```

For more information about Vindex hints and its usage, please consult the documentation.

#### <a id="update-limit"/> Update with Limit Support

Support is added for sharded update with limit.

Example: `update t1 set t1.foo = 'abc', t1.bar = 23 where t1.baz > 5 limit 1`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/update.html)

#### <a id="multi-table-update"/> Update with Multi Table Support

Support is added for sharded multi-table update with column update on single target table using multiple table join.

Example: `update t1 join t2 on t1.id = t2.id join t3 on t1.col = t3.col set t1.baz = 'abc', t1.apa = 23 where t3.foo = 5 and t2.bar = 7`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/update.html)

#### <a id="update-multi-target"/> Update with Multi Target Support

Support is added for sharded multi table target update.

Example: `update t1 join t2 on t1.id = t2.id set t1.foo = 'abc', t2.bar = 23`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/update.html)

#### <a id="delete-subquery"/> Delete with Subquery Support

Support is added for sharded table delete with subquery

Example: `delete from t1 where id in (select col from t2 where foo = 32 and bar = 43)`

#### <a id="delete-multi-target"/> Delete with Multi Target Support

Support is added for sharded multi table target delete.

Example: `delete t1, t3 from t1 join t2 on t1.id = t2.id join t3 on t1.col = t3.col`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/delete.html)

#### <a id="udf-support"/> User Defined Functions Support

VTGate can track any user defined functions for better planning.
User Defined Functions (UDFs) should be directly loaded in the underlying MySQL.

It should be enabled in VTGate with the `--track-udfs` flag.
This will enable the tracking of UDFs in VTGate and will be used for planning.
Without this flag, VTGate will not be aware that there might be aggregating user-defined functions in the query that need to be pushed down to MySQL.

More details about how to load UDFs is available in [MySQL Docs](https://dev.mysql.com/doc/extending-mysql/8.0/en/adding-loadable-function.html)

#### <a id="insert-row-alias-support"/> Insert Row Alias Support

Support is added to have row alias in Insert statement to be used with `on duplicate key update`.

Example:
- `insert into user(id, name, email) valies (100, 'Alice', 'alice@mail.com') as new on duplicate key update name = new.name, email = new.email`
- `insert into user(id, name, email) valies (100, 'Alice', 'alice@mail.com') as new(m, n, p) on duplicate key update name = n, email = p`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html)

### <a id="query-timeout"/>Query Timeout
On a query timeout, Vitess closed the connection using the `kill connection` statement. This leads to connection churn 
which is not desirable in some cases. To avoid this, Vitess now uses the `kill query` statement to cancel the query. 
This will only cancel the query and does not terminate the connection.

### <a id="flag-changes"/>Flag Changes

#### <a id="pprof-http-default"/> `pprof-http` Default Change

The `--pprof-http` flag, which was introduced in v19 with a default of `true`, has now been changed to default to `false`.
This makes HTTP `pprof` endpoints now an *opt-in* feature, rather than opt-out.
To continue enabling these endpoints, explicitly set `--pprof-http` when starting up Vitess components.

#### <a id="healthcheck-dial-concurrency-flag"/>New `--healthcheck-dial-concurrency` flag

The new `--healthcheck-dial-concurrency` flag defines the maximum number of healthcheck connections that can open concurrently. This limit is to avoid hitting Go runtime panics on deployments watching enough tablets [to hit the runtime's maximum thread limit of `10000`](https://pkg.go.dev/runtime/debug#SetMaxThreads) due to blocking network syscalls. This flag applies to `vtcombo`, `vtctld` and `vtgate` only and a value less than the runtime max thread limit _(`10000`)_ is recommended.

#### <a id="buffer_min_time_between_failovers-flag"/>New minimum for `--buffer_min_time_between_failovers`

The `--buffer_min_time_between_failovers` `vttablet` flag now has a minimum value of `1s`. This is because a value of 0 can cause issues with the buffering mechanics resulting in unexpected and unnecessary query errors — in particular during `MoveTables SwitchTraffic` operations. If you are currently specifying a value of 0 for this flag then you will need to update the config value to 1s *prior to upgrading to v20 or later* as `vttablet` will report an error and terminate if you attempt to start it with a value of 0.

#### <a id="vtgate-track-udfs-flag"/>New `--track-udfs` vtgate flag

The new `--track-udfs` flag enables VTGate to track user defined functions for better planning.

#### <a id="documentation-lock-timeout"/>Help text fix for `--lock-timeout`

The help text for the flag `--lock-timeout` was incorrect. We were documenting it as a flag that controlled the duration for which the shard lock was acquired. It is actually the maximum duration for which we wait while attempting to acquire a lock from the topology server.

## <a id="minor-changes"/>Minor Changes

### <a id="new-stats"/>New Stats

#### <a id="vttablet-query-cache-hits-and-misses"/>VTTablet Query Cache Hits and Misses

VTTablet exposes two new counter stats:

 * `QueryCacheHits`: Query engine query cache hits
 * `QueryCacheMisses`: Query engine query cache misses

### <a id="sighup-reload-of-grpc-client-auth-creds"/>`SIGHUP` reload of gRPC client static auth creds

The internal gRPC client now caches the static auth credentials and supports reloading via the `SIGHUP` signal. Previous to v20 the credentials were not cached. They were re-loaded from disk on every use.

### <a id="vtadmin"/>VTAdmin

#### <a id="updated-node"/>vtadmin-web updated to node v20.12.2 (LTS)

Building `vtadmin-web` now requires node >= v20.12.0 (LTS). Breaking changes from v18 to v20 can be found at https://nodejs.org/en/blog/release/v20.12.0 -- with no known issues that apply to VTAdmin.
Full details on the node v20.12.2 release can be found at https://nodejs.org/en/blog/release/v20.12.2.

#### <a id="replaced-highcharts"/>Replaced highcharts with d3

The vtadmin-web UI no longer has a dependency on highcharts for licensing reasons. The tablet QPS, tablet VReplication QPS, and workflow streams lag charts have all been replaced by d3. We'll be iteratively improving the d3 charts until they reach feature parity with the original highcharts charts. 
