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

## vtadmin-web updated to node v16.13.0 (LTS)

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
