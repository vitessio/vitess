## Major Changes

### vtgate -gateway_implementation flag is deprecated (and ignored)
Support for `discoverygateway` is being dropped. `tabletgateway` is now the only supported implementation. Scripts using this flag should be updated to remove the flag as it will be deleted in the next release.

### vttablet -use_super_read_only flag now defaults to true
The default value used to be false. What this means is that during a failover, we will set `super_read_only` on database flavors that support them (MySQL 5.7+ and Percona 5.7+).
In addition, all Vitess-managed databases will be started with `super-read-only` in the cnf file.
It is expected that this change is safe and backwards-compatible. Anyone who is relying on the current behavior should pass `-use_super_read_only=false` on the vttablet command line, and make sure they are using a custom my.cnf instead of the one provided as the default by Vitess.

### vtgate -buffer_implementation now defaults to keyspace_events
The default value used to be `healthcheck`. The new `keyspace_events` implementation has been tested in production with good results and shows more consistent buffering behavior during PlannedReparentShard operations. The `keyspace_events` implementation utilizes heuristics to detect additional cluster states where buffering is safe to perform, including cases where the primary may be down. If there is a need to revert back to the previous buffer implementation, ensure buffering is enabled in vtgate and pass the flag `-buffer_implementation=healthcheck`. 

### ddl_strategy: -postpone-completion flag

`ddl_strategy` (either `@@ddl_strategy` in VtGate or `-ddl_strategy` in `vtctlclient ApplySchema`) supports the flag `-postpone-completion`

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
This query tells Vitess that a migration's artifacts are good to be cleaned up asap. This allows Vitess to free disk resources sooner. As a reminder, once a migration's artifacts are cleaned up, the migration is no
longer revertible.

### alter vitess_migration ... complete

A new query is supported:

```sql
alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' complete
```

This command indicates that a migration executed with `-postpone-completion` is good to complete. Behavior:

- For running `ALTER`s (`online` and `gh-ost`) which are ready to cut-over: cut-over imminently (though not immediately - cut-over depends on polling interval, replication lag, etc)
- For running `ALTER`s (`online` and `gh-ost`) which are only partly through the migration: they will cut-over automatically when they complete their work, as if `-postpone-completion` wasn't indicated
- For queued `CREATE` and `DROP` migrations: "unblock" them from being scheduled. They'll be scheduled at the scheduler's discretion. there is no guarantee that they will be scheduled to run immediately.

### vtctl/vtctlclient ApplySchema: ALTER VITESS_MIGRATION

`vtctl ApplySchema` now supports `ALTER VITESS_MIGRATION ...` statements. Example:

```shell
$ vtctl ApplySchema -skip_preflight -sql "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' complete" commerce
```

### vtctl/vtctlclient ApplySchema: allow zero in date

`vtctl/vtctlclient ApplySchema` now respects `-allow-zero-in-date` for `direct` strategy. For example, the following statement is now accepted:

```shell
vtctlclient ApplySchema -skip_preflight -ddl_strategy='direct -allow-zero-in-date' -sql "create table if not exists t2(id int primary key, dt datetime default '0000-00-00 00:00:00')" commerce
```

### vtctl/vtctlclient ApplySchema -uuid_list

`vtctlient ApplySchema` now support a new optional `-uuid_list` flag. It is possible for the user to explicitly specify the UUIDs for given migration(s). UUIDs must be in a specific format. If given, number of UUIDs must match the number of DDL statements. Example:

```shell
vtctlclient OnlineDDL ApplySchema -sql "drop table t1, drop table t2" -uuid_list "d08f0000_51c9_11ec_9cf2_0a43f95f28a3,d08f0001_51c9_11ec_9cf2_0a43f95f28a3" commerce
```

Vitess will assign each migration with given UUID in order of appearance.
It is the user's responsibility to ensure given UUIDs are globally unique. If the user submits a migration with an already existing UUID, that migration never gets scheduled nor executed.

### vtctl/vtctlclient ApplySchema -migration_context

`-migration_context` flag is synonymous to `-request_context`. Either will work. We will encourage use of `-migration_context` as it is more consistent with output of `SHOW VITESS_MIGRATIONS ...` which includes the `migration_context` column.

### vtctl/vtctlclient OnlineDDL ... complete

Complementing the `alter vitess_migration ... complete` query, a migration can also be completed via `vtctl` or `vtctlclient`:

```shell
vtctlclient OnlineDDL <keyspace> complete <uuid>
```

For example:

```shell
vtctlclient OnlineDDL commerce complete d08ffe6b_51c9_11ec_9cf2_0a43f95f28a3
```

### vtctl/vtctlclient OnlineDDL -json

The command now accepts an optional `-json` flag. With this flag, the output is a valid JSON listing all columns and rows.

## vtadmin-web updated to node v16.13.0 (LTS)

Building vtadmin-web now requires node >= v16.13.0 (LTS). Upgrade notes are given in https://github.com/vitessio/vitess/pull/9136. 

## Incompatible Changes

## Deprecations
