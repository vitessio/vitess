## Enhancements

- `vtctl/vtctlclient ApplySchema` now respects `-allow-zero-in-date` for `direct` strategy. For example, the following statement is now accepted: `vtctlclient ApplySchema -skip_preflight -ddl_strategy='direct -allow-zero-in-date' -sql "create table if not exists t2(id int primary key, dt datetime default '0000-00-00 00:00:00')" commerce`

## Major Changes

### vttablet -use_super_read_only flag now defaults to true
The default value used to be false. What this means is that during a failover, we will set `super_read_only` on database flavors that support them (MySQL 5.7+ and Percona 5.7+).
In addition, all Vitess-managed databases will be started with `super-read-only` in the cnf file.
It is expected that this change is safe and backwards-compatible. Anyone who is relying on the current behavior should pass `-use_super_read_only=false` on the vttablet command line, and make sure they are using a custom my.cnf instead of the one provided as the default by Vitess.

### vtgate -buffer_implementation now defaults to keyspace_events
The default value used to be `healthcheck`. The new `keyspace_events` implementation has been tested in production with good results and shows more consistent buffering behavior during PlannedReparentShard operations. The `keyspace_events` implementation utilizes heuristics to detect additional cluster states where buffering is safe to perform, including cases where the primary may be down. If there is a need to revert back to the previous buffer implementation, ensure buffering is enabled in vtgate and pass the flag `-buffer_implementation=healthcheck`.

The default buffer timings have also been adjusted to span additional time. The `buffer_window` has been adjusted from `10s` to `1m`, the `buffer_max_failover_duration` has been changed from `20s` to `1m`; and finally the `buffer_min_time_between_failovers` has been adjusted from `1m` to `2m`. 

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

### vtctl/vtctlclient ApplySchema -uuid_list

`vtctlient ApplySchema` now support a new optional `-uuid_list` flag. It is possible for the user to explicitly specify the UUIDs for given migration(s). UUIDs must be in a specific format. If given, number of UUIDs must match the number of DDL statements. Example:

```shell
vtctlclient OnlineDDL ApplySchema -sql "drop table t1, drop table t2" -uuid_list "d08f0000_51c9_11ec_9cf2_0a43f95f28a3,d08f0001_51c9_11ec_9cf2_0a43f95f28a3" commerce
```

Vitess will assign each migration with given UUID in order of appearance.
It is the user's responsibility to ensure given UUIDs are globally unique. If the user submits a migration with an already existing UUID, that migration never gets scheduled nor executed.

### vtctl/vtctlclient OnlineDDL ... complete

Complementing the `alter vitess_migration ... complete` query, a migration can also be completed via `vtctl` or `vtctlclient`:

```shell
vtctlclient OnlineDDL <keyspace> complete <uuid>
```

For example:

```shell
vtctlclient OnlineDDL commerce complete d08ffe6b_51c9_11ec_9cf2_0a43f95f28a3
```

## Incompatible Changes

## Deprecations
