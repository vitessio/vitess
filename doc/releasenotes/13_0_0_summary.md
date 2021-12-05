## Enhancements

- `vtctl/vtctlclient ApplySchema` now respects `-allow-zero-in-date` for `direct` strategy. For example, the following statement is now accepted: `vtctlclient ApplySchema -skip_preflight -ddl_strategy='direct -allow-zero-in-date' -sql "create table if not exists t2(id int primary key, dt datetime default '0000-00-00 00:00:00')" commerce`

## Major Changes

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

### vtctl ApplySchema: ALTER VITESS_MIGRATION

`vtctl ApplySchema` now supports `ALTER VITESS_MIGRATION ...` statements. Example:

```shell
$ vtctl ApplySchema -skip_preflight -sql "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' complete" commerce
```

### vtctl OnlineDDL ... complete

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
