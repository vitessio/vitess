## Major Changes

### Online DDL changes

#### ddl_strategy: 'vitess'

`ddl_strategy` now takes the value of `vitess` to indicate VReplication-based migrations. It is a synonym to `online` and uses the exact same functionality. In the future, the `online` term will phase out, and `vitess` will remain the term of preference.

Example:

```shell
vtctlclient ApplySchema -skip_preflight -ddl_strategy='vitess' -sql "alter table my_table add column my_val int not null default 0" commerce
```

#### Behavior changes

- `vtctl ApplySchema -uuid_list='...'` now rejects a migration if an existing migration has the same UUID but with different `migration_context`.

### Table lifecycle

#### Views

Table lifecycle now supports views. It ensures to not purge rows from views, and does not keep views in `EVAC` state (they are immediately transitioned to `DROP` state).

#### Fast drops

On Mysql `8.0.23` or later, the states `PURGE` and `EVAC` are automatically skipped, thanks to `8.0.23` improvement to `DROP TABLE` speed of operation.

### Compatibility

#### Join with `USING`

In previous versions of Vitess our planners (v3 and Gen4) were rewriting the `USING` condition of a join to an `ON` condition.
This rewriting was causing an incompatible behavior with MySQL due to how MySQL handles queries with `ON` and `USING` differently.

Thanks to this rewriting we were previously able to plan sharded queries with an `USING` condition. This is no longer the case.
Queries with an `USING` condition that need to be sent to a sharded keyspace are no longer supported and will return an `unsupported` planner error.

This change was made through pull request [#9767](https://github.com/vitessio/vitess/pull/9767).
