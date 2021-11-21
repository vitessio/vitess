## Major Changes


## Enhacements

- `vtctl ApplySchema` now respects `-allow-zero-in-date` for `direct` strategy. For example, the following statement is now accepted: `vtctlclient ApplySchema -skip_preflight -ddl_strategy='direct -allow-zero-in-date' -sql "create table if not exists t2(id int primary key, dt datetime default '0000-00-00 00:00:00')" commerce`

## Incompatible Changes

## Syntax changes

### alter vitess_migration ... cleanup
A new query is supported:
```sql
alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' cleanup
```
This query tells Vitess that a migration's artifacts are good to be cleaned up asap. This allows Vitess to free disk resources sooner. As a reminder, once a migration's artifacts are cleaned up, the migration is no
longer revertible.

## Deprecations
