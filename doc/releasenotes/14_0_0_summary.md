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
