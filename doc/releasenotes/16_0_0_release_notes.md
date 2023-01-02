# Release of Vitess v16.0.0
## Major Changes

### Online DDL

Introducing a new DDL strategy: `mysql`. This strategy is a hybrid between `direct` (which is completely non-Online) and the various online strategies.

A migration submitted with `mysql` strategy is _managed_. That is, it gets a migration UUID. The scheduler queues it, reviews it, runs it. The user may cancel or retry it, much like all Online DDL migrations. The difference is that when the scheduler runs the migration via normal MySQL `CREATE/ALTER/DROP TABLE` statements.

The user may add `ALGORITHM=INPLACE` or `ALGORITHM=INSTANT` as they please, and the scheduler will attempt to run those as is. Some migrations will be completely blocking. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html). In particular, consider that for non-`INSTANT` DDLs, replicas will accumulate substantial lag.

