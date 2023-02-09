# Release of Vitess v16.0.0

## Major Changes

### Online DDL

#### 'mysql' strategy

Introducing a new DDL strategy: `mysql`. This strategy is a hybrid between `direct` (which is completely non-Online) and the various online strategies.

A migration submitted with `mysql` strategy is _managed_. That is, it gets a migration UUID. The scheduler queues it, reviews it, runs it. The user may cancel or retry it, much like all Online DDL migrations. The difference is that when the scheduler runs the migration via normal MySQL `CREATE/ALTER/DROP TABLE` statements.

The user may add `ALGORITHM=INPLACE` or `ALGORITHM=INSTANT` as they please, and the scheduler will attempt to run those as is. Some migrations will be completely blocking. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html). In particular, consider that for non-`INSTANT` DDLs, replicas will accumulate substantial lag.

#### '--in-order-completion' strategy flag

A migration that runs with this DDL strategy flag may only complete if no prior migrations are still pending (pending means either `queued`, `ready` or `running` states).

`--in-order-completion` considers the order by which migrations were submitted. For example:

- In two sequential `ApplySchema` commands, the first is considered to be "earlier" and the second is "later".
- In a single `ApplySchema` command, and with multiple queries in `--sql` command line argument, the order of migrations is the same as the order of SQL statements.

Internally, that order is implied by the `id` column of `_vt.schema_migrations` table.

Note that `--in-order-completion` still allows concurrency. In fact, it is designed to work with concurrent migrations. The idea is that many migrations may run concurrently, but the way they finally `complete` is in-order.

This lets the user submit multiple migrations which may have some dependencies (for example, introduce two views, one of which reads from the other). As long as the migrations are submitted in a valid order, the user can then expect Vitess to complete the migrations successfully (and in that order).

This strategy flag applies to any `CREATE|DROP TABLE|VIEW` statements, and to `ALTER TABLE` with `vitess|online` strategy.

It _does not_ apply to `ALTER TABLE` in `gh-ost`, `pt-osc`, `mysql` and `direct` strategies.
