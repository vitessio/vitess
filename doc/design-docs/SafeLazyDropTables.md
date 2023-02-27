# Safe, lazy DROP TABLE

`DROP TABLE` is a risky MySQL operation in production. There seem to be multiple components involved, the major being that if the table has pages in InnoDB's buffer pool (one or many), then the buffer pool is locked for the duration of the `DROP`. The duration of the `DROP` is also related with the time it takes the operating system to delete the `.ibd` file associated with the table (assuming `innodb_file_per_table`).

Noteworthy that the problem is in particular on the `primary` MySQL server; replicas are not affected as much.

Different companies solve `DROP TABLE` in different ways. An interesting discussion is found on `gh-ost`'s repo and on mysql bugs:

- https://github.com/github/gh-ost/issues/307
- https://bugs.mysql.com/bug.php?id=91977

Solutions differ in implementation, but all suggest _waiting_ for some time before actually dropping the table. That alone requires management around `DROP TABLE` operations. As explained below, _waiting_ enables reverting the operation.

Vitess should automate table drops and make the problem transparent to the user as much as possible. Breakdown of the suggested solution follows.

## Illustrating custom DROP workaround steps

We can make the `DROP` management stateful or stateless. Opting for stateless: no meta tables to describe the progress of the `DROP`. The state should be inferred from the tables themselves. Specifically, we will encode hints in the table names.

We wish to manage `DROP` requests. Most managed `DROP` requests will _wait_ before destroying data. If the user issued a `DROP TABLE` only to realize the app still expects the table to exist, then we make it possible to revert the operation.

This is done by first issuing a `RENAME TABLE my_table TO something_else`. To the app, it seems like the table is gone; but the user may easily restore it by running the revert query: `RENAME TABLE something_else TO my_table`.

That `something_else` name can be e.g. `_vt_HOLD_2201058f_f266_11ea_bab4_0242c0a8b007_20200910113042`.

At some point we decide that we can destroy the data. The "hold" period can either be determined by vitess or explicitly by the user. e.g. On a successful schema migration completion, Vitess can choose to purge the "old" table right away.
At that stage we rename the table to e.g. `_vt_PURGE_63b5db0c_f25c_11ea_bab4_0242c0a8b007_20200911070228`.
A table by that name is eligible to have its data purged.

By experience (see `gh-ost` issue above), a safe method to purge data is to slowly remove rows, until the table is empty. Note:

- This operation needs to be throttled (see #6661, #6668,  https://github.com/vitessio/website/pull/512)
- We can `SET SQL_LOG_BIN=0` and only purge the table on the `primary`. This reduces binlog size and also does not introduce replication lag. One may argue that lag-based throttling is not needed, but in my experience it's still wise to use, since replication lag can imply load on `primary`, and it's best to not overload the `primary`.
- We issue a `DELETE FROM my_table LIMIT 50` ; 10-100 are normally good chunk sizes. Order of purging does not matter.

It's important to note that the `DELETE` statement actually causes table pages to _load into the buffer pool_, which works against our objective.

Once all rows are purged from a table, we rename it again to e.g. `_vt_DROP_8a797518_f25c_11ea_bab4_0242c0a8b007_20210211234156`. At this time we point out that `20200911234156` is actually a readable timestamp, and stands for `2021-02-11 23:41:56`. That timestamp can tell us when the table was last renamed. 

Vitess can then run an actual `DROP TABLE` for `_vt_DROP_...` tables whose timestamp is older than, say, 2 days. As mentioned above, purging the table actually caused the table to load onto the buffer pool, and we need to wait for it to naturally get evicted, before dropping it.

## Suggested implementation: table lifecycle aka table garbage collection

The actual implementation will be in `vttablet`. A `primary` vttablet will routinely check for tables that need work:

- A once per hour check is enough; we are going to spend _days_ in dropping a tables, so no need to check frequently
- Look for tables called `_vt_(HOLD|PURGE|DROP)_<UUID>_<TIMESTAMP>` (be very strict about the pattern search, because why not)
- If time is right, `RENAME` to the next step (`HOLD` renames to `PURGE`, `PURGE` renames to `DROP`)
- Continuously purge rows from tables in `PURGE` state. We may want to purge only one table at a time, e.g. purge oldest table, until it is empty, immediately rename it to `DROP` state, move on to the next table, begin purging it.
  - Use throttler in between purge chunks

This means a user-initiated `DROP ... TABLE` on a sharded cluster, is handled by each shard independently. The data is purged at different times, and the disk space reclaimed at different time, each shard will do its best, but will likely have somewhat different workloads to make the operations run faster on some, and slower on others.

We need to allow for alternate methods/flows for dropping tables:

- some users just wait and drop
- some need to purge rows
- some operate independently on replicas.

The way to support the above is by introducing user-defined states. The general table lifecycle flow is this, and in this order:

1. Real table (_alive_)
2. `HOLD`
3. `PURGE`
4. `EVAC`
5. `DROP`
6. _gone_

Vitess will transition the table through these states, _in order_. But it will also support skipping some states. Let's first explain the meaning of the states:

- Real table (alive): Table is in use in production.
- `HOLD`: Table is renamed to something like `_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410`. Vitess will not make changes to the table, will not drop data. The table is put away for safe keeping for X hours/days. If it turns out the app still needs the table, the user can `RENAME` is back to its original name, taking it out of this game.
- `PURGE`: Table renamed to e.g `_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200916080539`. Vitess purges (or will purge, based on workload and prior engagements) rows from `PURGE` tables. Data is lost and the user may not resurrect the table anymore.
Most likely we will settle for a `SQL_LOG_BIN=0`, ie purging will not go through replication. The replicas are not so badly affected by `DROP` statements as a `primary`.
- `EVAC`: Table renamed to e.g. `_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200918192031`. The table sits still for Y houtrs/days. I'm thinking this period will be pre-defined by vitess. The purpose of this state is to wait a _reasonable_ amount of time so that tabe's pages are evacuated from the innodb buffer pool by the natural succession of production IO/memory activity.
- `DROP`: Table renamed to e.g. `_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200921193202`. Vitess will `DROP TABLE` this table _imminently_.  
- gone: end of lifecycle

## Transitioning and skipping of states

The above lifecycle will be the default, and safest cycle. It is also the longest. Some users will use this sequence. Others have no issue dropping a table with millions of rows, and don't want to pay the IO+time of purging the data.

We introduce a `vttablet` command line flag: `-table_gc_lifecycle="hold,purge,evac,drop"`, that's the default value. `drop` is implicit, if you don't specify it, it's automatically appended. Otherwise, consider the following flows:

- some people just happy to drop tables with no wait, no purge, no nothing. They will set  `-table_gc_lifecycle="drop"`
- some people want to keep the table around for a few days, then just go ahead and drop it: `-table_gc_lifecycle="hold,drop"`
- some want to keep the table, then purge it, then immediately drop: `-table_gc_lifecycle="hold,purge,drop"`

This is both user-customizable and simple to implement.

 
## Asking for a safe DROP

Safe `DROP TABLE`s participate in the Online DDL flow. When a user chooses an online `ddl_strategy`, the user's `DROP TABLE` statement implicitly translates to a `RENAME TABLE` statement which sends the table into our lifecycle mechanism, from where it is garbage collected.

