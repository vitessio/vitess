# Vitess, MySQL Replication, and Schema Changes

## Statement vs Row Based Replication

MySQL supports two primary modes of replication in its binary logs: Statement or
Row based.

**Statement Based Replication**:

* The statements executed on the master are copied almost as-is in the master
  logs.
* The slaves replay these statements as is.
* If the statements are expensive (especially an update with a complicated WHERE
  clause), they will be expensive on the slaves too.
* For current timestamp and auto-increment values, the master also puts
  additional SET statements in the logs to make the statement have the same
  effect, so the slaves end up with the same values.

**Row Based Replication**:

* The statements executed on the master result in updated rows. The new full
  values for these rows are copied to the master logs.
* The slaves change their records for the rows they receive. The update is by
  primary key, and contains the new values for each column, so it’s very fast.
* Each updated row contains the entire row, not just the columns that were
  updated. (this is inefficient if only one column out of a large number has
  changed, but it’s more efficient on the slave to just swap out the row with
  the new one).
* The replication stream is harder to read, as it contains almost binary data,
  that don’t easily map to the original statements.
* There is a configurable limit on how many rows can be affected by one
  statement, so the master logs are not flooded.
* The format of the logs depends on the master schema: each row has a list of
  values, one value for each column. So if the master schema is different from
  the slave schema, updates will misbehave (exception being if slave has extra
  columns at the end).
* It is possible to revert to statement based replication for some commands to
  avoid these drawbacks (for instance for DELETE statements that affect a large
  number of rows).
* Schema changes revert to Statement Based Replication.
* If comments are added to a statement, there are stripped from the the
  replication stream (as only rows are transmitted). There is a debug flag to
  add the original statement to each row update, but it is costly and very
  verbose.

For the longest time, MySQL replication has been single-threaded: only one
statement is applied by the slaves at a time. Since the master applies more
statements in parallel, replication can fall behind on the slaves fairly easily,
under higher load. Even though the situation has improved (group commit), the
slave replication speed is still a limiting factor for a lot of
applications. Since Row Based Replication achieves higher update rates on the
slaves, it has been the only viable option for most performance sensitive
applications.

Schema changes however are not easy to achieve with Row Based
Replication. Adding columns can be done offline, but removing or changing
columns cannot easily be done (there are multiple ways to achieve this, but they
all have limitations or performance implications, and are not that easy to
setup).

Vitess helps by using Statement Based Replication (therefore allowing complex
schema changes), while at the same time simplifying the replication stream (so
slaves can be fast), by rewriting Update statements.

Then, with Statement Based Replication, it becomes easier to perform offline
advanced schema changes, or large data updates. Vitess’s solution is called
Pivot.

We plan to also support Row Based replication in the future, and adapt our tools
to provide the same features when possible.

## Rewriting Update Statements

Vitess re-writes ‘UPDATE’ SQL statements to always know what rows will be
affected. For instance, this statement:

```
UPDATE <table> SET <set values> WHERE <clause>
```

Will be rewritten into:

```
SELECT <primary key columns> FROM <table> WHERE <clause> FOR UPDATE
UPDATE <tablet> SET <set values> WHERE <primary key columns> IN <result from previous SELECT> /* primary key values: … */
```

With this rewrite in effect, we know exactly which rows are affected, by primary
key, and we also document them as a SQL comment.

The replication stream then doesn’t contain the expensive WHERE clauses, but
only the UPDATE statements by primary key. In a sense, it is combining the best
of Row Based and Statement Based replication: the slaves only do primary key
based updates, but the replication stream is very friendly for schema changes.

Also, Vitess adds comments to the rewritten statements that identify the primary
key affected by that statement. This allows us to produce an Update Stream (see
section below).

## Vitess Pivot

In a Vitess shard, we have a master and multiple slaves (replicas, batch,
…). Slaves are brought up by restoring a recent backup, and catching up on
replication. The Master can be re-parented to a slave at any time. We use
Statement Based Replication. The combination of all these features makes our
Pivot workflow work very well.

The Pivot operation works as follows:

* Pick a slave, take it out of service. It is not used by clients any more.
* Stop replication on the slave.
* Apply whatever schema or large data change is needed, on the slave.
* Take a backup of that slave.
* On all the other slaves, one at a time, take them out of service, restore the
  backup, catch up on replication, put them back into service.
* When all slaves are done, reparent to a slave that has applied the change.
* The old master can then be restored from a backup again, and put back into
  service.

With this process, the only guarantees we need is for the change (schema or data) to be backward compatible: the clients won’t know if they talk to a server that has applied the change yet or not. This is usually fairly easy to deal with:

* When adding a column, it cannot be referenced by the client until the pivot is
  done.
* When removing a column, the clients should not reference it before starting
  the pivot.
* A column rename is still tricky: the best way to do it is to add a new column
  with the new name in one pivot, then change the client to populate both (and
  possibly backfill the values), then change the client again to use the new
  column only, then use another pivot to remove the original column.
* A whole bunch of operations are really easy to perform though: index changes,
  optimize table, …

Note the real change is only applied to one instance. We then rely on the backup
/ restore process to propagate the change. This is a very good improvement from
letting the changes through the replication stream, where they are applied to
all hosts, not just one. Since Vitess’s backup / restore and reparent processes
are very reliable (they need to be reliable on their own, independently of this
process!), this does not add much more complexity to a running system.

However, the Pivot operations are fairly involved, and may take a long time, so
they need to be resilient and automated. We are in the process of streamlining
them, with the goal of making them completely automated.

## Update Stream

Since the replication stream also contains comments of which primary key is
affected by a change, it is possible to look at the replication stream and know
exactly what objects have changed. This Vitess feature is called Update Stream.

By subscribing to the Update Stream for a given shard, one can know what values
change. This stream can be used to create a stream of data changes (export to an
Apache Kafka for instance), or even invalidate an application layer cache.

Note: the Update Stream only reliably contains the primary key values of the
rows that have changed, not the actual values for all columns. To get these
values, it is necessary to re-query the database.

We have plans to make this Update Stream feature more consistent, very
resilient, fast, and shard-unaware.
