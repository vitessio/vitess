# Consistent lookup vindexes

## Problem

Vitess supports the concept of a lookup vindex, or what is also commonly known as a cross-shard index. It's implemented as a mysql table that maps a column value to the keyspace id. This is usually needed when someone needs to efficiently find a row using a where clause that does not contain the primary vindex.

This lookup table can be sharded or unsharded. No matter which option one chooses, the lookup row is most likely not going to be in the same shard as the keyspace id it points to.

Vitess allows the transparent population of these rows by assigning an owner table, which is the main table that requires this lookup. When a row is inserted into the main table, the lookup row for it is created in the lookup table. The lookup row is also deleted on a delete of the main row. These essentially result in distributed transactions, which require 2PC to guarantee atomicity.

There have been requests to see if we can avoid 2PC and find a solution that could still give the same guarantees. This proposal is for such a solution.

## Existing solution

The best existing solution is one where the lookup row is created immediately (autocommited) irrespective of whether the original transaction completes or not. This can generally work if values are not reused by the original table. For example, it’s a perfect use case where the column is an auto-increment. In the worst case, the lookup row will have a dangling reference. A select on such a value will result in an extra lookup on the target shard, but will result in no rows being found, as expected.

However, if we want to allow for values to be reused, then the presence of this row will prevent a new row from being created. We cannot confidently overwrite the row because it may be referencing an actual row in the main table.

At the same time, we also cannot allow deletes. This has to do with the order of operations: a delete is currently performed before the main row is deleted. In autocommit mode, this delete will be committed first. If the original transaction is later rolled back, then we have data that is not referenceable from the lookup, which is a more serious problem.

The above use case assumes that we’re dealing with a unique lookup. Similar concerns exist for non-unique lookups also.

## Approach

The proposed approach is to extend the existing behavior to better handle the case of a reuse. When we find that a lookup row already exists, we don’t know if it’s dangling or not. But it’s possible to find out: all we have to do is look for the owner row. If it doesn’t exist, then we can conclude that it’s dangling and allow for it to be overwritten. Otherwise, we can return a duplicate key error.

## Implementation

Even though the approach looks simple, the implementation gets tricky because of race conditions. For example, if we autocommitted the lookup row. A new insert can immediately find the row, but it could appear as dangling because the main row for the original transaction might not have been created yet. Even if it was created, it might not have been committed.

Similar race conditions exist with various combinations including update and delete operations.

To prevent these race conditions, we have to go through an elaborate sequence of operations, transactions, and row locking as explained below:

### Extend the VTGate transaction model

The VTGate transaction currently has a Session variable that keeps track of the current transactions it has open with the vttablets. To solve the above problem correctly, we need more fine-grained control over the order in which those transactions are committed. Additionally, even if some operations end up on the same shards, they may have to be in different transactions. To satisfy the needs that a consistent lookup needs, we need to introduce two independent transactions: they would ideally be names as prequel and sequel. However, since `sequel` can be confusing in this context, we’ll call them PreTransaction and PostTransaction. A PreTransaction is always committed first and a PostTransaction is always committed last.

We can now dive into the details of how each vindex operation will utilize the above new features:

### Create

The Create will start with a `BeginPreTransaction` (if one is not already started), and insert the lookup row. If it succeeds, we return success.

_Why do we need a separate transaction instead of autocommiting: If a new insert comes in, it will find this row, but won’t find the owner because it has yet to be created. If we tried to lock that row it will succeed with zero rows returned, and make us think that it’s ok for us to update it. If we were in a transaction, the new insert will wait till the PreTransaction is committed, at which time the owner row would have already been created._

If the insert results in a duplicate key error, then it issues a `select for update` for the lookup row as well as the owner. The locking of the owner row is performed using the main transaction (not pre or post). If this select returns a row, then we return a duplicate key error.

_Why do we need to lock the owner row: if a previous operation just committed the PreTransaction and is yet to commit the main transaction, then a simple select of the row will result in a row not found, which will make the current operation think that the row doesn’t exist._

_Why do we need to also lock the lookup row: two inserts could race and try to lock the owner row, and they will both succeed, which will cause them to both try to update the lookup row. This also means that the owner row must be updated using values obtained from the select for update._

If the row is not found, the existing row is updated to point at the new keyspace id and return success. If this operation fails with a dup key error, we return that error also.

In the above sequence when the combined transaction is committed, it’s safe to commit the PreTransaction first. The correct behavior will be observed irrespective of whether the main transaction succeeds or not.

It’s important to note that the most common code path where the insert succeeds is as efficient as it was before. Things are less efficient in the case where a row already exists, but it’s the less common case.

We can also write a watchdog process later that harvests dangling rows.

### Delete

The Delete operation will start with a `BeginPostTransaction`. The delete of lookup rows must happen after the main transaction that’s deleting the rows is committed. If the main transaction commits and the PostTransaction fails, we’ll just be in a dangling row situation that we already know how to handle.

The delete operation already selects the owned rows for an update before issuing the delete. So, there are no other corner cases to handle for the delete. Even otherwise, row locks will eventually be obtained when the actual delete is performed.

### Update

The update operation is currently implemented as a delete and insert. This means that an update that changes a vindex column will result in three transactions. However, it’s a small price to pay for the consistency gained.

Special case: the case where the update does not change the column must be detected and be converted to a no-op. Otherwise, we’ll end up in an indefinite lock wait because we’ll be trying to change the same row via two different transactions (pre and post).

## Extending the Vindex API

Apart from the pre and post-transaction changes, we’ll need to extend the vindex API: A Vindex will export an optional `SetOwnerColumns` function. If present, the vschema builder will call this function at the time of creation and supply the column names of the owner table. This will allow the `Create` to issue the `select for update` to lock the owner row.

## New vindexes

In order to remain backward compatible. We’ll implement these new vindexes under different names, likely `consistent_lookup` and `consistent_lookup_unique`.

## Corner cases to keep in mind

Non-unique lookups: the above algorithm will work equally well for non-unique lookup. The only change will be the definition of a duplicate key. In the unique case, the `from` rows are the primary key, and in the non-unique case, the entire row is the pk.

Multi-column vindexes: These will also work as long as the primary keys are correctly defined.
Within a transaction, multiple different rows could be inserted that have owned lookups. This means that the pre and post transactions are full sessions that may themselves open multiple vttablet transactions (ShardSessions).

Commit order within a session: Previously there was an implicit commit order that was dictated by the order in which DMLs were issues. This was instituted as a best-effort attempt to cause the lookup rows to be committed first. Although not guaranteed, this would have been the most common commit order. In the new scheme, since we explicitly control the commit order, there’s no need to follow this implicit order. Instead, we can commit the shard sessions in parallel.

Bulk insert: Vitess supports bulk inserts. This results in a corresponding bulk insert for lookup rows also. If the lookup insert results in a duplicate key error, the algorithm will have to fall back to individual inserts in order to implement the above algorithm for rows that yield dup keys. For simplicity, we may not do bulk insert in the initial implementation.

## What won’t work

Autocommit of the main transaction will not work. For the lookup to be consistent, we have to start two transactions, create the rows in both transactions and commit in a specific order. If the lookup transaction was created with the row inserted without a commit, and if the main row is autocommitted, then the commit of the lookup row can later fail, which will result queries returning incorrect results.

The feature should be used only with 2PC disabled. Of course, you don’t need this feature if you’re using 2PC already.