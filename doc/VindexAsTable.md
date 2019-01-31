# Proposal: Expose Vindexes as Tables

This proposal is in response to issues like [#3076](https://github.com/vitessio/vitess/issues/3076). Users want the ability to perform vindex functions independently from the tables they are associated with.

## Design

One can think of a vindex as a table that looks like this:

```
create my_vdx(id int, keyspace_id varbinary(255)) // id can be of any type.
```

Looking at the vindex interface defined [here](https://github.com/vitessio/vitess/blob/master/go/vt/vtgate/vindexes/vindex.go), we can come up with SQL syntax that represents them:
* Map: `select id, keyspace_id from my_vdx where id = :id`.
* Create: `insert into my_vdx values(:id, :keyspace_id)`.
* Delete: `delete from my_vdx where id = :id and keyspace_id :keyspace_id`.
* Verify: `select 1 from my_vdx where id = :id and keyspace_id = :keyspace_id`.
* ReverseMap: `select id from my_vdx where keyspace_id = :keyspace_id`.

The supported SQL syntax will be limited because of the limited functions that vindexes can perform. However, we can expand this meaningully. For example, we can allow `IN` clauses and multi-value constructs in the above cases. We can also add additional convenience functions like `vt_shard(keyspace_id)` that will map a keyspace_id to a shard.

The advantage of this approach is that we don't need to build new APIs to support these functionalities.

## Issues
If vindexes are seen as tables, there are issues about name collisions. This can be resolved a few ways:
1. Make the vindex names be part of the table name space in a keyspace. This will allow the application to address a vindex as `keyspace.my_vdx`. However, we still have a problem if a table of that same name exists. In such situations, the table will hide the vindex. This is not a big issue because a vindex is easy to rename because it does not affect anything beyond the vschema itself.
2. Make vindexes part of a special keyspce `vindexes`. The problem with this approach is that there can be name collisions between vindexes of different keyspaces. This is a problem that's harder to address.
3. Provide a special syntax like `vindex(keyspace.my_vdx)`. This feels like overkill and aesthetically unpleasing.

Given the above considerations, we can go with option 1, which seems to have the best trade-offs.

## Implementation

A new `engine` primitive `vindexFunc` will be created to perform vindex functions. There will be an opcode for each of those functions. The required inputs will depend on the opcode. At the time of execution, the primitive will produce a `*sqltypes.Result` just like any other primitive.

For `select` statements, we can follow the V3 design principles, there will be a mirror `vindexFunc` under planbuilder that will build the engine primitive. While analyzing the `FROM` clause in `from.go`, if the table is identified as a vindex, we create a `vindexFunc` instead of a `route`. This will also cause corresponding entries to be created in the symbol table. The opcode itself will be unknown at this point.

While analyzing the `WHERE` clause, if the primitive is a `vindexFunc`, we look for the three possible combinations listed above. Once they're matched, we can assign the corresponding opcode.

While analyzing the `SELECT` expression list, we verify that the user has specified expressions as required by each opcode.

Joins and subqueries will not be allowed, at least for now.

For `INSERT` and `DELETE` the primitive will be built using the DML analysis code path.

## Plan

The first implementation will only address the `Map` functionality. This will put the framework in place for us to iterate on the rest of the features.
