# Proposal: Expose Vindexes as Tables

This proposal is in response to issues like [#3076](https://github.com/youtube/vitess/issues/3076). Users want the ability to perform vindex functions independently from the tables they are associated with.

## Design

One can think of a vindex as a table that looks like this:

```
create my_vdx(id int, val varbinary(255)) // id can be of any type.
```

Looking at the vindex interface defined [here](https://github.com/youtube/vitess/blob/master/go/vt/vtgate/vindexes/vindex.go), we can come up with SQL syntax that represents them:
* Map: `selec val from my_vdx where id = :id`
* Create: `insert into my_vdx values(:id, "val)`.
* Delete: `delete from my_vdx where id = :id and val :val`.
* Verify: `select 1 from my_vdx where id = :id and val = :val`
* ReverseMap: `select id from my_vdx where val = :val`

More SQL constructs can be supported. For example, we can allow `IN` clauses and multi-value constructs in the above cases. We can also add additional convenience functions like `vt_shard(val)` that will map a keyspace_id to a shard.

The advantage of this approach is that we don't need to build new APIs to support these functionalities.

## Issues
If vindexes are seen as tables, there are issues about name collisions. This can be resolved a few ways:
1. Make the vindex names be part of the table name space in a keyspace. This will allow you to address a vindex as `keyspace.my_vdx`. However, we still have a problem if a table of that same name exists. In such situations, the table will hide the vindex. This is not a big issue because a vindex is easy to rename because it does not affect anything beyond the vschema itself.
2. Make vindexes part of a special keyspce `vindexes`. The problem with this approach is that there can be name collisions between vindexes of different keyspaces. This is a problem that's harder to address.
3. Provide a special syntax like `vindex(keyspace.my_vdx)`. This feels like an overkill and aesthetically unpleasing.

Given the above considerations, we can go with option 1, which seems to have the best trade-offs.

## Implementation
