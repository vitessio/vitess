# VSchema User Guide

VSchema stands for Vitess Schema. In contrast to a traditional database schema that contains metadata about tables, a VSchema contains metadata about how tables are organized across keyspaces and shards. Simply put, it contains the information needed to make Vitess look like a single database server.

For example, the VSchema will contain the information about the sharding key for a sharded table. When the application issues a query with a where clause that references the key, the VSchema information will be used to route the query to the appropriate shard.

## Sharding model

In Vitess, a `keyspace` is sharded by `keyspace id` ranges. Each row is assigned a keyspace id, which acts like a street addres, and it determines the shard where the row lives. In some respect, one could say that the `keyspace id` is the equivalent of a NoSQL sharding key. However, there are some differences:

1. The `keyspace id` is a concept that's internal to Vitess. The application does not need to know anything about it.
2. There is no physical column that stores the actual `keyspace id`. This value is computed as needed.

This difference is significant enough that we don't refer to the keyspace id as the sharding key. We'll later introduce the concept of a Primary Vindex which more closely ressembles the NoSQL sharding key.

Mapping to a `keyspace id`, and then to a shard, gives us the flexibility to reshard the data with minimal disruption because the `keyspace id` of each row remains unchanged through the process.

## Vindex

The Sharding Key is a concept that was introduced by NoSQL datastores. It's based on the fact that there's only one access path to the data, which is the Key. However, relational databases are more rich about the data and their relationships. So, sharding a database by only designating a sharding key is often insufficient.

If one were to draw an analogy, the indexes in a database would be the equivalent of the key in a NoSQL datastore, except that databases allow you to define multiple indexes per table, and there are many types of indexes. Extending this analogy to a sharded database results in different types of cross-shard indexes. In Vitess, these are called Vindexes.

Simplistically stated, a Vindex provides a way to map a column value to a `keyspace id`. This mapping can be used to identify the location of a row. A variety of vindexes are available to choose from with different trade-offs, and you can choose one that best suits your needs.

Vindexes offer many flexibilities:

* A table can have multiple Vindexes.
* Vindexes could be NonUnique, which allows a column value to yield multiple keyspace ids.
* They could be a simple function or be based on a lookup table.
* They could be shared across multiple tables.
* Custom Vindexes can be plugged in, and Vitess will still know how to reshard using such Vindexes.

### The Primary Vindex

The Primary Vindex is analogous to a database primary key. Every sharded table must have one defined. A Primary Vindex must be unique: given an input value, it must produce a single keyspace id. This unique mapping will be used at the time of insert to decide the target shard for a row. Conceptually, this is also equivalent to the NoSQL Sharding Key, and we often refer to the Primary Vindex as the Sharding Key.

However, there is a subtle difference: NoSQL datastores let you choose the Sharding Key, but the Sharding Scheme is generally hardcoded in the engine. In Vitess, the choice of Vindex lets you control how a column value maps to a keyspace id. In other words, a Primary Vindex in Vitess not only defines the Sharding Key, it also decides the Sharding Scheme.

Vindexes come in many varieties. Some of them can be used as Primary Vindex, and others have different purposes. The following sections will describe their properties.

### Unique and NonUnique Vindex

A Unique Vindex is one that yields at most one keyspace id for a given input. Knowing that a Vindex is Unique is useful because VTGate can push down some complex queries into VTTablet if it knows that the scope of that query cannot exceed a shard. Uniqueness is also a prerequisite for a Vindex to be used as Primary Vindex.

A NonUnique Vindex is analogous to a database non-unique index. It's a secondary index for searching by an alternate where clause. An input value could yield multiple keyspace ids, and rows could be matched from multiple shards. For example, if a table has a `name` colmun that allows duplicates, you can define a cross-shard NonUnique Vindex for it, and this will let you efficiently search for users that match a certain `name`.

### Functional and Lookup Vindex

A Functional Vindex is one where the column value to keyspace id mapping is pre-established, typically through an algorithmic function. In contrast, a Lookup Vindex is one that gives you the ability to create an association between a value and a keyspace id, and recall it later when needed.

Typically, the Primary Vindex is Functional. In some cases, it's the identity function where the input value yields itself as the kesypace id. However, one could also choose other algorithms like hashing or mod functions.

A Lookup Vindex is usually backed by a lookup table. This is analogous to the traditional database index, except that it's cross-shard. At the time of insert, the computed keyspace id of the row is stored in the lookup table against the column value.

### Shared Vindexes

Relational databases encourage normalization, which lets you split data into different tables to avoid duplication in the case of one-to-many relationships. In such cases, a key is shared between the two tables to indicate that the rows are related, aka `Foreign Key`.

In a sharded environment, it's often beneficial to keep those rows in the same shard. If a Lookup Vindex was created on the foreign key column of each of those tables, you'd find that the backing tables would actually be identical. In such cases, Vitess lets you share a single Lookup Vindex for multiple tables. Of these, one of them is designated as the owner, which is responsible for creating and deleting these associations. The other tables just reuse these associations.

Caveat: If you delete a row from the owner table, Vitess will not perform cascading deletes. This is mainly for efficiency reasons; The application is likely capable of doing this more efficiently.

Functional Vindexes can be also be shared. However, there's no concept of ownership because the column to keyspace id mapping is pre-established.

### Orthogonality

The previously described properties are mostly orthogonal. Combining them gives rise to the following valid categories:

* **Functional Unique**: This is the most popular category because it's the one best suited to be a Primary Vindex.
* **Functional NonUnique**: There are currently no use cases that need this category.
* **Lookup Unique Owned**: This gets used for optimizing high QPS queries that don't use the Primary Vindex columns in their where clause. There is a price to pay: You incur an extra write to the lookup table for insert and delete operations, and an extra lookup for read operations. However, it's worth it if you don't want these high QPS queries to hit all shards.
* **Lookup Unique Unowned**: This category is used as an optimization as described in the Shared Vindexes section.
* **Lookup NonUnique Owned**: This gets used for high QPS queries on columns that are non-unique.
* **Lookup NonUnique Unowned**: You would rarely have to use this category because it's unlikely that you'll be using a column as foreign key that's not unique within a shard. But it's theoretically possible.

Of the above categories, `Functional Unique` and `Lookup Unique Unowned` Vindexes can be Primary. This is because those are the only ones that are unique and have the column to keyspace id mapping pre-established. This is required because the Primary Vindex is responsible for assigning the keyspace id for a row when it's created. However, it's generally not recommended to use a Lookup vindex as Primary because it's too slow for resharding.

*If absolutely unavoidable, you can use a Lookup Vindex as Primary. In such cases, it's recommended that you add a `keyspace id` column to such tables. While resharding, Vitess can use that column to efficiently compute the target shard. You can even configure Vitess to auto-populate that column on inserts. This is done using the reverse map feature explained below.*

### How vindexes are used

#### Cost

Vindexes have costs. For routing a query, the Vindex with the lowest cost is chosen. The current costs are:

Vindex Type | Cost
----------- | ----
Indentity Vindex | 0
Functional Vindex | 1
Lookup Vindex | 2

While analyzing a query, if multiple vindexes qualify, the one with the lowest cost is chosen to determine the route.

#### Select

In the case of a simple select, Vitess scans the where clause to match references to Vindex columns and chooses the best one to use. If there's no match and the query is simple wihtout complex constructs like aggreates, etc, it's sent to all shards.

Vitess can handle more complex queries. For now, you can refer to the [design doc](https://github.com/youtube/vitess/blob/master/doc/V3HighLevelDesign.md) on how it handles them.

#### Insert

* The Primary Vindex is used to generate a keyspace id.
* The keyspace id is validated against the rest of the Vindexes on the table. There must exist a mapping from the column value to the keyspace id.
* If a column value was not provided for a Vindex and the Vindex is capable of reverse mapping a keyspace id to an input value, that function is used to auto-fill the column. If there is no reverse map, it's an error.

#### Update

The where clause is used to route the update. Changing the value of a Vindex column is unsupported because this may result in a row being migrated from one shard to another.

#### Delete

If the table owns lookup vindexes, then the rows to be deleted are first read and the associated Vindex entries are deleted. Following this, DML is routed according to the where clause.
