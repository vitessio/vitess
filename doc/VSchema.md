# VSchema User Guide

VSchema stands for Vitess Schema. In contrast to a traditional database schema that contains metadata about tables, a VSchema contains metadata about how tables are organized across keyspaces and shards. Simply put, it contains the information needed to make Vitess look like a single database server.

For example, the VSchema will contain the information about the sharding key for a sharded table. When the application issues a query with a where clause that references the key, the VSchema information will be used to route the query to the appropriate shard.

## Concepts

### Sharding model

In Vitess, a `keyspace` is sharded by `keyspace id` ranges. Each row is assigned a keyspace id, which acts like a street addres, and it determines the shard where the row lives. In some respect, one could say that the `keyspace id` is the equivalent of a NoSQL sharding key. However, there are some differences:

1. The `keyspace id` is a concept that's internal to Vitess. The application does not need to know anything about it.
2. There is no physical column that stores the actual `keyspace id`. This value is computed as needed.

This difference is significant enough that we don't refer to the keyspace id as the sharding key. We'll later introduce the concept of a Primary Vindex which more closely ressembles the NoSQL sharding key.

Mapping to a `keyspace id`, and then to a shard, gives us the flexibility to reshard the data with minimal disruption because the `keyspace id` of each row remains unchanged through the process.

### Vindex

The Sharding Key is a concept that was introduced by NoSQL datastores. It's based on the fact that there's only one access path to the data, which is the Key. However, relational databases are more rich about the data and their relationships. So, sharding a database by only designating a sharding key is often insufficient.

If one were to draw an analogy, the indexes in a database would be the equivalent of the key in a NoSQL datastore, except that databases allow you to define multiple indexes per table, and there are many types of indexes. Extending this analogy to a sharded database results in different types of cross-shard indexes. In Vitess, these are called Vindexes.

Simplistically stated, a Vindex provides a way to map a column value to a `keyspace id`. This mapping can be used to identify the location of a row. A variety of vindexes are available to choose from with different trade-offs, and you can choose one that best suits your needs.

Vindexes offer many flexibilities:

* A table can have multiple Vindexes.
* Vindexes could be NonUnique, which allows a column value to yield multiple keyspace ids.
* They could be a simple function or be based on a lookup table.
* They could be shared across multiple tables.
* Custom Vindexes can be plugged in, and Vitess will still know how to reshard using such Vindexes.

#### The Primary Vindex

The Primary Vindex is analogous to a database primary key. Every sharded table must have one defined. A Primary Vindex must be unique: given an input value, it must produce a single keyspace id. This unique mapping will be used at the time of insert to decide the target shard for a row. Conceptually, this is also equivalent to the NoSQL Sharding Key, and we often refer to the Primary Vindex as the Sharding Key.

However, there is a subtle difference: NoSQL datastores let you choose the Sharding Key, but the Sharding Scheme is generally hardcoded in the engine. In Vitess, the choice of Vindex lets you control how a column value maps to a keyspace id. In other words, a Primary Vindex in Vitess not only defines the Sharding Key, it also decides the Sharding Scheme.

Vindexes come in many varieties. Some of them can be used as Primary Vindex, and others have different purposes. The following sections will describe their properties.

#### Unique and NonUnique Vindex

A Unique Vindex is one that yields at most one keyspace id for a given input. This means that a Unique Vindex can also be designated as the Primary Vindex. If used as a Primary Vindex, the Unique Vindex should be able to produce a keyspace id for any input. Otherwise, inserts will fail.

A NonUnique Vindex is analogous to a database non-unique index. It's a secondary index for searching by an alternate where clause. An input value could yield multiple keyspace ids, and rows could be matched from multiple shards. For example, if a table has a `name` colmun that allows duplicates, you can define a cross-shard NonUnique Vindex for it, and this will let you efficiently search for users that match a certain `name`.

#### Functional and Lookup Vindex

A Functional Vindex is one where the keyspace id can be computed by just looking at the input value. In contrast, a Lookup Vindex is one that gives you the ability to create an association between the input value and a keyspace id, and recall it later when needed.

Typically, the Primary Vindex is Functional. In some cases, it's the identity function where the input value is also the kesypace id. However, one could also choose other algorithms like hashing or mod functions.

A Lookup Vindex is usually backed by a lookup table. This is analogous to the traditional database index, except that it's cross-shard. At the time of insert, the computed keyspace id of the row is stored in the lookup table against the input value of that column.

#### Shared Vindexes

Relational databases encourage normalization, which lets you split data into different tables to avoid duplication in the case of one-to-many relationships. In such cases, a key is shared between the two tables to indicate that the rows are related, aka `Foreign Key`.

In a sharded environment, it's often beneficial to keep those rows in the same shard. If a cross-shard Lookup Vindex was created for each of those tables, you'd find that the backing tables would actually be identical. In such cases, Vitess lets you share a single Lookup Vindex for multiple tables. Of these, one of them is designated as the owner: A row created on the owner table results in the lookup entry created, and a delete causes the entry to be deleted. Vitess currently does not support cascading deletes. This is because the application is capable of performing this more efficiently.

If a table has a Lookup Vindex it does not own, it's treated like a Functional Vindex: the keysapce id is instead used to verify integrity.

#### Orthogonal concepts

The previously described properties are mostly orthogonal. Combining them gives rise to the following categories:

* **Functional Unique**: This is the most common vindex because most sharding keys use this. In most storage systems, this is predefined. However, Vitess lets you choose a functional Vindex that best suits your needs. If necessary, you can also define your own. A Primary Vindex must be of this category.
* **Functional NonUnique**: This is a less common use case. Bloom filters fall in this category.
* **Lookup Unique Owned**: This is typically a lookup table which can be sharded or unsharded. However, you could choose to define your own Lookup Vindex that's backed by a different data store. If a Vindex is owned by a table, an insert causes an association to be created between the column value and the keyspace id. This implies that the keyspace id for the row is already known. Since the Primary Vindex is responsible for computing this, an Owned Lookup Vindex cannot be a Primary Vindex.
* **Lookup Unique Unowned**: For an unowned lookup Vindex, the association from column value to keyspace id was supposed to be created by the owner table and is expected to already exist at the time of insertion. This means that a Unique Lookup Vindex that's not owned by a table can be the Primary Vindex. Although this is possible, it's generally not recommend as it may not perform well during resharding. If you must, then it's recommended that you also create a column with a Functional Vindex. This could be the identity function, and since Vitess knows the keyspace id, it's capable of auto-populating the value, which can then be efficiently used during resharding.
* **Lookup NonUnique Owned/Unowned**: This is the extension of a non-unique database index. Most NonUnique Lookup Vindexes are owned. It's generally rare to see tables sharing such Vindexes.

## Advanced concepts

A Unique Vindex yields at the most one `keyspace id` per input. The reverse is not necessary. Different inputs are allowed to yield the same keyspace id. This is slightly different from a database UNIQUE index that requires input values to also be unique. One can create vindexes that are unique both ways. If so, Vitess can exploit this property to auto-fill values during inserts.

For tables that don't own a Lookup Vindex, the entries must first be created by inserting the corresponding row in the owner table. This also means that the Lookup Vindex can be the Primary Vindex for a table that does not own it. However, this is not practical for resharding. So, there's a soft enforcement against this usage.
