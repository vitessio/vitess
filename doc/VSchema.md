# VSchema User Guide

VSchema stands for Vitess Schema. In contrast to a traditional database schema that contains metadata about tables, a VSchema contains metadata about how tables are organized across keyspaces and shards. Simply put, it contains the information needed to make Vitess look like a single database server.

For example, the VSchema will contain the information about the sharding key for a sharded table. When the application issues a query with a where clause that references the key, the VSchema information will be used to route the query to the appropriate shard.

## Concepts

### Vindex

The Sharding Key is a concept that was introduced by NoSQL datastores that only provide key-based access to the data. However, relational databases are more rich about data relationships. So, sharding a database by only designating a sharding is often insufficient.

If one were to draw an analogy, the indexes in a database would be the equivalent of the key in a NoSQL datastore, except that databases allow you to define multiple indexes per table. If this analogy is extended to a sharded database, we end up with cross-shard indexes. In Vitess, this is called a Vindex.

#### The Primary Vindex

In a relational database, all tables must have a primary key. The main purpose of the primary key is to uniquely identify a row and its location in storage.

In Vitess, every sharded table must have a Primary Vindex. Just like the primary key, this can be used to uniquely identify the shard where a row lives. Conceptually, this is equivalent to the NoSQL Sharding Key, and we often refer to the Primary Vindex as the Sharding Key. Functionally, a Vindex is more versatile. The details are be covered below.

#### Unique and NonUnique Vindex

Relational databases allow you to create additional indexes that may or may not be unique. Vindexes provide the same flexibility, but it works across shards. Beyond the Primary Vindex (that must be unique), you can define Vindexes against other columns of a table. Those may or may not be unique.

If a table has multiple Unique Vindexes, only one of them can be the Primary: At the time of insert, the Primary Vindex is used to compute the `keyspace id`, which determines the target shard. This id is then validated against the rest of the Vindexes to make sure that they also map to this id; A row has one and only one `keyspace id`. In the case of a NonUnique Vindex, at least one of the values must yield the `keyspace id` of the row.

While performing a select, if one of the vindex columns is in the where clause, its mapping function is used to compute the keyspace ids, and the query is routed to the shards that contain those ids.

#### Functional and Lookup Vindex

Vindexes can be Functional or Lookup. For this concept, there is no direct analogy with respect to a relational database.

A Functional Vindex is one where the keyspace id can be computed by just looking at the input value. In contrast, a Lookup Vindex is one where the keyspace id can only be computed based on a stored association between the value and the keyspace id.

Typically, the Primary Vindex is Functional. In some cases, it's the identity function where the input value is also the kesypace id. However, one could also choose other algorithms like hashing or mod functions. At the time of insert, the keyspace id will be computed based on the mapping function and this allows us to route the insert to the appropriate shard. The separation of the mapping function from the sharding scheme makes Vitess uniquely versatile.

A Lookup Vindex can be defined against a different column than the Primary Vindex, and is usually backed by a lookup table. This is analogous to the traditional database index, except that it's cross-shard. At the time of insert, the computed keyspace id of the row is stored in the lookup table against the input value of that column. If a select is issued using the secondary index column as the where clause, then Vitess will first read the lookup table to identify the keyspace id and then route the query to the appropriate shard.

#### Orthogonal concepts

A Vindex being Unique or NonUnique is orthogonal to its being Functional or Lookup. This essentially yields four types of vindexes:

* **Functional Unique**: This is the most common vindex because most sharding keys use this. In most storage systems, this is predefined. However, Vitess lets you choose a functional Vindex that best suits your needs. If necessary, you can also define your own. A Primary Vindex must be of this category.
* **Functional NonUnique**: This is a less common use case. Bloom filters fall in this category.
* **Lookup Unique**: This is typically a lookup table which can be sharded or unsharded. However, you could choose to define your own Lookup Vindex that's backed by a different data store.
* **Lookup NonUnique**: This is the extension of a non-unique database index.

#### Shared Vindexes

Relational databases encourage normalization, which lets you split data into different tables to avoid duplication in the case of one-to-many relationships. In such cases, a key is shared between the two tables to indicate that the rows are related, aka `Foreign Key`.

In a sharded environment, it's often beneficial to keep those rows in the same shard. If a cross-shard Lookup Vindex was created for each of those tables, you'd find that the backing tables would actually be identical. In such cases, Vitess lets you share a single Lookup Vindex for multiple tables. Of these, one of them is designated as the owner: A row created on the owner table results in the lookup entry created, and a delete causes the entry to be deleted. Vitess currently does not support cascading deletes. This is because the application is capable of performing this more efficiently.

## Advanced concepts

A Unique Vindex yields at the most one `keyspace id` per input. The reverse is not necessary. Different inputs are allowed to yield the same keyspace id. This is slightly different from a database UNIQUE index that requires input values to also be unique. One can create vindexes that are unique both ways. If so, Vitess can exploit this property to auto-fill values during inserts.

For tables that don't own a Lookup Vindex, the entries must first be created by inserting the corresponding row in the owner table. This also means that the Lookup Vindex can be the Primary Vindex for a table that does not own it. However, this is not practical for resharding. So, there's a soft enforcement against this usage.
