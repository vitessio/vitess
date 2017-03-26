# VSchema User Guide

VSchema stands for Vitess Schema. A traditional database has a schema that contains metadata about tables. A VSchema contains metadata about how tables are organized across keyspaces and shards. Simply put, it contains the information needed to make Vitess look like a single database server.

For example, the VSchema will contain the information about the sharding key for a sharded table. When the application issues a query with a where clause that references the key, the VSchema information will be used to route the query to the appropriate shard.

## Concepts

### Vindex

The Sharding Key is a concept that was introduced by NoSQL datastores that only provide key-based access to the data. However, relational databases are more rich about data relationships. So, sharding a database by only designating a sharding is often insufficient.

If one were to draw an analogy, the indexes in a database would be the equivalent of the key in a NoSQL datastore, except that databases allow you to define multiple indexes per table. If this analogy is extended to a sharded database, we end up with cross-shard indexes. In Vitess, this is called a Vindex.

#### The Primary Vindex

In a relational database, all tables must have a primary key. If the app doesn't specify one, the database engine usually creates a hidden primary key. The primary key can be used to uniquely identify a row and its location in storage.

In Vitess, every sharded table must have a Primary Vindex. Just like the primary key, this can be used to uniquely identify the shard where a row lives. Conceptually, this is equivalent to the NoSQL Sharding Key, and we often refer to the Primary Vindex as the Sharding Key. Functionally, a Primary Vindex is more versatile. Those flexibilities will be covered later.

#### Unique and NonUnique Vindex

Relational databases allow you to create additional indexes that may or may not be unique. Vindexes provide the same flexibility. This means that you can designage other columns to have a Unique Vindex beyond the Primary Vindex.

During creation of a row, a database uses the primary key to decide where to store the row. In some cases, the primary key itself acts as the street address. In other cases, there is a mapping from the primary key to the physical location of the row. For Vitess, this street address is called the `keyspace id`. The Primary Vindex is mapped to a keyspace id and this mapping function is user-configurable. Just like the street address, a row can have only one keyspace id. If a table has another unique Vindex, then they must yield the same keyspace id for any given row. In database terms, this is called a constraint.

If a Vindex is NonUniqe, an input value can yield multiple keyspace ids. At the time of creation of a row, Vitess ensures that at least one of those values corresponds to the row being created. In other words, the Primary Vindex is used to compute the keyspace id, and the id is validated against the rest of the vindexes on the table.

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
* **Lookup Unique**: This is typically a lookup table. However, you could choose to define your own Lookup Vindex that's backed by a different data store. Although not recommended, it's theoretically possible to use a Lookup Unique Vindex as a Primary Vindex. This is explained in the next section.
* **Lookup NonUnique**: This is the extension of a non-unique database index.

#### Shared Vindexes

Relational databases encourage normalization, which lets you to split data into different tables to avoid duplication in the case of one-to-many relationships. In such cases, a key is shared between the two tables to indicate that the rows are related, aka `Foreign Key`.

In a sharded environment, it's often beneficial to keep those rows together. If a cross-shard Lookup Vindex was created for each of those tables, you'd find that the backing tables would actually be identical. In such cases, Vitess lets you share a single Lookup Vindex for multiple tables. Of these, one of them is designated as the owner: A row created on the owner table results in the lookup entry created, and a delete causes the entry to be deleted. Vitess currently does not support cascading deletes. This is because the application is capable of performing this more efficiently.

For tables that don't own a Lookup Vindex, the entries must first be created by inserting the corresponding row in the owner table. This also means that the Lookup Vindex can be the Primary Vindex for a table that does not own it. However, this is not practical for resharding. So, there's a soft enforcement against this usage.
