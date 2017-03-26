# VSchema User Guide

VSchema stands for Vitess Schema. A traditional database has a schema that contains metadata about tables. A VSchema contains metadata about how tables are organized across keyspaces and shards. Simply put, it contains the information needed to make Vitess look like a single database server.

For example, the VSchema will contain the information about the sharding key for a sharded table. When the application issues a query with a where clause that references the key, the VSchema information will be used to route the query to the appropriate shard.

## Concepts

### Vindex

The Sharding Key is a concept that was introduced by NoSQL datastores that only provide key-based access to the data. However, relational databases are more rich about data relationships. So, sharding a database by only designating a sharding is often insufficient.

If one were to draw an analogy, the indexes in a database would be the equivalent of the key in a NoSQL datastore, except that databases allow you to define multiple indexes per table. If this analogy is extended to a sharded database, we end up with cross-shard indexes. In Vitess, this is called a Vindex.

#### The Primary Vindex

All relational tables must have a primary key. If the app doesn't specify one, the database engine usually creates a hidden primary key. The primary key can be used to uniquely identify a row and its location in storage.

In Vitess, every sharded table must have a Primary Vindex. Just like the primary key, this can be used to uniquely identify the shard where a row lives. For all practical purposes, this is the Sharding Key of the table. Functionally, a Vitess Primary Vindex is more versatile. Those flexibilities will be covered later.

Every sharded table must have a Primary Vindex.

#### Unique and NonUnique Vindex

Relational databases allow you to create additional indexes that may or may not be unique. Vindexes provide the same flexibility. You can create unique Vindexes beyond the primary Vindex.

During creation of a row, a database uses the primary key to decide where to store the row. In some cases, the primary key itself acts as the street address. In other cases, there is a mapping from the primary key to the physical location of the row. For Vitess, this street address is called the `keyspace id`. The Primary Vindex is mapped to a keyspace id and this mapping function is user-configurable. Just like the street address, a row can have only one keyspace id. If a table has another unique Vindex, then they must yield the same keyspace id for any given row. In database terms, this is called a constraint.

If a Vindex is NonUniqe, and input value can yield multiple keyspace ids. At the time of creation of a row, Vitess ensures that at least one of those values corresponds to the row being created. In other words, the Primary Vindex is used to compute the keyspace id, and the id is validated against the rest of the vindexes on the table.

While performing a select, if one of the vindex columns is in the where clause, the vindex is used to compute the keyspace ids, and the query is routed to the shards that contain it.
