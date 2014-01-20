# Concepts
We need to introduce some common terminolgies that are used in Vitess:
### Keyspace
A keyspace is a logical database.
In its simplest form, it directly maps to a MySQL database name.
When you read data from a keyspace, it is as if you read from a MySQL database.
Vitess could fetch that data from a master or a replica depending
on the consistency requirements of a read.

When a database gets [sharded](http://en.wikipedia.org/wiki/Shard_(database_architecture\)),
a keyspace maps to multiple MySQL databases,
and the necessary data is fetched from one of the shards.
Reading from a keyspace gives you the impression that the data is read from
a single MySQL database.

### Keyspace id
A keyspace id (keyspace_id) is a column that is used to identify a primary entity
of a keyspace, like user, video, or items.
In order to shard a database, all tables in a keyspace need to
contain a keyspace id column.
Vitess sharding ensures that all rows that have a common keyspace id are
always together.

It's recommended, but not necessary, that the keyspace id be the leading primary
key column of all tables in a keyspace. If you do not intend to shard a database,
you do not have to designate a keyspace id.

A keyspace id can be an unsigned number or a binary character column.
Other data types are not allowed because of ambiguous equality or inequality rules.

TODO: The keyspace id rules need to be solidified once VTGate features are finalized.

### Shard graph
The shard graph defines how a keyspace has been sharded. It's basically a list
of non-intersecting ranges that cover all possible values a keyspace id can cover.
In other words, any given keypsace id is guaranteed to map to one and only one
shard of the shard graph.
