# Concepts
We need to introduce some common terminolgies that are used in Vitess:
### Keyspace
A keyspace is a logical database.
In its simplest form, it directly maps to a MySQL database name.
When you read data from a keyspace, it is as if you read from a MySQL database.
Vitess could fetch that data from a master or a replica depending
on the consistency requirements of a read.

When a database gets [sharded](http://en.wikipedia.org/wiki/Shard_(database_architecture),
a keyspace maps to multiple MySQL databases,
and the necessary data is fetched from one of the shards.
