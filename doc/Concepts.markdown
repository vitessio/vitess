# Concepts
We need to introduce some common terminolgies that are used in Vitess:
### Keyspace
A keyspace is a logical database.
In its simplest form, it directly maps to a MySQL database name.
When you read data from a keyspace, it is as if you read from a MySQL database.
Vitess could fetch that data from a master or a replica depending
on the consistency requirements of the read.

When a database gets [sharded](http://en.wikipedia.org/wiki/Shard_(database_architecture)),
a keyspace maps to multiple MySQL databases,
and the necessary data is fetched from one of the shards.
Reading from a keyspace gives you the impression that the data is read from
a single MySQL database.

### Keyspace id
A keyspace id (keyspace_id) is a column that is used to identify a primary entity
of a keyspace, like user, video, order, etc.
In order to shard a database, all tables in a keyspace need to
contain a keyspace id column.
Vitess sharding ensures that all rows that have a common keyspace id are
always together.

It's recommended, but not necessary, that the keyspace id be the leading primary
key column of all tables in a keyspace.

If you do not intend to shard a database, you do not have to
designate a keyspace_id.
However, you'll be required to designate a keyspace_id
if you decide to shard a currently unsharded database.

A keyspace_id can be an unsigned number or a binary character column.
Other data types are not allowed because of ambiguous equality or inequality rules.

TODO: The keyspace id rules need to be solidified once VTGate features are finalized.

### Shard graph
The shard graph defines how a keyspace has been sharded. It's basically a per-keyspace
list of non-intersecting ranges that cover all possible values a keyspace id can cover.
In other words, any given keypsace id is guaranteed to map to one and only one
shard of the shard graph.

We are going with range based sharding.
The main advantage of this scheme is that the shard map is a simple in-memory lookup.
The downside of this scheme is that it creates hot-spots for sequentially increasing keys.
In such cases, we recommend that the application hash the keys so they
distribute more randomly.

### Replication graph
The replication graph represents the relationships between the master
databases and their respective replicas.
This data is particularly useful during a master failover.
Once a new master has been designated, all existing replicas have to
repointed to the new master so that replication can resume.

### Serving graph
The serving graph is derived from the shard and replication graph.
It represens the list of active servers that are available to serve
queries.
VTGate (or smart clients) query the serving graph to find out which servers
they are allowed to send queries to.
