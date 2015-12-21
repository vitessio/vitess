This document defines common Vitess concepts and terminology.

## Keyspace

A *keyspace* is a logical database. In its simplest form, it maps directly
to a MySQL database name, but it can also map to multiple MySQL databases.

Reading data from a keyspace is like reading from a MySQL database. However,
depending on the consistency requirements of the read operation, Vitess
might fetch the data from a master database or from a replica. By routing
each query to the appropriate database, Vitess allows your code to be
structured as if it were reading from a single MySQL database.

When a database is
[sharded](http://en.wikipedia.org/wiki/Shard_(database_architecture)),
a keyspace maps to multiple MySQL databases. In that case, a read operation
fetches the necessary data from one of the shards.

## Keyspace id
A *keyspace ID* (keyspace_id) is a column that identifies the primary entity
of a keyspace. For example, a keyspace might identify a user, a video, a
product, or a purchase. The keyspace ID(s) that you use in your database
depend on the type of data that you are storing.

To shard a database, all of the tables in a keyspace must contain a
<code>keyspace_id</code> column. Vitess sharding ensures that all rows that have
a common keyspace ID are always placed in the same shard.

The <code>keyspace_id</code> column does not need to be in the primary key or even an index
for the table. The Vitess tools will either walk through all the rows (to copy
data during dynamic resharding for instance), or not use the keyspace_id.

If you do not intend to shard a database, you do not have to designated a
keyspace ID. However, you must designate a keyspace ID if you decide to
shard a currently unsharded database.

A keyspace ID can be an unsigned number or a binary character column
(<code>unsigned bigint</code> or <code>varbinary</code> in MySQL tables).
Other data types are not allowed due to ambiguous equality or inequality rules.

<div style="display:none">
TODO: keyspace ID rules must be solidified once VTGate features are finalized.
</div>

## Shard

A *shard* is a division within a keyspace. A shard typically contains one MySQL master and many MySQL slaves. 

Each MySQL instance within a shard has the same data or should have the same data, excepting some replication lag. The slaves can serve read-only traffic (with eventual consistency guarantees), execute long-running data analysis tools, or perform administrative tasks (backups, restore, diffs, etc.).

A keyspace that does not use sharding effectively has one shard. Vitess names the shard <code>0</code> by convention. When sharded a keyspace has <code>N</code> shards with non-overlapping data. Usually, <code>N</code> is a power of 2.

Vitess supports [dynamic resharding](http://vitess.io/user-guide/sharding.html#resharding), in which one shard is split into multiple shards for instance. During dynamic resharding, the data in the source shard is split into the destination shards. Then the source shard is deleted.

## Tablet

A *tablet* is a single server that runs:

* a MySQL instance
* a <code>vttablet</code> instance
* (optionally) a local row cache instance
* any other database-specific process necessary for operational purposes

A tablet has a type. Common types are listed below:

* **type**
  * master - The read-write database that is the MySQL master
  * replica - A MySQL slave that serves read-only traffic with guaranteed low replication latency
  * rdonly - A MySQL slave that serves read-only traffic for backend processing jobs, such as MapReduce-type jobs. This type of table does not have guaranteed replication latency.
  * spare - A MySQL slave that is not currently in use.

There are several other tablet types that each serve a specific purpose, including <code>experimental</code>, <code>schema</code>, <code>backup</code>, <code>restore</code>, <code>worker</code>.

Only <code>master</code>, <code>replica</code>, and <code>rdonly</code> tablets are included in the [serving graph](#serving-graph).

<div style="display:none">
TODO: Add pointer to complete list of types and explain how to update type?
</div>

## Shard graph

The *shard graph* maps keyspace IDs to the shards for that keyspace. The shard graph ensures that any given keyspace ID maps to exactly one shard.

Vitess uses range-based sharding. This basically means that the shard graph specifies a per-keyspace list of non-intersecting ranges that cover all possible values of a keyspace ID. As such, Vitess uses a simple, in-memory lookup to identify the appropriate shard for SQL queries.

In general, database sharding is most effective when the assigned [keyspace IDs](#keyspace-id) are evenly distributed among shards. With this in mind, a best practice is for keyspace IDs to use hashed key values rather than sequentially incrementing key values. This helps to ensure that assigned keyspace IDs are distributed more randomly among shards.

For example, an application that uses an incrementing UserID as its primary key for user records should use a hashed version of that ID as a keyspace ID. All data related to a particular user would share that keyspace ID and, thus, would be on the same shard.

## Replication graph

The *replication graph* identifies the relationships between master
databases and their respective replicas. During a master failover,
the replication graph enables Vitess to point all existing replicas
to a newly designated master database so that replication can continue.

## Serving graph

The *serving graph* represents the list of servers that are available
to serve queries. Vitess derives the serving graph from the
[shard](#shard-graph) and [replication](#replication-graph) graphs.

[VTGate](/overview/#vtgate) (or another smart client) queries the
serving graph to determine which servers it can send queries to.

## Topology Service

The *[Topology Service](https://github.com/youtube/vitess/blob/master/doc/TopologyService.md)* is a set of backend processes running on different servers. Those servers store topology data and provide a locking service.

The Vitess team does not design or maintain topology servers. The implementation in the Vitess source code tree uses ZooKeeper (Apache) as the locking service. On [Kubernetes](/getting-started/), Vitess uses etcd (CoreOS) as the locking service.

The topology service exists for several reasons:

* It stores rules that determine where data is located.
* It ensures that write operations execute successfully.
* It enables Vitess to transparently handle a data center (cell) being unavailable.
* It enables a data center to be taken offline and rebuilt as a unit.

A Vitess implementation has one global instance of the topology service and one local instance of the topology service in each data center, or cell. Vitess clients are designed to only need access to the local serving graph. As such, clients only need the local instance of the topology service to be constantly available.

* **Global instance**<br>
  The global instance stores global data that does not change frequently. Specifically, it contains data about keyspaces and shards as well as the master alias of the replication graph.<br><br>
  The global instance is used for some operations, including reparenting and resharding. By design, the global topology server is not used a lot.

* **Local instance**<br>
  Each local instance contains information about information specific to the cell where it is located. Specifically, it contains data about tablets in the cell, the serving graph for that cell, and the master-slave map for MySQL instances in that cell.<br><br>
  The local topology server must be available for Vitess to serve data.

<div style="display:none">
  To ensure reliability, the topology service has multiple server processes running on different servers. Those servers elect a master and perform chorum writes. In ZooKeeper, for a write to succeed, more than half of the servers must acknowledge it. Thus, a typical ZooKeeper configuration consists of either three or five servers, where two (out of three) or three (out of five) servers must agree for a write operation to succeed.
The instance is the set of servers providing topology services. So, in a Vitess implementation using ZooKeeper, the global and local instances likely consist of three or five servers apiece.
  To be reliable, the global instance needs to have server processes spread across all regions and cells. Read-only replicas of the global instance can be maintained in each data center (cell).
</div>

## Cell (Data Center)

A *cell* is a group of servers and network infrastructure collocated in an area. It is typically either a full data center or a subset of a data center. Vitess gracefully handles cell-level failures, such as when a cell is cut off the network.

Each cell in a Vitess implementation has a [local topology server](#topology-service), which is hosted in that cell. The topology server contains most of the information about the Vitess tablets in its cell. This enables a cell to be taken down and rebuilt as a unit.

Vitess limits cross-cell traffic for both data and metadata. While it is useful to also have the ability to route client traffic to individual cells, Vitess does not provide that feature.
