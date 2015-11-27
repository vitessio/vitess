# VTGate V3 features

## Overview
Historically, Vitess was built from underneath YouTube. This required us to take an iterative approach, which resulted in many versions:
* V0: This version had no VTGate. The application was expected to talk directly to the tablet servers. So, it had to know the sharding scheme, topology, etc.
* V1: This was the first version of VTGate. In this version, the app only needed to know the number of shards, and how to map the sharding key to the correct shard. The rest was done by VTGate. In this version, the app was still exposed to resharding events.
* V2: In this version, the keyspace id was required instead of the shard. This allowed the app to be agnostic of the number of shards.

With V3, the app does not need to specify any routing info. It just sends the query to VTGate as if it's a single database. Apart from simplifying the API, we gain some additional benefits from this approach:
* We can build database compliant drivers for each client language, which will allow us to integrate with third party tools that work with such drivers.
* V3 can aspire to satisfy the full SQL syntax. This means that it will be able to perform cross-shard joins, aggregations and sorting.
* Easier migration: an application that was written to use a single database can be trivially changed to use Vitess, and then the database can be scaled from underneath without changing much of the app.

## Feature set
The [V3 design](https://github.com/youtube/vitess/blob/master/doc/VTGateV3.md) is quite elaborate. If necessary, it will allow you to plug in custom indexes and sharding schemes. However, it comes equipped with some pre-cooked recipes that satisfy the immediate needs of the real-world:

### Knowing where tables are
As your database grows, you will not only be sharding it, you will also be splitting it vertically by migrating tables from one database to another. V3 will be able to keep track of this. The app will only have to refer a table by name, and VTGate will figure out how to route the query to the correct database.

The vitess workflow also ensures that such migrations are done transparently with virtually no downtime.

### Sharding schemes
At its core, vitess uses range-based sharding, where the sharding column is typically a number or a varbinary. However, allowing data to be accessed only by the sharding key severely limits the flexibility of an application. V3 comes with a set of new indexing schemes that are built on top of range-based sharding.

#### The basic sharding key
If the application already has a well-distributed sharding key, you just have to tell VTGate what those keys are for each table. VTgate will correctly route your queries based on input values or the WHERE clause.

#### Hashed sharding key
If the application's sharding key is a monotonically increasing number, then you may not get well-balanced shards. In such cases, you can ask V3 to route queries based on the hash of the main sharding key.

Vitess's filtered replication currently requires that the hash value be physically present as a column in each table. To satisfy this need, you still need to create a column to store this hash value. However, V3 will take care of populating this on your behalf. We will soon be removing this restriction once we change filtered replication to also perform the same hashing.

#### Auto-increment columns
When a table gets sharded, you are no longer able to use MySQL's auto increment functionality. V3 allows you to designate a table in an unsharded database as the source of auto-increment ids. Once you've specified this, V3 will transparently use generated to values from this table to keep the auto-increment going. Additionally, this column can be a hashed sharding key. This means that the insert will also get routed to the correct shard based on the hashed routing value.

#### Cross-shard indexes
As your application evolves, you'll invariably find yourself wanting to fetch rows based on columns other than the main sharding key. For example, if you've sharded your database by user id, you may still want to be able find users by their username. If you only had the sharding key, such queries can only be answered by sending it to all shards. This could become very expensive as the number of shards grow.

The typical strategy to address this problem is to build a separate lookup table and keep it up-to-date. In the above case, you may build a separate username->user_id relationship table. Once you've informed V3 of this table, it will immediately know what to do with a query like 'select * from user where username=:value'. You can also configure V3 to keep this table up-to-date as you insert or delete data. In other words, the application can be completely agnostic of this table's existence.

We will soon develop the workflow to build such indexes on-the-fly.
