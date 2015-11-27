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
