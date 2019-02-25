Sharding is a method of horizontally partitioning a database to store
data across two or more database servers. This document explains how
sharding works in Vitess and the types of sharding that Vitess supports.

## Overview

A keyspace in Vitess can be sharded or unsharded. An unsharded keyspace
maps directly to a MySQL database. If sharded, the rows of the keyspace
are partitioned into different databases of identical schema.

For example, if an application's "user" keyspace is split into two
shards, each shard contains records for approximately half of the
application's users. Similarly, each user's information is stored
in only one shard.

Note that sharding is orthogonal to (MySQL) replication.
A Vitess shard typically contains one MySQL master and many MySQL
slaves. The master handles write operations, while slaves handle
read-only traffic, batch processing operations, and other tasks.
Each MySQL instance within the shard should have the same data,
excepting some replication lag.

### Supported Operations

Vitess supports the following types of sharding operations:

* **Horizontal sharding:** Splitting or merging shards in a sharded keyspace
* **Vertical sharding:** Moving tables from an unsharded keyspace to
  a different keyspace.

With these features, you can start with a single keyspace that contains
all of your data (in multiple tables). As your database grows, you can
move tables to different keyspaces (vertical split) and shard some or
all of those keyspaces (horizontal split) without any real downtime
for your application.

## Sharding scheme

Vitess allows you to choose the type of sharding scheme by the choice of
your Primary Vindex for the tables of a shard. Once you have chosen
the Primary Vindex, you can choose the partitions depending on how the
resulting keyspace IDs are distributed.

Vitess calculates the sharding key or keys for each query and then
routes that query to the appropriate shards. For example, a query
that updates information about a particular user might be directed to
a single shard in the application's "user" keyspace. On the other hand,
a query that retrieves information about several products might be
directed to one or more shards in the application's "product" keyspace.

### Key Ranges and Partitions

Vitess uses key ranges to determine which shards should handle any
particular query.

* A **key range** is a series of consecutive keyspace ID values. It
    has starting and ending values. A key falls inside the range if
    it is equal to or greater than the start value and strictly less
    than the end value.
* A **partition** represents a set of key ranges that covers the entire
    space.

When building the serving graph for a sharded keyspace,
Vitess ensures that each shard is valid and that the shards
collectively constitute a full partition. In each keyspace, one shard
must have a key range with an empty start value and one shard, which
could be the same shard, must have a key range with an empty end value.

* An empty start value represents the lowest value, and all values are
    greater than it.
* An empty end value represents a value larger than the highest possible
    value, and all values are strictly lower than it.

Vitess always converts sharding keys to a left-justified binary string for
computing a shard. This left-justification makes the right-most zeroes
insignificant and optional. Therefore, the value <code>0x80</code> is
always the middle value for sharding keys.
So, in a keyspace with two shards, sharding keys that have a binary
value lower than 0x80 are assigned to one shard. Keys with a binary
value equal to or higher than 0x80 are assigned to the other shard.

Several sample key ranges are shown below:

``` sh
Start=[], End=[]: Full Key Range
Start=[], End=[0x80]: Lower half of the Key Range.
Start=[0x80], End=[]: Upper half of the Key Range.
Start=[0x40], End=[0x80]: Second quarter of the Key Range.
Start=[0xFF00], End=[0xFF80]: Second to last 1/512th of the Key Range.
```

Two key ranges are consecutive if the end value of one range equals the
start value of the other range.

### Shard Names

A shard's name identifies the start
and end of the shard's key range, printed in hexadecimal and separated
by a hyphen. For instance, if a shard's key range is the array of bytes
beginning with [ 0x80 ] and ending, noninclusively, with [ 0xc0], then
the shard's name is <code>80-c0</code>.

Using this naming convention, the following four shards would be a valid
full partition:

* -40
* 40-80
* 80-c0
* c0-

Shards do not need to handle the same size portion of the key space. For example, the following five shards would also be a valid full partition, possibly with a highly uneven distribution of keys.

* -80
* 80-c0
* c0-dc00
* dc00-dc80
* dc80-

## Resharding

Resharding describes the process of updating the sharding
scheme for a keyspace and dynamically reorganizing data to match the
new scheme. During resharding, Vitess copies, verifies, and keeps
data up-to-date on new shards while the existing shards continue to
serve live read and write traffic. When you're ready to switch over,
the migration occurs with only a few seconds of read-only downtime.
During that time, existing data can be read, but new data cannot be
written.

The table below lists the sharding (or resharding) processes that you
would typically perform for different types of requirements:

Requirement | Action
----------- | ------
Uniformly increase read capacity | Add replicas or split shards
Uniformly increase write capacity | Split shards
Reclaim overprovisioned resources | Merge shards and/or keyspaces
Increase geo-diversity | Add new cells and replicas
Cool a hot tablet | For read access, add replicas or split shards. For write access, split shards.

### Filtered Replication

The cornerstone of resharding is replicating the right data. Vitess
implements the following functions to support filtered replication,
the process that ensures that the correct source tablet data is
transferred to the proper destination tablets.

#### Statement-based Replication

If you've configured the MySQL servers to use Statement-based Replication (SBR),
then Vitess must be able to identify the destination for such statements during
the filtered replication process. This performed the following way:

1. The source tablet tags transactions with comments so that MySQL binlogs
    contain the filtering data needed during the resharding process. The
    comments describe the scope of each transaction (its keyspace ID,
    table, etc.).
1. A server process uses the comments to filter the MySQL binlogs and
    stream the correct data to the destination tablet.
1. A client process on the destination tablet applies the filtered logs,
    which are just regular SQL statements at this point.
    
#### Row-based Replication

If MySQL is configured to use Row-based Replication (RBR), the filtered replication
is performed the following way:

1. The server process uses the primary vindex to compute the keyspace ID for every
    row coming throug the replication stream, and sends that row to the corresponding
    target shard.
1. The target shard converts the row into the corresponding DML (Data Manipulation Language)
    and applies the statement.

If using RBR, it's generally required that you have full image turned on. However, if your
Primary Vindex is also part of the Primary key, it's not required, because every RBR event
will always contain the full primary key of its affected row.

### Additional Tools and Processes

Vitess provides the following tools to help manage range-based shards:

* The [vtctl]({% link reference/vtctl.md %}) command-line tool supports
    functions for managing keyspaces, shards, tablets, and more.
* Client APIs account for sharding operations.
* The [MapReduce framework](https://github.com/vitessio/vitess/tree/master/java/hadoop/src/main/java/io/vitess/hadoop)
    fully utilizes key ranges to read data as quickly as possible,
    concurrently from all shards and all replicas.
