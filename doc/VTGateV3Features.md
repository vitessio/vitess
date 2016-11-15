# Vitess Query Features

## Overview

Client applications connect to Vitess through VTGate. VTGate is a proxy that
routes queries to the right vttablets. It is basically a fat client, but provided
as a service. Over time, we moved away from a fat client, which had to be
incorporated into the application. This way, the routing logic is more decoupled
from the application and easier to maintain separately.

The previous versions of the VTGate API existed as transition points while the
logic was gradually moved away from the client into this new server:

* V0: This version had no VTGate. The application was expected to fetch the
  sharding info and tablet locations from a 'toposerver' and use it to talk
  directly to the tablet servers.
* V1: This was the first version of VTGate. In this version, the app only needed
  to know the number of shards, and how to map the sharding key to the correct
  shard. The rest was done by VTGate. In this version, the app was still exposed
  to resharding events.
* V2: In this version, the keyspace id was required instead of the shard. This
  allowed the app to be agnostic of the number of shards.

With V3, the app does not need to specify any routing info. It just sends the
query to VTGate as if it's a single database. Apart from simplifying the API,
there are some additional benefits:

* Database compliant drivers can be built for each client language. This will
  also allow for integration with third party tools that work with such drivers.
* V3 can aspire to satisfy the full SQL syntax. This means that it will be able
  to perform cross-shard joins, aggregations and sorting.
* Easier migration: an application that was written to use a single database can
  be trivially changed to use Vitess, and then the database can be scaled from
  underneath without changing much of the app.

The
[V3 design](https://github.com/youtube/vitess/blob/master/doc/V3VindexDesign.md)
is quite elaborate. If necessary, it will allow you to plug in custom indexes
and sharding schemes. However, it comes equipped with some pre-cooked recipes
that satisfy the immediate needs of the real-world:

## Sharding schemes

At its core, vitess uses range-based sharding, where the sharding column is
typically a number or a varbinary. However, allowing data to be accessed only by
the sharding key limits the flexibility of an application. V3 comes with a set
of new indexing schemes that are built on top of range-based sharding.

### Basic sharding key

If the application already has a well-distributed sharding key, you just have to
tell VTGate what those keys are for each table. VTGate will correctly route your
queries based on input values or the WHERE clause.

### Hashed sharding key

If the application's sharding key is a monotonically increasing number, then you
may not get well-balanced shards. In such cases, you can ask V3 to route queries
based on the hash of the main sharding key.

With V3, it is no longer necessary to store the sharding key as an extra column
in the table. Even during resharding, filtered replication can now compute the
hashed values.

### Auto-increment columns

When a table gets sharded, you are no longer able to use MySQL's auto increment
functionality. V3 allows you to designate a table in an unsharded database as
the source of auto-increment ids. Once you've specified this, V3 will
transparently use generated values from this table to keep the auto-increment
going. The auto-increment column can in turn be a basic or hashed sharding
key. If it's a hashed sharding key, the newly generated value will be hashed
before the query is routed.

### Cross-shard indexes

As your application evolves, you'll invariably find yourself wanting to fetch
rows based on columns other than the main sharding key. For example, if you've
sharded your database by user id, you may still want to be able find users by
their username. If you only had the sharding key, such queries can only be
answered by sending it to all shards. This could become very expensive as the
number of shards grow.

The typical strategy to address this problem is to build a separate lookup table
and keep it up-to-date. In the above case, you may build a separate
username->user_id relationship table. Once you've informed V3 of this table, it
will know what to do with a query like 'select * from user where
username=:value'. You can also configure V3 to keep this table up-to-date as you
insert or delete data. In other words, the application can be completely
agnostic of this table's existence.

#### Non-unique indexes

Cross-shard indexes don't have to be unique. It is possible that rows may exist
in multiple shards for a given where clause. V3 allows you to specify indexes as
unique or non-unique, and accordingly enforces such constraints during
changes. This flexibility is currently available for lookup indexes. For
example, you may want to index users by their last name. If so, there would be a
last_name->user_id index, which could result in multiple user ids being returned
for a given last_name.

#### Shared indexes

There are situations where multiple tables share the same foreign key. A typical
use case is a situation where there is a customer table, an order table and an
order_detail table. The order table would have a customer_id column. In order to
efficiently access all orders of a customer, it would be beneficial to shard
this table by customer_id. This will co-locate order rows with their
corresponding customer row.

The order table would also need an order_id column. As mentioned above, you can
create an order_id->customer_id cross-shard index for this table. This will
allow you to efficiently access orders by their order_id.

In the case of an order_detail table, it may only need an order_id foreign
key. Since this foreign key means the same thing as the order_id in order,
creating a cross-shard index for it will result in a duplication of the
order_id->customer_id index. In such situations, V3 allows you to just reuse the
existing index for the order_detail table also. This saves disk space and also
reduces the overall write load.

## Knowing where tables are

As your database grows, you will not only be sharding it, you will also be
splitting it vertically by migrating tables from one database to another. V3
will be able to keep track of this. The app will only have to refer a table by
name, and VTGate will figure out how to route the query to the correct database.

The vitess workflow also ensures that such migrations are done transparently
with virtually no downtime.

## Consistency

Once you add multiple indexes to tables, it's possible that the application
could make inconsistent requests. V3 makes sure that none of the specified
constraints are broken. For example, if a table had both a basic sharding key
and a hashed sharding key, it will enforce the rule that the hash of the basic
sharding key matches that of the hashed sharding key.

Some of the changes require updates to be performed across multiple
databases. For example, inserting a row into a table that has a cross-shard key
requires an additional row to be inserted into the lookup table. This results in
distributed transactions. Currently, this is a best effort update. It is
possible that partial commits happen if databases fail in the middle of a
distributed commit.

*The 2PC transactions feature is coming very soon to overcome this limitation.*

## Query Diversity

V3 does not support the full SQL feature set. The current implementation
supports the following queries:

* Single table DML statements: This is a vitess-wide restriction where you can
  affect only one table and one sharding key per statement. *This restriction
  may be removed in the future.*
* Single table SELECT statements:
  * All constructs allowed if the statement targets only a single sharding key
  * Aggregation and sorting not allowed if the statement targets more than one
    sharding key. Selects are allowed to target multiple sharding keys as long
    as the results from individual shards can be simply combined together to
    form the final result.
* Joins that can be served by sending the query to a single shard.
* Joins that can be served by sending the query to multiple shards, and
  trivially combined to form the final result.

Work is underway to support the following additional constructs:

* Cross-shard joins that can be served through a simple nested lookup.
* Sorting that can be trivially merged from the results of multiple shards
  (merge-sort).
* Aggregations (and grouping) that can be trivially combined from multiple
  shards.
* A combination of the above constructs as long as the results remain trivially
  combinable.

SQL is a very powerful language. You can build queries that can result in large
amount of work and memory consumption involving big intermediate results. Such
constructs where the scope of work is open-ended will not be immediately
supported. In such cases, it's recommended that you use map-reduce techniques
for which there is a separate API.

*On-the-fly map-reducers can be built to address the more complex needs in the future.*

## Special Queries

MySQL supports a number of tables that are not storing data, but rather expose
meta-data about the database. For instance, the information_schema database.

The V3 API is meant to resemble a single database instance (using the Execute
API calls). But it can be backed by multiple keyspaces and each can have multiple
shards. In the long term, we want to behave as if all keyspaces and tables are
in one single instance: each keyspace would be shown as a database, each table
in a sharded keyspace would only be shown once (and not once per shard).

*We do not support these queries just yet.*

For administrators who want to target individual shards, it is possible to use
the V1 API (with the ExecuteShards API). A deeper knowledge of the existing
shards is then required, but administrators have that knowledge.

## VSchema

The above features require metadata like configuration of sharding key and
cross-shard indexes to be configured and stored in some place. This is known as
the vschema.

*A way to edit the VSchema using SQL-like commands will be built to integrate
VSchema changes with regular SQL schema changes. For now, the VSchema needs to
be specified as a JSON file.*

Under the covers, the vschema is a JSON file. There are low level vtctl commands
to upload it also. This will allow you to build workflows for tracking and
managing changes.
