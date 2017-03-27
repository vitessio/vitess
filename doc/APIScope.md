# API Scope

This document describes the scope of the Vitess APIs, and how they map to
traditional concepts like database and tables.

## Introduction

Vitess is exposed as a single database by clients, but can be composed of any
arbitrary number of databases, some of them possibly sharded with large numbers
of shards. It is not obvious to map this system with clients that expect only a
single database.

## Shard Names and Key Ranges

For unsharded keyspaces, or custom sharded keyspaces, the shard names have
traditionally been numbers e.g. `0`. (It is important to note that Vitess *does
not* interpret these shard names, including name `0`, as range-based shard.)

For keyspaces that are sharded by keyrange, we use the range as the shard
name. For instance `40-80` contains all records whose sharding key is between
0x40..... and 0x80....

The conventions Vitess follow is:

* if a single shard should be targeted, the shard name should be used. This
  allows single-shard keyspaces and custom sharded keyspaces to be accessed.

* if a subset of the data should be targeted, and we are using ranged-based
  sharding, then a key range should be used. Vitess is then responsible for
  mapping the keyrange to a list of shards, and for any aggregation.

## Execute API

The main entry point of a Vitess cluster is the 'Execute' call (or StreamExecute
for streaming queries). It takes a keyspace, a tablet type, and a query. The
VSchema helps Vitess route the query to the right shard and tablet type. This is
the most transparent way of accessing Vitess. Keyspace is the database, and
inside the query, a table is referenced either just by name, or by
`keyspace.name`.

We are adding a `shard` parameter to this entry point. It will work as follows:

* TODO(sougou) document this.

We want to add support for DBA statements:

* DDL statements in the short term will be sent to all shards of a
  keyspace. Note for complex schema changes, using Schema Swap is recommended.
  Longer term, we want to instead trigger a workflow that will use the best
  strategy for the schema change.

* Read-only statements (like `describe table`) will be sent to the first shard
  of the keyspace. This somewhat assumes the schema is consistent across all
  shards, which may or may not be true. Guaranteeing schema consistency across
  shards is however out of scope for this.

* Read-only statements that return table statistics (like data size, number of
  rows, ...) will need to be scattered and aggregated across all shards. The
  first version of the API won't support this.

We also want to add support for routing sequence queries through this Execute
query (right now only the VSchema engine can use a sequence).

Then, we also want to support changing the VSchema via SQL-like statements, like
`ALTER TABLE ADD VINDEX()`.

## Client Connectors

Client connectors (like JDBC) usually have a connection string that describes
the connection. It usually includes the keyspace and the tablet type, and uses
the Execute API.

## MySQL Server Protocol API

vtgate now exposes an API endpoint that implements the regular MySQL server
protocol. All calls are forwarded to the Execute API. The database provided on
the connection is used as the keyspace, if any. The tablet type is also Master
for now.

## Update Stream and Message Stream

These APIs are meant to target a keyspace and optionally a subset of its shards.

* The keyspace is provided in the API.

* To specify the shard, two options are provided:

  * a shard name, to target an individual shard by name.
  
  * a key range, to target a subset of shards in a way that will survive a
    resharding event.

Note the current implementation for these services only supports a key range
that exactly maps to one shard. We want to make that better and support
aggregating multiple shards in a keyrange.
