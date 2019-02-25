# Reference Tables

This document describes a proposed design and implementation guidelines for
the `Reference Tables` Vitess feature.

The idea is to have a `reference keyspace` that contains a small number of
`reference tables`, and replicate these tables to every shard of another
keyspace, the `destination keyspace`. Any update to the reference tables will be
propagated to the destination keyspace. The reference tables in the destination
keyspace can then be used directly, in read-only mode (in `JOIN` queries for
instance). This provides for much better performance than cross-keyspace joins.

Since the data is replicated to every shard on the destination keyspace, the
write QPS on the reference keyspace is also applied to every shard on the
destination keyspace. So the change rate in the reference keyspace cannot be
very high, and so let's also assume it is not sharded.

Vitess already has all the right components to support this scenario, it's just
a matter of plumbing it the right way. Let's explore the required changes.

## Replication Setup

We can copy all the data and then setup `Filtered Replication` between the
reference keyspace and each shard of the destination keyspace. This is really
just a corner case of the vertical splits Vitess already supports.

Action items:

* First, this setup probably needs to be explicitly mentioned somewhere in the
  topology, not just as SourceShard objects in the destination keyspace, so
  Vitess can know about this setup at a higher level. Let's add a `repeated
  ReferenceKeyspace` field to the Keyspace object. Each `ReferenceKeyspace`
  object contains the name of the reference keyspace, the list of tables to
  copy, and the UID of the SourceShard entry (the same UID in all shards). By
  making this a repeated field, the destination keyspace should be able to
  support multiple reference keyspaces to copy data from, if necessary.

* `vtctl CopySchemaShard` can already be used to copy the schema from the
  reference keyspace to each destination shard.

* A new vtworker data copy job needs to be added. `vtworker VerticalSplitClone`
  would be a good start, but the new copy has a few special requirements: the
  destination keyspace needs the data in all its shards, and the write rate
  cannot cause the destination shards to be overloaded (or lag behind on
  replication). This job would also populate an entry in the
  `\_vt/blp\_checkpoint` table in the destination shards.
  
* Setting up Filtered Replication after the copy is easy, each destination Shard
  just needs to have a SourceShard with the proper data, and after a
  RefreshTablet, the destination masters will start the replication.
  
All these steps can be supported by a vtctld workflow.

## Supporting Horizontal Resharding in the Destination Keyspace

We still need to support horizontal resharding in the Destination Keyspace while
the Reference Tables feature is enabled.

Action items:

* Each step of the process would know what to do because of the
  `ReferenceKeyspace` entries in the destination keyspace.

* `vtctl CopySchemaShard` needs to also copy the schema of the reference tables.

* `vtworker SplitClone` needs to also copy all of the reference table data, and
  the `\_vt/blp\_checkpoint` entry for the reference keyspace. It needs to do
  that copy from the first source shard to each destination shard only once. So
  in case of a split, the source shard data is copied to each destination
  shard. In case of a merge, only the first source shard data is copied to the
  destination shard.

* Enabling filtered replication on the destination shards needs to not use the
  same UID for replication as the reference keyspace entries. Right now, their
  UID are hardcoded to start at 0. But since the reserved UIDs are documented in
  the `ReferenceKeyspace` entries, it's easy.

* At this point, the destination shards will also replicate from the reference
  keyspace. When the `vtctl MigrateServedType master` command is issued, it
  needs to just remove the horizontal resharding Filtered Replication entries,
  not the `ReferenceKeyspace` entries entries.

## Other Use Cases

Other scenarios might also need to be supported, or explicitly disabled:

* Simple schema changes, or complicated Schema Swap in the reference keyspace:
  They would also need to be applied to the destination keyspace, the same way.

* Vertical Split of the reference keyspace: Since it is replicated, splitting it
  will be more complicated.

## Query Routing

This would be handled by the vtgate and the VSchema. Once the reference tables
are documented in the VSchema, vtgate will know to do the following:

* DMLs on the reference tables are routed to the reference keyspace.

* Select queries on the reference tables only are also routed to the reference
  keyspace.
  
* JOIN queries between reference tables and destination keyspace tables can be
  routed only to the right destination keyspace (based on that keyspace sharding
  situation).

Note this introduces some corner cases: for instance, if the client is asking
for a JOIN between reference tables and destination keyspace tables, with tablet
type `master`. Routing this to the destination keyspace would satisfy the
critical read for the destination tables, but not for the reference
tables. vtgate may need to perform the JOIN to both masters at this point.

Action Items:

* Find the right way to represent reference tables in the VSchema.

* Implement corresponding query routing.

## Notes

### Vitess Keyspace vs MySQL Database

This may force us to revisit the use of databases in our tablets. The current
assumption is that a keyspace only has one MySQL database (with a name usually
derived from the keyspace name with a `vt_` prefix, but that can also be
changed):

* When vttablet connects to MySQL for data queries, it uses that database name
  by default.

* The VSchema also maps tables to keyspaces, so it can just send queries that
  have no keyspace to the right shard (which in turns is configured properly for
  that database).

* Vitess' Filtered Replication only replicates data related to that single
  database. The database name has to be the same when we horizontally split a
  keyspace, so statements from the source shards can be applied on the
  destination shards.

* Vitess' Query Service only loads the schema for that single database.

Maybe it's time to change this assumption:

* A keyspace could be defined as a group of databases, each having a group of
  tables.

* When addressing a table, we could support the `keyspace.database.table`
  syntax.

* We could support moving databases from one keyspace to another.

But maybe this is too many indirections for nothing? Saying one keyspace is one
database may be just the complexity we need.
