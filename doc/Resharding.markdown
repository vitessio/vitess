# Resharding

In Vitess, resharding describes the process of re-organizing data dynamically, with very minimal downtime (we manage to
completely perform most data transitions with less than 5 seconds of read-only downtime - new data cannot be written,
existing data can still be read).

## Process

The process to achieve this goal is composed of the following steps:
- pick the original shard(s)
- pick the destination shard(s) coverage
- create the destination shard(s) tablets (in a mode where they are not used to serve traffic yet)
- bring up the destination shard(s) tablets, with read-only masters.
- backup and split the data from the original shard(s)
- merge and import the data on the destination shard(s)
- start and run filtered replication from original to destination shard(s), catch up
- move the read-only traffic to the destination shard(s), stop serving read-only traffic from original shard(s). This transition can take a few hours. We might want to move rdonly separately from replica traffic.
- in quick succession:
 - make original master(s) read-only
 - flush filtered replication on all filtered replication source servers (after making sure they were caught up with their masters)
 - wait until replication is caught up on all destination shard(s) masters
 - move the write traffic to the destination shard(s)
 - make destination master(s) read-write
- scrap the original shard(s)

## Applications

The main application we currently support:
- in a sharded keyspace, split or merge shards (horizontal sharding)
- in a non-sharded keyspace, break out some tables into a different keyspace (vertical sharding)

With these supported features, it is very easy to start with a single keyspace containing all the data (multiple tables),
and then as the data grows, move tables to different keyspaces, start sharding some keyspaces, ... without any real
downtime for the application.

## Filtered Replication

The cornerstone of Resharding is being able to replicate the right data. Mysql doesn't support any filtering, so the
Vitess project implements it entirely:
- the tablet server tags transactions with comments that describe what the scope of the statements are (which keyspace_id,
which table, ...). That way the MySQL binlogs contain all filtering data.
- a server process can filter and stream the MySQL binlogs (using the comments).
- a client process can apply the filtered logs locally (they are just regular SQL statements at this point).
