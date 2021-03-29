# rowlog

`rowlog` is a diagnostic tool for researching issues reported by users in vreplication workflows. These are usually
reported as data mismatches or loss and are not reproducible because of the huge amounts of data involved and data
privacy reasons. Also, the database configurations vary greatly: number/type/size of columns, collations, keys/indexes
etc.

In order to rule out that the problem is not related to the core vreplication algorithm this tool has been built.

`rowlog` is a standalone command-line tool. For a given set of primary keys for a table (only non-composite integer
primary keys are supported) it outputs the binlogs associated with these ids, both on the source and on the target.

The binlogs on the source and target will usually not be identical because vreplication can "merge" multiple binlog
entries during the initial copy phase.

`rowlog` uses the vstream API to stream events for the specified table from both the source and target keyspaces.

Here is an example of how to invoke it, after the resharding step in the Vitess local example:

```
go build
./rowlog -ids 1,3,4 -table customer -pk customer_id -source_keyspace customer -target_keyspace customer 
        -source_shard=0 -target_shard=-80 -vtctld localhost:15999 
        -vtgate localhost:15991 -cells zone1 -topo_implementation etcd2 -topo_global_server_address localhost:2379 
        -topo_global_root /vitess/global
```

The resulting binlog entries are output to two tab-separated files which can be inspected to validate if data being
copied is consistent.

```text
::::::::::::::
customer_0.log
::::::::::::::
customer_id	email	op	timestamp	gtid
1	alice@domain.com	insert	2021-03-29T09:46:59+02:00	MySQL56/cfd39ffd-9062-11eb-9bc1-04ed332e05c2:1-37
2	bob@domain.com	insert	2021-03-29T09:46:59+02:00	MySQL56/cfd39ffd-9062-11eb-9bc1-04ed332e05c2:1-37
3	charlie@domain.com	insert	2021-03-29T09:46:59+02:00	MySQL56/cfd39ffd-9062-11eb-9bc1-04ed332e05c2:1-37
::::::::::::::
customer_-80.log
::::::::::::::
customer_id	email	op	timestamp	gtid
1	alice@domain.com	insert	2021-03-29T09:47:59+02:00	MySQL56/f34f9bef-9062-11eb-84b0-04ed332e05c2:1-38
2	bob@domain.com	insert	2021-03-29T09:47:59+02:00	MySQL56/f34f9bef-9062-11eb-84b0-04ed332e05c2:1-38
3	charlie@domain.com	insert	2021-03-29T09:47:59+02:00	MySQL56/f34f9bef-9062-11eb-84b0-04ed332e05c2:1-38

```

Events are streamed from one source shard and one target shard. The rows for the ids specified should be on both the
source and target shards.

A possible enhancement is to also stream the events to the _vt.vreplication table so that we can track the source gtid
positions related to the sql applied on the target. However this requires changes in vreplication:
currently the sidecar _vt database is not streamed.
