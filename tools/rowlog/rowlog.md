#rowlog

`rowlog` is a diagnostic tool for researching issues reported by users in vreplication workflows. These are usually
reported as data mismatches or loss and are not reproducible because of the huge amounts of data involved and 
data privacy reasons. Also, the database configurations vary greatly: number/type/size of columns, collations, 
keys/indexes etc.

In order to rule out that the problem is not related to the core vreplication algorithm this tool has been built.

`rowlog` is a standalone command-line tool. For a given set of primary keys for a table (only non-composite 
integer primary keys are supported) it outputs the binlogs associated with these ids, both on the source and
on the target. 

The binlogs on the source and target will usually not be identical because vreplication can "merge" multiple 
binlog entries during the initial copy phase. 

`rowlog` uses the vstream API to stream events for the specified table from both the source and target keyspaces.

Here is an example of how to invoke it:

```
go build
./rowlog -ids 1,3,4 -table customer -pk customer_id -source commerce -target customer -vtctld localhost:15999 
        -vtgate localhost:15991 -cells zone1 -topo_implementation etcd2 -topo_global_server_address localhost:2379 
        -topo_global_root /vitess/global
```

The resulting binlog entries are output to two tab-separated files which can be inspected to validate if 
data being copied is consistent.

Initial version is for unsharded keyspaces but can be easily extended for sharded. 

Another possible enhancement is to also stream the events to the _vt.vreplication table so that we can track the 
source gtid positions related to the sql applied on the target. However this requires changes in vreplication: 
currently the sidecar _vt database is not streamed.
