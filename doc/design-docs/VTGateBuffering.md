# Adding buffering to VTGate while switching traffic during a movetables operation

## Current buffering support in VTGate

VTGate currently supports buffering of queries during reparenting and resharding operations. This is done by buffering
the failing queries in the tablet gateway layer in vtgate. When a query fails, the reason for the failure is checked, to
see if is due to one of these.

To assist in diagnosing the root cause a _KeyspaceEventWatcher_ (aka *KEW*) was introduced. This watches the
SrvKeyspace (in a goroutine): if there is a change to the keyspace partitions in the topo it is considered that there is
a resharding operation in progress. The buffering logic subscribes to the keyspace event watcher.

Otherwise, if there are no tables to serve from, based on the health check results, it is assumed that there is a
cluster event where either the primary is being reparented or if the cluster is being restarted and all tablets are in
the process of starting up.

If either of these occurs, the _consistent_ flag is set to false for that keyspace. When that happens the keyspace
watcher checks, on every SrvKeyspace update, if the event has got resolved. This can happen when tablets are now
available (in case of a cluster event) or if the partition information indicates that resharding is complete.

When that happens. the keyspace event watcher publishes an event that the keyspace is now consistent. The buffers are
then drained and the queries retried by the tablet gateway.

## Adding buffering support for MoveTables

### Background

MoveTables does not affect the entire keyspace, just the tables being moved. Even if all tables are being moved there is
no change in existing keyspace or shard configurations. So the KEW doesn't detect a cluster event since the tablets are
still available and shard partitions are unchanged.

MoveTables moves tables from one keyspace to another. There are two flavors of MoveTables: one where the tables are
moved into all shards in the target keyspace. In Shard-By-Shard Migration user can specify a subset of shards to move
the tables into.

These are the topo attributes that are affected during a MoveTables (regular or shard-by-shard):

* *DeniedTables* in a shard's TabletControls. These are used to stop writes to the source keyspace for these tables.
  While switching writes we first create these entries, wait for the target to catchup to the source (using gtid
  positions), and then update the routing rules to point these tables to the target. When a primary sees a DeniedTables
  entry during a DML it will error with an "enforce denied tables".
* *RoutingRules* (for regular movetables) and *ShardRoutingRules* (for shard by shard migration). Routing rules are
  pointers for each table being moved to a keyspace. When a MoveTables is initiated, that keyspace is the source
  keyspace. After traffic is switched the pointer is changed to point to the target keyspace. If routing rules are
  specified, VTGate uses them to decide which keyspace to route each table.

### Changes

There are two main changes:

* The keyspace event watcher is enhanced to look at the topo attributes mentioned above. An SrvVSchema watcher looks for
  changes in the Routing Rules. DeniedTables are only in the Shard records in the topo. So any changes to the
  DeniedTables would not result in a notification. To get around that we change the traffic switcher to also rebuild
  SrvVSchema when DeniedTables are modified.
* The logic to start buffering needs to look for the "enforce denied tables" error that is thrown by the vttablets when
  it tries to execute a query on a table being switched.
* We cannot use the current query retry logic which is at the tablet gateway level: meaning the keyspace is already
  fixed by the planner and cannot be changed in that layer. We need to add a new retry logic at a higher level (the
  _newExecute_  method) and always replan before retrying a query. This also means that we need to bypass the plan cache
  while retrying. 



