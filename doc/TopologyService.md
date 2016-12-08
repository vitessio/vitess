# Topology Service

This document describes the Topology Service, a key part of the Vitess
architecture. This service is exposed to all Vitess processes, and is used to
store small pieces of configuration data about the Vitess cluster, and provide
cluster-wide locks. It also supports watches, which we will use soon.

Concretely, the Topology Service features are implemented by a
[Lock Server](http://en.wikipedia.org/wiki/Distributed_lock_manager), referred
to as Topology Server in the rest of this document. We use a plug-in
implementation and we support multiple Lock Servers (ZooKeeper, etcd, …) as
backends for the service.

## Requirements and Usage

The Topology Service is used to store information about the Keyspaces, the
Shards, the Tablets, the Replication Graph, and the Serving Graph. We store
small data structures (a few hundred bytes) per object.

The main contract for the Topology Server is to be very highly available and
consistent. It is understood it will come at a higher latency cost and very low
throughput.

We never use the Topology Server as an RPC mechanism, nor as a storage system
for logs. We never depend on the Topology Server being responsive and fast to
serve every query.

The Topology Server must also support a Watch interface, to signal when certain
conditions occur on a node. This is used for instance to know when keyspaces
topology changes (for resharding for instance).

### Global vs Local

We differentiate two instances of the Topology Server: the Global instance, and
the per-cell Local instance:

* The Global instance is used to store global data about the topology that
  doesn’t change very often, for instance information about Keyspaces and
  Shards. The data is independent of individual instances and cells, and needs
  to survive a cell going down entirely.
* There is one Local instance per cell, that contains cell-specific information,
  and also rolled-up data from the global + local cell to make it easier for
  clients to find the data. The Vitess local processes should not use the Global
  topology instance, but instead the rolled-up data in the Local topology
  server as much as possible.

The Global instance can go down for a while and not impact the local cells (an
exception to that is if a reparent needs to be processed, it might not work). If
a Local instance goes down, it only affects the local tablets in that instance
(and then the cell is usually in bad shape, and should not be used).

Furthermore, the Vitess processes will not use the Global nor the Local Topology
Server to serve individual queries. They only use the Topology Server to get the
topology information at startup and in the background, but never to directly
serve queries.

### Recovery

If a local Topology Server dies and is not recoverable, it can be wiped out. All
the tablets in that cell then need to be restarted so they re-initialize their
topology records (but they won’t lose any MySQL data).

If the global Topology Server dies and is not recoverable, this is more of a
problem. All the Keyspace / Shard objects have to be re-created. Then the cells
should recover.

## Global Data

This section describes the data structures stored in the global instance of the
topology server.

### Keyspace

The Keyspace object contains various information, mostly about sharding: how is
this Keyspace sharded, what is the name of the sharding key column, is this
Keyspace serving data yet, how to split incoming queries, …

An entire Keyspace can be locked. We use this during resharding for instance,
when we change which Shard is serving what inside a Keyspace. That way we
guarantee only one operation changes the keyspace data concurrently.

### Shard

A Shard contains a subset of the data for a Keyspace. The Shard record in the
global topology contains:

* the MySQL Master tablet alias for this shard
* the sharding key range covered by this Shard inside the Keyspace
* the tablet types this Shard is serving (master, replica, batch, …), per cell
  if necessary.
* if during filtered replication, the source shards this shard is replicating
  from
* the list of cells that have tablets in this shard
* shard-global tablet controls, like blacklisted tables no tablet should serve
  in this shard

A Shard can be locked. We use this during operations that affect either the
Shard record, or multiple tablets within a Shard (like reparenting), so multiple
jobs don’t concurrently alter the data.

### VSchema Data

The VSchema data contains sharding and routing information for
the [VTGate V3](https://github.com/youtube/vitess/blob/master/doc/VTGateV3Features.md) API.

## Local Data

This section describes the data structures stored in the local instance (per
cell) of the topology server.

### Tablets

The Tablet record has a lot of information about a single vttablet process
running inside a tablet (along with the MySQL process):

* the Tablet Alias (cell+unique id) that uniquely identifies the Tablet
* the Hostname, IP address and port map of the Tablet
* the current Tablet type (master, replica, batch, spare, …)
* which Keyspace / Shard the tablet is part of
* the sharding Key Range served by this Tablet
* user-specified tag map (to store per installation data for instance)

A Tablet record is created before a tablet can be running (either by `vtctl
InitTablet` or by passing the `init_*` parameters to vttablet). The only way a
Tablet record will be updated is one of:

* The vttablet process itself owns the record while it is running, and can
  change it.
* At init time, before the tablet starts
* After shutdown, when the tablet gets deleted.
* If a tablet becomes unresponsive, it may be forced to spare to make it
  unhealthy when it restarts.

### Replication Graph

The Replication Graph allows us to find Tablets in a given Cell / Keyspace /
Shard. It used to contain information about which Tablet is replicating from
which other Tablet, but that was too complicated to maintain. Now it is just a
list of Tablets.

### Serving Graph

The Serving Graph is what the clients use to find the per-cell topology of a
Keyspace. It is a roll-up of global data (Keyspace + Shard). vtgates only open a
small number of these objects and get all they need quickly.

#### SrvKeyspace

It is the local representation of a Keyspace. It contains information on what
shard to use for getting to the data (but not information about each individual
shard):

* the partitions map is keyed by the tablet type (master, replica, batch, …) and
  the values are list of shards to use for serving.
* it also contains the global Keyspace fields, copied for fast access.

It can be rebuilt by running `vtctl RebuildKeyspaceGraph`. It is
automatically rebuilt when a tablet starts up in a cell and the SrvKeyspace
for that cell / keyspace doesn't exist yet. It will also be changed
during horizontal and vertical splits.

#### SrvVSchema

It is the local roll-up for the VSchema. It contains the VSchema for all
keyspaces in a single object.

It can be rebuilt by running `vtctl RebuildVSchemaGraph`. It is automatically
rebuilt when using `vtctl ApplyVSchema` (unless prevented by flags).


## Workflows Involving the Topology Server

The Topology Server is involved in many Vitess workflows.

When a Tablet is initialized, we create the Tablet record, and add the Tablet to
the Replication Graph. If it is the master for a Shard, we update the global
Shard record as well.

Administration tools need to find the tablets for a given Keyspace / Shard:
first we get the list of Cells that have Tablets for the Shard (global topology
Shard record has these) then we use the Replication Graph for that Cell /
Keyspace / Shard to find all the tablets then we can read each tablet record.

When a Shard is reparented, we need to update the global Shard record with the
new master alias.

Finding a tablet to serve the data is done in two stages: vtgate maintains a
health check connection to all possible tablets, and they report which keyspace
/ shard / tablet type they serve. vtgate also reads the SrvKeyspace object, to
find out the shard map. With these two pieces of information, vtgate can route
the query to the right vttablet.

During resharding events, we also change the topology a lot. An horizontal split
will change the global Shard records, and the local SrvKeyspace records. A
vertical split will change the global Keyspace records, and the local
SrvKeyspace records.

## Implementations

The Topology Server interface is defined in our code in go/vt/topo/server.go and
we also have a set of unit tests for it in go/vt/topo/test.

This part describes the two implementations we have, and their specific
behavior.

### ZooKeeper

Our ZooKeeper implementation is based on a configuration file that describes
where the global and each local cell ZK instances are. When adding a cell, all
processes that may access that cell should be restarted with the new
configuration file.

The global cell typically has around 5 servers, distributed one in each
cell. The local cells typically have 3 or 5 servers, in different server racks /
sub-networks for higher resiliency. For our integration tests, we use a single
ZK server that serves both global and local cells.

We sometimes store both data and sub-directories in a path (for a keyspace for
instance). We use JSON to encode the data.

For locking, we use an auto-incrementing file name in the `/action` subdirectory
of the object directory. We also move them to `/actionlogs` when the lock is
released. And we have a purge process to clear the old locks (which should be
run on a crontab, typically).

Note the paths used to store global and per-cell data do not overlap, so a
single ZK can be used for both global and local ZKs. This is however not
recommended, for reliability reasons.

* Keyspace: `/zk/global/vt/keyspaces/<keyspace>`
* Shard: `/zk/global/vt/keyspaces/<keyspace>/shards/<shard>`
* Tablet: `/zk/<cell>/vt/tablets/<uid>`
* Replication Graph: `/zk/<cell>/vt/replication/<keyspace>/<shard>`
* SrvKeyspace: `/zk/<cell>/vt/ns/<keyspace>`
* SrvVSchema: `/zk/<cell>/vt/vschema`

We provide the 'zk' utility for easy access to the topology data in
ZooKeeper. For instance:

```
\# NOTE: You need to source zookeeper client config file, like so:
\#  export ZK_CLIENT_CONFIG=/path/to/zk/client.conf
$ zk ls /zk/global/vt/keyspaces/user
action
actionlog
shards
```

### Etcd

Our etcd implementation is based on a command-line parameter that gives the
location(s) of the global etcd server. Then we query the path `/vt/cells` and
each file in there is named after a cell, and contains the list of etcd servers
for that cell.

We use the `_Data` filename to store the data, JSON encoded.

For locking, we store a `_Lock` file with various contents in the directory that
contains the object to lock.

We use the following paths:

* Keyspace: `/vt/keyspaces/<keyspace>/_Data`
* Shard: `/vt/keyspaces/<keyspace>/<shard>/_Data`
* Tablet: `/vt/tablets/<cell>-<uid>/_Data`
* Replication Graph: `/vt/replication/<keyspace>/<shard>/_Data`
* SrvKeyspace: `/vt/ns/<keyspace>/_Data`
* SrvVSchema: `/vt/ns/_VSchema`

