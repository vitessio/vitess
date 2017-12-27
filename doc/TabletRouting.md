This document describes how Vitess routes queries to healthy tablets. It is
meant to be up to date with the current status of the code, and describe
possible extensions we're working on.

# Current architecture and concepts

## Vtgate and SrvKeyspace

Vtgate receives queries from the client. Depending on the API, it has:

* The shard to execute the query on.
* The Keyspace Id to use.
* Enough routing information with the VSchema to figure out the Keyspace Id.

At this point, vtgate has a cache of the SrvKeyspace object for each keyspace,
that contains the shard map. Each SrvKeyspace is specific to a cell. Vtgate
retrieves the SrvKeyspace for the current cell, and uses it to find out the
shard to use.

The SrvKeyspace object contains the following information about a Keyspace, for
a given cell:

* The served_from field: for a given tablet type (master, replica, read-only),
  another keyspace is used to serve the data. This is used for vertical
  resharding.
* The partitions field: for a given tablet type (master, replica, read-only),
  the list of shards to use. This can change when we perform horizontal
  resharding.

Both these fields are cell and tablet type specific, as when we reshard
(horizontally or vertically), we have the option to migrate serving of any type
in any cell forward or backward (with some constraints).

Note that the VSchema query engine uses this information in slightly different
ways that the older API, as it needs to know if two queries are on the same
shard, for instance.

## Tablet health

Each tablet is responsible for figuring out its own status and health. When a
tablet is considered for serving, a StreamHealth RPC is sent to the tablet. The
tablet in turn returns:

* Its keyspace, shard and tablet type.
* If it is serving or not (typically, if the query service is running).
* Realtime stats about the tablet, including the replication lag.
* the last time it received a 'TabletExternallyReparented' event (used to break
  ties during reparents).
* The tablet alias of this tablet (so the source of the StreamHealth RPC can
  check it is the right tablet, and not another table that restarted on the same
  host / port, as can happen in container environments).
  
That information is updated on a regular basis (every health check interval, and
when something changes), and streamed back.

## Discovery module

The go/vt/discovery module provides libraries to keep track of the health of a
group of tablets.

As an input, it needs the list of tablets to watch. The TopologyWatcher object
is responsible for finding the tablets to watch. It can watch:

* All the tablets in a cell. Uses polling of the `tablets/` directory of the
  topo service in that cell to figure out a list.
* The tablets for a given cell / keyspace / shard. It polls the ShardReplication
  object.

Tablets to watch are added / removed from the main list kept by the
TabletStatsCache object. It just contains a giant map indexed by cell, keyspace,
shard and tablet type of all the tablets it is in contact with.

When a tablet is in the list of tablets to watch, this module maintains a
StreamHealth streaming connection to the tablet, as described in the previous
section.

This module also provides helper methods to find a tablet to route traffic
to. Since it knows the health status of all tablets for a given keyspace / shard
/ tablet type, and their replication lag, it can provide a good list of tablets.

## Vtgate Gateway interface

An important interface inside vtgate is the Gateway interface. It can send
queries to a tablet by keyspace, shard, and tablet type.

As mentioned previously, the higher levels inside vtgate can resolve queries to
a keyspace, shard and tablet type. The queries are then passed to the Gateway
implementation inside vtgate, to route them to the right tablet.

There are two implementations of the Gateway interface:

* discoveryGateway: It combines a set of TopologyWatchers (described in the
  discovery section, one per cell) as a source of tablets, a HealthCheck module
  to watch their health, and a TabletStatsCache to collect all the health
  information. Based on this data, it can find the best tablet to use.
* l2VTGateGateway: It keeps a map of l2vtgate processes to send queries to. See
  next section for more details.
  
## l2vtgate

As we started increasing the number of tablets in a cell, it became clear that a
bottleneck of the system was going to be how many tablets a single vtgate is
connecting to. Since vtgate maintains a streaming health check connection per
tablet, the number of these connections can grow to large numbers. It is common
for vtgate to watch tablets in other cells, to be able to find the master
tablet.

So l2vtgate came to exist, based on very similar concepts and interfaces:

* l2vtgate is an extra hop between a vtgate pool and tablets.
* A l2vtgate pool connects to a subset of tablets, therefore it can have a
  reasonable number of streaming health connections. Externally, it exposes the
  QueryService RPC interface (that has the Target for the query, keyspace /
  shard / tablet type). Internally, it uses a discoveryGateway, as usual.
* vtgate connects to l2vtgate pools (using the l2VTGateGateway instead of the
  discoveryGateway). It has a map of which keyspace / shard / tablet type needs
  to go to wich l2vtgate pool. At this point, vtgate doesn't maintain any health
  information about the tablets, it lets l2vtgate handle it.

Note l2vtgate is not an ideal solution as it is now. For instance, if there are
two cells, and the master for a shard can be in either, l2vtgate still has to
watch the tablets in both cells, to know where the master is. Ideally, we'd want
l2vtgate to be collocated with the tablets in a given cell, and not go
cross-cell.

# Extensions, work in progress

## Regions, cross-cell targeting

At first, the discoveryGateway was routing queries the following way:

* For master queries, just find the master tablet, whichever cell it's in, and
  send the query to it. This was meant to handle cross-cell master fail-over.
* For non-master queries, just route to the local cell vtgate is in.

This turned out to be a bit limiting, as if no local tablet was available in the
cell, the queries would just fail. We added the concept of Region, and if
tablets in the same Region are available to serve, they can be used instead.
This fix however is not entirely correct, as it uses the SrvKeyspace of the
current cell, instead of the SrvKeyspace of the other cell. Both ServedFrom and
the shard partition may be different in a different cell.

## Percolating the cell back up

We think the correct fix is to percolate the cell back up at the vtgate routing
layer. That way when the higher level is asking for the SrvKeyspace object, it
can ask for the right one, in the right cell. For instance, we can read the
current SrvKeyspace for the current cell, see if it has healthy tablets for the
shard(s) we're interested in, and if not find another cell in the same region
that has healthy tablets.

In the l2vtgate case though, this is not as easy, as the vtgate process has no
idea what tablets are healthy. We propose to solve this by adding a healthcheck
between vtgate and l2vtgate:

* vtgate would maintain a healthcheck connection to the l2vtgate pool (a single
  connection should be good enough, but we may be more aggressive to collect
  more up to date information, so we don't depend on a single l2vtgate).
* l2vtgate will report on a regular basis which keyspace / shard / tablet type
  it can serve. It would also report the minimum and maximum replication lag
  (and whatever other pertinent information vtgate needs).
* l2vtgateGateway can then provide the health information back up inside vtgate.
* This would also lift the limitation previously noted about l2vtgate going
  cross-cell. When a master is moved from one cell to another, the l2vtgate in
  the old cell would advertize back that they lost the ability to serve the
  master tablet type, and the l2vtgate in the new cell would start advertizing
  it. Vtgates would then know where to look.

This would also be a good time to merge the vtgate code that uses the VSchema
with the code that doesn't for SrvKeyspace access.

## Hybrid Gateway

It would be nice to re-organize the code a bit inside vtgate to allow for an
hybrid gateway, and get rid of l2vtgate alltogether:

* vtgate would use the discoveryGateway to watch the tablets in the current cell
  (and optionally to any other cell we still want to consider local).
* vtgate would use l2vtgateGateway to watch the tablets in a different cell.
* vtgate would expose the RPC APIs currently exposed by the l2vtgate process.

So vtgate would watch the tablets in the local cell only, but also know what
healthy tablets are in the other cells, and be able to send query to them
through their vtgate. The extra hop to the other cell vtgate should be a small
latency price to pay, compared to going cross-cell already.

So queries would go one of two routes:

* client(cell1) -> vtgate(cell1) -> tablet(cell1)
* client(cell1) -> vtgate(cell1) -> vtgate(cell2) -> tablet(cell2)

If the number of tablets in a given cell is still too high for the local vtgate
pool, two or more pools can still be created, each of them knowing about a
subset of the tablets. And they would just forward queries to each others when
addressing the other tablet set.

## Config-based routing

Another possible extension would be to group all routing options for vtgate in a
configuration (and possibly distribute that configuration in the topology
service). The following parameters now exist in different places:

* vttablet has a replication delay threshold after which it reports
  unhealthy, `-unhealthy_threshold`. Unrelated to vtgate's
  `discovery_high_replication_lag_minimum_serving` parameter.
* vttablet has a `-degraded_threshold` parameter after which it shows as
  unhealthy in its status page. No impact on serving. Independent from vtgate's
  `-discovery_low_replication_lag` parameter, although if they match the user
  experience is better.
* vtgate also has a `-min_number_serving_vttablets` that is used to not just
  return one tablet and overwhelm it.

We also now have the capability to route to different Cells in the same
Region. Configuring when to use a different cell in corner cases is hard. If
there is only one tablet with somewhat high replication lag in the current cell,
is it better than up-to-date tablets in other cells?

A possible solution for this would be a configuration file, that lists what to
do, in order of preference, something like:

* use local tablets if more than two with replication lag smaller than 30s.
* use remote tablets if all local tablets are above 30s lag.
* use local tablets if lag is lower than 2h, minimum 2.
* ...
