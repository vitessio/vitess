# Serving Graph

The Serving Graph is a roll-up view of the state of the system in a consistent state. It is used by clients to know
the general topology of the system, and find servers to connect to (EndPoints).

The serving Graph is maintained independently in every cell, and only usually contains information about the servers
in that cell.

## SrvKeyspace

The toplevel object of the serving graph is the SrvKeyspace object. It exists in each cell that is serving data for
that keyspace. It contains:
- a shard map, for each serving type.
- (optional) information about the sharding key for that keyspace.
- (optional) a redirection map, in case some keyspaces are served by another keyspace (this feature is used during
vrtical splits).

It is rebuilt by running 'vtctl RebuildKeyspaceGraph'. It is not automatically rebuilt when adding new tablets in a cell.
It may also be changed during horizontal and vertical splits.

## SrvShard

This object has information about a given shard in a cell. Clients usually skip reading that object, as SrvKeyspace + EndPoints
is usually enough.

It is rebuilt:
- using 'vtctl RebuildShardGraph'
- when a SrvKeyspace is rebuilt
- automatically after Reparenting and other similar actions that may affect the graph.

## EndPoints

This is just a list of possible servers to connect (host, port map) for a given cell, keyspace, shard and server type
(master, replica, ...). It is rebuilt at the same time as SrvShard objects.

## Rebuilding the Serving Graph

We mentioned the commands to rebuild the serving graph earlier.

For a given tablet, before adding it to the Serving Graph, we will check its master is the Shard's master. So some hosts
may be missing when they were expected. The rebuild process will echo a warning in that case.
