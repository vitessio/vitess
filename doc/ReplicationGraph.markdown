# Replication Graph

The replication graph contains the mysql replication information for a shard. Currently, we only support one layer
of replication (a single master wiht multiple slaves), but the design doesn't preclude us from supporting
hierarchical replication later on.

## Master

The current master for a shard is represented in the Shard object as MasterAlias, in the global topology server.

When running 'vtctl InitTablet ... master', the record is updated.

## Slaves

The slaves are added to the ShardReplication object present on each local topology server. So for slaves, the
replication graph is colocated in the same cell as the tablets themselves. This makes disaster recovery much easier:
when loosing a data center, the replication graph for other data centers is not lost.
