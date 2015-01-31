# Zookeeper Data

This document describes the information we keep in zookeeper, how it is generated, and how the python client uses it.

## Keyspace / Shard / Tablet Data

### Keyspace

Each keyspace is now a global zookeeper path, with sub-directories for its shards and action / actionlog. The Keyspace
object there contains very basic information.

```go
// see go/vt/topo/keyspace.go for latest version
type Keyspace struct {
        // name of the column used for sharding
        // empty if the keyspace is not sharded
        ShardingColumnName string

        // type of the column used for sharding
        // KIT_UNSET if the keyspace is not sharded
        ShardingColumnType key.KeyspaceIdType

        // ServedFrom will redirect the appropriate traffic to
        // another keyspace
        ServedFrom map[TabletType]string
}
```

```
# NOTE: You need to source zookeeper client config file, like so:
#  export ZK_CLIENT_CONFIG=/path/to/zk/client.conf
$ zk ls /zk/global/vt/keyspaces/ruser
action
actionlog
shards
```

The path and sub-paths are created by 'vtctl CreateKeyspace'.

We use the action and actionlog paths for locking only, no process is actively watching these paths.

### Shard

A shard is a global zookeeper path, with sub-directories for its action / actionlog, and a node for some more data and replication graph.

```go
// see go/vt/topo/shard.go for latest version
// A pure data struct for information stored in topology server.  This
// node is used to present a controlled view of the shard, unaware of
// every management action. It also contains configuration data for a
// shard.
type Shard struct {
        // There can be only at most one master, but there may be none. (0)
        MasterAlias TabletAlias

        // This must match the shard name based on our other conventions, but
        // helpful to have it decomposed here.
        KeyRange key.KeyRange

        // ServedTypes is a list of all the tablet types this shard will
        // serve. This is usually used with overlapping shards during
        // data shuffles like shard splitting.
        ServedTypes []TabletType

        // SourceShards is the list of shards we're replicating from,
        // using filtered replication.
        SourceShards []SourceShard

        // Cells is the list of cells that have tablets for this shard.
        // It is populated at InitTablet time when a tabelt is added
        // in a cell that is not in the list yet.
        Cells []string
}

// SourceShard represents a data source for filtered replication
// across shards. When this is used in a destination shard, the master
// of that shard will run filtered replication.
type SourceShard struct {
        // Uid is the unique ID for this SourceShard object.
        // It is for instance used as a unique index in blp_checkpoint
        // when storing the position. It should be unique whithin a
        // destination Shard, but not globally unique.
        Uid uint32

        // the source keyspace
        Keyspace string

        // the source shard
        Shard string

        // The source shard keyrange
        // If partial, len(Tables) has to be zero
        KeyRange key.KeyRange

        // The source table list to replicate
        // If non-empty, KeyRange must not be partial (must be KeyRange{})
        Tables []string
}

```

```
$ zk ls /zk/global/vt/keyspaces/ruser/shards/10-20
action
actionlog
nyc-0000200278
```

We use the action and actionlog paths for locking only, no process is actively watching these paths.

```
$ zk cat /zk/global/vt/keyspaces/ruser/shards/10-20
{
  "MasterAlias": {
    "Cell": "nyc",
    "Uid": 200278
  },
  "KeyRange": {
    "Start": "10",
    "End": "20"
  },
 "Cells": [
    "oe",
    "yh"
 ]
}
```

The shard path and sub-directories are created when the first tablet in that shard is created.

The Shard object is changed when we add tablets in unknown cells, or when we change the master.

### Tablet

A tablet has a path in zookeeper, with its pid file:

```
$ zk ls /zk/nyc/vt/tablets/0000200308
pid
```

A tablet also has a node of type Tablet:

```go
// see go/vt/topo/tablet.go for latest version
type Tablet struct {
        Parent      TabletAlias // the globally unique alias for our replication parent - zero if this is the global master

        // What is this tablet?
        Alias TabletAlias

        // Locaiton of the tablet
        Hostname string
        IPAddr   string

        // Named port names. Currently supported ports: vt, vts,
        // mysql.
        Portmap map[string]int

        // Tags contain freeform information about the tablet.
        Tags map[string]string

        // Information about the tablet inside a keyspace/shard
        Keyspace string
        Shard    string
        Type     TabletType

        // Is the tablet read-only?
        State TabletState

        // Normally the database name is implied by "vt_" + keyspace. I
        // really want to remove this but there are some databases that are
        // hard to rename.
        DbNameOverride string
        KeyRange       key.KeyRange
}
```

```
$ zk cat /zk/nyc/vt/tablets/0000200308
{
  "Alias": {
    "Cell": "nyc",
    "Uid": 200308,
  },
  "Parent": {
    "Cell": "",
    "Uid": 0
  },
  "Keyspace": "",
  "Shard": "",
  "Type": "idle",
  "State": "ReadOnly",
  "DbNameOverride": "",
  "KeyRange": {
    "Start": "",
    "End": ""
  }
}
```

The Tablet object is created by 'vtctl InitTablet'. Up-to-date information (port numbers, ...) is maintained by the vttablet process. 'vtctl ChangeSlaveType' will also change the Tablet record.

## Replication Graph

The data maintained by vt tools is as follows:
- it is stored in the global zk cell
- the master tablet alias is stored in the Shard object
- each cell then has a ShardReplication object that stores to master -> slave pairs.

## Serving Graph

The serving graph for a shard is maintained in every cell that contains tablets for that shard. To get all the available keyspaces in a cell, just list the top-level cell serving graph directory:

```
$ zk ls /zk/nyc/vt/ns
keyspace1
keyspace2
```

The python client lists that directory at startup to find all the keyspaces.

### SrvKeyspace

The keyspace data is stored under `/zk/<cell>/vt/ns/<keyspace>`

```go
// see go/vt/topo/srvshard.go for latest version
type SrvShard struct {
        // Copied from Shard
        KeyRange    key.KeyRange
        ServedTypes []TabletType

        // TabletTypes represents the list of types we have serving tablets
        // for, in this cell only.
        TabletTypes []TabletType

        // For atomic updates
        version int64
}

// A distilled serving copy of keyspace detail stored in the local
// cell for fast access. Derived from the global keyspace, shards and
// local details.
// In zk, it is in /zk/local/vt/ns/<keyspace>
type SrvKeyspace struct {
        // Shards to use per type, only contains complete partitions.
        Partitions map[TabletType]*KeyspacePartition

        // List of available tablet types for this keyspace in this cell.
        // May not have a server for every shard, but we have some.
        TabletTypes []TabletType

        // Copied from Keyspace
        ShardingColumnName string
        ShardingColumnType key.KeyspaceIdType
        ServedFrom         map[TabletType]string

        // For atomic updates
        version int64
}

// KeyspacePartition represents a continuous set of shards to
// serve an entire data set.
type KeyspacePartition struct {
        // List of non-overlapping continuous shards sorted by range.
        Shards []SrvShard
}

```

```
$ zk cat /zk/nyc/vt/ns/rlookup
{
  "Shards": [
    {
      "KeyRange": {
        "Start": "",
        "End": ""
      },
    }
  ],
  "TabletTypes": [
    "master",
    "rdonly",
    "replica"
  ]
}
```

The only way to build this data is to run the following vtctl command:

```
$ vtctl RebuildKeyspaceGraph <keyspace>
```

When building a new Cell, this command should be run for every keyspace.

Rebuilding a keyspace graph will:
- find all the shard names in the keyspace from looking at the children of `/zk/global/vt/keyspaces/<keyspace>/shards`
- rebuild the graph for every shard in the keyspace (see below)
- find the list of cells to update by collecting the cells for each tablet of the first shard
- compute, sanity check and update the keyspace graph object in every cell

The python client reads the nodes to find the shard map (KeyRanges, TabletTypes, ...)

### SrvShard

The shard data is stored under `/zk/<cell>/vt/ns/<keyspace>/<shard>`

```
$ zk cat /zk/nyc/vt/ns/rlookup/0
{
  "KeyRange": {
    "Start": "",
    "End": ""
  }
}
```

### EndPoints

We also have per serving type data under `/zk/<cell>/vt/ns/<keyspace>/<shard>/<type>`

```
$ zk cat /zk/nyc/vt/ns/rlookup/0/master
{
  "entries": [
    {
      "uid": 200274,
      "host": "nyc-db274.nyc.youtube.com",
      "port": 0,
      "named_port_map": {
        "mysql": 3306,
        "vt": 8101,
        "vts": 8102
      }
    }
  ]
}
```

The shard serving graph can be re-built using the 'vtctl RebuildShardGraph <keyspace>/<shard>' command. However, it is also triggered by any 'vtctl ChangeSlaveType' command, so it is usually not needed. For instance, when vt_bns_monitor takes servers in and out of serving state, it will rebuild the shard graph.

Note this will rebuild the serving graph for all cells, not just one cell.

Rebuilding a shard serving graph will:
- compute the data to write by looking at all the tablets from the replicaton graph
- write all the `/zk/<cell>/vt/ns/<keyspace>/<shard>/<type>` nodes everywhere
- delete any pre-existing `/zk/<cell>/vt/ns/<keyspace>/<shard>/<type>` that is not in use any more
- compute and write all the `/zk/<cell>/vt/ns/<keyspace>/<shard>` nodes everywhere

The clients read the per-type data nodes to find servers to talk to. When resolving ruser.10-20.master, it will try to read /zk/local/vt/ns/ruser/10-20/master.
