package events

import (
	base "vitess.io/vitess/go/vt/events"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// MigrateServedFrom is an event that describes a single step in the process of
// adding or removing a forwarding rule to have certain ServedTypes served by
// another keyspace.
type MigrateServedFrom struct {
	base.StatusUpdater

	KeyspaceName     string
	SourceShard      topo.ShardInfo
	DestinationShard topo.ShardInfo
	ServedType       topodatapb.TabletType
	Reverse          bool
}

// MigrateServedTypes is an event that describes a single step in the process of
// switching a ServedType from one set of shards to another.
type MigrateServedTypes struct {
	base.StatusUpdater

	KeyspaceName      string
	SourceShards      []*topo.ShardInfo
	DestinationShards []*topo.ShardInfo
	ServedType        topodatapb.TabletType
	Reverse           bool
}
