package events

import (
	"github.com/henryanand/vitess/go/vt/topo"
)

// ShardChange is an event that describes changes to a shard.
type ShardChange struct {
	ShardInfo topo.ShardInfo
	Status    string
}
