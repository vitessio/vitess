package events

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// ShardChange is an event that describes changes to a shard.
type ShardChange struct {
	KeyspaceName string
	ShardName    string
	Shard        *topodatapb.Shard
	Status       string
}
