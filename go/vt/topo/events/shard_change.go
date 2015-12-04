package events

import (
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ShardChange is an event that describes changes to a shard.
type ShardChange struct {
	KeyspaceName string
	ShardName    string
	Shard        *topodatapb.Shard
	Status       string
}
