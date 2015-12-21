package events

import (
	"github.com/youtube/vitess/go/vt/proto/topodatapb"
)

// ShardChange is an event that describes changes to a shard.
type ShardChange struct {
	KeyspaceName string
	ShardName    string
	Shard        *topodatapb.Shard
	Status       string
}
