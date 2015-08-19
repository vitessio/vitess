package events

import (
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ShardChange is an event that describes changes to a shard.
type ShardChange struct {
	KeyspaceName string
	ShardName    string
	Shard        *pb.Shard
	Status       string
}
