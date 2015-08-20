package events

import (
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// KeyspaceChange is an event that describes changes to a keyspace.
type KeyspaceChange struct {
	KeyspaceName string
	Keyspace     *pb.Keyspace
	Status       string
}
