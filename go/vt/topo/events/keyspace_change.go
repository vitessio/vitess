package events

import (
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// KeyspaceChange is an event that describes changes to a keyspace.
type KeyspaceChange struct {
	KeyspaceName string
	Keyspace     *topodatapb.Keyspace
	Status       string
}
