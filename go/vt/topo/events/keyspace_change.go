package events

import (
	"github.com/youtube/vitess/go/vt/proto/topodatapb"
)

// KeyspaceChange is an event that describes changes to a keyspace.
type KeyspaceChange struct {
	KeyspaceName string
	Keyspace     *topodatapb.Keyspace
	Status       string
}
