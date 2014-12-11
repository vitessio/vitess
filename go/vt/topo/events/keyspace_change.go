package events

import (
	"github.com/henryanand/vitess/go/vt/topo"
)

// KeyspaceChange is an event that describes changes to a keyspace.
type KeyspaceChange struct {
	KeyspaceInfo topo.KeyspaceInfo
	Status       string
}
