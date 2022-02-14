// Package events defines the structures used for events dispatched from the
// wrangler package.
package events

import (
	base "vitess.io/vitess/go/vt/events"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Reparent is an event that describes a single step in the reparent process.
type Reparent struct {
	base.StatusUpdater

	ShardInfo              topo.ShardInfo
	OldPrimary, NewPrimary *topodatapb.Tablet
	ExternalID             string
}
