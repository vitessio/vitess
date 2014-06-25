// Package events defines the structures used for events dispatched from the
// wrangler package.
package events

import (
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/topo"
)

// Reparent is an event that describes a single step in the reparent process.
type Reparent struct {
	ShardInfo topo.ShardInfo

	OldMaster, NewMaster topo.Tablet

	Status string
}

// UpdateStatus sets a new status and then dispatches the event.
func (r *Reparent) UpdateStatus(status string) {
	r.Status = status

	// make a copy since we're calling Dispatch asynchronously
	ev := *r
	go event.Dispatch(&ev)
}
