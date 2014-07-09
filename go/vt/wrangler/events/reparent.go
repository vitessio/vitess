// Package events defines the structures used for events dispatched from the
// wrangler package.
package events

import (
	"time"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/topo"
)

// Reparent is an event that describes a single step in the reparent process.
type Reparent struct {
	ShardInfo topo.ShardInfo

	OldMaster, NewMaster topo.Tablet

	Status string

	// eventID is used to group the steps of a single reparent in progress.
	// It is set internally the first time UpdateStatus() is called.
	eventID int64
}

// UpdateStatus sets a new status and then dispatches the event.
func (r *Reparent) UpdateStatus(status string) {
	r.Status = status

	// initialize event ID
	if r.eventID == 0 {
		r.eventID = time.Now().UnixNano()
	}

	// Dispatch must be synchronous here to avoid dropping events that are
	// queued up just before main() returns.
	event.Dispatch(r)
}
