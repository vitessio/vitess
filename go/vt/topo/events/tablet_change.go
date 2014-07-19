package events

import (
	"github.com/youtube/vitess/go/vt/topo"
)

// TabletChange is an event that describes changes to a tablet.
type TabletChange struct {
	Tablet topo.Tablet
	Status string
}
