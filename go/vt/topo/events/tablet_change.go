package events

import topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

// TabletChange is an event that describes changes to a tablet's topo record.
// It is triggered when the CURRENT process changes ANY tablet's record.
// It is NOT triggered when a DIFFERENT process changes THIS tablet's record.
// To be notified when THIS tablet's record changes, even if it was changed
// by a different process, listen for go/vt/tabletmanager/events.StateChange.
type TabletChange struct {
	Tablet topodatapb.Tablet
	Status string
}
