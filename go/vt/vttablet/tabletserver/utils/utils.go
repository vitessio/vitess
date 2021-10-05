package utils

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// IsServingType returns true if the tablet type is one that should be serving to be healthy, or false if the tablet type
// should not be serving in it's healthy state.
func IsServingType(tabletType topodatapb.TabletType) bool {
	switch tabletType {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_BATCH, topodatapb.TabletType_EXPERIMENTAL:
		return true
	default:
		return false
	}
}
