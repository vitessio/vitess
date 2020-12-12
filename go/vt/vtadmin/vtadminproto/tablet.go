package vtadminproto

import vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"

// ParseTabletServingState returns a ServingState value from the given string.
// If the string does not map to a valid value, this function returns UNKNOWN.
func ParseTabletServingState(state string) vtadminpb.Tablet_ServingState {
	if s, ok := vtadminpb.Tablet_ServingState_value[state]; ok {
		return vtadminpb.Tablet_ServingState(s)
	}

	return vtadminpb.Tablet_UNKNOWN
}
