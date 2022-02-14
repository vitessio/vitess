package testutil

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// TopodataTabletsFromVTAdminTablets returns a slice of topodatapb.Tablet
// objects from a slice of vtadminpb.Tablet objects. It is the equivalent of
//
//		map(func(t *vtadminpb.Tablet) (*topodatapb.Tablet) { return t.Tablet }, tablets)
//
func TopodataTabletsFromVTAdminTablets(tablets []*vtadminpb.Tablet) []*topodatapb.Tablet {
	results := make([]*topodatapb.Tablet, len(tablets))

	for i, tablet := range tablets {
		results[i] = tablet.Tablet
	}

	return results
}
