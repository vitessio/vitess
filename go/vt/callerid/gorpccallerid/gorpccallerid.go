package gorpccallerid

import (
	"github.com/youtube/vitess/go/vt/callerid"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// CallerID is the BSON implementation of the proto3 vtrpc.CallerID
type CallerID struct {
	Principal    string
	Component    string
	Subcomponent string
}

// VTGateCallerID is the BSON implementation of the proto3 query.VTGateCallerID
type VTGateCallerID struct {
	Username string
}

// GoRPCImmediateCallerID creates new ImmediateCallerID(querypb.VTGateCallerID)
// from GoRPC's VTGateCallerID
func GoRPCImmediateCallerID(v *VTGateCallerID) *querypb.VTGateCallerID {
	if v == nil {
		return nil
	}
	return callerid.NewImmediateCallerID(v.Username)
}

// GoRPCEffectiveCallerID creates new EffectiveCallerID(vtrpcpb.CallerID)
// from GoRPC's CallerID
func GoRPCEffectiveCallerID(c *CallerID) *vtrpcpb.CallerID {
	if c == nil {
		return nil
	}
	return callerid.NewEffectiveCallerID(c.Principal, c.Component, c.Subcomponent)
}
