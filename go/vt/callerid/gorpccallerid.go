package callerid

import (
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

// GoRPCImmediateCallerID creates new ImmediateCallerID(querypb.VTGateCallerID)
// from GoRPC's VTGateCallerID
func GoRPCImmediateCallerID(v *proto.VTGateCallerID) *querypb.VTGateCallerID {
	if v == nil {
		return nil
	}
	return NewImmediateCallerID(v.Username)
}

// GoRPCEffectiveCallerID creates new EffectiveCallerID(vtpb.CallerID)
// from GoRPC's CallerID
func GoRPCEffectiveCallerID(c *proto.CallerID) *vtpb.CallerID {
	if c == nil {
		return nil
	}
	return NewEffectiveCallerID(c.Principal, c.Component, c.Subcomponent)
}
