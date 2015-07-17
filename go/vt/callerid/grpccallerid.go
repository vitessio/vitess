package callerid

import (
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// GRPCImmediateCallerID creates new ImmediateCallerID structure
// from GRPC's VTGateCallerID
func GRPCImmediateCallerID(v *query.VTGateCallerID) *ImmediateCallerID {
	if v == nil {
		return nil
	}
	return NewImmediateCallerID(v.Username)
}

// GRPCEffectiveCallerID creates new EffectiveCallerID structure
// from GRPC's CallerID
func GRPCEffectiveCallerID(c *vtrpc.CallerID) *EffectiveCallerID {
	if c == nil {
		return nil
	}
	return NewEffectiveCallerID(c.Principal, c.Component, c.Subcomponent)
}
