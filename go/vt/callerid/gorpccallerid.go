package callerid

import (
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

// GoRPCImmediateCallerID creates new ImmediateCallerID structure
// from GoRPC's VTGateCallerID
func GoRPCImmediateCallerID(v *proto.VTGateCallerID) *ImmediateCallerID {
	if v == nil {
		return nil
	}
	return NewImmediateCallerID(v.Username)
}

// GoRPCEffectiveCallerID creates new EffectiveCallerID structure
// from GoRPC's CallerID
func GoRPCEffectiveCallerID(c *proto.CallerID) *EffectiveCallerID {
	if c == nil {
		return nil
	}
	return NewEffectiveCallerID(c.Principal, c.Component, c.Subcomponent)
}
