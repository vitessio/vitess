package callerid

import (
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// GRPCImmediateCallerID creates new ImmediateCallerID structure
// by type-conversion from GRPC's VTGateCallerID
func GRPCImmediateCallerID(v *query.VTGateCallerID) *ImmediateCallerID {
	return (*ImmediateCallerID)(v)
}

// GRPCEffectiveCallerID creates new EffectiveCallerID structure
// from GRPC's CallerID by type-conversion from GRPC's CallerID
func GRPCEffectiveCallerID(c *vtrpc.CallerID) *EffectiveCallerID {
	return (*EffectiveCallerID)(c)
}
