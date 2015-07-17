package callerid

import (
	"testing"

	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestGRPCCallerID(t *testing.T) {
	im := query.VTGateCallerID{
		Username: FakeUsername,
	}
	ef := vtrpc.CallerID{
		Principal:    FakePrincipal,
		Component:    FakeComponent,
		Subcomponent: FakeSubcomponent,
	}
	// Test nil cases
	if n := GRPCImmediateCallerID(nil); n != nil {
		t.Errorf("Expect nil from GRPCImmediateCallerID(nil), but got %v", n)
	}
	if n := GRPCEffectiveCallerID(nil); n != nil {
		t.Errorf("Expect nil from GRPCEffectiveCallerID(nil), but got %v", n)
	}
	Tests(t, GRPCImmediateCallerID(&im), GRPCEffectiveCallerID(&ef))
}
