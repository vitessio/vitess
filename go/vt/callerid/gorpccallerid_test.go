package callerid

import (
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

func TestGoRPCCallerID(t *testing.T) {
	im := proto.VTGateCallerID{
		Username: FakeUsername,
	}
	ef := proto.CallerID{
		Principal:    FakePrincipal,
		Component:    FakeComponent,
		Subcomponent: FakeSubcomponent,
	}
	// Test nil cases
	if n := GoRPCImmediateCallerID(nil); n != nil {
		t.Errorf("Expect nil from GoRPCImmediateCallerID(nil), but got %v", n)
	}
	if n := GoRPCEffectiveCallerID(nil); n != nil {
		t.Errorf("Expect nil from GoRPCEffectiveCallerID(nil), but got %v", n)
	}
	Tests(t, GoRPCImmediateCallerID(&im), GoRPCEffectiveCallerID(&ef))
}
