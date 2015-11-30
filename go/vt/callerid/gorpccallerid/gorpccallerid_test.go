package gorpccallerid

import (
	"testing"

	"github.com/youtube/vitess/go/vt/callerid"
)

func TestGoRPCCallerID(t *testing.T) {
	im := VTGateCallerID{
		Username: callerid.FakeUsername,
	}
	ef := CallerID{
		Principal:    callerid.FakePrincipal,
		Component:    callerid.FakeComponent,
		Subcomponent: callerid.FakeSubcomponent,
	}
	// Test nil cases
	if n := GoRPCImmediateCallerID(nil); n != nil {
		t.Errorf("Expect nil from GoRPCImmediateCallerID(nil), but got %v", n)
	}
	if n := GoRPCEffectiveCallerID(nil); n != nil {
		t.Errorf("Expect nil from GoRPCEffectiveCallerID(nil), but got %v", n)
	}
	callerid.Tests(t, GoRPCImmediateCallerID(&im), GoRPCEffectiveCallerID(&ef))
}
