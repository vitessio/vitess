package gorpccallerid

import (
	"testing"

	"github.com/youtube/vitess/go/vt/callerid/testsuite"
)

func TestGoRPCCallerID(t *testing.T) {
	im := VTGateCallerID{
		Username: testsuite.FakeUsername,
	}
	ef := CallerID{
		Principal:    testsuite.FakePrincipal,
		Component:    testsuite.FakeComponent,
		Subcomponent: testsuite.FakeSubcomponent,
	}
	// Test nil cases
	if n := GoRPCImmediateCallerID(nil); n != nil {
		t.Errorf("Expect nil from GoRPCImmediateCallerID(nil), but got %v", n)
	}
	if n := GoRPCEffectiveCallerID(nil); n != nil {
		t.Errorf("Expect nil from GoRPCEffectiveCallerID(nil), but got %v", n)
	}
	testsuite.RunTests(t, GoRPCImmediateCallerID(&im), GoRPCEffectiveCallerID(&ef))
}
