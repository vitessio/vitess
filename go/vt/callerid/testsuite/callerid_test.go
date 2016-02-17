package testsuite

import (
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestFakeCallerID(t *testing.T) {
	im := querypb.VTGateCallerID{
		Username: FakeUsername,
	}
	ef := vtrpcpb.CallerID{
		Principal:    FakePrincipal,
		Component:    FakeComponent,
		Subcomponent: FakeSubcomponent,
	}
	RunTests(t, &im, &ef)
}
