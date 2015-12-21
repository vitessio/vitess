package testsuite

import (
	"testing"

	"github.com/youtube/vitess/go/vt/proto/querypb"
	"github.com/youtube/vitess/go/vt/proto/vtrpcpb"
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
