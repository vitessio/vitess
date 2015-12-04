package callerid

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
	Tests(t, &im, &ef)
}
