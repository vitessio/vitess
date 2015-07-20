package callerid

import (
	"testing"

	qrpb "github.com/youtube/vitess/go/vt/proto/query"
	vtpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestFakeCallerID(t *testing.T) {
	im := qrpb.VTGateCallerID{
		Username: FakeUsername,
	}
	ef := vtpb.CallerID{
		Principal:    FakePrincipal,
		Component:    FakeComponent,
		Subcomponent: FakeSubcomponent,
	}
	Tests(t, &im, &ef)
}
