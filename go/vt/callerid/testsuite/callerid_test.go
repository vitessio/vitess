package testsuite

import (
	"testing"

	"github.com/gitql/vitess/go/vt/callerid"
	querypb "github.com/gitql/vitess/go/vt/proto/query"
	vtrpcpb "github.com/gitql/vitess/go/vt/proto/vtrpc"
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
	RunTests(t, &im, &ef, callerid.NewContext)
}
