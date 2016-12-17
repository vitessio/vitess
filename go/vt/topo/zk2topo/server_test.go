package zk2topo

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestZk2Topo(t *testing.T) {
	test.TopoServerTestSuite(t, func() topo.Impl {
		impl := NewFakeServer()
		ts := topo.Server{Impl: impl}
		ctx := context.Background()
		if err := ts.CreateCellInfo(ctx, "test", &topodatapb.CellInfo{
			Root: "/",
		}); err != nil {
			t.Fatalf("ts.CreateCellInfo failed: %v", err)
		}
		return impl
	})
}
