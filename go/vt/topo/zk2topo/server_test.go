package zk2topo

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
)

func TestZk2Topo(t *testing.T) {
	test.TopoServerTestSuite(t, func() topo.Impl {
		ts := NewFakeServer("test")
		return ts.Impl
	})
}
