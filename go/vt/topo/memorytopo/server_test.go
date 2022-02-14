package memorytopo

import (
	"testing"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"
)

func TestMemoryTopo(t *testing.T) {
	// Run the TopoServerTestSuite tests.
	test.TopoServerTestSuite(t, func() *topo.Server {
		return NewServer(test.LocalCellName)
	})
}
