package helpers

import (
	"testing"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/test"
)

func TestTeeTopo(t *testing.T) {
	test.TopoServerTestSuite(t, func() *topo.Server {
		s1 := memorytopo.NewServer(test.LocalCellName)
		s2 := memorytopo.NewServer(test.LocalCellName)
		tee, err := NewTee(s1, s2, false)
		if err != nil {
			t.Fatalf("NewTee() failed: %v", err)
		}
		return tee
	})
}
