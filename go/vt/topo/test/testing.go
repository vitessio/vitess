// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"

	"github.com/henryanand/vitess/go/vt/key"
	"github.com/henryanand/vitess/go/vt/topo"
)

func newKeyRange(value string) key.KeyRange {
	_, result, err := topo.ValidateShardName(value)
	if err != nil {
		panic(err)
	}
	return result
}

func getLocalCell(t *testing.T, ts topo.Server) string {
	cells, err := ts.GetKnownCells()
	if err != nil {
		t.Fatalf("GetKnownCells: %v", err)
	}
	if len(cells) < 1 {
		t.Fatalf("provided topo.Server doesn't have enough cells (need at least 1): %v", cells)
	}
	return cells[0]
}
