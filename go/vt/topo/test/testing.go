// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

// PrepareTSFunc is a function that returns a fully functional
// topo.Server, that should contain at least two cells, one of which
// is global.
type PrepareTSFunc func(t *testing.T) topo.Server

// CheckFunc is a function that tests the implementation of a fragment
// of the topo.Server API.
type CheckFunc func(t *testing.T, ts topo.Server)

// CheckKeyspace runs test
func CheckKeyspace(t *testing.T, ts topo.Server) {
	if err := ts.CreateKeyspace("test_keyspace"); err != nil {
		t.Errorf("%v CreateKeyspace: %v", err)
	}

	keyspaces, err := ts.GetKeyspaces()
	if err != nil {
		t.Errorf("%v GetKeyspaces: %v", ts, err)
	}
	if len(keyspaces) != 1 && keyspaces[0] != "test_keyspace" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace"}, keyspaces)
	}
}

func CheckTablet(t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)
	tablet := &topo.Tablet{
		Cell:     cell,
		Uid:      1,
		Parent:   topo.TabletAlias{},
		Addr:     "localhost:3333",
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
		State:    topo.STATE_READ_WRITE,
		KeyRange: key.KeyRange{Start: "-", End: "10"},
	}
	if err := ts.CreateTablet(tablet); err != nil {
		t.Errorf("ts.CreateTablet: %v")
	}

	ti, err := ts.GetTablet(tablet.Alias())
	if err != nil {
		t.Errorf("ts.GetTablet %v: %v", tablet.Alias(), err)
	}

	if ti.Cell != tablet.Cell || ti.Uid != tablet.Uid || ti.State != tablet.State {
		t.Errorf("put and got tablets are not identical: %v\n%v", tablet, ti)
	}

	inCell, err := ts.GetTabletsByCell(cell)
	if err != nil {
		t.Errorf("ts.GetTabletsByCell: %v", err)
	}
	if len(inCell) != 1 || inCell[0] != tablet.Alias() {
		t.Errorf("ts.GetTabletsByCell: want [%v], got %v", tablet.Alias(), inCell)
	}

	ti.State = topo.STATE_READ_ONLY
	if _, err := ts.UpdateTablet(ti, -1); err != nil {
		t.Errorf("ts.UpdateTablet: %v", err)
	}

	ti, err = ts.GetTablet(tablet.Alias())
	if err != nil {
		t.Errorf("ts.GetTablet %v: %v", tablet.Alias(), err)
	}
	if want := topo.STATE_READ_ONLY; ti.State != want {
		t.Errorf("ti.State: want %v, got %v", want, ti.State)
	}

	if err := ts.UpdateTabletFields(tablet.Alias(), func(t *topo.Tablet) error {
		t.State = topo.STATE_READ_WRITE
		return nil
	}); err != nil {
		t.Errorf("ts.UpdateTabletFields: %v", err)
	}
	ti, err = ts.GetTablet(tablet.Alias())
	if err != nil {
		t.Errorf("ts.GetTablet %v: %v", tablet.Alias(), err)
	}

	if want := topo.STATE_READ_WRITE; ti.State != want {
		t.Errorf("ti.State: want %v, got %v", want, ti.State)
	}

	if err := ts.DeleteTablet(tablet.Alias()); err != nil {
		t.Errorf("ts.DeleteTablet: %v", err)
	}

	if _, err := ts.GetTablet(tablet.Alias()); err == nil {
		t.Errorf("ts.GetTablet: expected error, tablet was deleted")
	}

}

func getLocalCell(t *testing.T, ts topo.Server) string {
	cells, err := ts.GetKnownCells()
	if err != nil {
		t.Fatalf("ts.GetKnownCells: %v", err)
	}
	if len(cells) < 1 {
		t.Fatalf("provided topo.Server doesn't have enough cells (need at least 1): %v", cells)
	}
	return cells[0]
}

// AllChecks is a list of functions that are called by CheckAll.
var AllChecks = []CheckFunc{
	CheckKeyspace,
	CheckTablet,
}

// CheckAll runs all available checks. For each check, a fresh
// topo.Server is created by calling prepareTS. The server will be
// closed after running the check.
func CheckAll(t *testing.T, prepareTS PrepareTSFunc) {
	for _, checkFunc := range AllChecks {
		func() {
			ts := prepareTS(t)
			defer ts.Close()
			checkFunc(t, ts)
		}()
	}
}
