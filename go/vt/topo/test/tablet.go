// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

func tabletEqual(left, right *topo.Tablet) (bool, error) {
	lj, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rj, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return string(lj) == string(rj), nil
}

func CheckTablet(t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)
	tablet := &topo.Tablet{
		Cell:        cell,
		Uid:         1,
		Parent:      topo.TabletAlias{},
		Addr:        "localhost:3333",
		MysqlAddr:   "localhost:3334",
		MysqlIpAddr: "10.11.12.13:3334",

		Alias:    topo.TabletAlias{Cell: cell, Uid: 1},
		Hostname: "localhost",
		IPAddr:   "10.11.12.13",
		Portmap: map[string]int{
			"vt":    3333,
			"mysql": 3334,
		},

		Tags:     map[string]string{"tag": "value"},
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
		State:    topo.STATE_READ_WRITE,
		KeyRange: newKeyRange("-10"),
	}
	if err := ts.CreateTablet(tablet); err != nil {
		t.Errorf("CreateTablet: %v", err)
	}
	if err := ts.CreateTablet(tablet); err != topo.ErrNodeExists {
		t.Errorf("CreateTablet(again): %v", err)
	}

	if _, err := ts.GetTablet(topo.TabletAlias{Cell: cell, Uid: 666}); err != topo.ErrNoNode {
		t.Errorf("GetTablet(666): %v", err)
	}

	ti, err := ts.GetTablet(tablet.Alias)
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias, err)
	}
	if eq, err := tabletEqual(ti.Tablet, tablet); err != nil {
		t.Errorf("cannot compare tablets: %v", err)
	} else if !eq {
		t.Errorf("put and got tablets are not identical:\n%#v\n%#v", tablet, ti.Tablet)
	}

	if _, err := ts.GetTabletsByCell("666"); err != topo.ErrNoNode {
		t.Errorf("GetTabletsByCell(666): %v", err)
	}

	inCell, err := ts.GetTabletsByCell(cell)
	if err != nil {
		t.Errorf("GetTabletsByCell: %v", err)
	}
	if len(inCell) != 1 || inCell[0] != tablet.Alias {
		t.Errorf("GetTabletsByCell: want [%v], got %v", tablet.Alias, inCell)
	}

	ti.State = topo.STATE_READ_ONLY
	if err := topo.UpdateTablet(ts, ti); err != nil {
		t.Errorf("UpdateTablet: %v", err)
	}

	ti, err = ts.GetTablet(tablet.Alias)
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias, err)
	}
	if want := topo.STATE_READ_ONLY; ti.State != want {
		t.Errorf("ti.State: want %v, got %v", want, ti.State)
	}

	if err := ts.UpdateTabletFields(tablet.Alias, func(t *topo.Tablet) error {
		t.State = topo.STATE_READ_WRITE
		return nil
	}); err != nil {
		t.Errorf("UpdateTabletFields: %v", err)
	}
	ti, err = ts.GetTablet(tablet.Alias)
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias, err)
	}

	if want := topo.STATE_READ_WRITE; ti.State != want {
		t.Errorf("ti.State: want %v, got %v", want, ti.State)
	}

	if err := ts.DeleteTablet(tablet.Alias); err != nil {
		t.Errorf("DeleteTablet: %v", err)
	}
	if err := ts.DeleteTablet(tablet.Alias); err != topo.ErrNoNode {
		t.Errorf("DeleteTablet(again): %v", err)
	}

	if _, err := ts.GetTablet(tablet.Alias); err != topo.ErrNoNode {
		t.Errorf("GetTablet: expected error, tablet was deleted: %v", err)
	}

}

func CheckPid(t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)
	tablet := &topo.Tablet{
		Cell: cell,
		Uid:  1,
		Addr: "localhost:3333",

		Alias:    topo.TabletAlias{Cell: cell, Uid: 1},
		Hostname: "localhost",
		Portmap: map[string]int{
			"vt": 3333,
		},

		Parent:   topo.TabletAlias{},
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
		State:    topo.STATE_READ_WRITE,
		KeyRange: newKeyRange("-10"),
	}
	if err := ts.CreateTablet(tablet); err != nil {
		t.Fatalf("CreateTablet: %v", err)
	}
	tabletAlias := topo.TabletAlias{Cell: cell, Uid: 1}

	done := make(chan struct{}, 1)
	if err := ts.CreateTabletPidNode(tabletAlias, "contents", done); err != nil {
		t.Errorf("ts.CreateTabletPidNode: %v", err)
	}

	// wait for up to ten seconds for the pid to appear
	timeout := 10
	for {
		err := ts.ValidateTabletPidNode(tabletAlias)
		if err == nil {
			// exists, we're good
			break
		}

		timeout -= 1
		if timeout == 0 {
			t.Fatalf("ts.ValidateTabletPidNode: %v", err)
		}
		time.Sleep(time.Second)
	}

	close(done)
}
