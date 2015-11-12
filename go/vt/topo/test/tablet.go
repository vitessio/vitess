// Package test contains utilities to test topo.Impl
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"encoding/json"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func tabletEqual(left, right *topodatapb.Tablet) (bool, error) {
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

// CheckTablet verifies the topo server API is correct for managing tablets.
func CheckTablet(ctx context.Context, t *testing.T, ts topo.Impl) {
	cell := getLocalCell(ctx, t, ts)
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: cell, Uid: 1},
		Hostname: "localhost",
		Ip:       "10.11.12.13",
		PortMap: map[string]int32{
			"vt":    3333,
			"mysql": 3334,
		},

		Tags:     map[string]string{"tag": "value"},
		Keyspace: "test_keyspace",
		Type:     topodatapb.TabletType_MASTER,
		KeyRange: newKeyRange("-10"),
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Errorf("CreateTablet: %v", err)
	}
	if err := ts.CreateTablet(ctx, tablet); err != topo.ErrNodeExists {
		t.Errorf("CreateTablet(again): %v", err)
	}

	if _, _, err := ts.GetTablet(ctx, &topodatapb.TabletAlias{Cell: cell, Uid: 666}); err != topo.ErrNoNode {
		t.Errorf("GetTablet(666): %v", err)
	}

	nt, nv, err := ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias, err)
	}
	if eq, err := tabletEqual(nt, tablet); err != nil {
		t.Errorf("cannot compare tablets: %v", err)
	} else if !eq {
		t.Errorf("put and got tablets are not identical:\n%#v\n%#v", tablet, t)
	}

	if _, err := ts.GetTabletsByCell(ctx, "666"); err != topo.ErrNoNode {
		t.Errorf("GetTabletsByCell(666): %v", err)
	}

	inCell, err := ts.GetTabletsByCell(ctx, cell)
	if err != nil {
		t.Errorf("GetTabletsByCell: %v", err)
	}
	if len(inCell) != 1 || *inCell[0] != *tablet.Alias {
		t.Errorf("GetTabletsByCell: want [%v], got %v", tablet.Alias, inCell)
	}

	nt.Hostname = "remotehost"
	if _, err := ts.UpdateTablet(ctx, nt, nv); err != nil {
		t.Errorf("UpdateTablet: %v", err)
	}

	nt, nv, err = ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias, err)
	}
	if want := "remotehost"; nt.Hostname != want {
		t.Errorf("nt.Hostname: want %v, got %v", want, nt.Hostname)
	}

	if _, err := ts.UpdateTabletFields(ctx, tablet.Alias, func(t *topodatapb.Tablet) error {
		t.Hostname = "anotherhost"
		return nil
	}); err != nil {
		t.Errorf("UpdateTabletFields: %v", err)
	}
	nt, nv, err = ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias, err)
	}

	if want := "anotherhost"; nt.Hostname != want {
		t.Errorf("nt.Hostname: want %v, got %v", want, nt.Hostname)
	}

	if err := ts.DeleteTablet(ctx, tablet.Alias); err != nil {
		t.Errorf("DeleteTablet: %v", err)
	}
	if err := ts.DeleteTablet(ctx, tablet.Alias); err != topo.ErrNoNode {
		t.Errorf("DeleteTablet(again): %v", err)
	}

	if _, _, err := ts.GetTablet(ctx, tablet.Alias); err != topo.ErrNoNode {
		t.Errorf("GetTablet: expected error, tablet was deleted: %v", err)
	}

}
