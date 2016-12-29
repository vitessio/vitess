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

// checkTablet verifies the topo server API is correct for managing tablets.
func checkTablet(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	tts := topo.Server{Impl: ts}

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
		t.Fatalf("CreateTablet: %v", err)
	}
	if err := ts.CreateTablet(ctx, tablet); err != topo.ErrNodeExists {
		t.Fatalf("CreateTablet(again): %v", err)
	}

	if _, _, err := ts.GetTablet(ctx, &topodatapb.TabletAlias{Cell: cell, Uid: 666}); err != topo.ErrNoNode {
		t.Fatalf("GetTablet(666): %v", err)
	}

	nt, nv, err := ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet %v: %v", tablet.Alias, err)
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
		t.Fatalf("GetTabletsByCell: %v", err)
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
		t.Fatalf("GetTablet %v: %v", tablet.Alias, err)
	}
	if want := "remotehost"; nt.Hostname != want {
		t.Errorf("nt.Hostname: want %v, got %v", want, nt.Hostname)
	}

	// unconditional tablet update
	nt.Hostname = "remotehost2"
	if _, err := ts.UpdateTablet(ctx, nt, -1); err != nil {
		t.Errorf("UpdateTablet(-1): %v", err)
	}

	nt, nv, err = ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet %v: %v", tablet.Alias, err)
	}
	if want := "remotehost2"; nt.Hostname != want {
		t.Errorf("nt.Hostname: want %v, got %v", want, nt.Hostname)
	}

	// test UpdateTabletFields works
	updatedTablet, err := tts.UpdateTabletFields(ctx, tablet.Alias, func(t *topodatapb.Tablet) error {
		t.Hostname = "anotherhost"
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateTabletFields: %v", err)
	}
	if got, want := updatedTablet.Hostname, "anotherhost"; got != want {
		t.Errorf("updatedTablet.Hostname = %q, want %q", got, want)
	}
	nt, nv, err = ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet %v: %v", tablet.Alias, err)
	}
	if got, want := nt.Hostname, "anotherhost"; got != want {
		t.Errorf("nt.Hostname = %q, want %q", got, want)
	}

	// test UpdateTabletFields that returns ErrNoUpdateNeeded works
	if _, err := tts.UpdateTabletFields(ctx, tablet.Alias, func(t *topodatapb.Tablet) error {
		return topo.ErrNoUpdateNeeded
	}); err != nil {
		t.Errorf("UpdateTabletFields: %v", err)
	}
	if nnt, nnv, nnerr := ts.GetTablet(ctx, tablet.Alias); nnv != nv {
		t.Fatalf("GetTablet %v: %v %v %v", tablet.Alias, nnt, nnv, nnerr)
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
