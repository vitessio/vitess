/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package test

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// checkTablet verifies the topo server API is correct for managing tablets.
func checkTablet(t *testing.T, ts *topo.Server) {
	ctx := context.Background()

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: LocalCellName,
			Uid:  1,
		},
		Hostname:      "localhost",
		MysqlHostname: "localhost",
		PortMap: map[string]int32{
			"vt": 3333,
		},

		Tags:     map[string]string{"tag": "value"},
		Keyspace: "test_keyspace",
		Type:     topodatapb.TabletType_MASTER,
		KeyRange: newKeyRange("-10"),
	}
	topoproto.SetMysqlPort(tablet, 3334)
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet: %v", err)
	}
	if err := ts.CreateTablet(ctx, tablet); !topo.IsErrType(err, topo.NodeExists) {
		t.Fatalf("CreateTablet(again): %v", err)
	}

	if _, err := ts.GetTablet(ctx, &topodatapb.TabletAlias{
		Cell: LocalCellName,
		Uid:  666,
	}); !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("GetTablet(666): %v", err)
	}

	ti, err := ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet %v: %v", tablet.Alias, err)
	}
	if !proto.Equal(ti.Tablet, tablet) {
		t.Errorf("put and got tablets are not identical:\n%#v\n%#v", tablet, t)
	}

	if _, err := ts.GetTabletsByCell(ctx, "666"); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("GetTabletsByCell(666): %v", err)
	}

	inCell, err := ts.GetTabletsByCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("GetTabletsByCell: %v", err)
	}
	if len(inCell) != 1 || !proto.Equal(inCell[0], tablet.Alias) {
		t.Errorf("GetTabletsByCell: want [%v], got %v", tablet.Alias, inCell)
	}

	ti.Tablet.Hostname = "remotehost"
	if err := ts.UpdateTablet(ctx, ti); err != nil {
		t.Errorf("UpdateTablet: %v", err)
	}

	ti, err = ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet %v: %v", tablet.Alias, err)
	}
	if want := "remotehost"; ti.Tablet.Hostname != want {
		t.Errorf("nt.Hostname: want %v, got %v", want, ti.Tablet.Hostname)
	}

	// test UpdateTabletFields works
	updatedTablet, err := ts.UpdateTabletFields(ctx, tablet.Alias, func(t *topodatapb.Tablet) error {
		t.Hostname = "anotherhost"
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateTabletFields: %v", err)
	}
	if got, want := updatedTablet.Hostname, "anotherhost"; got != want {
		t.Errorf("updatedTablet.Hostname = %q, want %q", got, want)
	}
	ti, err = ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet %v: %v", tablet.Alias, err)
	}
	if got, want := ti.Tablet.Hostname, "anotherhost"; got != want {
		t.Errorf("nt.Hostname = %q, want %q", got, want)
	}

	// test UpdateTabletFields that returns ErrNoUpdateNeeded works
	if _, err := ts.UpdateTabletFields(ctx, tablet.Alias, func(t *topodatapb.Tablet) error {
		return topo.NewError(topo.NoUpdateNeeded, tablet.Alias.String())
	}); err != nil {
		t.Errorf("UpdateTabletFields: %v", err)
	}
	if nti, nerr := ts.GetTablet(ctx, tablet.Alias); nti.Version() != ti.Version() {
		t.Fatalf("GetTablet %v: %v %v", tablet.Alias, nti, nerr)
	}

	if want := "anotherhost"; ti.Tablet.Hostname != want {
		t.Errorf("nt.Hostname: want %v, got %v", want, ti.Tablet.Hostname)
	}

	if err := ts.DeleteTablet(ctx, tablet.Alias); err != nil {
		t.Errorf("DeleteTablet: %v", err)
	}
	if err := ts.DeleteTablet(ctx, tablet.Alias); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("DeleteTablet(again): %v", err)
	}

	if _, err := ts.GetTablet(ctx, tablet.Alias); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("GetTablet: expected error, tablet was deleted: %v", err)
	}

}
