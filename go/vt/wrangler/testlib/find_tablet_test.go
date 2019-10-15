/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testlib

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestFindTablet(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, two good slaves
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	goodSlave1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodSlave2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1", "cell2"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// make sure we can find the tablets
	// even with a datacenter being down.
	tabletMap, err := ts.GetTabletMapForShardByCell(ctx, "test_keyspace", "0", []string{"cell1"})
	if err != nil {
		t.Fatalf("GetTabletMapForShardByCell should have worked but got: %v", err)
	}
	master, err := topotools.FindTabletByHostAndPort(tabletMap, oldMaster.Tablet.Hostname, "vt", oldMaster.Tablet.PortMap["vt"])
	if err != nil || !topoproto.TabletAliasEqual(master, oldMaster.Tablet.Alias) {
		t.Fatalf("FindTabletByHostAndPort(master) failed: %v %v", err, master)
	}
	slave1, err := topotools.FindTabletByHostAndPort(tabletMap, goodSlave1.Tablet.Hostname, "vt", goodSlave1.Tablet.PortMap["vt"])
	if err != nil || !topoproto.TabletAliasEqual(slave1, goodSlave1.Tablet.Alias) {
		t.Fatalf("FindTabletByHostAndPort(slave1) failed: %v %v", err, master)
	}
	slave2, err := topotools.FindTabletByHostAndPort(tabletMap, goodSlave2.Tablet.Hostname, "vt", goodSlave2.Tablet.PortMap["vt"])
	if !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("FindTabletByHostAndPort(slave2) worked: %v %v", err, slave2)
	}

	// Make sure the master is not exported in other cells
	tabletMap, _ = ts.GetTabletMapForShardByCell(ctx, "test_keyspace", "0", []string{"cell2"})
	master, err = topotools.FindTabletByHostAndPort(tabletMap, oldMaster.Tablet.Hostname, "vt", oldMaster.Tablet.PortMap["vt"])
	if !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("FindTabletByHostAndPort(master) worked in cell2: %v %v", err, master)
	}

	// Get tablet map for all cells.  If there were to be failures talking to local cells, this will return the tablet map
	// and forward a partial result error
	tabletMap, err = ts.GetTabletMapForShard(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetTabletMapForShard should nil but got: %v", err)
	}
	master, err = topotools.FindTabletByHostAndPort(tabletMap, oldMaster.Tablet.Hostname, "vt", oldMaster.Tablet.PortMap["vt"])
	if err != nil || !topoproto.TabletAliasEqual(master, oldMaster.Tablet.Alias) {
		t.Fatalf("FindTabletByHostAndPort(master) failed: %v %v", err, master)
	}

}
