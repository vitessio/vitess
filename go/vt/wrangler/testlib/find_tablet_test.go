/*
Copyright 2018 The Vitess Authors.

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
	"context"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestFindTablet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	wr := wrangler.New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old primary, two good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldPrimary.Tablet.Keyspace, []string{"cell1", "cell2"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// make sure we can find the tablets
	// even with a datacenter being down.
	tabletMap, err := ts.GetTabletMapForShardByCell(ctx, "test_keyspace", "0", []string{"cell1"})
	if err != nil {
		t.Fatalf("GetTabletMapForShardByCell should have worked but got: %v", err)
	}
	primary, err := topotools.FindTabletByHostAndPort(tabletMap, oldPrimary.Tablet.Hostname, "vt", oldPrimary.Tablet.PortMap["vt"])
	if err != nil || !topoproto.TabletAliasEqual(primary, oldPrimary.Tablet.Alias) {
		t.Fatalf("FindTabletByHostAndPort(primary) failed: %v %v", err, primary)
	}
	replica1, err := topotools.FindTabletByHostAndPort(tabletMap, goodReplica1.Tablet.Hostname, "vt", goodReplica1.Tablet.PortMap["vt"])
	if err != nil || !topoproto.TabletAliasEqual(replica1, goodReplica1.Tablet.Alias) {
		t.Fatalf("FindTabletByHostAndPort(replica1) failed: %v %v", err, primary)
	}
	replica2, err := topotools.FindTabletByHostAndPort(tabletMap, goodReplica2.Tablet.Hostname, "vt", goodReplica2.Tablet.PortMap["vt"])
	if !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("FindTabletByHostAndPort(replica2) worked: %v %v", err, replica2)
	}

	// Make sure the primary is not exported in other cells
	tabletMap, _ = ts.GetTabletMapForShardByCell(ctx, "test_keyspace", "0", []string{"cell2"})
	primary, err = topotools.FindTabletByHostAndPort(tabletMap, oldPrimary.Tablet.Hostname, "vt", oldPrimary.Tablet.PortMap["vt"])
	if !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("FindTabletByHostAndPort(primary) worked in cell2: %v %v", err, primary)
	}

	// Get tablet map for all cells.  If there were to be failures talking to local cells, this will return the tablet map
	// and forward a partial result error
	tabletMap, err = ts.GetTabletMapForShard(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetTabletMapForShard should nil but got: %v", err)
	}
	primary, err = topotools.FindTabletByHostAndPort(tabletMap, oldPrimary.Tablet.Hostname, "vt", oldPrimary.Tablet.PortMap["vt"])
	if err != nil || !topoproto.TabletAliasEqual(primary, oldPrimary.Tablet.Alias) {
		t.Fatalf("FindTabletByHostAndPort(primary) failed: %v %v", err, primary)
	}

}
