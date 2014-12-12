// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"testing"
	"time"

	"code.google.com/p/go.net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
)

func TestTabletExternallyReparented(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, time.Minute, time.Second)

	// Create an old master, a new master, two good slaves, one bad slave
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))
	goodSlave1 := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))
	goodSlave2 := NewFakeTablet(t, wr, "cell2", 3, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))
	badSlave := NewFakeTablet(t, wr, "cell1", 4, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))

	// Add a new Cell to the Shard, that doesn't map to any read topo cell,
	// to simulate a data center being unreachable.
	si, err := ts.GetShard("test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.Cells = append(si.Cells, "cell666")
	if err := topo.UpdateShard(ctx, ts, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}

	// Slightly unrelated test: make sure we can find the tablets
	// even with a datacenter being down.
	tabletMap, err := topo.GetTabletMapForShardByCell(ctx, ts, "test_keyspace", "0", []string{"cell1"})
	if err != nil {
		t.Fatalf("GetTabletMapForShardByCell should have worked but got: %v", err)
	}
	master, err := topotools.FindTabletByIPAddrAndPort(tabletMap, oldMaster.Tablet.IPAddr, "vt", oldMaster.Tablet.Portmap["vt"])
	if err != nil || master != oldMaster.Tablet.Alias {
		t.Fatalf("FindTabletByIPAddrAndPort(master) failed: %v %v", err, master)
	}
	slave1, err := topotools.FindTabletByIPAddrAndPort(tabletMap, goodSlave1.Tablet.IPAddr, "vt", goodSlave1.Tablet.Portmap["vt"])
	if err != nil || slave1 != goodSlave1.Tablet.Alias {
		t.Fatalf("FindTabletByIPAddrAndPort(slave1) failed: %v %v", err, master)
	}
	slave2, err := topotools.FindTabletByIPAddrAndPort(tabletMap, goodSlave2.Tablet.IPAddr, "vt", goodSlave2.Tablet.Portmap["vt"])
	if err != topo.ErrNoNode {
		t.Fatalf("FindTabletByIPAddrAndPort(slave2) worked: %v %v", err, slave2)
	}

	// Make sure the master is not exported in other cells
	tabletMap, err = topo.GetTabletMapForShardByCell(ctx, ts, "test_keyspace", "0", []string{"cell2"})
	master, err = topotools.FindTabletByIPAddrAndPort(tabletMap, oldMaster.Tablet.IPAddr, "vt", oldMaster.Tablet.Portmap["vt"])
	if err != topo.ErrNoNode {
		t.Fatalf("FindTabletByIPAddrAndPort(master) worked in cell2: %v %v", err, master)
	}

	tabletMap, err = topo.GetTabletMapForShard(ctx, ts, "test_keyspace", "0")
	if err != topo.ErrPartialResult {
		t.Fatalf("GetTabletMapForShard should have returned ErrPartialResult but got: %v", err)
	}
	master, err = topotools.FindTabletByIPAddrAndPort(tabletMap, oldMaster.Tablet.IPAddr, "vt", oldMaster.Tablet.Portmap["vt"])
	if err != nil || master != oldMaster.Tablet.Alias {
		t.Fatalf("FindTabletByIPAddrAndPort(master) failed: %v %v", err, master)
	}

	// On the elected master, we will respond to
	// TABLET_ACTION_SLAVE_WAS_PROMOTED
	newMaster.FakeMysqlDaemon.MasterAddr = ""
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	oldMaster.FakeMysqlDaemon.MasterAddr = newMaster.Tablet.MysqlIpAddr()
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// On the good slaves, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	goodSlave1.FakeMysqlDaemon.MasterAddr = newMaster.Tablet.MysqlIpAddr()
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	goodSlave2.FakeMysqlDaemon.MasterAddr = newMaster.Tablet.MysqlIpAddr()
	goodSlave2.StartActionLoop(t, wr)
	defer goodSlave2.StopActionLoop(t)

	// On the bad slave, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED with bad data.
	badSlave.FakeMysqlDaemon.MasterAddr = "234.0.0.1:3301"
	badSlave.StartActionLoop(t, wr)
	defer badSlave.StopActionLoop(t)

	// First test: reparent to the same master, make sure it works
	// as expected.
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if err := tmc.TabletExternallyReparented(wr.Context(), ti, ""); err != nil {
		t.Fatalf("TabletExternallyReparented(same master) should have worked")
	}

	// Second test: reparent to a replica, and pretend the old
	// master is still good to go.

	// This tests a bad case; the new designated master is a slave,
	// but we should do what we're told anyway
	ti, err = ts.GetTablet(goodSlave1.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if err := tmc.TabletExternallyReparented(wr.Context(), ti, ""); err != nil {
		t.Fatalf("TabletExternallyReparented(slave) error: %v", err)
	}

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	ti, err = ts.GetTablet(newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if err := tmc.TabletExternallyReparented(wr.Context(), ti, ""); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// Now double-check the serving graph is good.
	// Should only have one good replica left.
	addrs, err := ts.GetEndPoints("cell1", "test_keyspace", "0", topo.TYPE_REPLICA)
	if err != nil {
		t.Fatalf("GetEndPoints failed at the end: %v", err)
	}
	if len(addrs.Entries) != 1 {
		t.Fatalf("GetEndPoints has too many entries: %v", addrs)
	}
}

// TestTabletExternallyReparentedWithDifferentMysqlPort makes sure
// that if mysql is restarted on the master-elect tablet and has a different
// port, we pick it up correctly.
func TestTabletExternallyReparentedWithDifferentMysqlPort(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, time.Minute, time.Second)

	// Create an old master, a new master, two good slaves, one bad slave
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))
	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))

	// Now we're restarting mysql on a different port, 3301->3303
	// but without updating the Tablet record in topology.

	// On the elected master, we will respond to
	// TABLET_ACTION_SLAVE_WAS_PROMOTED, so we need a MysqlDaemon
	// that returns no master, and the new port (as returned by mysql)
	newMaster.FakeMysqlDaemon.MasterAddr = ""
	newMaster.FakeMysqlDaemon.MysqlPort = 3303
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED and point to the new mysql port
	oldMaster.FakeMysqlDaemon.MasterAddr = "101.0.0.1:3303"
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// On the good slaves, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED and point to the new mysql port
	goodSlave.FakeMysqlDaemon.MasterAddr = "101.0.0.1:3303"
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if err := tmc.TabletExternallyReparented(wr.Context(), ti, ""); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
}

// TestTabletExternallyReparentedContinueOnUnexpectedMaster makes sure
// that we ignore mysql's master if the flag is set
func TestTabletExternallyReparentedContinueOnUnexpectedMaster(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, time.Minute, time.Second)

	// Create an old master, a new master, two good slaves, one bad slave
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))
	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))

	// On the elected master, we will respond to
	// TABLET_ACTION_SLAVE_WAS_PROMOTED, so we need a MysqlDaemon
	// that returns no master, and the new port (as returned by mysql)
	newMaster.FakeMysqlDaemon.MasterAddr = ""
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED and point to a bad host
	oldMaster.FakeMysqlDaemon.MasterAddr = "1.2.3.4:6666"
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// On the good slave, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED and point to a bad host
	goodSlave.FakeMysqlDaemon.MasterAddr = "1.2.3.4:6666"
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if err := tmc.TabletExternallyReparented(wr.Context(), ti, ""); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
}

func TestTabletExternallyReparentedFailedOldMaster(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, time.Minute, time.Second)

	// Create an old master, a new master, two good slaves
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))
	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA,
		TabletParent(oldMaster.Tablet.Alias))

	// Reparent to a replica, and pretend the old master is not responding

	// On the elected master, we will respond to
	// TABLET_ACTION_SLAVE_WAS_PROMOTED
	newMaster.FakeMysqlDaemon.MasterAddr = ""
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only get a
	// TABLET_ACTION_SLAVE_WAS_RESTARTED call, let's just not
	// respond to it at all

	// On the good slave, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	goodSlave.FakeMysqlDaemon.MasterAddr = newMaster.Tablet.MysqlIpAddr()
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// The reparent should work as expected here
	t.Logf("TabletExternallyReparented(new master) expecting success")
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if err := tmc.TabletExternallyReparented(wr.Context(), ti, ""); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// Now double-check the serving graph is good.
	// Should only have one good replica left.
	addrs, err := ts.GetEndPoints("cell1", "test_keyspace", "0", topo.TYPE_REPLICA)
	if err != nil {
		t.Fatalf("GetEndPoints failed at the end: %v", err)
	}
	if len(addrs.Entries) != 1 {
		t.Fatalf("GetEndPoints has too many entries: %v", addrs)
	}

	// check the old master was converted to spare
	tablet, err := ts.GetTablet(oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
	}
	if tablet.Type != topo.TYPE_SPARE {
		t.Fatalf("old master should be spare but is: %v", tablet.Type)
	}
	if tablet.Parent != newMaster.Tablet.Alias {
		t.Fatalf("old master has the wrong master, got %v expected %v", tablet.Parent, newMaster.Tablet.Alias)
	}
}
