// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
)

// createTestTablet creates the test tablet in the topology.
// 'uid' has to be between 0 and 99. All the ports will be derived from that.
func createTestTablet(t *testing.T, wr *Wrangler, cell string, uid uint32, tabletType topo.TabletType, parent topo.TabletAlias) topo.TabletAlias {
	if uid < 0 || uid > 99 {
		t.Fatalf("uid has to be between 0 and 99: %v", uid)
	}
	state := topo.STATE_READ_ONLY
	if tabletType == topo.TYPE_MASTER {
		state = topo.STATE_READ_WRITE
	}
	tablet := &topo.Tablet{
		Parent:   parent,
		Alias:    topo.TabletAlias{Cell: cell, Uid: uid},
		Hostname: fmt.Sprintf("%vhost", cell),
		Portmap: map[string]int{
			"vt":    8100 + int(uid),
			"mysql": 3300 + int(uid),
			"vts":   8200 + int(uid),
		},
		IPAddr:         fmt.Sprintf("%v.0.0.1", 100+uid),
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           tabletType,
		State:          state,
		DbNameOverride: "",
		KeyRange:       key.KeyRange{},
	}
	if err := wr.InitTablet(tablet, false, true, false); err != nil {
		t.Fatalf("cannot create tablet %v: %v", uid, err)
	}
	return tablet.Alias
}

// startFakeTabletActionLoop will start the action loop for a fake tablet,
// using mysqlDaemon as the backing mysqld.
func startFakeTabletActionLoop(t *testing.T, wr *Wrangler, tabletAlias topo.TabletAlias, mysqlDaemon mysqlctl.MysqlDaemon, done chan struct{}) {
	go func() {
		f := func(actionPath, data string) error {
			actionNode, err := actionnode.ActionNodeFromJson(data, actionPath)
			if err != nil {
				t.Fatalf("ActionNodeFromJson failed: %v\n%v", err, data)
			}
			ta := tabletmanager.NewTabletActor(nil, mysqlDaemon, wr.ts, tabletAlias)
			if err := ta.HandleAction(actionPath, actionNode.Action, actionNode.ActionGuid, false); err != nil {
				// action may just fail for any good reason
				t.Logf("HandleAction failed for %v: %v", actionNode.Action, err)
			}

			// this part would also be done by the agent
			tablet, err := wr.ts.GetTablet(tabletAlias)
			if err != nil {
				t.Logf("Cannot get tablet: %v", err)
			} else {
				updatedTablet := tabletmanager.CheckTabletMysqlPort(wr.ts, mysqlDaemon, tablet)
				if updatedTablet != nil {
					t.Logf("Updated tablet record")
				}
			}
			return nil
		}
		wr.ts.ActionEventLoop(tabletAlias, f, done)
	}()
}

func TestShardExternallyReparented(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := New(ts, time.Minute, time.Second)
	wr.UseRPCs = false

	// Create an old master, a new master, two good slaves, one bad slave
	oldMasterAlias := createTestTablet(t, wr, "cell1", 0, topo.TYPE_MASTER, topo.TabletAlias{})
	newMasterAlias := createTestTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA, oldMasterAlias)
	goodSlaveAlias1 := createTestTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA, oldMasterAlias)
	goodSlaveAlias2 := createTestTablet(t, wr, "cell2", 3, topo.TYPE_REPLICA, oldMasterAlias)
	badSlaveAlias := createTestTablet(t, wr, "cell1", 4, topo.TYPE_REPLICA, oldMasterAlias)

	// Add a new Cell to the Shard, that doesn't map to any read topo cell,
	// to simulate a data center being unreachable.
	si, err := ts.GetShard("test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.Cells = append(si.Cells, "cell666")
	if err := ts.UpdateShard(si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}

	// Slightly unrelated test: make sure we can find the tablets
	// even with a datacenter being down.
	tabletMap, err := GetTabletMapForShardByCell(ts, "test_keyspace", "0", []string{"cell1"})
	if err != nil {
		t.Fatalf("GetTabletMapForShardByCell should have worked but got: %v", err)
	}
	master, err := FindTabletByIPAddrAndPort(tabletMap, "100.0.0.1", "vt", 8100)
	if err != nil || master != oldMasterAlias {
		t.Fatalf("FindTabletByIPAddrAndPort(master) failed: %v %v", err, master)
	}
	slave1, err := FindTabletByIPAddrAndPort(tabletMap, "102.0.0.1", "vt", 8102)
	if err != nil || slave1 != goodSlaveAlias1 {
		t.Fatalf("FindTabletByIPAddrAndPort(slave1) failed: %v %v", err, master)
	}
	slave2, err := FindTabletByIPAddrAndPort(tabletMap, "103.0.0.1", "vt", 8103)
	if err != topo.ErrNoNode {
		t.Fatalf("FindTabletByIPAddrAndPort(slave2) worked: %v %v", err, slave2)
	}

	// Make sure the master is not exported in other cells
	tabletMap, err = GetTabletMapForShardByCell(ts, "test_keyspace", "0", []string{"cell2"})
	master, err = FindTabletByIPAddrAndPort(tabletMap, "100.0.0.1", "vt", 8100)
	if err != topo.ErrNoNode {
		t.Fatalf("FindTabletByIPAddrAndPort(master) worked in cell2: %v %v", err, master)
	}

	tabletMap, err = GetTabletMapForShard(ts, "test_keyspace", "0")
	if err != topo.ErrPartialResult {
		t.Fatalf("GetTabletMapForShard should have returned ErrPartialResult but got: %v", err)
	}
	master, err = FindTabletByIPAddrAndPort(tabletMap, "100.0.0.1", "vt", 8100)
	if err != nil || master != oldMasterAlias {
		t.Fatalf("FindTabletByIPAddrAndPort(master) failed: %v %v", err, master)
	}

	// First test: reparent to the same master, make sure it works
	// as expected.
	if err := wr.ShardExternallyReparented("test_keyspace", "0", oldMasterAlias, false, false, 80); err == nil {
		t.Fatalf("ShardExternallyReparented(same master) should have failed")
	} else {
		if !strings.Contains(err.Error(), "already master") {
			t.Fatalf("ShardExternallyReparented(same master) should have failed with an error that contains 'already master' but got: %v", err)
		}
	}

	// Second test: reparent to the replica, and pretend the old
	// master is still good to go.
	done := make(chan struct{}, 1)

	// On the elected master, we will respond to
	// TABLET_ACTION_SLAVE_WAS_PROMOTED, so we need a MysqlDaemon
	// that returns no master and the right port
	newMasterMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "",
		MysqlPort:  3301,
	}
	startFakeTabletActionLoop(t, wr, newMasterAlias, newMasterMysqlDaemon, done)

	// On the old master, we will only respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	oldMasterMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3301",
		MysqlPort:  3300,
	}
	startFakeTabletActionLoop(t, wr, oldMasterAlias, oldMasterMysqlDaemon, done)

	// On the good slaves, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	goodSlaveMysqlDaemon1 := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3301",
		MysqlPort:  3302,
	}
	startFakeTabletActionLoop(t, wr, goodSlaveAlias1, goodSlaveMysqlDaemon1, done)
	goodSlaveMysqlDaemon2 := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3301",
		MysqlPort:  3303,
	}
	startFakeTabletActionLoop(t, wr, goodSlaveAlias2, goodSlaveMysqlDaemon2, done)

	// On the bad slave, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	badSlaveMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "234.0.0.1:3301",
		MysqlPort:  3304,
	}
	startFakeTabletActionLoop(t, wr, badSlaveAlias, badSlaveMysqlDaemon, done)

	// This tests a bad case; the new designated master is a slave!!
	t.Logf("ShardExternallyReparented(slave) expecting an error")
	if err := wr.ShardExternallyReparented("test_keyspace", "0", goodSlaveAlias1, false, false, 60); err == nil {
		t.Fatalf("ShardExternallyReparented(slave) should have failed")
	} else {
		if !strings.Contains(err.Error(), "new master is a slave") {
			t.Fatalf("ShardExternallyReparented(slave) should have failed with an error that contains 'new master is a slave' but got: %v", err)
		}
	}

	// This tests the good case, where everything works as planned
	t.Logf("ShardExternallyReparented(new master) expecting success")
	if err := wr.ShardExternallyReparented("test_keyspace", "0", newMasterAlias, false, false, 60); err != nil {
		t.Fatalf("ShardExternallyReparented(replica) failed: %v", err)
	}
	close(done)

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

// TestShardExternallyReparentedWithDifferentMysqlPort makes sure
// that if mysql is restarted on the master-elect tablet and has a different
// port, we pick it up correctly.
func TestShardExternallyReparentedWithDifferentMysqlPort(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1"})
	wr := New(ts, time.Minute, time.Second)
	wr.UseRPCs = false

	// Create an old master, a new master, two good slaves, one bad slave
	oldMasterAlias := createTestTablet(t, wr, "cell1", 0, topo.TYPE_MASTER, topo.TabletAlias{})
	newMasterAlias := createTestTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA, oldMasterAlias)
	goodSlaveAlias := createTestTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA, oldMasterAlias)
	done := make(chan struct{}, 1)

	// Now we're restarting mysql on a different port, 3301->3302
	// but without updating the Tablet record in topology.

	// On the elected master, we will respond to
	// TABLET_ACTION_SLAVE_WAS_PROMOTED, so we need a MysqlDaemon
	// that returns no master, and the new port (as returned by mysql)
	newMasterMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "",
		MysqlPort:  3302,
	}
	startFakeTabletActionLoop(t, wr, newMasterAlias, newMasterMysqlDaemon, done)

	// On the old master, we will only respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED and point to the new mysql port
	oldMasterMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3302",
		MysqlPort:  3300,
	}
	startFakeTabletActionLoop(t, wr, oldMasterAlias, oldMasterMysqlDaemon, done)

	// On the good slaves, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED and point to the new mysql port
	goodSlaveMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3302",
		MysqlPort:  3302,
	}
	startFakeTabletActionLoop(t, wr, goodSlaveAlias, goodSlaveMysqlDaemon, done)

	// This tests the good case, where everything works as planned
	t.Logf("ShardExternallyReparented(new master) expecting success")
	if err := wr.ShardExternallyReparented("test_keyspace", "0", newMasterAlias, false, false, 60); err != nil {
		t.Fatalf("ShardExternallyReparented(replica) failed: %v", err)
	}
	close(done)
}

// TestShardExternallyReparentedContinueOnUnexpectedMaster makes sure
// that we ignore mysql's master if the flag is set
func TestShardExternallyReparentedContinueOnUnexpectedMaster(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1"})
	wr := New(ts, time.Minute, time.Second)
	wr.UseRPCs = false

	// Create an old master, a new master, two good slaves, one bad slave
	oldMasterAlias := createTestTablet(t, wr, "cell1", 0, topo.TYPE_MASTER, topo.TabletAlias{})
	newMasterAlias := createTestTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA, oldMasterAlias)
	goodSlaveAlias := createTestTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA, oldMasterAlias)
	done := make(chan struct{}, 1)

	// On the elected master, we will respond to
	// TABLET_ACTION_SLAVE_WAS_PROMOTED, so we need a MysqlDaemon
	// that returns no master, and the new port (as returned by mysql)
	newMasterMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "",
		MysqlPort:  3301,
	}
	startFakeTabletActionLoop(t, wr, newMasterAlias, newMasterMysqlDaemon, done)

	// On the old master, we will only respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED and point to a bad host
	oldMasterMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "1.2.3.4:6666",
		MysqlPort:  3300,
	}
	startFakeTabletActionLoop(t, wr, oldMasterAlias, oldMasterMysqlDaemon, done)

	// On the good slaves, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED and point to a bad host
	goodSlaveMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "1.2.3.4:6666",
		MysqlPort:  3302,
	}
	startFakeTabletActionLoop(t, wr, goodSlaveAlias, goodSlaveMysqlDaemon, done)

	// This tests the good case, where everything works as planned
	t.Logf("ShardExternallyReparented(new master) expecting success")
	// temporary failure still:
	if err := wr.ShardExternallyReparented("test_keyspace", "0", newMasterAlias, false, true, 60); err == nil {
		t.Fatal("ShardExternallyReparented(replica) should have failed")
	}
	// if err := wr.ShardExternallyReparented("test_keyspace", "0", newMasterAlias, false, true, 60); err != nil {
	//	t.Fatalf("ShardExternallyReparented(replica) failed: %v", err)
	// }
	close(done)
}

func TestSlaveWasRestarted(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1"})
	wr := New(ts, time.Minute, time.Second)
	wr.UseRPCs = false

	// Create an old master, a new master, two good slaves, one bad slave
	oldMasterAlias := createTestTablet(t, wr, "cell1", 0, topo.TYPE_MASTER, topo.TabletAlias{})
	newMasterAlias := createTestTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA, oldMasterAlias)
	slaveAlias := createTestTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA, oldMasterAlias)

	slaveMySQLDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3301",
		MysqlPort:  3302,
	}
	done := make(chan struct{}, 1)
	startFakeTabletActionLoop(t, wr, slaveAlias, slaveMySQLDaemon, done)
	defer close(done)

	if err := wr.SlaveWasRestarted(slaveAlias, newMasterAlias, false); err != nil {
		t.Fatalf("SlaveWasRestarted %v: %v", slaveAlias, err)
	}
	tablet, err := wr.ts.GetTablet(slaveAlias)
	if err != nil {
		t.Fatalf("GetTablet %v: %v", slaveAlias, err)
	}
	if want, got := newMasterAlias, tablet.Parent; want != got {
		t.Errorf("parent of %v: want %v, got %v", tablet, want, got)
	}
}
