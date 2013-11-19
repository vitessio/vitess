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
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
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
			actionNode, err := tm.ActionNodeFromJson(data, actionPath)
			if err != nil {
				t.Fatalf("ActionNodeFromJson failed: %v\n%v", err, data)
			}
			ta := tm.NewTabletActor(nil, mysqlDaemon, wr.ts, tabletAlias)
			if err := ta.HandleAction(actionPath, actionNode.Action, actionNode.ActionGuid, false); err != nil {
				// action may just fail for any good reason
				t.Logf("HandleAction failed for %v: %v", actionNode.Action, err)
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

	// First test: reparent to the same master, make sure it works
	// as expected.
	if err := wr.ShardExternallyReparented("test_keyspace", "0", oldMasterAlias, false, 80); err == nil {
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
	// that returns no master
	newMasterMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "",
	}
	startFakeTabletActionLoop(t, wr, newMasterAlias, newMasterMysqlDaemon, done)

	// On the old master, we will only respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	oldMasterMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3301",
	}
	startFakeTabletActionLoop(t, wr, oldMasterAlias, oldMasterMysqlDaemon, done)

	// On the good slaves, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	goodSlaveMysqlDaemon1 := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3301",
	}
	startFakeTabletActionLoop(t, wr, goodSlaveAlias1, goodSlaveMysqlDaemon1, done)
	goodSlaveMysqlDaemon2 := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "101.0.0.1:3301",
	}
	startFakeTabletActionLoop(t, wr, goodSlaveAlias2, goodSlaveMysqlDaemon2, done)

	// On the bad slave, we will respond to
	// TABLET_ACTION_SLAVE_WAS_RESTARTED.
	badSlaveMysqlDaemon := &mysqlctl.FakeMysqlDaemon{
		MasterAddr: "234.0.0.1:3301",
	}
	startFakeTabletActionLoop(t, wr, badSlaveAlias, badSlaveMysqlDaemon, done)

	// This tests a bad case; the new designated master is a slave!!
	t.Logf("ShardExternallyReparented(slave) expecting an error")
	if err := wr.ShardExternallyReparented("test_keyspace", "0", goodSlaveAlias1, false, 60); err == nil {
		t.Fatalf("ShardExternallyReparented(slave) should have failed")
	} else {
		if !strings.Contains(err.Error(), "new master is a slave") {
			t.Fatalf("ShardExternallyReparented(slave) should have failed with an error that contains 'new master is a slave' but got: %v", err)
		}
	}

	// This tests the good case, where everything works as planned
	t.Logf("ShardExternallyReparented(new master) expecting success")
	if err := wr.ShardExternallyReparented("test_keyspace", "0", newMasterAlias, false, 60); err != nil {
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
