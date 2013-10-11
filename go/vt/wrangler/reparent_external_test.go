// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
)

func TestShardExternallyReparented(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := New(ts, time.Minute, time.Second)

	// Creating the tablets with createShardAndKeyspace=true
	// so we don't need to create the keyspace and shard
	if err := wr.InitTablet(&topo.Tablet{
		Cell:           "cell1",
		Uid:            123,
		Parent:         topo.TabletAlias{},
		Addr:           "masterhost:8101",
		SecureAddr:     "masterhost:8102",
		MysqlAddr:      "masterhost:3306",
		MysqlIpAddr:    "1.2.3.4:3306",
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topo.TYPE_MASTER,
		State:          topo.STATE_READ_WRITE,
		DbNameOverride: "",
		KeyRange:       key.KeyRange{},
	}, false, true, false); err != nil {
		t.Fatalf("cannot create master tablet: %v", err)
	}

	if err := wr.InitTablet(&topo.Tablet{
		Cell: "cell1",
		Uid:  234,
		Parent: topo.TabletAlias{
			Cell: "cell1",
			Uid:  123,
		},
		Addr:           "slavehost:8101",
		SecureAddr:     "slavehost:8102",
		MysqlAddr:      "slavehost:3306",
		MysqlIpAddr:    "2.3.4.5:3306",
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topo.TYPE_REPLICA,
		State:          topo.STATE_READ_ONLY,
		DbNameOverride: "",
		KeyRange:       key.KeyRange{},
	}, false, false, false); err != nil {
		t.Fatalf("cannot create slave tablet: %v", err)
	}

	// First test: reparent to the same master, make sure it works
	// as expected.
	if err := wr.ShardExternallyReparented("test_keyspace", "0", topo.TabletAlias{Cell: "cell1", Uid: 123}, false, 80); err == nil {
		t.Fatalf("ShardExternallyReparented(same master) should have failed")
	} else {
		if !strings.Contains(err.Error(), "already master") {
			t.Fatalf("ShardExternallyReparented(same master) should have failed with an error that contains 'already master' but got: %v", err)
		}
	}

	// Second test: reparent to the replica, and pretend the old
	// master is still good to go.
	done := make(chan struct{}, 1)
	go func() {
		// On the elected master, respond to TABLET_ACTION_SLAVE_WAS_PROMOTED.
		// We can use HandleAction here as it doesn't use mysqld.
		f := func(actionPath, data string) error {
			actionNode, err := tm.ActionNodeFromJson(data, actionPath)
			if err != nil {
				t.Fatalf("ActionNodeFromJson failed: %v\n%v", err, data)
			}
			switch actionNode.Action {
			case tm.TABLET_ACTION_SLAVE_WAS_PROMOTED:
				ta := tm.NewTabletActor(nil, ts, topo.TabletAlias{Cell: "cell1", Uid: 234})
				if err := ta.HandleAction(actionPath, actionNode.Action, actionNode.ActionGuid, false); err != nil {
					t.Fatalf("HandleAction failed for %v: %v", actionNode.Action, err)
				}
			}
			return nil
		}
		ts.ActionEventLoop(topo.TabletAlias{Cell: "cell1", Uid: 234}, f, done)
	}()
	go func() {
		// On the old master, respond to TABLET_ACTION_SLAVE_WAS_RESTARTED.
		// We cannot use HandleAction as this one uses
		// TabletActor.mysqld, and that's not a fakeable interface.
		f := func(actionPath, data string) error {
			// get the version and info for actionNode
			tabletAlias, data, version, err := ts.ReadTabletActionPath(actionPath)
			if err != nil {
				t.Fatalf("ReadTabletActionPath failed: %v", err)
			}
			actionNode, err := tm.ActionNodeFromJson(data, actionPath)
			if err != nil {
				t.Fatalf("ActionNodeFromJson failed: %v\n%v", err, data)
			}
			switch actionNode.Action {
			case tm.TABLET_ACTION_SLAVE_WAS_RESTARTED:
				// update the actionNode
				actionNode.State = tm.ACTION_STATE_RUNNING
				actionNode.Pid = os.Getpid()
				newData := tm.ActionNodeToJson(actionNode)
				if err := ts.UpdateTabletAction(actionPath, newData, version); err != nil {
					t.Fatalf("UpdateTabletAction failed: %v", err)
				}

				ta := tm.NewTabletActor(nil, ts, tabletAlias)
				actionErr := ta.SlaveWasRestarted(actionNode, "2.3.4.5:3306")
				if err := tm.StoreActionResponse(ts, actionNode, actionPath, actionErr); err != nil {
					t.Fatalf("StoreActionResponse failed: %v", err)
				}

				if err := ts.UnblockTabletAction(actionPath); err != nil {
					t.Fatalf("UnblockTabletAction failed: %v", err)
				}
			}
			return nil
		}
		ts.ActionEventLoop(topo.TabletAlias{Cell: "cell1", Uid: 123}, f, done)
	}()
	if err := wr.ShardExternallyReparented("test_keyspace", "0", topo.TabletAlias{Cell: "cell1", Uid: 234}, false, 80); err != nil {
		t.Fatalf("ShardExternallyReparented(replica) failed: %v", err)
	}
}
