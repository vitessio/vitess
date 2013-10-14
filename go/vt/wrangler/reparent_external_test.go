// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
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
	if err := wr.InitTablet(&topo.Tablet{
		Cell:           cell,
		Uid:            100 + uid,
		Parent:         parent,
		Addr:           fmt.Sprintf("%vhost:%v", cell, 8100+uid),
		SecureAddr:     fmt.Sprintf("%vhost:%v", cell, 8200+uid),
		MysqlAddr:      fmt.Sprintf("%vhost:%v", cell, 3300+uid),
		MysqlIpAddr:    fmt.Sprintf("%v.0.0.1:%v", 100+uid, 3300+uid),
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           tabletType,
		State:          state,
		DbNameOverride: "",
		KeyRange:       key.KeyRange{},
	}, false, true, false); err != nil {
		t.Fatalf("cannot create tablet %v: %v", uid, err)
	}
	return topo.TabletAlias{cell, 100 + uid}
}

func TestShardExternallyReparented(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := New(ts, time.Minute, time.Second)

	// Create a master and a replica
	masterAlias := createTestTablet(t, wr, "cell1", 0, topo.TYPE_MASTER, topo.TabletAlias{})
	slaveAlias := createTestTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA, masterAlias)

	// First test: reparent to the same master, make sure it works
	// as expected.
	if err := wr.ShardExternallyReparented("test_keyspace", "0", masterAlias, false, 80); err == nil {
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
				ta := tm.NewTabletActor(nil, ts, slaveAlias)
				if err := ta.HandleAction(actionPath, actionNode.Action, actionNode.ActionGuid, false); err != nil {
					t.Fatalf("HandleAction failed for %v: %v", actionNode.Action, err)
				}
			}
			return nil
		}
		ts.ActionEventLoop(slaveAlias, f, done)
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
				actionErr := ta.SlaveWasRestarted(actionNode, "101.0.0.1:3301")
				if err := tm.StoreActionResponse(ts, actionNode, actionPath, actionErr); err != nil {
					t.Fatalf("StoreActionResponse failed: %v", err)
				}

				if err := ts.UnblockTabletAction(actionPath); err != nil {
					t.Fatalf("UnblockTabletAction failed: %v", err)
				}
			}
			return nil
		}
		ts.ActionEventLoop(masterAlias, f, done)
	}()
	if err := wr.ShardExternallyReparented("test_keyspace", "0", slaveAlias, false, 80); err != nil {
		t.Fatalf("ShardExternallyReparented(replica) failed: %v", err)
	}
	close(done)
}
