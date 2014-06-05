// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package testlib contains utility methods to include in unit tests to
deal with topology common tasks, liek fake tablets and action loops.
*/
package testlib

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/actor"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

// This file contains utility methods for unit tests.
// We allow the creation of fake tablets, and running their event loop based
// on a FakeMysqlDaemon.

// FakeTablet keeps track of a fake tablet in memory. It has:
// - a Tablet record (used for creating the tablet, kept for user's information)
// - a FakeMysqlDaemon (used by the fake event loop)
// - a 'done' channel (used to terminate the fake event loop)
type FakeTablet struct {
	// Tablet and FakeMysqlDaemon are populated at NewFakeTablet time.
	Tablet          *topo.Tablet
	FakeMysqlDaemon *mysqlctl.FakeMysqlDaemon

	// Done is created when we start the event loop for the tablet,
	// and closed / cleared when we stop it.
	Done chan struct{}
}

// CreateTestTablet creates the test tablet in the topology.  'uid'
// has to be between 0 and 99. All the tablet info will be derived
// from that. Look at the implementation if you need values.
func NewFakeTablet(t *testing.T, wr *wrangler.Wrangler, cell string, uid uint32, tabletType topo.TabletType, parent topo.TabletAlias) *FakeTablet {
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

	// create a FakeMysqlDaemon with the right information by default
	fakeMysqlDaemon := &mysqlctl.FakeMysqlDaemon{}
	if !parent.IsZero() {
		fakeMysqlDaemon.MasterAddr = fmt.Sprintf("%v.0.0.1:%v", 100+parent.Uid, 3300+int(parent.Uid))
	}
	fakeMysqlDaemon.MysqlPort = 3300 + int(uid)

	return &FakeTablet{
		Tablet:          tablet,
		FakeMysqlDaemon: fakeMysqlDaemon,
	}
}

// StartActionLoop will start the action loop for a fake tablet,
// using ft.FakeMysqlDaemon as the backing mysqld.
func (ft *FakeTablet) StartActionLoop(t *testing.T, wr *wrangler.Wrangler) {
	if ft.Done != nil {
		t.Fatalf("ActionLoop for %v is already running", ft.Tablet.Alias)
	}
	ft.Done = make(chan struct{}, 1)
	go func() {
		wr.TopoServer().ActionEventLoop(ft.Tablet.Alias, func(actionPath, data string) error {
			actionNode, err := actionnode.ActionNodeFromJson(data, actionPath)
			if err != nil {
				t.Fatalf("ActionNodeFromJson failed: %v\n%v", err, data)
			}
			ta := actor.NewTabletActor(nil, ft.FakeMysqlDaemon, wr.TopoServer(), ft.Tablet.Alias)
			if err := ta.HandleAction(actionPath, actionNode.Action, actionNode.ActionGuid, false); err != nil {
				// action may just fail for any good reason
				t.Logf("HandleAction failed for %v: %v", actionNode.Action, err)
			}

			// this part would also be done by the agent
			tablet, err := wr.TopoServer().GetTablet(ft.Tablet.Alias)
			if err != nil {
				t.Logf("Cannot get tablet: %v", err)
			} else {
				updatedTablet := actor.CheckTabletMysqlPort(wr.TopoServer(), ft.FakeMysqlDaemon, tablet)
				if updatedTablet != nil {
					t.Logf("Updated tablet record")
				}
			}
			return nil
		}, ft.Done)
	}()
}

// StopActionLoop will stop the Action Loop for the given FakeTablet
func (ft *FakeTablet) StopActionLoop(t *testing.T) {
	if ft.Done == nil {
		t.Fatalf("ActionLoop for %v is not running", ft.Tablet.Alias)
	}
	close(ft.Done)
	ft.Done = nil
}
