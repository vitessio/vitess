// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	"time"
)

func TestPlannedReparentShard(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	// Create a master, a couple good slaves
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)
	goodSlave1 := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA)
	goodSlave2 := NewFakeTablet(t, wr, "cell2", 3, topo.TYPE_REPLICA)

	// new master
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.WaitMasterPosition = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   7,
			Server:   123,
			Sequence: 990,
		},
	}
	newMaster.FakeMysqlDaemon.PromoteSlaveResult = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   7,
			Server:   456,
			Sequence: 991,
		},
	}
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master
	oldMaster.FakeMysqlDaemon.ReadOnly = false
	oldMaster.FakeMysqlDaemon.DemoteMasterPosition = newMaster.FakeMysqlDaemon.WaitMasterPosition
	oldMaster.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v,%v", newMaster.Tablet.Hostname, newMaster.Tablet.Portmap["mysql"], 10)
	oldMaster.FakeMysqlDaemon.SetMasterCommandsResult = []string{"set master cmd 1"}
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"set master cmd 1",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.Agent.QueryServiceControl.(*tabletserver.TestQueryServiceControl).QueryServiceEnabled = true

	// good slave 1
	goodSlave1.FakeMysqlDaemon.ReadOnly = true
	goodSlave1.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v,%v", newMaster.Tablet.Hostname, newMaster.Tablet.Portmap["mysql"], 10)
	goodSlave1.FakeMysqlDaemon.SetMasterCommandsResult = []string{"set master cmd 1"}
	goodSlave1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"set master cmd 1",
	}
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	// good slave 2
	goodSlave2.FakeMysqlDaemon.ReadOnly = true
	goodSlave2.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v,%v", newMaster.Tablet.Hostname, newMaster.Tablet.Portmap["mysql"], 10)
	goodSlave2.FakeMysqlDaemon.SetMasterCommandsResult = []string{"set master cmd 1"}
	goodSlave2.StartActionLoop(t, wr)
	goodSlave2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"set master cmd 1",
	}
	defer goodSlave2.StopActionLoop(t)

	// run PlannedReparentShard
	if err := wr.PlannedReparentShard(ctx, newMaster.Tablet.Keyspace, newMaster.Tablet.Shard, newMaster.Tablet.Alias, 10*time.Second); err != nil {
		t.Fatalf("PlannedReparentShard failed: %v", err)
	}

	// check what was run
	if err := newMaster.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("newMaster.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := oldMaster.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("oldMaster.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSlave1.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("goodSlave1.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSlave2.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("goodSlave2.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if newMaster.FakeMysqlDaemon.ReadOnly {
		t.Errorf("newMaster.FakeMysqlDaemon.ReadOnly set")
	}
	if !oldMaster.FakeMysqlDaemon.ReadOnly {
		t.Errorf("oldMaster.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodSlave1.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodSlave1.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodSlave2.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodSlave2.FakeMysqlDaemon.ReadOnly not set")
	}
	if oldMaster.Agent.QueryServiceControl.(*tabletserver.TestQueryServiceControl).QueryServiceEnabled {
		t.Errorf("oldMaster...QueryServiceEnabled set")
	}

}
