// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttablet/tabletservermock"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestPlannedReparentShard(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good slaves
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSlave1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodSlave2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new master
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true
	newMaster.FakeMysqlDaemon.WaitMasterPosition = replication.Position{
		GTIDSet: replication.MariadbGTID{
			Domain:   7,
			Server:   123,
			Sequence: 990,
		},
	}
	newMaster.FakeMysqlDaemon.PromoteSlaveResult = replication.Position{
		GTIDSet: replication.MariadbGTID{
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
	oldMaster.FakeMysqlDaemon.Replicating = false
	oldMaster.FakeMysqlDaemon.DemoteMasterPosition = newMaster.FakeMysqlDaemon.WaitMasterPosition
	oldMaster.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v", newMaster.Tablet.Hostname, newMaster.Tablet.PortMap["mysql"])
	oldMaster.FakeMysqlDaemon.SetMasterCommandsResult = []string{"set master cmd 1"}
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"set master cmd 1",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good slave 1 is replicating
	goodSlave1.FakeMysqlDaemon.ReadOnly = true
	goodSlave1.FakeMysqlDaemon.Replicating = true
	goodSlave1.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v", newMaster.Tablet.Hostname, newMaster.Tablet.PortMap["mysql"])
	goodSlave1.FakeMysqlDaemon.SetMasterCommandsResult = []string{"set master cmd 1"}
	goodSlave1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"set master cmd 1",
		"START SLAVE",
	}
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	// good slave 2 is not replicating
	goodSlave2.FakeMysqlDaemon.ReadOnly = true
	goodSlave2.FakeMysqlDaemon.Replicating = false
	goodSlave2.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v", newMaster.Tablet.Hostname, newMaster.Tablet.PortMap["mysql"])
	goodSlave2.FakeMysqlDaemon.SetMasterCommandsResult = []string{"set master cmd 1"}
	goodSlave2.StartActionLoop(t, wr)
	goodSlave2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"set master cmd 1",
	}
	defer goodSlave2.StopActionLoop(t)

	// run PlannedReparentShard
	if err := vp.Run([]string{"PlannedReparentShard", "-wait_slave_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard, "-new_master", topoproto.TabletAliasString(newMaster.Tablet.Alias)}); err != nil {
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
	if !oldMaster.Agent.QueryServiceControl.IsServing() {
		t.Errorf("oldMaster...QueryServiceControl not serving")
	}

	// verify the old master was told to start replicating (and not
	// the slave that wasn't replicating in the first place)
	if !oldMaster.FakeMysqlDaemon.Replicating {
		t.Errorf("oldMaster.FakeMysqlDaemon.Replicating not set")
	}
	if !goodSlave1.FakeMysqlDaemon.Replicating {
		t.Errorf("goodSlave1.FakeMysqlDaemon.Replicating not set")
	}
	if goodSlave2.FakeMysqlDaemon.Replicating {
		t.Errorf("goodSlave2.FakeMysqlDaemon.Replicating set")
	}

	checkSemiSyncEnabled(t, true, true, newMaster)
	checkSemiSyncEnabled(t, false, true, goodSlave1, goodSlave2, oldMaster)
}

func TestPlannedReparentNoMaster(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a few replicas.
	replica1 := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_REPLICA, nil)
	NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	err := vp.Run([]string{"PlannedReparentShard", "-wait_slave_timeout", "10s", "-keyspace_shard", replica1.Tablet.Keyspace + "/" + replica1.Tablet.Shard, "-new_master", topoproto.TabletAliasString(replica1.Tablet.Alias)})
	if err == nil {
		t.Fatalf("PlannedReparentShard succeeded: %v", err)
	}
	if !strings.Contains(err.Error(), "the shard has no master") {
		t.Fatalf("PlannedReparentShard failed with the wrong error: %v", err)
	}
}
