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
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestPlannedReparentShardNoMasterProvided(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good slaves
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSlave1 := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, nil)

	// new master
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true
	newMaster.FakeMysqlDaemon.WaitMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMaster.FakeMysqlDaemon.PromoteSlaveResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
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
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good slave 1 is replicating
	goodSlave1.FakeMysqlDaemon.ReadOnly = true
	goodSlave1.FakeMysqlDaemon.Replicating = true
	goodSlave1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodSlave1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	// run PlannedReparentShard
	if err := vp.Run([]string{"PlannedReparentShard", "-wait_slave_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard}); err != nil {
		t.Fatalf("PlannedReparentShard failed: %v", err)
	}

	// // check what was run
	if err := newMaster.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("newMaster.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := oldMaster.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("oldMaster.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSlave1.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("goodSlave1.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
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
	if !oldMaster.Agent.QueryServiceControl.IsServing() {
		t.Errorf("oldMaster...QueryServiceControl not serving")
	}

	// // verify the old master was told to start replicating (and not
	// // the slave that wasn't replicating in the first place)
	if !oldMaster.FakeMysqlDaemon.Replicating {
		t.Errorf("oldMaster.FakeMysqlDaemon.Replicating not set")
	}
	if !goodSlave1.FakeMysqlDaemon.Replicating {
		t.Errorf("goodSlave1.FakeMysqlDaemon.Replicating not set")
	}
	checkSemiSyncEnabled(t, true, true, newMaster)
	checkSemiSyncEnabled(t, false, true, goodSlave1, oldMaster)
}

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
	newMaster.FakeMysqlDaemon.WaitMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMaster.FakeMysqlDaemon.PromoteSlaveResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
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
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good slave 1 is replicating
	goodSlave1.FakeMysqlDaemon.ReadOnly = true
	goodSlave1.FakeMysqlDaemon.Replicating = true
	goodSlave1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodSlave1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	// good slave 2 is not replicating
	goodSlave2.FakeMysqlDaemon.ReadOnly = true
	goodSlave2.FakeMysqlDaemon.Replicating = false
	goodSlave2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodSlave2.StartActionLoop(t, wr)
	goodSlave2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
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

func TestPlannedReparentShardPromoteSlaveFail(t *testing.T) {
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
	newMaster.FakeMysqlDaemon.WaitMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   6,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMaster.FakeMysqlDaemon.PromoteSlaveResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
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
	// to make promote fail on WaitForMasterPos
	oldMaster.FakeMysqlDaemon.DemoteMasterPosition = newMaster.FakeMysqlDaemon.PromoteSlaveResult
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good slave 1 is replicating
	goodSlave1.FakeMysqlDaemon.ReadOnly = true
	goodSlave1.FakeMysqlDaemon.Replicating = true
	goodSlave1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodSlave1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	// good slave 2 is not replicating
	goodSlave2.FakeMysqlDaemon.ReadOnly = true
	goodSlave2.FakeMysqlDaemon.Replicating = false
	goodSlave2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodSlave2.StartActionLoop(t, wr)
	goodSlave2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodSlave2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_slave_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard, "-new_master", topoproto.TabletAliasString(newMaster.Tablet.Alias)})

	if err == nil {
		t.Fatalf("PlannedReparentShard succeeded: %v", err)
	}
	if !strings.Contains(err.Error(), "master-elect tablet cell1-0000000001 failed to catch up with replication or be upgraded to master") {
		t.Fatalf("PlannedReparentShard failed with the wrong error: %v", err)
	}

	// now check that DemoteMaster was undone and old master is still master
	if !newMaster.FakeMysqlDaemon.ReadOnly {
		t.Errorf("newMaster.FakeMysqlDaemon.ReadOnly not set")
	}
	if oldMaster.FakeMysqlDaemon.ReadOnly {
		t.Errorf("oldMaster.FakeMysqlDaemon.ReadOnly set")
	}
}
