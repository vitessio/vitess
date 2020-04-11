/*
Copyright 2019 The Vitess Authors.

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
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestEmergencyReparentShard(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good replicas
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new master
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true
	newMaster.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	newMaster.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master, will be scrapped
	oldMaster.FakeMysqlDaemon.ReadOnly = false
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 455,
			},
		},
	}
	goodReplica1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 454,
			},
		},
	}
	goodReplica2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run EmergencyReparentShard
	if err := vp.Run([]string{"EmergencyReparentShard", "-wait_slave_timeout", "10s", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard, topoproto.TabletAliasString(newMaster.Tablet.Alias)}); err != nil {
		t.Fatalf("EmergencyReparentShard failed: %v", err)
	}

	// check what was run
	if err := newMaster.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("newMaster.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := oldMaster.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("oldMaster.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodReplica1.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("goodReplica1.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodReplica2.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("goodReplica2.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if newMaster.FakeMysqlDaemon.ReadOnly {
		t.Errorf("newMaster.FakeMysqlDaemon.ReadOnly set")
	}
	// old master read-only flag doesn't matter, it is scrapped
	if !goodReplica1.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodReplica2.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodReplica2.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodReplica1.FakeMysqlDaemon.Replicating {
		t.Errorf("goodReplica1.FakeMysqlDaemon.Replicating not set")
	}
	if goodReplica2.FakeMysqlDaemon.Replicating {
		t.Errorf("goodReplica2.FakeMysqlDaemon.Replicating set")
	}
	checkSemiSyncEnabled(t, true, true, newMaster)
	checkSemiSyncEnabled(t, false, true, goodReplica1, goodReplica2)
}

// TestEmergencyReparentShardMasterElectNotBest tries to emergency reparent
// to a host that is not the latest in replication position.
func TestEmergencyReparentShardMasterElectNotBest(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create a master, a couple good replicas
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	moreAdvancedReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// new master
	newMaster.FakeMysqlDaemon.Replicating = true
	// this server has executed upto 455, which is the highest among replicas
	newMaster.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 455,
			},
		},
	}
	// It has more transactions in its relay log, but not as many as
	// moreAdvancedReplica
	newMaster.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master, will be scrapped
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// more advanced replica
	moreAdvancedReplica.FakeMysqlDaemon.Replicating = true
	// position up to which this replica has executed is behind desired new master
	moreAdvancedReplica.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 454,
			},
		},
	}
	// relay log position is more advanced than desired new master
	moreAdvancedReplica.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	moreAdvancedReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
	}
	moreAdvancedReplica.StartActionLoop(t, wr)
	defer moreAdvancedReplica.StopActionLoop(t)

	// run EmergencyReparentShard
	if err := wr.EmergencyReparentShard(ctx, newMaster.Tablet.Keyspace, newMaster.Tablet.Shard, newMaster.Tablet.Alias, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is more advanced than master elect tablet") {
		t.Fatalf("EmergencyReparentShard returned the wrong error: %v", err)
	}

	// check what was run
	if err := newMaster.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("newMaster.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := oldMaster.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("oldMaster.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := moreAdvancedReplica.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("moreAdvancedReplica.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
}
