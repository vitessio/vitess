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
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestInitMasterShard is the good scenario test, where everything
// works as planned
func TestInitMasterShard(t *testing.T) {
	ctx := context.Background()
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	db.AddQuery("CREATE DATABASE IF NOT EXISTS `vt_test_keyspace`", &sqltypes.Result{})

	// Create a master, a couple good slaves
	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)
	goodSlave1 := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	goodSlave2 := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, db)

	// Master: set a plausible ReplicationPosition to return,
	// and expect to add entry in _vt.reparent_journal
	master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 890,
			},
		},
	}
	master.FakeMysqlDaemon.ReadOnly = true
	master.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// Slave1: expect to be reset and re-parented
	goodSlave1.FakeMysqlDaemon.ReadOnly = true
	goodSlave1.FakeMysqlDaemon.SetSlavePositionPos = master.FakeMysqlDaemon.CurrentMasterPosition
	goodSlave1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(master.Tablet)
	goodSlave1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	// Slave2: expect to be re-parented
	goodSlave2.FakeMysqlDaemon.ReadOnly = true
	goodSlave2.FakeMysqlDaemon.SetSlavePositionPos = master.FakeMysqlDaemon.CurrentMasterPosition
	goodSlave2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(master.Tablet)
	goodSlave2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSlave2.StartActionLoop(t, wr)
	defer goodSlave2.StopActionLoop(t)

	// run InitShardMaster
	if err := vp.Run([]string{"InitShardMaster", "-wait_slave_timeout", "10s", master.Tablet.Keyspace + "/" + master.Tablet.Shard, topoproto.TabletAliasString(master.Tablet.Alias)}); err != nil {
		t.Fatalf("InitShardMaster failed: %v", err)
	}

	// check what was run
	if master.FakeMysqlDaemon.ReadOnly {
		t.Errorf("master was not turned read-write")
	}
	si, err := ts.GetShard(ctx, master.Tablet.Keyspace, master.Tablet.Shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if !topoproto.TabletAliasEqual(si.MasterAlias, master.Tablet.Alias) {
		t.Errorf("unexpected shard master alias, got %v expected %v", si.MasterAlias, master.Tablet.Alias)
	}
	if err := master.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("master.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSlave1.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("goodSlave1.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSlave2.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("goodSlave2.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	checkSemiSyncEnabled(t, true, true, master)
	checkSemiSyncEnabled(t, false, true, goodSlave1, goodSlave2)
}

// TestInitMasterShardChecks makes sure the safety checks work
func TestInitMasterShardChecks(t *testing.T) {
	ctx := context.Background()
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)

	// InitShardMaster with an unknown tablet
	if err := wr.InitShardMaster(ctx, master.Tablet.Keyspace, master.Tablet.Shard, &topodatapb.TabletAlias{
		Cell: master.Tablet.Alias.Cell,
		Uid:  master.Tablet.Alias.Uid + 1,
	}, false /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is not in the shard") {
		t.Errorf("InitShardMaster with unknown alias returned wrong error: %v", err)
	}

	// InitShardMaster with two masters in the shard, no force flag
	// (master2 needs to run InitTablet with -force, as it is the second
	// master in the same shard)
	master2 := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_MASTER, db, ForceInitTablet())
	if err := wr.InitShardMaster(ctx, master2.Tablet.Keyspace, master2.Tablet.Shard, master2.Tablet.Alias, false /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is not the only master in the shard") {
		t.Errorf("InitShardMaster with two masters returned wrong error: %v", err)
	}

	// InitShardMaster where the new master fails (use force flag
	// as we have 2 masters). We force the failure by making the
	// SQL commands executed on the master unexpected by the test fixture
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)
	master2.StartActionLoop(t, wr)
	defer master2.StopActionLoop(t)
	if err := wr.InitShardMaster(ctx, master.Tablet.Keyspace, master.Tablet.Shard, master.Tablet.Alias, true /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "unexpected extra query") {
		t.Errorf("InitShardMaster with new master failing in new master InitMaster returned wrong error: %v", err)
	}
}

// TestInitMasterShardOneSlaveFails makes sure that if one slave fails to
// proceed, the action completes anyway
func TestInitMasterShardOneSlaveFails(t *testing.T) {
	ctx := context.Background()
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create a master, a couple slaves
	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)
	goodSlave := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	badSlave := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, db)

	// Master: set a plausible ReplicationPosition to return,
	// and expect to add entry in _vt.reparent_journal
	master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 890,
			},
		},
	}
	master.FakeMysqlDaemon.ReadOnly = true
	master.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// goodSlave: expect to be re-parented
	goodSlave.FakeMysqlDaemon.ReadOnly = true
	goodSlave.FakeMysqlDaemon.SetSlavePositionPos = master.FakeMysqlDaemon.CurrentMasterPosition
	goodSlave.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(master.Tablet)
	goodSlave.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// badSlave: insert an error by failing the master hostname input
	// on purpose
	badSlave.FakeMysqlDaemon.ReadOnly = true
	badSlave.FakeMysqlDaemon.SetSlavePositionPos = master.FakeMysqlDaemon.CurrentMasterPosition
	badSlave.FakeMysqlDaemon.SetMasterInput = fmt.Sprintf("%v:%v", "", topoproto.MysqlPort(master.Tablet))
	badSlave.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
	}
	badSlave.StartActionLoop(t, wr)
	defer badSlave.StopActionLoop(t)

	// also change the master alias in the Shard object, to make sure it
	// is set back.
	_, err := ts.UpdateShardFields(ctx, master.Tablet.Keyspace, master.Tablet.Shard, func(si *topo.ShardInfo) error {
		// note it's OK to retry this and increment multiple times,
		// we just want it to be different
		si.MasterAlias.Uid++
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// run InitShardMaster without force, it fails because master is
	// changing.
	if err := wr.InitShardMaster(ctx, master.Tablet.Keyspace, master.Tablet.Shard, master.Tablet.Alias, false /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is not the shard master") {
		t.Errorf("InitShardMaster with mismatched new master returned wrong error: %v", err)
	}

	// run InitShardMaster
	if err := wr.InitShardMaster(ctx, master.Tablet.Keyspace, master.Tablet.Shard, master.Tablet.Alias, true /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "wrong input for SetMasterCommands") {
		t.Errorf("InitShardMaster with one failed slave returned wrong error: %v", err)
	}

	// check what was run: master should still be good
	if master.FakeMysqlDaemon.ReadOnly {
		t.Errorf("master was not turned read-write")
	}
	si, err := ts.GetShard(ctx, master.Tablet.Keyspace, master.Tablet.Shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if !topoproto.TabletAliasEqual(si.MasterAlias, master.Tablet.Alias) {
		t.Errorf("unexpected shard master alias, got %v expected %v", si.MasterAlias, master.Tablet.Alias)
	}
}
