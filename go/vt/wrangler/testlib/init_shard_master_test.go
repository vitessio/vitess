// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"strings"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	"testing"
)

// TestInitMasterShard is the good scenario test, where everything
// works as planned
func TestInitMasterShard(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good slaves
	master := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
	goodSlave1 := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)
	goodSlave2 := NewFakeTablet(t, wr, "cell2", 2, topo.TYPE_REPLICA)

	// Master: set a plausible ReplicationPosition to return,
	// and expect to add entry in _vt.reparent_journal
	master.FakeMysqlDaemon.CurrentMasterPosition = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   5,
			Server:   456,
			Sequence: 890,
		},
	}
	master.FakeMysqlDaemon.ReadOnly = true
	master.FakeMysqlDaemon.ResetReplicationResult = []string{"reset rep 1"}
	master.FakeMysqlDaemon.StartReplicationCommandsResult = []string{"new master shouldn't use this"}
	master.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"reset rep 1",
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
	goodSlave1.FakeMysqlDaemon.ResetReplicationResult = []string{"reset rep 1"}
	goodSlave1.FakeMysqlDaemon.StartReplicationCommandsStatus = &myproto.ReplicationStatus{
		Position:           master.FakeMysqlDaemon.CurrentMasterPosition,
		MasterHost:         master.Tablet.Hostname,
		MasterPort:         master.Tablet.Portmap["mysql"],
		MasterConnectRetry: 10,
	}
	goodSlave1.FakeMysqlDaemon.StartReplicationCommandsResult = []string{"cmd1"}
	goodSlave1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"reset rep 1",
		"cmd1",
	}
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	// Slave2: expect to be re-parented
	goodSlave2.FakeMysqlDaemon.ReadOnly = true
	goodSlave2.FakeMysqlDaemon.ResetReplicationResult = []string{"reset rep 2"}
	goodSlave2.FakeMysqlDaemon.StartReplicationCommandsStatus = &myproto.ReplicationStatus{
		Position:           master.FakeMysqlDaemon.CurrentMasterPosition,
		MasterHost:         master.Tablet.Hostname,
		MasterPort:         master.Tablet.Portmap["mysql"],
		MasterConnectRetry: 10,
	}
	goodSlave2.FakeMysqlDaemon.StartReplicationCommandsResult = []string{"cmd1", "cmd2"}
	goodSlave2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"reset rep 2",
		"cmd1",
		"cmd2",
	}
	goodSlave2.StartActionLoop(t, wr)
	defer goodSlave2.StopActionLoop(t)

	// run InitShardMaster
	if err := vp.Run([]string{"InitShardMaster", "-wait_slave_timeout", "10s", master.Tablet.Keyspace + "/" + master.Tablet.Shard, master.Tablet.Alias.String()}); err != nil {
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
	if topo.ProtoToTabletAlias(si.MasterAlias) != master.Tablet.Alias {
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
}

// TestInitMasterShardChecks makes sure the safety checks work
func TestInitMasterShardChecks(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	master := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)

	// InitShardMaster with an unknown tablet
	if err := wr.InitShardMaster(ctx, master.Tablet.Keyspace, master.Tablet.Shard, topo.TabletAlias{
		Cell: master.Tablet.Alias.Cell,
		Uid:  master.Tablet.Alias.Uid + 1,
	}, false /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is not in the shard") {
		t.Errorf("InitShardMaster with unknown alias returned wrong error: %v", err)
	}

	// InitShardMaster with two masters in the shard, no force flag
	// (master2 needs to run InitTablet with -force, as it is the second
	// master in the same shard)
	master2 := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_MASTER, ForceInitTablet())
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
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	// Create a master, a couple slaves
	master := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
	goodSlave := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)
	badSlave := NewFakeTablet(t, wr, "cell2", 2, topo.TYPE_REPLICA)

	// Master: set a plausible ReplicationPosition to return,
	// and expect to add entry in _vt.reparent_journal
	master.FakeMysqlDaemon.CurrentMasterPosition = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   5,
			Server:   456,
			Sequence: 890,
		},
	}
	master.FakeMysqlDaemon.ReadOnly = true
	master.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
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
	goodSlave.FakeMysqlDaemon.StartReplicationCommandsStatus = &myproto.ReplicationStatus{
		Position:           master.FakeMysqlDaemon.CurrentMasterPosition,
		MasterHost:         master.Tablet.Hostname,
		MasterPort:         master.Tablet.Portmap["mysql"],
		MasterConnectRetry: 10,
	}
	goodSlave.FakeMysqlDaemon.StartReplicationCommandsResult = []string{"cmd1"}
	goodSlave.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = goodSlave.FakeMysqlDaemon.StartReplicationCommandsResult
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// badSlave: insert an error by failing the ReplicationStatus input
	// on purpose
	badSlave.FakeMysqlDaemon.ReadOnly = true
	badSlave.FakeMysqlDaemon.StartReplicationCommandsStatus = &myproto.ReplicationStatus{
		Position:           master.FakeMysqlDaemon.CurrentMasterPosition,
		MasterHost:         "",
		MasterPort:         0,
		MasterConnectRetry: 10,
	}
	badSlave.StartActionLoop(t, wr)
	defer badSlave.StopActionLoop(t)

	// also change the master alias in the Shard object, to make sure it
	// is set back.
	si, err := ts.GetShard(ctx, master.Tablet.Keyspace, master.Tablet.Shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.MasterAlias.Uid++
	if err := topo.UpdateShard(ctx, ts, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}

	// run InitShardMaster without force, it fails because master is
	// changing.
	if err := wr.InitShardMaster(ctx, master.Tablet.Keyspace, master.Tablet.Shard, master.Tablet.Alias, false /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is not the shard master") {
		t.Errorf("InitShardMaster with mismatched new master returned wrong error: %v", err)
	}

	// run InitShardMaster
	if err := wr.InitShardMaster(ctx, master.Tablet.Keyspace, master.Tablet.Shard, master.Tablet.Alias, true /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "wrong status for StartReplicationCommands") {
		t.Errorf("InitShardMaster with one failed slave returned wrong error: %v", err)
	}

	// check what was run: master should still be good
	if master.FakeMysqlDaemon.ReadOnly {
		t.Errorf("master was not turned read-write")
	}
	si, err = ts.GetShard(ctx, master.Tablet.Keyspace, master.Tablet.Shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if topo.ProtoToTabletAlias(si.MasterAlias) != master.Tablet.Alias {
		t.Errorf("unexpected shard master alias, got %v expected %v", si.MasterAlias, master.Tablet.Alias)
	}
}
