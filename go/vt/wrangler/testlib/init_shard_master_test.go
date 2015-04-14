// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
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

func TestInitMasterShard(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

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
	master.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// Slave1: expect to be re-parented
	goodSlave1.FakeMysqlDaemon.ReadOnly = true
	goodSlave1.FakeMysqlDaemon.StartReplicationCommandsStatus = &myproto.ReplicationStatus{
		Position:           master.FakeMysqlDaemon.CurrentMasterPosition,
		MasterHost:         master.Tablet.Hostname,
		MasterPort:         master.Tablet.Portmap["mysql"],
		MasterConnectRetry: 10,
	}
	goodSlave1.FakeMysqlDaemon.StartReplicationCommandsResult = []string{"cmd1"}
	goodSlave1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = goodSlave1.FakeMysqlDaemon.StartReplicationCommandsResult
	goodSlave1.StartActionLoop(t, wr)
	defer goodSlave1.StopActionLoop(t)

	// Slave2: expect to be re-parented
	goodSlave2.FakeMysqlDaemon.ReadOnly = true
	goodSlave2.FakeMysqlDaemon.StartReplicationCommandsStatus = &myproto.ReplicationStatus{
		Position:           master.FakeMysqlDaemon.CurrentMasterPosition,
		MasterHost:         master.Tablet.Hostname,
		MasterPort:         master.Tablet.Portmap["mysql"],
		MasterConnectRetry: 10,
	}
	goodSlave2.FakeMysqlDaemon.StartReplicationCommandsResult = []string{"cmd1", "cmd2"}
	goodSlave2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = goodSlave2.FakeMysqlDaemon.StartReplicationCommandsResult
	goodSlave2.StartActionLoop(t, wr)
	defer goodSlave2.StopActionLoop(t)

	// run InitShardMaster
	if err := wr.InitShardMaster(ctx, "test_keyspace", "0", master.Tablet.Alias, false /*force*/, 10*time.Second); err != nil {
		t.Fatalf("original InitShardMaster failed: %v", err)
	}

	// check what was run
	if master.FakeMysqlDaemon.ReadOnly {
		t.Errorf("master was not turned read-write")
	}
	si, err := ts.GetShard("test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if si.MasterAlias != master.Tablet.Alias {
		t.Errorf("unexpected shard master alias, got %v expected %v", si.MasterAlias, master.Tablet.Alias)
	}
}
