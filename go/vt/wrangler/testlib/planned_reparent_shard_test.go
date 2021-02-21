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
	"context"
	"errors"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good replicas
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, nil)

	// new master
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true
	newMaster.FakeMysqlDaemon.WaitMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMaster.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master
	oldMaster.FakeMysqlDaemon.ReadOnly = false
	oldMaster.FakeMysqlDaemon.Replicating = false
	oldMaster.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldMaster.FakeMysqlDaemon.CurrentMasterPosition = newMaster.FakeMysqlDaemon.WaitMasterPosition
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
		// we end up calling SetMaster twice on the old master
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetMaster is called on new master to make sure it's replicating before reparenting.
	newMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(oldMaster.Tablet)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	// using deprecated flag until it is removed completely. at that time this should be replaced with -wait_replicas_timeout
	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard})
	require.NoError(t, err)

	// check what was run
	err = newMaster.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	err = oldMaster.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newMaster.FakeMysqlDaemon.ReadOnly, "newMaster.FakeMysqlDaemon.ReadOnly is set")
	assert.True(t, oldMaster.FakeMysqlDaemon.ReadOnly, "oldMaster.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, oldMaster.TM.QueryServiceControl.IsServing(), "oldMaster...QueryServiceControl not serving")

	// verify the old master was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, oldMaster.FakeMysqlDaemon.Replicating, "oldMaster.FakeMysqlDaemon.Replicating not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
	checkSemiSyncEnabled(t, true, true, newMaster)
	checkSemiSyncEnabled(t, false, true, goodReplica1, oldMaster)
}

func TestPlannedReparentShardNoError(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

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
	newMaster.FakeMysqlDaemon.WaitMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMaster.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master
	oldMaster.FakeMysqlDaemon.ReadOnly = false
	oldMaster.FakeMysqlDaemon.Replicating = false
	oldMaster.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldMaster.FakeMysqlDaemon.CurrentMasterPosition = newMaster.FakeMysqlDaemon.WaitMasterPosition
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
		// we end up calling SetMaster twice on the old master
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetMaster is called on new master to make sure it's replicating before reparenting.
	newMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(oldMaster.Tablet)

	// goodReplica1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// goodReplica2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard, "-new_master",
		topoproto.TabletAliasString(newMaster.Tablet.Alias)})
	require.NoError(t, err)

	// check what was run
	err = newMaster.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = oldMaster.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica2.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newMaster.FakeMysqlDaemon.ReadOnly, "newMaster.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, oldMaster.FakeMysqlDaemon.ReadOnly, "oldMaster.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")

	assert.True(t, goodReplica2.FakeMysqlDaemon.ReadOnly, "goodReplica2.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, oldMaster.TM.QueryServiceControl.IsServing(), "oldMaster...QueryServiceControl not serving")

	// verify the old master was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, oldMaster.FakeMysqlDaemon.Replicating, "oldMaster.FakeMysqlDaemon.Replicating not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
	assert.False(t, goodReplica2.FakeMysqlDaemon.Replicating, "goodReplica2.FakeMysqlDaemon.Replicating set")

	checkSemiSyncEnabled(t, true, true, newMaster)
	checkSemiSyncEnabled(t, false, true, goodReplica1, goodReplica2, oldMaster)
}

func TestPlannedReparentNoMaster(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a few replicas.
	replica1 := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_REPLICA, nil)
	NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", replica1.Tablet.Keyspace + "/" + replica1.Tablet.Shard, "-new_master", topoproto.TabletAliasString(replica1.Tablet.Alias)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "the shard has no current primary")
}

// TestPlannedReparentShardWaitForPositionFail simulates a failure of the WaitForPosition call
// on the desired new master tablet
func TestPlannedReparentShardWaitForPositionFail(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

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
	newMaster.FakeMysqlDaemon.WaitMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMaster.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master
	oldMaster.FakeMysqlDaemon.ReadOnly = false
	oldMaster.FakeMysqlDaemon.Replicating = false
	// set to incorrect value to make promote fail on WaitForMasterPos
	oldMaster.FakeMysqlDaemon.CurrentMasterPosition = newMaster.FakeMysqlDaemon.PromoteResult
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)
	// SetMaster is called on new master to make sure it's replicating before reparenting.
	newMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(oldMaster.Tablet)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
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
	goodReplica2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard, "-new_master", topoproto.TabletAliasString(newMaster.Tablet.Alias)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "replication on primary-elect cell1-0000000001 did not catch up in time")

	// now check that DemoteMaster was undone and old master is still master
	assert.True(t, newMaster.FakeMysqlDaemon.ReadOnly, "newMaster.FakeMysqlDaemon.ReadOnly not set")
	assert.False(t, oldMaster.FakeMysqlDaemon.ReadOnly, "oldMaster.FakeMysqlDaemon.ReadOnly set")
}

// TestPlannedReparentShardWaitForPositionTimeout simulates a context timeout
// during the WaitForPosition call to the desired new master
func TestPlannedReparentShardWaitForPositionTimeout(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

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
	newMaster.FakeMysqlDaemon.TimeoutHook = func() error { return context.DeadlineExceeded }
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true
	newMaster.FakeMysqlDaemon.WaitMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMaster.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master
	oldMaster.FakeMysqlDaemon.ReadOnly = false
	oldMaster.FakeMysqlDaemon.Replicating = false
	oldMaster.FakeMysqlDaemon.CurrentMasterPosition = newMaster.FakeMysqlDaemon.WaitMasterPosition
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetMaster is called on new master to make sure it's replicating before reparenting.
	newMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(oldMaster.Tablet)
	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START replica",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard, "-new_master", topoproto.TabletAliasString(newMaster.Tablet.Alias)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "replication on primary-elect cell1-0000000001 did not catch up in time")

	// now check that DemoteMaster was undone and old master is still master
	assert.True(t, newMaster.FakeMysqlDaemon.ReadOnly, "newMaster.FakeMysqlDaemon.ReadOnly not set")
	assert.False(t, oldMaster.FakeMysqlDaemon.ReadOnly, "oldMaster.FakeMysqlDaemon.ReadOnly set")
}

func TestPlannedReparentShardRelayLogError(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good replicas
	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// old master
	master.FakeMysqlDaemon.ReadOnly = false
	master.FakeMysqlDaemon.Replicating = false
	master.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	master.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)
	master.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// goodReplica1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(master.Tablet)
	// simulate error that will trigger a call to RestartReplication
	goodReplica1.FakeMysqlDaemon.SetMasterError = errors.New("Slave failed to initialize relay log info structure from the repository")
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", master.Tablet.Keyspace + "/" + master.Tablet.Shard, "-new_master",
		topoproto.TabletAliasString(master.Tablet.Alias)})
	require.NoError(t, err)
	// check what was run
	err = master.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, master.FakeMysqlDaemon.ReadOnly, "master.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, master.TM.QueryServiceControl.IsServing(), "master...QueryServiceControl not serving")

	// verify the old master was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
}

// TestPlannedReparentShardRelayLogErrorStartReplication is similar to
// TestPlannedReparentShardRelayLogError with the difference that goodReplica1
// is not replicating to start with (IO_Thread is not running) and we
// simulate an error from the attempt to start replication
func TestPlannedReparentShardRelayLogErrorStartReplication(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good replicas
	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// old master
	master.FakeMysqlDaemon.ReadOnly = false
	master.FakeMysqlDaemon.Replicating = false
	master.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	master.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)
	master.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// goodReplica1 is not replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.IOThreadRunning = false
	goodReplica1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(master.Tablet)
	goodReplica1.FakeMysqlDaemon.CurrentMasterHost = master.Tablet.MysqlHostname
	goodReplica1.FakeMysqlDaemon.CurrentMasterPort = int(master.Tablet.MysqlPort)
	// simulate error that will trigger a call to RestartReplication
	goodReplica1.FakeMysqlDaemon.StartReplicationError = errors.New("Slave failed to initialize relay log info structure from the repository")
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", master.Tablet.Keyspace + "/" + master.Tablet.Shard, "-new_master",
		topoproto.TabletAliasString(master.Tablet.Alias)})
	require.NoError(t, err)
	// check what was run
	err = master.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, master.FakeMysqlDaemon.ReadOnly, "master.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, master.TM.QueryServiceControl.IsServing(), "master...QueryServiceControl not serving")

	// verify the old master was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
}

// TestPlannedReparentShardPromoteReplicaFail simulates a failure of the PromoteReplica call
// on the desired new master tablet
func TestPlannedReparentShardPromoteReplicaFail(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

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
	// make promote fail
	newMaster.FakeMysqlDaemon.PromoteError = errors.New("some error")
	newMaster.FakeMysqlDaemon.WaitMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMaster.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master
	oldMaster.FakeMysqlDaemon.ReadOnly = false
	oldMaster.FakeMysqlDaemon.Replicating = false
	oldMaster.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldMaster.FakeMysqlDaemon.CurrentMasterPosition = newMaster.FakeMysqlDaemon.WaitMasterPosition
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetMaster is called on new master to make sure it's replicating before reparenting.
	newMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(oldMaster.Tablet)
	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
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
	goodReplica2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard, "-new_master", topoproto.TabletAliasString(newMaster.Tablet.Alias)})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "some error")

	// when promote fails, we don't call UndoDemoteMaster, so the old master should be read-only
	assert.True(t, newMaster.FakeMysqlDaemon.ReadOnly, "newMaster.FakeMysqlDaemon.ReadOnly")
	assert.True(t, oldMaster.FakeMysqlDaemon.ReadOnly, "oldMaster.FakeMysqlDaemon.ReadOnly")

	// retrying should work
	newMaster.FakeMysqlDaemon.PromoteError = nil
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		// extra commands because of retry
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
		// extra commands because of retry
		"FAKE SET MASTER",
		"START SLAVE",
	}

	// run PlannedReparentShard
	err = vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard, "-new_master", topoproto.TabletAliasString(newMaster.Tablet.Alias)})
	require.NoError(t, err)

	// check that mastership changed correctly
	assert.False(t, newMaster.FakeMysqlDaemon.ReadOnly, "newMaster.FakeMysqlDaemon.ReadOnly")
	assert.True(t, oldMaster.FakeMysqlDaemon.ReadOnly, "oldMaster.FakeMysqlDaemon.ReadOnly")
}

// TestPlannedReparentShardSameMaster tests PRS with oldMaster works correctly
// Simulate failure of previous PRS and oldMaster is ReadOnly
// Verify that master correctly gets set to ReadWrite
func TestPlannedReparentShardSameMaster(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good replicas
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// old master
	oldMaster.FakeMysqlDaemon.ReadOnly = true
	oldMaster.FakeMysqlDaemon.Replicating = false
	oldMaster.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldMaster.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	oldMaster.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(oldMaster.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// goodReplica2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(oldMaster.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_replicas_timeout", "10s", "-keyspace_shard", oldMaster.Tablet.Keyspace + "/" + oldMaster.Tablet.Shard, "-new_master", topoproto.TabletAliasString(oldMaster.Tablet.Alias)})
	require.NoError(t, err)
	assert.False(t, oldMaster.FakeMysqlDaemon.ReadOnly, "oldMaster.FakeMysqlDaemon.ReadOnly")
}
