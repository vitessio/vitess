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

	"vitess.io/vitess/go/vt/mysqlctl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestPlannedReparentShardNoPrimaryProvided(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", "semi_sync")

	// new primary
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = []mysql.Position{{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}}
	newPrimary.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary
	oldPrimary.FakeMysqlDaemon.ReadOnly = false
	oldPrimary.FakeMysqlDaemon.Replicating = false
	oldPrimary.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = newPrimary.FakeMysqlDaemon.WaitPrimaryPositions[0]
	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// we end up calling SetReplicationSource twice on the old primary
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)
	oldPrimary.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetReplicationSource is called on new primary to make sure it's replicating before reparenting.
	newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard})
	require.NoError(t, err)

	// check what was run
	err = newPrimary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	err = oldPrimary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newPrimary.FakeMysqlDaemon.ReadOnly, "newPrimary.FakeMysqlDaemon.ReadOnly is set")
	assert.True(t, oldPrimary.FakeMysqlDaemon.ReadOnly, "oldPrimary.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, oldPrimary.TM.QueryServiceControl.IsServing(), "oldPrimary...QueryServiceControl not serving")

	// verify the old primary was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, oldPrimary.FakeMysqlDaemon.Replicating, "oldPrimary.FakeMysqlDaemon.Replicating not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
	checkSemiSyncEnabled(t, true, true, newPrimary)
	checkSemiSyncEnabled(t, false, true, goodReplica1, oldPrimary)
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

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", "semi_sync")

	// new primary
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = []mysql.Position{{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}}
	newPrimary.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary
	oldPrimary.FakeMysqlDaemon.ReadOnly = false
	oldPrimary.FakeMysqlDaemon.Replicating = false
	oldPrimary.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = newPrimary.FakeMysqlDaemon.WaitPrimaryPositions[0]
	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// we end up calling SetReplicationSource twice on the old primary
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)
	oldPrimary.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetReplicationSource is called on new primary to make sure it's replicating before reparenting.
	newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))

	// goodReplica1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// goodReplica2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
	}
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.Replicating = false
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard, "--new_primary",
		topoproto.TabletAliasString(newPrimary.Tablet.Alias)})
	require.NoError(t, err)

	// check what was run
	err = newPrimary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = oldPrimary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica2.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newPrimary.FakeMysqlDaemon.ReadOnly, "newPrimary.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, oldPrimary.FakeMysqlDaemon.ReadOnly, "oldPrimary.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")

	assert.True(t, goodReplica2.FakeMysqlDaemon.ReadOnly, "goodReplica2.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, oldPrimary.TM.QueryServiceControl.IsServing(), "oldPrimary...QueryServiceControl not serving")

	// verify the old primary was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, oldPrimary.FakeMysqlDaemon.Replicating, "oldPrimary.FakeMysqlDaemon.Replicating not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
	assert.False(t, goodReplica2.FakeMysqlDaemon.Replicating, "goodReplica2.FakeMysqlDaemon.Replicating set")

	checkSemiSyncEnabled(t, true, true, newPrimary)
	checkSemiSyncEnabled(t, false, true, goodReplica1, goodReplica2, oldPrimary)
}

func TestPlannedReparentInitialization(t *testing.T) {
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
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", "semi_sync")

	// new primary
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true
	newPrimary.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		mysqlctl.GenerateInitialBinlogEntry(),
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// goodReplica1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// goodReplica2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard, "--new_primary", topoproto.TabletAliasString(newPrimary.Tablet.Alias)})
	require.NoError(t, err)

	// check what was run
	err = newPrimary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica2.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newPrimary.FakeMysqlDaemon.ReadOnly, "newPrimary.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica2.FakeMysqlDaemon.ReadOnly, "goodReplica2.FakeMysqlDaemon.ReadOnly not set")

	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
	assert.False(t, goodReplica2.FakeMysqlDaemon.Replicating, "goodReplica2.FakeMysqlDaemon.Replicating set")

	checkSemiSyncEnabled(t, true, true, newPrimary)
	checkSemiSyncEnabled(t, false, true, goodReplica1, goodReplica2)
}

// TestPlannedReparentShardWaitForPositionFail simulates a failure of the WaitForPosition call
// on the desired new primary tablet
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

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new primary
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = []mysql.Position{{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}}
	newPrimary.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary
	oldPrimary.FakeMysqlDaemon.ReadOnly = false
	oldPrimary.FakeMysqlDaemon.Replicating = false
	// set to incorrect value to make promote fail on WaitForReplicationPos
	oldPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = newPrimary.FakeMysqlDaemon.PromoteResult
	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)
	oldPrimary.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)
	// SetReplicationSource is called on new primary to make sure it's replicating before reparenting.
	newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
	}
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.Replicating = false
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard, "--new_primary", topoproto.TabletAliasString(newPrimary.Tablet.Alias)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "replication on primary-elect cell1-0000000001 did not catch up in time")

	// now check that DemotePrimary was undone and old primary is still primary
	assert.True(t, newPrimary.FakeMysqlDaemon.ReadOnly, "newPrimary.FakeMysqlDaemon.ReadOnly not set")
	assert.False(t, oldPrimary.FakeMysqlDaemon.ReadOnly, "oldPrimary.FakeMysqlDaemon.ReadOnly set")
}

// TestPlannedReparentShardWaitForPositionTimeout simulates a context timeout
// during the WaitForPosition call to the desired new primary
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

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new primary
	newPrimary.FakeMysqlDaemon.TimeoutHook = func() error { return context.DeadlineExceeded }
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = []mysql.Position{{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}}
	newPrimary.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary
	oldPrimary.FakeMysqlDaemon.ReadOnly = false
	oldPrimary.FakeMysqlDaemon.Replicating = false
	oldPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = newPrimary.FakeMysqlDaemon.WaitPrimaryPositions[0]
	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)
	oldPrimary.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetReplicationSource is called on new primary to make sure it's replicating before reparenting.
	newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))
	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
	}
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.Replicating = false
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard, "--new_primary", topoproto.TabletAliasString(newPrimary.Tablet.Alias)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "replication on primary-elect cell1-0000000001 did not catch up in time")

	// now check that DemotePrimary was undone and old primary is still primary
	assert.True(t, newPrimary.FakeMysqlDaemon.ReadOnly, "newPrimary.FakeMysqlDaemon.ReadOnly not set")
	assert.False(t, oldPrimary.FakeMysqlDaemon.ReadOnly, "oldPrimary.FakeMysqlDaemon.ReadOnly set")
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

	// Create a primary, a couple good replicas
	primary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// old primary
	primary.FakeMysqlDaemon.ReadOnly = false
	primary.FakeMysqlDaemon.Replicating = false
	primary.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	primary.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	primary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	primary.StartActionLoop(t, wr)
	defer primary.StopActionLoop(t)
	primary.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// goodReplica1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// simulate error that will trigger a call to RestartReplication
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceError = errors.New("Slave failed to initialize relay log info structure from the repository")
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", primary.Tablet.Keyspace + "/" + primary.Tablet.Shard, "--new_primary",
		topoproto.TabletAliasString(primary.Tablet.Alias)})
	require.NoError(t, err)
	// check what was run
	err = primary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, primary.FakeMysqlDaemon.ReadOnly, "primary.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, primary.TM.QueryServiceControl.IsServing(), "primary...QueryServiceControl not serving")

	// verify the old primary was told to start replicating (and not
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

	// Create a primary, a couple good replicas
	primary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", "semi_sync")

	// old primary
	primary.FakeMysqlDaemon.ReadOnly = false
	primary.FakeMysqlDaemon.Replicating = false
	primary.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	primary.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	primary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	primary.StartActionLoop(t, wr)
	defer primary.StopActionLoop(t)
	primary.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// goodReplica1 is not replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.IOThreadRunning = false
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))
	goodReplica1.FakeMysqlDaemon.CurrentSourceHost = primary.Tablet.MysqlHostname
	goodReplica1.FakeMysqlDaemon.CurrentSourcePort = int(primary.Tablet.MysqlPort)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// simulate error that will trigger a call to RestartReplication
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// In SetReplicationSource, we find that the source host and port was already set correctly,
		// So we try to stop and start replication. The first STOP SLAVE comes from there
		"STOP SLAVE",
		// During the START SLAVE call, we find a relay log error, so we try to restart replication.
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	goodReplica1.FakeMysqlDaemon.StartReplicationError = errors.New("Slave failed to initialize relay log info structure from the repository")
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", primary.Tablet.Keyspace + "/" + primary.Tablet.Shard, "--new_primary",
		topoproto.TabletAliasString(primary.Tablet.Alias)})
	require.NoError(t, err)
	// check what was run
	err = primary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, primary.FakeMysqlDaemon.ReadOnly, "primary.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, primary.TM.QueryServiceControl.IsServing(), "primary...QueryServiceControl not serving")

	// verify the old primary was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
}

// TestPlannedReparentShardPromoteReplicaFail simulates a failure of the PromoteReplica call
// on the desired new primary tablet
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

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new primary
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true
	// make promote fail
	newPrimary.FakeMysqlDaemon.PromoteError = errors.New("some error")
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = []mysql.Position{{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}}
	newPrimary.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary
	oldPrimary.FakeMysqlDaemon.ReadOnly = false
	oldPrimary.FakeMysqlDaemon.Replicating = false
	oldPrimary.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = newPrimary.FakeMysqlDaemon.WaitPrimaryPositions[0]
	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)
	oldPrimary.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetReplicationSource is called on new primary to make sure it's replicating before reparenting.
	newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))
	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
	}
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.Replicating = false
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard, "--new_primary", topoproto.TabletAliasString(newPrimary.Tablet.Alias)})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "some error")

	// when promote fails, we don't call UndoDemotePrimary, so the old primary should be read-only
	assert.True(t, newPrimary.FakeMysqlDaemon.ReadOnly, "newPrimary.FakeMysqlDaemon.ReadOnly")
	assert.True(t, oldPrimary.FakeMysqlDaemon.ReadOnly, "oldPrimary.FakeMysqlDaemon.ReadOnly")

	// retrying should work
	newPrimary.FakeMysqlDaemon.PromoteError = nil
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// extra commands because of retry
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// extra commands because of retry
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}

	// run PlannedReparentShard
	err = vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard, "--new_primary", topoproto.TabletAliasString(newPrimary.Tablet.Alias)})
	require.NoError(t, err)

	// check that primary changed correctly
	assert.False(t, newPrimary.FakeMysqlDaemon.ReadOnly, "newPrimary.FakeMysqlDaemon.ReadOnly")
	assert.True(t, oldPrimary.FakeMysqlDaemon.ReadOnly, "oldPrimary.FakeMysqlDaemon.ReadOnly")
}

// TestPlannedReparentShardSamePrimary tests PRS with oldPrimary works correctly
// Simulate failure of previous PRS and oldPrimary is ReadOnly
// Verify that primary correctly gets set to ReadWrite
func TestPlannedReparentShardSamePrimary(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// old primary
	oldPrimary.FakeMysqlDaemon.ReadOnly = true
	oldPrimary.FakeMysqlDaemon.Replicating = false
	oldPrimary.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)
	oldPrimary.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// goodReplica2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
	}
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.Replicating = false
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "--wait_replicas_timeout", "10s", "--keyspace_shard", oldPrimary.Tablet.Keyspace + "/" + oldPrimary.Tablet.Shard, "--new_primary", topoproto.TabletAliasString(oldPrimary.Tablet.Alias)})
	require.NoError(t, err)
	assert.False(t, oldPrimary.FakeMysqlDaemon.ReadOnly, "oldPrimary.FakeMysqlDaemon.ReadOnly")
}
