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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestEmergencyReparentShard(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	wr := wrangler.New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(ctx, t, ts)
	defer vp.Close()

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", policy.DurabilitySemiSync)

	sourceUUID, err := replication.ParseSID("3E11FA47-71CA-11E1-9E33-C80AA9429562")
	require.NoError(t, err)

	oldPrimaryPos, err := replication.ParseMysql56GTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-7")
	require.NoError(t, err)
	oldPrimary.FakeMysqlDaemon.Replicating = false
	oldPrimary.FakeMysqlDaemon.SetPrimaryPositionLocked(replication.Position{
		GTIDSet: oldPrimaryPos,
	})
	oldPrimary.FakeMysqlDaemon.CurrentRelayLogPosition = replication.Position{
		GTIDSet: oldPrimaryPos,
	}

	// new primary (equal GTID to old primary)
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true
	newPrimary.FakeMysqlDaemon.CurrentSourceUUID = sourceUUID
	newPrimary.FakeMysqlDaemon.SetPrimaryPositionLocked(replication.Position{
		GTIDSet: oldPrimaryPos,
	})
	newPrimary.FakeMysqlDaemon.CurrentRelayLogPosition = replication.Position{
		GTIDSet: oldPrimaryPos,
	}
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = append(newPrimary.FakeMysqlDaemon.WaitPrimaryPositions, newPrimary.FakeMysqlDaemon.CurrentRelayLogPosition)
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA IO_THREAD",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.FakeMysqlDaemon.PromoteResult = replication.Position{
		GTIDSet: oldPrimaryPos,
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary, will be scrapped
	oldPrimary.FakeMysqlDaemon.ReadOnly = false
	oldPrimary.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA",
	}
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	// good replica 1 is replicating
	goodReplica1Pos, err := replication.ParseMysql56GTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-6")
	require.NoError(t, err)
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.CurrentSourceUUID = sourceUUID
	goodReplica1.FakeMysqlDaemon.SetPrimaryPositionLocked(replication.Position{
		GTIDSet: goodReplica1Pos,
	})
	goodReplica1.FakeMysqlDaemon.CurrentRelayLogPosition = replication.Position{
		GTIDSet: goodReplica1Pos,
	}
	goodReplica1.FakeMysqlDaemon.WaitPrimaryPositions = append(goodReplica1.FakeMysqlDaemon.WaitPrimaryPositions, goodReplica1.FakeMysqlDaemon.CurrentRelayLogPosition)
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
		"STOP REPLICA IO_THREAD",
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2Pos, err := replication.ParseMysql56GTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:1")
	require.NoError(t, err)
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.CurrentSourceUUID = sourceUUID
	goodReplica2.FakeMysqlDaemon.SetPrimaryPositionLocked(replication.Position{
		GTIDSet: goodReplica2Pos,
	})
	goodReplica2.FakeMysqlDaemon.CurrentRelayLogPosition = replication.Position{
		GTIDSet: goodReplica2Pos,
	}
	goodReplica2.FakeMysqlDaemon.WaitPrimaryPositions = append(goodReplica2.FakeMysqlDaemon.WaitPrimaryPositions, goodReplica2.FakeMysqlDaemon.CurrentRelayLogPosition)
	goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
		"FAKE SET SOURCE",
	}
	goodReplica2.StartActionLoop(t, wr)
	defer goodReplica2.StopActionLoop(t)

	// run EmergencyReparentShard
	waitReplicaTimeout := time.Second * 2
	err = vp.Run([]string{
		"EmergencyReparentShard", "--wait_replicas_timeout", waitReplicaTimeout.String(), newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard,
		topoproto.TabletAliasString(newPrimary.Tablet.Alias),
	})
	require.NoError(t, err)
	// check what was run
	err = newPrimary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newPrimary.FakeMysqlDaemon.ReadOnly, "newPrimary.FakeMysqlDaemon.ReadOnly set")
	checkSemiSyncEnabled(t, true, true, newPrimary)
}

// TestEmergencyReparentShardPrimaryElectNotBest tries to emergency reparent
// to a host that is not the latest in replication position.
func TestEmergencyReparentShardPrimaryElectNotBest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	wr := wrangler.New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	moreAdvancedReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", policy.DurabilitySemiSync)

	sourceUUID, err := replication.ParseSID("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	require.NoError(t, err)

	// new primary
	newPrimary.FakeMysqlDaemon.Replicating = true
	newPrimary.FakeMysqlDaemon.CurrentSourceUUID = sourceUUID
	// It has transactions in its relay log, but not as many as
	// moreAdvancedReplica
	newPrimaryPos, err := replication.ParseMysql56GTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-456")
	require.NoError(t, err)
	newPrimary.FakeMysqlDaemon.SetPrimaryPositionLocked(replication.Position{
		GTIDSet: newPrimaryPos,
	})
	newPrimaryRelayLogPos, err := replication.ParseMysql56GTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-456")
	require.NoError(t, err)
	newPrimary.FakeMysqlDaemon.CurrentRelayLogPosition = replication.Position{
		GTIDSet: newPrimaryRelayLogPos,
	}
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = append(newPrimary.FakeMysqlDaemon.WaitPrimaryPositions, newPrimary.FakeMysqlDaemon.CurrentRelayLogPosition)
	newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(moreAdvancedReplica.Tablet))
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA IO_THREAD",
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary, will be scrapped
	oldPrimary.FakeMysqlDaemon.ReplicationStatusError = errors.New("old primary stopped working")
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	// more advanced replica
	moreAdvancedReplica.FakeMysqlDaemon.Replicating = true
	moreAdvancedReplica.FakeMysqlDaemon.CurrentSourceUUID = sourceUUID
	// relay log position is more advanced than desired new primary
	moreAdvancedReplicaPrimaryLogPos, err := replication.ParseMysql56GTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-457")
	require.NoError(t, err)
	moreAdvancedReplica.FakeMysqlDaemon.SetPrimaryPositionLocked(replication.Position{GTIDSet: moreAdvancedReplicaPrimaryLogPos})
	moreAdvancedReplicaLogPos, err := replication.ParseMysql56GTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-457")
	require.NoError(t, err)
	moreAdvancedReplica.FakeMysqlDaemon.CurrentRelayLogPosition = replication.Position{
		GTIDSet: moreAdvancedReplicaLogPos,
	}
	moreAdvancedReplica.FakeMysqlDaemon.SetReplicationSourceInputs = append(moreAdvancedReplica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	moreAdvancedReplica.FakeMysqlDaemon.WaitPrimaryPositions = append(moreAdvancedReplica.FakeMysqlDaemon.WaitPrimaryPositions, moreAdvancedReplica.FakeMysqlDaemon.CurrentRelayLogPosition)
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = append(newPrimary.FakeMysqlDaemon.WaitPrimaryPositions, moreAdvancedReplica.FakeMysqlDaemon.GetPrimaryPositionLocked())
	moreAdvancedReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
		"STOP REPLICA IO_THREAD",
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
	}
	moreAdvancedReplica.StartActionLoop(t, wr)
	defer moreAdvancedReplica.StopActionLoop(t)

	// run EmergencyReparentShard
	err = wr.EmergencyReparentShard(ctx, newPrimary.Tablet.Keyspace, newPrimary.Tablet.Shard, reparentutil.EmergencyReparentOptions{
		NewPrimaryAlias:           newPrimary.Tablet.Alias,
		WaitAllTablets:            false,
		WaitReplicasTimeout:       10 * time.Second,
		IgnoreReplicas:            sets.New[string](),
		PreventCrossCellPromotion: false,
	})
	cancel()

	assert.NoError(t, err)
	// check what was run
	err = newPrimary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = oldPrimary.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = moreAdvancedReplica.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
}
