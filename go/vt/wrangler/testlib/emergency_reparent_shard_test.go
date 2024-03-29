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
	"fmt"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	wr := wrangler.New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a primary, a couple good replicas
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", "semi_sync")

	oldPrimary.FakeMysqlDaemon.Replicating = false
	oldPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = replication.Position{
		GTIDSet: replication.MariadbGTIDSet{
			2: replication.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	currentPrimaryFilePosition, _ := replication.ParseFilePosGTIDSet("mariadb-bin.000010:456")
	oldPrimary.FakeMysqlDaemon.CurrentSourceFilePosition = replication.Position{
		GTIDSet: currentPrimaryFilePosition,
	}

	// new primary
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true
	newPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = replication.Position{
		GTIDSet: replication.MariadbGTIDSet{
			2: replication.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newPrimaryRelayLogPos, _ := replication.ParseFilePosGTIDSet("relay-bin.000004:456")
	newPrimary.FakeMysqlDaemon.CurrentSourceFilePosition = replication.Position{
		GTIDSet: newPrimaryRelayLogPos,
	}
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = append(newPrimary.FakeMysqlDaemon.WaitPrimaryPositions, newPrimary.FakeMysqlDaemon.CurrentSourceFilePosition)
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE IO_THREAD",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.FakeMysqlDaemon.PromoteResult = replication.Position{
		GTIDSet: replication.MariadbGTIDSet{
			2: replication.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary, will be scrapped
	oldPrimary.FakeMysqlDaemon.ReadOnly = false
	oldPrimary.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
	}
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.CurrentPrimaryPosition = replication.Position{
		GTIDSet: replication.MariadbGTIDSet{
			2: replication.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 455,
			},
		},
	}
	goodReplica1RelayLogPos, _ := replication.ParseFilePosGTIDSet("relay-bin.000004:455")
	goodReplica1.FakeMysqlDaemon.CurrentSourceFilePosition = replication.Position{
		GTIDSet: goodReplica1RelayLogPos,
	}
	goodReplica1.FakeMysqlDaemon.WaitPrimaryPositions = append(goodReplica1.FakeMysqlDaemon.WaitPrimaryPositions, goodReplica1.FakeMysqlDaemon.CurrentSourceFilePosition)
	goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica1.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE IO_THREAD",
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.CurrentPrimaryPosition = replication.Position{
		GTIDSet: replication.MariadbGTIDSet{
			2: replication.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 454,
			},
		},
	}
	goodReplica2RelayLogPos, _ := replication.ParseFilePosGTIDSet("relay-bin.000004:454")
	goodReplica2.FakeMysqlDaemon.CurrentSourceFilePosition = replication.Position{
		GTIDSet: goodReplica2RelayLogPos,
	}
	goodReplica2.FakeMysqlDaemon.WaitPrimaryPositions = append(goodReplica2.FakeMysqlDaemon.WaitPrimaryPositions, goodReplica2.FakeMysqlDaemon.CurrentSourceFilePosition)
	goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica2.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"FAKE SET MASTER",
	}
	goodReplica2.StartActionLoop(t, wr)
	defer goodReplica2.StopActionLoop(t)

	// run EmergencyReparentShard
	waitReplicaTimeout := time.Second * 2
	err := vp.Run([]string{"EmergencyReparentShard", "--wait_replicas_timeout", waitReplicaTimeout.String(), newPrimary.Tablet.Keyspace + "/" + newPrimary.Tablet.Shard,
		topoproto.TabletAliasString(newPrimary.Tablet.Alias)})
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
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", "semi_sync")

	// new primary
	newPrimary.FakeMysqlDaemon.Replicating = true
	// It has transactions in its relay log, but not as many as
	// moreAdvancedReplica
	newPrimary.FakeMysqlDaemon.CurrentPrimaryPosition = replication.Position{
		GTIDSet: replication.MariadbGTIDSet{
			2: replication.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newPrimaryRelayLogPos, _ := replication.ParseFilePosGTIDSet("relay-bin.000004:456")
	newPrimary.FakeMysqlDaemon.CurrentSourceFilePosition = replication.Position{
		GTIDSet: newPrimaryRelayLogPos,
	}
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = append(newPrimary.FakeMysqlDaemon.WaitPrimaryPositions, newPrimary.FakeMysqlDaemon.CurrentSourceFilePosition)
	newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(newPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(moreAdvancedReplica.Tablet))
	newPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE IO_THREAD",
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, primary_alias, replication_position) VALUES",
	}
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// old primary, will be scrapped
	oldPrimary.FakeMysqlDaemon.ReplicationStatusError = fmt.Errorf("old primary stopped working")
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	// more advanced replica
	moreAdvancedReplica.FakeMysqlDaemon.Replicating = true
	// relay log position is more advanced than desired new primary
	moreAdvancedReplica.FakeMysqlDaemon.CurrentPrimaryPosition = replication.Position{
		GTIDSet: replication.MariadbGTIDSet{
			2: replication.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	moreAdvancedReplicaLogPos, _ := replication.ParseFilePosGTIDSet("relay-bin.000004:457")
	moreAdvancedReplica.FakeMysqlDaemon.CurrentSourceFilePosition = replication.Position{
		GTIDSet: moreAdvancedReplicaLogPos,
	}
	moreAdvancedReplica.FakeMysqlDaemon.SetReplicationSourceInputs = append(moreAdvancedReplica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	moreAdvancedReplica.FakeMysqlDaemon.WaitPrimaryPositions = append(moreAdvancedReplica.FakeMysqlDaemon.WaitPrimaryPositions, moreAdvancedReplica.FakeMysqlDaemon.CurrentSourceFilePosition)
	newPrimary.FakeMysqlDaemon.WaitPrimaryPositions = append(newPrimary.FakeMysqlDaemon.WaitPrimaryPositions, moreAdvancedReplica.FakeMysqlDaemon.CurrentPrimaryPosition)
	moreAdvancedReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE IO_THREAD",
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	moreAdvancedReplica.StartActionLoop(t, wr)
	defer moreAdvancedReplica.StopActionLoop(t)

	// run EmergencyReparentShard
	err := wr.EmergencyReparentShard(ctx, newPrimary.Tablet.Keyspace, newPrimary.Tablet.Shard, reparentutil.EmergencyReparentOptions{
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
