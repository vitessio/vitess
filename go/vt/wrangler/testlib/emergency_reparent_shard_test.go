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
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
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

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a master, a couple good replicas
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	oldMaster.FakeMysqlDaemon.Replicating = false
	oldMaster.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	currentMasterFilePosition, _ := mysql.ParseFilePosGTIDSet("mariadb-bin.000010:456")
	oldMaster.FakeMysqlDaemon.CurrentMasterFilePosition = mysql.Position{
		GTIDSet: currentMasterFilePosition,
	}

	// new master
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true
	newMaster.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMasterRelayLogPos, _ := mysql.ParseFilePosGTIDSet("relay-bin.000004:456")
	newMaster.FakeMysqlDaemon.CurrentMasterFilePosition = mysql.Position{
		GTIDSet: newMasterRelayLogPos,
	}
	newMaster.FakeMysqlDaemon.WaitMasterPosition = newMaster.FakeMysqlDaemon.CurrentMasterFilePosition
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE IO_THREAD",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}
	newMaster.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
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
	oldMaster.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
	}
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 455,
			},
		},
	}
	goodReplica1RelayLogPos, _ := mysql.ParseFilePosGTIDSet("relay-bin.000004:455")
	goodReplica1.FakeMysqlDaemon.CurrentMasterFilePosition = mysql.Position{
		GTIDSet: goodReplica1RelayLogPos,
	}
	goodReplica1.FakeMysqlDaemon.WaitMasterPosition = goodReplica1.FakeMysqlDaemon.CurrentMasterFilePosition
	goodReplica1.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
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
	goodReplica2.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 454,
			},
		},
	}
	goodReplica2RelayLogPos, _ := mysql.ParseFilePosGTIDSet("relay-bin.000004:454")
	goodReplica2.FakeMysqlDaemon.CurrentMasterFilePosition = mysql.Position{
		GTIDSet: goodReplica2RelayLogPos,
	}
	goodReplica2.FakeMysqlDaemon.WaitMasterPosition = goodReplica2.FakeMysqlDaemon.CurrentMasterFilePosition
	goodReplica2.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run EmergencyReparentShard
	// using deprecated flag until it is removed completely. at that time this should be replaced with -wait_replicas_timeout
	waitReplicaTimeout := time.Second * 2
	err := vp.Run([]string{"EmergencyReparentShard", "-wait_replicas_timeout", waitReplicaTimeout.String(), newMaster.Tablet.Keyspace + "/" + newMaster.Tablet.Shard,
		topoproto.TabletAliasString(newMaster.Tablet.Alias)})
	require.NoError(t, err)
	// check what was run
	err = newMaster.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newMaster.FakeMysqlDaemon.ReadOnly, "newMaster.FakeMysqlDaemon.ReadOnly set")
	checkSemiSyncEnabled(t, true, true, newMaster)
}

// TestEmergencyReparentShardMasterElectNotBest tries to emergency reparent
// to a host that is not the latest in replication position.
func TestEmergencyReparentShardMasterElectNotBest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

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
			2: mysql.MariadbGTID{
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
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMasterRelayLogPos, _ := mysql.ParseFilePosGTIDSet("relay-bin.000004:456")
	newMaster.FakeMysqlDaemon.CurrentMasterFilePosition = mysql.Position{
		GTIDSet: newMasterRelayLogPos,
	}
	newMaster.FakeMysqlDaemon.WaitMasterPosition = newMaster.FakeMysqlDaemon.CurrentMasterFilePosition
	newMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE IO_THREAD",
	}
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// old master, will be scrapped
	oldMaster.FakeMysqlDaemon.ReplicationStatusError = mysql.ErrNotReplica
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// more advanced replica
	moreAdvancedReplica.FakeMysqlDaemon.Replicating = true
	// position up to which this replica has executed is behind desired new master
	moreAdvancedReplica.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 454,
			},
		},
	}
	// relay log position is more advanced than desired new master
	moreAdvancedReplica.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	moreAdvancedReplicaLogPos, _ := mysql.ParseFilePosGTIDSet("relay-bin.000004:457")
	moreAdvancedReplica.FakeMysqlDaemon.CurrentMasterFilePosition = mysql.Position{
		GTIDSet: moreAdvancedReplicaLogPos,
	}
	moreAdvancedReplica.FakeMysqlDaemon.WaitMasterPosition = moreAdvancedReplica.FakeMysqlDaemon.CurrentMasterFilePosition
	moreAdvancedReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE IO_THREAD",
	}
	moreAdvancedReplica.StartActionLoop(t, wr)
	defer moreAdvancedReplica.StopActionLoop(t)

	// run EmergencyReparentShard
	err := wr.EmergencyReparentShard(ctx, newMaster.Tablet.Keyspace, newMaster.Tablet.Shard, newMaster.Tablet.Alias, 10*time.Second, sets.NewString())
	cancel()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is not fully caught up")
	// check what was run
	err = newMaster.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = oldMaster.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = moreAdvancedReplica.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
}
