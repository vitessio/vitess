/*
Copyright 2021 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestDeleteMasterTabletFromShardRecordInTopo(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	var uid uint32 = 100
	keyspace := "ks"
	shard := "0"
	tm := newTestTM(t, ts, uid, keyspace, shard)
	defer tm.Stop()

	err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_MASTER, DBActionSetReadWrite)
	require.NoError(t, err)

	addMasterTabletInShardRecordInTopo(ctx, t, tm)

	tablet := tm.Tablet()
	si, err := ts.GetShard(ctx, keyspace, shard)
	require.NoError(t, err)
	require.Equal(t, tablet.Alias, si.MasterAlias)
	require.NotNil(t, si.MasterTermStartTime)

	err = tm.deleteMasterTabletFromShardRecordInTopo(ctx)
	require.NoError(t, err)
	si, err = ts.GetShard(ctx, keyspace, shard)
	require.NoError(t, err)
	require.Nil(t, si.MasterAlias)
}

func TestUpdateTabletRecordInTopoDemote(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	var uid uint32 = 100
	keyspace := "ks"
	shard := "0"
	tm := newTestTM(t, ts, uid, keyspace, shard)
	defer tm.Stop()

	err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_MASTER, DBActionSetReadWrite)
	require.NoError(t, err)

	tablet := tm.Tablet()
	ti, err := ts.GetTablet(ctx, tablet.Alias)
	require.NoError(t, err)
	require.Equal(t, topodatapb.TabletType_MASTER, ti.Type)
	require.NotNil(t, ti.MasterTermStartTime)

	err = tm.updateTabletRecordInTopoDemote(ctx)
	require.NoError(t, err)

	ti, err = ts.GetTablet(ctx, tablet.Alias)
	require.NoError(t, err)
	require.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
	require.Nil(t, ti.MasterTermStartTime)
}

func TestDemotePrimaryAndUpdateTopo(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	var uid uint32 = 100
	keyspace := "ks"
	shard := "0"
	tm := newTestTM(t, ts, uid, keyspace, shard)
	defer tm.Stop()

	err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_MASTER, DBActionSetReadWrite)
	require.NoError(t, err)
	addMasterTabletInShardRecordInTopo(ctx, t, tm)

	// initial configuration
	tablet := tm.Tablet()
	si, err := ts.GetShard(ctx, keyspace, shard)
	require.NoError(t, err)
	require.Equal(t, tablet.Alias, si.MasterAlias)
	require.NotNil(t, si.MasterTermStartTime)
	ti, err := ts.GetTablet(ctx, tablet.Alias)
	require.NoError(t, err)
	require.Equal(t, topodatapb.TabletType_MASTER, ti.Type)
	require.NotNil(t, ti.MasterTermStartTime)

	_, err = tm.DemotePrimaryAndUpdateTopo(ctx)
	require.NoError(t, err)

	// final configuration
	ti, err = ts.GetTablet(ctx, tablet.Alias)
	require.NoError(t, err)
	require.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
	require.Nil(t, ti.MasterTermStartTime)
	ti, err = ts.GetTablet(ctx, tablet.Alias)
	require.NoError(t, err)
	require.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
	require.Nil(t, ti.MasterTermStartTime)
}

func addMasterTabletInShardRecordInTopo(ctx context.Context, t *testing.T, tm *TabletManager) {
	tablet := tm.Tablet()
	// We lock the shard to not conflict with reparent operations.
	ctx, unlock, lockErr := tm.TopoServer.LockShard(ctx, tablet.Keyspace, tablet.Shard, fmt.Sprintf("addMasterTabletInShardRecordInTopo(%v)", topoproto.TabletAliasString(tablet.Alias)))
	require.NoError(t, lockErr)
	defer unlock(&lockErr)
	// update the shard record's master
	_, err := tm.TopoServer.UpdateShardFields(ctx, tablet.Keyspace, tablet.Shard, func(si *topo.ShardInfo) error {
		si.MasterAlias = tablet.Alias
		si.SetMasterTermStartTime(time.Now())
		return nil
	})
	require.NoError(t, err)
}
