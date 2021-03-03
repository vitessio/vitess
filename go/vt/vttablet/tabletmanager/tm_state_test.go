/*
Copyright 2020 The Vitess Authors.

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
	"encoding/json"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/servenv"

	"context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

func TestStateOpenClose(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 1, "ks", "0")

	// Re-Open should be a no-op
	tm.tmState.mu.Lock()
	savedCtx := tm.tmState.ctx
	tm.tmState.mu.Unlock()

	tm.tmState.Open()

	tm.tmState.mu.Lock()
	assert.Equal(t, savedCtx, tm.tmState.ctx)
	tm.tmState.mu.Unlock()

	tm.Close()
	tm.tmState.mu.Lock()
	assert.False(t, tm.tmState.isOpen)
	tm.tmState.mu.Unlock()
}

func TestStateRefreshFromTopo(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 1, "ks", "0")
	defer tm.Stop()

	err := tm.RefreshState(ctx)
	require.NoError(t, err)
}

func TestStateResharding(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 1, "ks", "0")
	defer tm.Stop()

	tm.tmState.mu.Lock()
	tm.tmState.tablet.Type = topodatapb.TabletType_MASTER
	tm.tmState.mu.Unlock()

	si := &topo.ShardInfo{
		Shard: &topodatapb.Shard{
			SourceShards: []*topodatapb.Shard_SourceShard{{
				Uid: 1,
			}},
		},
	}
	tm.tmState.RefreshFromTopoInfo(ctx, si, nil)
	tm.tmState.mu.Lock()
	assert.True(t, tm.tmState.isResharding)
	tm.tmState.mu.Unlock()

	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	assert.Equal(t, topodatapb.TabletType_MASTER, qsc.CurrentTarget().TabletType)
	assert.False(t, qsc.IsServing())
}

func TestStateBlacklist(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 1, "ks", "0")
	defer tm.Stop()

	fmd := tm.MysqlDaemon.(*fakemysqldaemon.FakeMysqlDaemon)
	fmd.Schema = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name: "t1",
		}},
	}
	si := &topo.ShardInfo{
		Shard: &topodatapb.Shard{
			TabletControls: []*topodatapb.Shard_TabletControl{{
				TabletType:        topodatapb.TabletType_REPLICA,
				Cells:             []string{"cell1"},
				BlacklistedTables: []string{"t1"},
			}},
		},
	}
	tm.tmState.RefreshFromTopoInfo(ctx, si, nil)
	tm.tmState.mu.Lock()
	assert.Equal(t, map[topodatapb.TabletType][]string{topodatapb.TabletType_REPLICA: {"t1"}}, tm.tmState.blacklistedTables)
	tm.tmState.mu.Unlock()

	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	b, _ := json.Marshal(qsc.GetQueryRules(blacklistQueryRules))
	assert.Equal(t, `[{"Description":"enforce blacklisted tables","Name":"blacklisted_table","TableNames":["t1"],"Action":"FAIL_RETRY"}]`, string(b))
}

func TestStateTabletControls(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 1, "ks", "0")
	defer tm.Stop()

	ks := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{{
			ServedType: topodatapb.TabletType_REPLICA,
			ShardTabletControls: []*topodatapb.ShardTabletControl{{
				Name:                 "0",
				QueryServiceDisabled: true,
			}},
		}},
	}
	tm.tmState.RefreshFromTopoInfo(ctx, nil, ks)
	want := map[topodatapb.TabletType]bool{
		topodatapb.TabletType_REPLICA: true,
	}
	tm.tmState.mu.Lock()
	assert.Equal(t, want, tm.tmState.tabletControls)
	tm.tmState.mu.Unlock()

	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	assert.Equal(t, topodatapb.TabletType_REPLICA, qsc.CurrentTarget().TabletType)
	assert.False(t, qsc.IsServing())
}

func TestStateNonServing(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 1, "ks", "0")
	defer tm.Stop()

	tm.tmState.mu.Lock()
	tm.tmState.tablet.Type = topodatapb.TabletType_SPARE
	tm.tmState.updateLocked(ctx)
	tm.tmState.mu.Unlock()

	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	assert.Equal(t, topodatapb.TabletType_SPARE, qsc.CurrentTarget().TabletType)
	assert.False(t, qsc.IsServing())
}

func TestStateChangeTabletType(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	statsTabletTypeCount.ResetAll()
	tm := newTestTM(t, ts, 2, "ks", "0")
	defer tm.Stop()

	assert.Equal(t, 1, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(1), statsTabletTypeCount.Counts()["replica"])

	alias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  2,
	}

	err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_MASTER, DBActionSetReadWrite)
	require.NoError(t, err)
	ti, err := ts.GetTablet(ctx, alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_MASTER, ti.Type)
	assert.NotNil(t, ti.MasterTermStartTime)
	assert.Equal(t, "master", statsTabletType.Get())
	assert.Equal(t, 2, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(1), statsTabletTypeCount.Counts()["master"])

	err = tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone)
	require.NoError(t, err)
	ti, err = ts.GetTablet(ctx, alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
	assert.Nil(t, ti.MasterTermStartTime)
	assert.Equal(t, "replica", statsTabletType.Get())
	assert.Equal(t, 2, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(2), statsTabletTypeCount.Counts()["replica"])
}

func TestPublishStateNew(t *testing.T) {
	defer func(saved time.Duration) { *publishRetryInterval = saved }(*publishRetryInterval)
	*publishRetryInterval = 1 * time.Millisecond

	// This flow doesn't test the failure scenario, which
	// we can't do using memorytopo, but we do test the retry
	// code path.

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 42, "ks", "0")
	ttablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tm.Tablet(), ttablet.Tablet)

	tab1 := tm.Tablet()
	tab1.Keyspace = "tab1"
	tm.tmState.mu.Lock()
	tm.tmState.tablet = tab1
	tm.tmState.publishStateLocked(ctx)
	tm.tmState.mu.Unlock()
	ttablet, err = tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tab1, ttablet.Tablet)

	tab2 := tm.Tablet()
	tab2.Keyspace = "tab2"
	tm.tmState.mu.Lock()
	tm.tmState.tablet = tab2
	tm.tmState.mu.Unlock()
	tm.tmState.retryPublish()
	ttablet, err = tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tab2, ttablet.Tablet)

	// If hostname doesn't match, it should not update.
	tab3 := tm.Tablet()
	tab3.Hostname = "tab3"
	tm.tmState.mu.Lock()
	tm.tmState.tablet = tab3
	tm.tmState.publishStateLocked(ctx)
	tm.tmState.mu.Unlock()
	ttablet, err = tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tab2, ttablet.Tablet)

	// Same for retryPublish.
	tm.tmState.retryPublish()
	ttablet, err = tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tab2, ttablet.Tablet)
}

func TestPublishDeleted(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 2, "ks", "0")
	defer tm.Stop()

	alias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  2,
	}

	err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_MASTER, DBActionSetReadWrite)
	require.NoError(t, err)

	err = ts.DeleteTablet(ctx, alias)
	require.NoError(t, err)

	// we need to make sure to catch the signal
	servenv.ExitChan = make(chan os.Signal, 1)
	// Now change the tablet type and publish
	err = tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone)
	require.NoError(t, err)
	tm.tmState.mu.Lock()
	assert.False(t, tm.tmState.isPublishing)
	tm.tmState.mu.Unlock()
}
