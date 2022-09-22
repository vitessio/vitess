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
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/faketopo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	tm.tmState.tablet.Type = topodatapb.TabletType_PRIMARY
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
	assert.Equal(t, topodatapb.TabletType_PRIMARY, qsc.CurrentTarget().TabletType)
	assert.False(t, qsc.IsServing())
}

func TestStateDenyList(t *testing.T) {
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
				TabletType:   topodatapb.TabletType_REPLICA,
				Cells:        []string{"cell1"},
				DeniedTables: []string{"t1"},
			}},
		},
	}
	tm.tmState.RefreshFromTopoInfo(ctx, si, nil)
	tm.tmState.mu.Lock()
	assert.Equal(t, map[topodatapb.TabletType][]string{topodatapb.TabletType_REPLICA: {"t1"}}, tm.tmState.deniedTables)
	tm.tmState.mu.Unlock()

	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	b, _ := json.Marshal(qsc.GetQueryRules(denyListQueryList))
	assert.Equal(t, `[{"Description":"enforce denied tables","Name":"denied_table","TableNames":["t1"],"Action":"FAIL_RETRY"}]`, string(b))
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

func TestStateIsShardServingisInSrvKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 1, "ks", "0")
	defer tm.Stop()

	tm.tmState.mu.Lock()
	tm.tmState.tablet.Type = topodatapb.TabletType_PRIMARY
	tm.tmState.updateLocked(ctx)
	tm.tmState.mu.Unlock()

	leftKeyRange, err := key.ParseShardingSpec("-80")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(leftKeyRange))
	}

	rightKeyRange, err := key.ParseShardingSpec("80-")
	if err != nil || len(rightKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(rightKeyRange))
	}

	keyRange, err := key.ParseShardingSpec("0")
	if err != nil || len(keyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(keyRange))
	}

	// Shard not in the SrvKeyspace, ServedType not in SrvKeyspace
	ks := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_DRAINED,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-80",
						KeyRange: leftKeyRange[0],
					},
					{
						Name:     "80-",
						KeyRange: rightKeyRange[0],
					},
				},
			},
		},
	}
	want := map[topodatapb.TabletType]bool{}
	tm.tmState.RefreshFromTopoInfo(ctx, nil, ks)

	tm.tmState.mu.Lock()
	assert.False(t, tm.tmState.isInSrvKeyspace)
	assert.Equal(t, want, tm.tmState.isShardServing)
	tm.tmState.mu.Unlock()

	assert.Equal(t, int64(0), statsIsInSrvKeyspace.Get())

	// Shard not in the SrvKeyspace, ServedType in SrvKeyspace
	ks = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-80",
						KeyRange: leftKeyRange[0],
					},
					{
						Name:     "80-",
						KeyRange: rightKeyRange[0],
					},
				},
			},
		},
	}
	want = map[topodatapb.TabletType]bool{}
	tm.tmState.RefreshFromTopoInfo(ctx, nil, ks)

	tm.tmState.mu.Lock()
	assert.False(t, tm.tmState.isInSrvKeyspace)
	assert.Equal(t, want, tm.tmState.isShardServing)
	tm.tmState.mu.Unlock()

	assert.Equal(t, int64(0), statsIsInSrvKeyspace.Get())

	// Shard in the SrvKeyspace, ServedType in the SrvKeyspace
	ks = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "0",
						KeyRange: keyRange[0],
					},
				},
			},
		},
	}
	want = map[topodatapb.TabletType]bool{
		topodatapb.TabletType_PRIMARY: true,
	}
	tm.tmState.RefreshFromTopoInfo(ctx, nil, ks)

	tm.tmState.mu.Lock()
	assert.True(t, tm.tmState.isInSrvKeyspace)
	assert.Equal(t, want, tm.tmState.isShardServing)
	tm.tmState.mu.Unlock()

	assert.Equal(t, int64(1), statsIsInSrvKeyspace.Get())

	// Shard in the SrvKeyspace, ServedType not in the SrvKeyspace
	ks = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "0",
						KeyRange: keyRange[0],
					},
				},
			},
		},
	}
	want = map[topodatapb.TabletType]bool{
		topodatapb.TabletType_RDONLY: true,
	}
	tm.tmState.RefreshFromTopoInfo(ctx, nil, ks)

	tm.tmState.mu.Lock()
	assert.False(t, tm.tmState.isInSrvKeyspace)
	assert.Equal(t, want, tm.tmState.isShardServing)
	tm.tmState.mu.Unlock()

	assert.Equal(t, int64(0), statsIsInSrvKeyspace.Get())

	// Test tablet type change - shard in the SrvKeyspace, ServedType in the SrvKeyspace
	err = tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_RDONLY, DBActionNone)
	require.NoError(t, err)
	tm.tmState.mu.Lock()
	assert.True(t, tm.tmState.isInSrvKeyspace)
	tm.tmState.mu.Unlock()

	assert.Equal(t, int64(1), statsIsInSrvKeyspace.Get())

	// Test tablet type change - shard in the SrvKeyspace, ServedType in the SrvKeyspace
	err = tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_DRAINED, DBActionNone)
	require.NoError(t, err)
	tm.tmState.mu.Lock()
	assert.False(t, tm.tmState.isInSrvKeyspace)
	tm.tmState.mu.Unlock()

	assert.Equal(t, int64(0), statsIsInSrvKeyspace.Get())

	// Test tablet isOpen
	tm.tmState.mu.Lock()
	tm.tmState.isOpen = false
	tm.tmState.isInSrvKeyspace = false
	tm.tmState.tablet.Type = topodatapb.TabletType_REPLICA
	tm.tmState.isShardServing = map[topodatapb.TabletType]bool{
		topodatapb.TabletType_REPLICA: true,
	}
	tm.tmState.mu.Unlock()

	tm.tmState.Open()

	tm.tmState.mu.Lock()
	assert.True(t, tm.tmState.isInSrvKeyspace)
	tm.tmState.mu.Unlock()

	assert.Equal(t, int64(1), statsIsInSrvKeyspace.Get())
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

	err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite)
	require.NoError(t, err)
	ti, err := ts.GetTablet(ctx, alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.NotNil(t, ti.PrimaryTermStartTime)
	assert.Equal(t, "primary", statsTabletType.Get())
	assert.Equal(t, 2, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(1), statsTabletTypeCount.Counts()["primary"])

	err = tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone)
	require.NoError(t, err)
	ti, err = ts.GetTablet(ctx, alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
	assert.Nil(t, ti.PrimaryTermStartTime)
	assert.Equal(t, "replica", statsTabletType.Get())
	assert.Equal(t, 2, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(2), statsTabletTypeCount.Counts()["replica"])
}

/*
	This test verifies, even if SetServingType returns error we should still publish

the new table type
*/
func TestStateChangeTabletTypeWithFailure(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	statsTabletTypeCount.ResetAll()
	// create TM with replica and put a hook to return error during SetServingType
	tm := newTestTM(t, ts, 2, "ks", "0")
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	qsc.SetServingTypeError = vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "mocking resource exhaustion error ")
	defer tm.Stop()

	assert.Equal(t, 1, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(1), statsTabletTypeCount.Counts()["replica"])

	alias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  2,
	}

	// change table type to primary
	err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite)
	errMsg := "Cannot start query service: Code: RESOURCE_EXHAUSTED\nmocking resource exhaustion error \n: mocking resource exhaustion error "
	require.EqualError(t, err, errMsg)

	ti, err := ts.GetTablet(ctx, alias)
	require.NoError(t, err)
	// even though SetServingType failed. It still is expected to publish the new table type
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.NotNil(t, ti.PrimaryTermStartTime)
	assert.Equal(t, "primary", statsTabletType.Get())
	assert.Equal(t, 2, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(1), statsTabletTypeCount.Counts()["primary"])

	err = tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone)
	require.EqualError(t, err, errMsg)
	ti, err = ts.GetTablet(ctx, alias)
	require.NoError(t, err)
	// even though SetServingType failed. It still is expected to publish the new table type
	assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
	assert.Nil(t, ti.PrimaryTermStartTime)
	assert.Equal(t, "replica", statsTabletType.Get())
	assert.Equal(t, 2, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(2), statsTabletTypeCount.Counts()["replica"])

	// since the table type is spare, it will exercise reason != "" in UpdateLocked and thus
	// populate error object differently as compare to above scenarios
	err = tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_SPARE, DBActionNone)
	errMsg = "SetServingType(serving=false) failed: Code: RESOURCE_EXHAUSTED\nmocking resource exhaustion error \n: mocking resource exhaustion error "
	require.EqualError(t, err, errMsg)
	ti, err = ts.GetTablet(ctx, alias)
	require.NoError(t, err)
	// even though SetServingType failed. It still is expected to publish the new table type
	assert.Equal(t, topodatapb.TabletType_SPARE, ti.Type)
	assert.Nil(t, ti.PrimaryTermStartTime)
	assert.Equal(t, "spare", statsTabletType.Get())
	assert.Equal(t, 3, len(statsTabletTypeCount.Counts()))
	assert.Equal(t, int64(1), statsTabletTypeCount.Counts()["spare"])
}

// TestChangeTypeErrorWhileWritingToTopo tests the case where we fail while writing to the topo-server
func TestChangeTypeErrorWhileWritingToTopo(t *testing.T) {
	testcases := []struct {
		name               string
		writePersists      bool
		numberOfReadErrors int
		expectedTabletType topodatapb.TabletType
		expectedError      string
	}{
		{
			name:               "Write persists even when error thrown from topo server",
			writePersists:      true,
			numberOfReadErrors: 5,
			expectedTabletType: topodatapb.TabletType_PRIMARY,
		}, {
			name:               "Topo server throws error and the write also fails",
			writePersists:      false,
			numberOfReadErrors: 17,
			expectedTabletType: topodatapb.TabletType_REPLICA,
			expectedError:      "deadline exceeded: tablets/cell1-0000000002/Tablet",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			factory := faketopo.NewFakeTopoFactory()
			// add cell1 to the factory. This returns a fake connection which we will use to set the get and update errors as we require.
			fakeConn := factory.AddCell("cell1")
			ts := faketopo.NewFakeTopoServer(factory)
			statsTabletTypeCount.ResetAll()
			tm := newTestTM(t, ts, 2, "ks", "0")
			defer tm.Stop()

			// ChangeTabletType calls topotools.ChangeType which in-turn issues
			// a GET request and an UPDATE request to the topo server.
			// We want the first GET request to pass without any failure
			// We want the UPDATE request to fail
			fakeConn.AddGetError(false)
			fakeConn.AddUpdateError(true, testcase.writePersists)
			// Since the UPDATE request failed, we will try a GET request on the
			// topo server until it succeeds.
			for i := 0; i < testcase.numberOfReadErrors; i++ {
				fakeConn.AddGetError(true)
			}
			ctx := context.Background()
			err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite)
			if testcase.expectedError != "" {
				require.EqualError(t, err, testcase.expectedError)
			} else {
				require.NoError(t, err)
			}

			alias := &topodatapb.TabletAlias{
				Cell: "cell1",
				Uid:  2,
			}
			ti, err := ts.GetTablet(ctx, alias)
			require.NoError(t, err)
			require.Equal(t, testcase.expectedTabletType, ti.Type)

			// assert that next change type succeeds irrespective of previous failures
			err = tm.tmState.ChangeTabletType(context.Background(), topodatapb.TabletType_REPLICA, DBActionNone)
			require.NoError(t, err)
		})
	}
}

func TestPublishStateNew(t *testing.T) {
	defer func(saved time.Duration) { publishRetryInterval = saved }(publishRetryInterval)
	publishRetryInterval = 1 * time.Millisecond

	// This flow doesn't test the failure scenario, which
	// we can't do using memorytopo, but we do test the retry
	// code path.

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	tm := newTestTM(t, ts, 42, "ks", "0")
	ttablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	utils.MustMatch(t, tm.Tablet(), ttablet.Tablet)

	tab1 := tm.Tablet()
	tab1.Keyspace = "tab1"
	tm.tmState.mu.Lock()
	tm.tmState.tablet = tab1
	tm.tmState.publishStateLocked(ctx)
	tm.tmState.mu.Unlock()
	ttablet, err = tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	utils.MustMatch(t, tab1, ttablet.Tablet)

	tab2 := tm.Tablet()
	tab2.Keyspace = "tab2"
	tm.tmState.mu.Lock()
	tm.tmState.tablet = tab2
	tm.tmState.mu.Unlock()
	tm.tmState.retryPublish()
	ttablet, err = tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	utils.MustMatch(t, tab2, ttablet.Tablet)

	// If hostname doesn't match, it should not update.
	tab3 := tm.Tablet()
	tab3.Hostname = "tab3"
	tm.tmState.mu.Lock()
	tm.tmState.tablet = tab3
	tm.tmState.publishStateLocked(ctx)
	tm.tmState.mu.Unlock()
	ttablet, err = tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	utils.MustMatch(t, tab2, ttablet.Tablet)

	// Same for retryPublish.
	tm.tmState.retryPublish()
	ttablet, err = tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	utils.MustMatch(t, tab2, ttablet.Tablet)
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

	err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite)
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
