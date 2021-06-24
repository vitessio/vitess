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
	"reflect"
	"testing"
	"time"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/proto/vttime"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/topo/memorytopo"
)

const (
	keyspace = "ks"
	shard    = "0"
)

func TestShardSync(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	statsTabletTypeCount.ResetAll()
	tm := newTestTM(t, ts, 100, keyspace, shard)
	defer tm.Stop()

	// update the master info in the shard record and set it to nil
	originalTime := time.Now()
	updateMasterInfoInShardRecord(ctx, t, tm, nil, originalTime)

	// now try to promote the tablet to primary
	err := tm.tmState.ChangeTabletType(ctx, topodata.TabletType_MASTER, DBActionSetReadWrite)
	require.NoError(t, err)
	// verify that the tablet record has been updated
	ti, err := ts.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, topodata.TabletType_MASTER, ti.Type)
	assert.NotNil(t, ti.MasterTermStartTime)

	// wait for syncing to work correctly
	// this should also have updated the shard record since it is a more recent operation
	// We check here that the shard record and the tablet record are in sync
	checkShardRecordWithTimeout(ctx, t, ts, ti.Alias, ti.MasterTermStartTime, 1*time.Second)

	// even if try to update the shard record with the old timestamp, it should be reverted again
	updateMasterInfoInShardRecord(ctx, t, tm, nil, originalTime)

	// this should have also updated the shard record because of the timestamp.
	checkShardRecordWithTimeout(ctx, t, ts, ti.Alias, ti.MasterTermStartTime, 1*time.Second)

	// updating the shard record with the latest time should trigger an update in the tablet
	newTime := time.Now()
	updateMasterInfoInShardRecord(ctx, t, tm, nil, newTime)

	// this should not have updated.
	checkShardRecordWithTimeout(ctx, t, ts, nil, protoutil.TimeToProto(newTime), 1*time.Second)

	// verify that the tablet record has been updated
	checkTabletRecordWithTimeout(ctx, t, ts, tm.tabletAlias, topodata.TabletType_REPLICA, nil, 1*time.Second)
}

func checkShardRecordWithTimeout(ctx context.Context, t *testing.T, ts *topo.Server, tabletAlias *topodata.TabletAlias, masterStartTime *vttime.Time, timeToWait time.Duration) {
	timeOut := time.After(timeToWait)
	for {
		select {
		case <-timeOut:
			t.Fatalf("timed out: waiting for shard record to update")
		default:
			si, err := ts.GetShard(ctx, keyspace, shard)
			require.NoError(t, err)
			if reflect.DeepEqual(tabletAlias, si.MasterAlias) && reflect.DeepEqual(masterStartTime, si.MasterTermStartTime) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func checkTabletRecordWithTimeout(ctx context.Context, t *testing.T, ts *topo.Server, tabletAlias *topodata.TabletAlias, tabletType topodata.TabletType, masterStartTime *vttime.Time, timeToWait time.Duration) {
	timeOut := time.After(timeToWait)
	for {
		select {
		case <-timeOut:
			t.Fatalf("timed out: waiting for tablet record to update")
		default:
			ti, err := ts.GetTablet(ctx, tabletAlias)
			require.NoError(t, err)
			if reflect.DeepEqual(tabletType, ti.Type) && reflect.DeepEqual(masterStartTime, ti.MasterTermStartTime) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func updateMasterInfoInShardRecord(ctx context.Context, t *testing.T, tm *TabletManager, masterAlias *topodata.TabletAlias, time time.Time) {
	ctx, unlock, lockErr := tm.TopoServer.LockShard(ctx, keyspace, shard, fmt.Sprintf("updateMasterInfoInShardRecord(%v)", topoproto.TabletAliasString(tm.tabletAlias)))
	require.NoError(t, lockErr)
	defer unlock(&lockErr)

	_, err := tm.TopoServer.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.MasterAlias = masterAlias
		si.SetMasterTermStartTime(time)
		return nil
	})
	require.NoError(t, err)
}
