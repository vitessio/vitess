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

package topotests

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"github.com/stretchr/testify/assert"
)

// waitForInitialSrvKeyspace waits for the initial SrvKeyspace to
// appear, and match the provided srvKeyspace.
func waitForInitialSrvKeyspace(t *testing.T, ts *topo.Server, cell, keyspace string) (current *topo.WatchSrvKeyspaceData, changes <-chan *topo.WatchSrvKeyspaceData, cancel context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	var err error
	for {
		current, changes, err = ts.WatchSrvKeyspace(ctx, cell, keyspace)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// hasn't appeared yet
			if time.Since(start) > 10*time.Second {
				t.Fatalf("time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		case err == nil:
			return
		default:
			require.NoError(t, err)
		}
	}
}

func TestWatchSrvKeyspaceNoNode(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	// No SrvKeyspace -> ErrNoNode
	_, _, err := ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if !topo.IsErrType(err, topo.NoNode) {
		assert.NoError(t, err)
	}
}

func TestWatchSrvKeyspace(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	// Create initial value
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, &topodatapb.SrvKeyspace{}); err != nil {
		require.NoError(t, err)
	}

	// Starting the watch should now work, and return an empty
	// SrvKeyspace.
	wanted := &topodatapb.SrvKeyspace{}
	current, changes, cancel := waitForInitialSrvKeyspace(t, ts, cell, keyspace)
	if !proto.Equal(current.Value, wanted) {
		t.Fatalf("got bad data: %v expected: %v", current.Value, wanted)
	}

	// Update the value with bad data, wait until error.
	conn, err := ts.ConnForCell(ctx, cell)
	require.NoError(t, err)
	if _, err := conn.Update(ctx, "/keyspaces/"+keyspace+"/SrvKeyspace", []byte("BAD PROTO DATA"), nil); err != nil {
		require.NoError(t, err)
	}
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			if strings.Contains(wd.Err.Error(), "error unpacking SrvKeyspace object") {
				break
			}
			t.Fatalf("watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		if !proto.Equal(wd.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", wd.Value, wanted)
		}
		t.Log("got duplicate right value, skipping.")
	}

	// Cancel should still work here, although it does nothing.
	cancel()

	// Bad data in topo, setting the watch should now fail.
	_, _, err = ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if err == nil || !strings.Contains(err.Error(), "error unpacking initial SrvKeyspace object") {
		require.NoError(t, err)
	}

	// Update content, wait until Watch works again
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, wanted); err != nil {
		require.NoError(t, err)
	}
	start := time.Now()
	for {
		current, changes, err = ts.WatchSrvKeyspace(ctx, cell, keyspace)
		if err != nil {
			if strings.Contains(err.Error(), "error unpacking initial SrvKeyspace object") {
				// hasn't changed yet
				if time.Since(start) > 10*time.Second {
					t.Fatalf("time out waiting for file to appear")
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			require.NoError(t, err)
		}
		if !proto.Equal(current.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", current.Value, wanted)
		}
		break
	}

	// Delete node, wait for error (skip any duplicate).
	if err := ts.DeleteSrvKeyspace(ctx, cell, keyspace); err != nil {
		require.NoError(t, err)
	}
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if topo.IsErrType(wd.Err, topo.NoNode) {
			break
		}
		if wd.Err != nil {
			t.Fatalf("watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		if !proto.Equal(wd.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", wd.Value, wanted)
		}
		t.Log("got duplicate right value, skipping.")
	}
}

func TestWatchSrvKeyspaceCancel(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	// No SrvKeyspace -> ErrNoNode
	_, _, err := ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if !topo.IsErrType(err, topo.NoNode) {
		assert.NoError(t, err)
	}

	// Create initial value
	wanted := &topodatapb.SrvKeyspace{}
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, wanted); err != nil {
		require.NoError(t, err)
	}

	// Starting the watch should now work.
	current, changes, cancel := waitForInitialSrvKeyspace(t, ts, cell, keyspace)
	if !proto.Equal(current.Value, wanted) {
		t.Fatalf("got bad data: %v expected: %v", current.Value, wanted)
	}

	// Cancel watch, wait for error.
	cancel()
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if topo.IsErrType(wd.Err, topo.Interrupted) {
			break
		}
		if wd.Err != nil {
			t.Fatalf("watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		if !proto.Equal(wd.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", wd.Value, wanted)
		}
		t.Log("got duplicate right value, skipping.")
	}

	// Cancel should still work here, although it does nothing.
	cancel()
}

func TestUpdateSrvKeyspacePartitions(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, cell, cell2)
	defer ts.Close()

	keyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(keyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(keyRange))
	}
	// Create initial value
	initial := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: keyRange[0],
					},
				},
			},
		},
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		require.NoError(t, err)
	}

	leftKeyRange, err := key.ParseShardingSpec("-80")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(leftKeyRange))
	}

	rightKeyRange, err := key.ParseShardingSpec("80-")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(rightKeyRange))
	}

	targetKs := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: keyRange[0],
					},
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

	shards := []*topo.ShardInfo{
		topo.NewShardInfo(keyspace, "-80", &topodatapb.Shard{KeyRange: leftKeyRange[0]}, nil),
		topo.NewShardInfo(keyspace, "80-", &topodatapb.Shard{KeyRange: rightKeyRange[0]}, nil),
	}

	ctx, unlock, err := ts.LockKeyspace(ctx, keyspace, "Locking for tests")
	require.NoError(t, err)
	defer unlock(&err)

	if err := ts.AddSrvKeyspacePartitions(ctx, keyspace, shards, topodatapb.TabletType_PRIMARY, []string{cell}); err != nil {
		require.NoError(t, err)
	}

	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)

	got, err := json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err := json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("AddSrvKeyspacePartitions() failure. Got %v, want: %v", string(got), string(want))
	}

	// removing works
	targetKs = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: keyRange[0],
					},
				},
			},
		},
	}

	if err = ts.DeleteSrvKeyspacePartitions(ctx, keyspace, shards, topodatapb.TabletType_PRIMARY, []string{cell}); err != nil {
		require.NoError(t, err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("DeleteSrvKeyspacePartitions() failure. Got %v, want: %v", string(got), string(want))
	}

	// You can add to partitions that do not exist
	targetKs = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: keyRange[0],
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_REPLICA,
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

	if err = ts.AddSrvKeyspacePartitions(ctx, keyspace, shards, topodatapb.TabletType_REPLICA, []string{cell}); err != nil {
		require.NoError(t, err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("SrvKeyspacePartitions() failure. Got %v, want: %v", string(got), string(want))
	}

	// it works in multiple cells

	if err = ts.AddSrvKeyspacePartitions(ctx, keyspace, shards, topodatapb.TabletType_REPLICA, nil); err != nil {
		require.NoError(t, err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("AddSrvKeyspacePartitions() failure. Got %v, want: %v", string(got), string(want))
	}

	// Now let's get the srvKeyspace in cell2. Partition should have been added there too.
	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell2, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("GetSrvKeyspace() failure. Got %v, want: %v", string(got), string(want))
	}
}

func TestUpdateUpdateDisableQueryService(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, cell, cell2)
	defer ts.Close()

	leftKeyRange, err := key.ParseShardingSpec("-80")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(leftKeyRange))
	}

	rightKeyRange, err := key.ParseShardingSpec("80-")
	if err != nil || len(rightKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(rightKeyRange))
	}
	// Create initial value
	initial := &topodatapb.SrvKeyspace{
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

	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		require.NoError(t, err)
	}

	targetKs := &topodatapb.SrvKeyspace{
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
				ShardTabletControls: []*topodatapb.ShardTabletControl{
					{
						Name:                 "-80",
						KeyRange:             leftKeyRange[0],
						QueryServiceDisabled: true,
					},
					{
						Name:                 "80-",
						KeyRange:             rightKeyRange[0],
						QueryServiceDisabled: true,
					},
				},
			},
		},
	}

	shards := []*topo.ShardInfo{
		topo.NewShardInfo(keyspace, "-80", &topodatapb.Shard{KeyRange: leftKeyRange[0]}, nil),
		topo.NewShardInfo(keyspace, "80-", &topodatapb.Shard{KeyRange: rightKeyRange[0]}, nil),
	}

	ctx, unlock, err := ts.LockKeyspace(ctx, keyspace, "Locking for tests")
	require.NoError(t, err)
	defer unlock(&err)

	if err := ts.UpdateDisableQueryService(ctx, keyspace, shards, topodatapb.TabletType_PRIMARY, []string{cell}, true /* disableQueryService */); err != nil {
		require.NoError(t, err)
	}

	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)

	got, err := json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err := json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("UpdateDisableQueryService() failure. Got %v, want: %v", string(got), string(want))
	}

	// cell2 is untouched
	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell2, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(initial)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("UpdateDisableQueryService() failure. Got %v, want: %v", string(got), string(want))
	}

	// You can enable query service
	targetKs = &topodatapb.SrvKeyspace{
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
				ShardTabletControls: []*topodatapb.ShardTabletControl{
					{
						Name:                 "-80",
						KeyRange:             leftKeyRange[0],
						QueryServiceDisabled: false,
					},
					{
						Name:                 "80-",
						KeyRange:             rightKeyRange[0],
						QueryServiceDisabled: false,
					},
				},
			},
		},
	}

	shards = []*topo.ShardInfo{
		topo.NewShardInfo(keyspace, "-80", &topodatapb.Shard{KeyRange: leftKeyRange[0]}, nil),
		topo.NewShardInfo(keyspace, "80-", &topodatapb.Shard{KeyRange: rightKeyRange[0]}, nil),
	}

	if err := ts.UpdateDisableQueryService(ctx, keyspace, shards, topodatapb.TabletType_PRIMARY, []string{cell}, false /* disableQueryService */); err != nil {
		require.NoError(t, err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("UpdateDisableQueryService() failure. Got %v, want: %v", string(got), string(want))
	}
}

func TestGetShardServingTypes(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, cell, cell2)
	defer ts.Close()

	leftKeyRange, err := key.ParseShardingSpec("-80")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(leftKeyRange))
	}

	rightKeyRange, err := key.ParseShardingSpec("80-")
	if err != nil || len(rightKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(rightKeyRange))
	}
	// Create initial value
	initial := &topodatapb.SrvKeyspace{
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
			{
				ServedType: topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "80-",
						KeyRange: rightKeyRange[0],
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-80",
						KeyRange: leftKeyRange[0],
					},
				},
			},
		},
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		require.NoError(t, err)
	}

	shardInfo := topo.NewShardInfo(keyspace, "-80", &topodatapb.Shard{KeyRange: leftKeyRange[0]}, nil)

	got, err := ts.GetShardServingTypes(ctx, shardInfo)
	require.NoError(t, err)

	want := []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_RDONLY}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingTypes() failure. Got %v, want: %v", got, want)
	}

	shardInfo = topo.NewShardInfo(keyspace, "80-", &topodatapb.Shard{KeyRange: rightKeyRange[0]}, nil)

	got, err = ts.GetShardServingTypes(ctx, shardInfo)
	require.NoError(t, err)

	want = []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingTypes() failure. Got %v, want: %v", got, want)
	}

	keyRange, _ := key.ParseShardingSpec("-")

	shardInfo = topo.NewShardInfo(keyspace, "-", &topodatapb.Shard{KeyRange: keyRange[0]}, nil)

	got, err = ts.GetShardServingTypes(ctx, shardInfo)
	require.NoError(t, err)

	want = []topodatapb.TabletType{}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingTypes() failure. Got %v, want: %v", got, want)
	}
}

func TestGetShardServingCells(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, cell, cell2)
	defer ts.Close()

	leftKeyRange, err := key.ParseShardingSpec("-80")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(leftKeyRange))
	}

	rightKeyRange, err := key.ParseShardingSpec("80-")
	if err != nil || len(rightKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(rightKeyRange))
	}
	// Create initial value
	initial := &topodatapb.SrvKeyspace{
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
			{
				ServedType: topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "80-",
						KeyRange: rightKeyRange[0],
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-80",
						KeyRange: leftKeyRange[0],
					},
				},
			},
		},
	}

	shardInfo := topo.NewShardInfo(keyspace, "-80", &topodatapb.Shard{KeyRange: leftKeyRange[0]}, nil)

	got, err := ts.GetShardServingCells(ctx, shardInfo)
	require.NoError(t, err)

	want := []string{}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingCells() failure. Got %v, want: %v", got, want)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		require.NoError(t, err)
	}

	got, err = ts.GetShardServingCells(ctx, shardInfo)
	require.NoError(t, err)

	want = []string{cell}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingCells() failure. Got %v, want: %v", got, want)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	got, err = ts.GetShardServingCells(ctx, shardInfo)
	require.NoError(t, err)

	want = []string{cell, cell2}

	sort.Strings(got)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingTypes() failure. Got %v, want: %v", got, want)
	}
}

func TestMasterMigrateServedType(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, cell, cell2)
	defer ts.Close()

	initialKeyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(initialKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(initialKeyRange))
	}

	leftKeyRange, err := key.ParseShardingSpec("-80")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(leftKeyRange))
	}

	rightKeyRange, err := key.ParseShardingSpec("80-")
	if err != nil || len(rightKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(rightKeyRange))
	}
	// Create initial value
	initial := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: initialKeyRange[0],
					},
				},
				ShardTabletControls: []*topodatapb.ShardTabletControl{
					{
						Name:                 "-",
						KeyRange:             initialKeyRange[0],
						QueryServiceDisabled: true,
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: initialKeyRange[0],
					},
				},
				ShardTabletControls: []*topodatapb.ShardTabletControl{
					{
						Name:                 "-",
						KeyRange:             initialKeyRange[0],
						QueryServiceDisabled: true,
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: initialKeyRange[0],
					},
				},
			},
		},
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		require.NoError(t, err)
	}

	sourceShards := []*topo.ShardInfo{
		topo.NewShardInfo(keyspace, "-", &topodatapb.Shard{KeyRange: initialKeyRange[0]}, nil),
	}

	destinationShards := []*topo.ShardInfo{
		topo.NewShardInfo(keyspace, "-80", &topodatapb.Shard{KeyRange: leftKeyRange[0]}, nil),
		topo.NewShardInfo(keyspace, "80-", &topodatapb.Shard{KeyRange: rightKeyRange[0]}, nil),
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		require.NoError(t, err)
	}

	ctx, unlock, err := ts.LockKeyspace(ctx, keyspace, "Locking for tests")
	require.NoError(t, err)
	defer unlock(&err)

	// You can migrate a single cell at a time

	err = ts.MigrateServedType(ctx, keyspace, destinationShards, sourceShards, topodatapb.TabletType_RDONLY, []string{cell})
	require.NoError(t, err)

	targetKs := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: initialKeyRange[0],
					},
				},
				ShardTabletControls: []*topodatapb.ShardTabletControl{
					{
						Name:                 "-",
						KeyRange:             initialKeyRange[0],
						QueryServiceDisabled: true,
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: initialKeyRange[0],
					},
				},
				ShardTabletControls: []*topodatapb.ShardTabletControl{
					{
						Name:                 "-",
						KeyRange:             initialKeyRange[0],
						QueryServiceDisabled: true,
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_RDONLY,
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

	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)

	got, err := json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err := json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("MigrateServedType() failure. Got %v, want: %v", string(got), string(want))
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell2, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(initial)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("MigrateServedType() failure. Got %v, want: %v", string(got), string(want))
	}

	// migrating all cells

	err = ts.MigrateServedType(ctx, keyspace, destinationShards, sourceShards, topodatapb.TabletType_RDONLY, nil)
	require.NoError(t, err)

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell2, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("MigrateServedType() failure. Got %v, want: %v", string(got), string(want))
	}

	// migrating primary type cleans up shard tablet controls records

	targetKs = &topodatapb.SrvKeyspace{
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
			{
				ServedType: topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: initialKeyRange[0],
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_RDONLY,
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

	err = ts.MigrateServedType(ctx, keyspace, destinationShards, sourceShards, topodatapb.TabletType_PRIMARY, nil)
	require.NoError(t, err)

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)

	got, err = json2.MarshalPB(srvKeyspace)
	require.NoError(t, err)

	want, err = json2.MarshalPB(targetKs)
	require.NoError(t, err)

	if string(got) != string(want) {
		t.Errorf("MigrateServedType() failure. Got %v, want: %v", string(got), string(want))
	}
}

func TestValidateSrvKeyspace(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, cell, cell2)
	defer ts.Close()

	leftKeyRange, err := key.ParseShardingSpec("-80")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(leftKeyRange))
	}

	rightKeyRange, err := key.ParseShardingSpec("80-")
	if err != nil || len(rightKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(rightKeyRange))
	}

	correct := &topodatapb.SrvKeyspace{
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

	incorrect := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "80-",
						KeyRange: rightKeyRange[0],
					},
				},
			},
		},
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, correct); err != nil {
		require.NoError(t, err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, incorrect); err != nil {
		require.NoError(t, err)
	}
	errMsg := "keyspace partition for PRIMARY in cell cell2 does not start with min key"
	err = ts.ValidateSrvKeyspace(ctx, keyspace, "cell1,cell2")
	require.EqualError(t, err, errMsg)

	err = ts.ValidateSrvKeyspace(ctx, keyspace, "cell1")
	require.NoError(t, err)
	err = ts.ValidateSrvKeyspace(ctx, keyspace, "cell2")
	require.EqualError(t, err, errMsg)
	err = ts.ValidateSrvKeyspace(ctx, keyspace, "")
	require.EqualError(t, err, errMsg)
}
