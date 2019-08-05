/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topotests

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// waitForInitialSrvKeyspace waits for the initial SrvKeyspace to
// appear, and match the provided srvKeyspace.
func waitForInitialSrvKeyspace(t *testing.T, ts *topo.Server, cell, keyspace string) (current *topo.WatchSrvKeyspaceData, changes <-chan *topo.WatchSrvKeyspaceData, cancel topo.CancelFunc) {
	ctx := context.Background()
	start := time.Now()
	for {
		current, changes, cancel = ts.WatchSrvKeyspace(ctx, cell, keyspace)
		switch {
		case topo.IsErrType(current.Err, topo.NoNode):
			// hasn't appeared yet
			if time.Since(start) > 10*time.Second {
				t.Fatalf("time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		case current.Err == nil:
			return
		default:
			t.Fatalf("watch failed: %v", current.Err)
		}
	}
}

func TestWatchSrvKeyspaceNoNode(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// No SrvKeyspace -> ErrNoNode
	current, _, _ := ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if !topo.IsErrType(current.Err, topo.NoNode) {
		t.Errorf("Got invalid result from WatchSrvKeyspace(not there): %v", current.Err)
	}
}

func TestWatchSrvKeyspace(t *testing.T) {

	cell := "cell1"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// Create initial value
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, &topodatapb.SrvKeyspace{}); err != nil {
		t.Fatalf("Update(/keyspaces/ks1/SrvKeyspace) failed: %v", err)
	}

	// Starting the watch should now work, and return an empty
	// SrvKeyspace.
	wanted := &topodatapb.SrvKeyspace{}
	current, changes, cancel := waitForInitialSrvKeyspace(t, ts, cell, keyspace)
	if !proto.Equal(current.Value, wanted) {
		t.Fatalf("got bad data: %v expected: %v", current.Value, wanted)
	}

	// Update the value with good data, wait until we see it
	wanted.ShardingColumnName = "scn1"
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, wanted); err != nil {
		t.Fatalf("Update(/keyspaces/ks1/SrvKeyspace) failed: %v", err)
	}
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			t.Fatalf("watch channel unexpectedly got error: %v", wd.Err)
		}
		if proto.Equal(wd.Value, wanted) {
			break
		}
		if proto.Equal(wd.Value, &topodatapb.SrvKeyspace{}) {
			t.Log("got duplicate empty value, skipping.")
		}
		t.Fatalf("got bad data: %v expected: %v", wd.Value, wanted)
	}

	// Update the value with bad data, wait until error.
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		t.Fatalf("ConnForCell failed: %v", err)
	}
	if _, err := conn.Update(ctx, "/keyspaces/"+keyspace+"/SrvKeyspace", []byte("BAD PROTO DATA"), nil); err != nil {
		t.Fatalf("Update(/keyspaces/ks1/SrvKeyspace) failed: %v", err)
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
	current, _, _ = ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if current.Err == nil || !strings.Contains(current.Err.Error(), "error unpacking initial SrvKeyspace object") {
		t.Fatalf("expected an initial error setting watch on bad content, but got: %v", current.Err)
	}

	// Update content, wait until Watch works again
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, wanted); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}
	start := time.Now()
	for {
		current, changes, _ = ts.WatchSrvKeyspace(ctx, cell, keyspace)
		if current.Err != nil {
			if strings.Contains(current.Err.Error(), "error unpacking initial SrvKeyspace object") {
				// hasn't changed yet
				if time.Since(start) > 10*time.Second {
					t.Fatalf("time out waiting for file to appear")
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatalf("got unexpected error while setting watch: %v", err)
		}
		if !proto.Equal(current.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", current.Value, wanted)
		}
		break
	}

	// Delete node, wait for error (skip any duplicate).
	if err := ts.DeleteSrvKeyspace(ctx, cell, keyspace); err != nil {
		t.Fatalf("DeleteSrvKeyspace() failed: %v", err)
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
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// No SrvKeyspace -> ErrNoNode
	current, _, _ := ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if !topo.IsErrType(current.Err, topo.NoNode) {
		t.Errorf("Got invalid result from WatchSrvKeyspace(not there): %v", current.Err)
	}

	// Create initial value
	wanted := &topodatapb.SrvKeyspace{
		ShardingColumnName: "scn2",
	}
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, wanted); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
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
	ctx := context.Background()
	ts := memorytopo.NewServer(cell, cell2)

	keyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(keyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(keyRange))
	}
	// Create initial value
	initial := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
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
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		t.Fatalf("CreateKeyspace() failed: %v", err)
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
				ServedType: topodatapb.TabletType_MASTER,
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
	if err != nil {
		t.Fatalf("LockKeyspace() failed: %v", err)
	}
	defer unlock(&err)

	if err := ts.AddSrvKeyspacePartitions(ctx, keyspace, shards, topodatapb.TabletType_MASTER, []string{cell}); err != nil {
		t.Fatalf("AddSrvKeyspacePartitions() failed: %v", err)
	}

	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err := json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err := json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("AddSrvKeyspacePartitions() failure. Got %v, want: %v", string(got), string(want))
	}

	// removing works
	targetKs = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "-",
						KeyRange: keyRange[0],
					},
				},
			},
		},
	}

	if err = ts.DeleteSrvKeyspacePartitions(ctx, keyspace, shards, topodatapb.TabletType_MASTER, []string{cell}); err != nil {
		t.Fatalf("DeleteSrvKeyspacePartitions() failed: %v", err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("DeleteSrvKeyspacePartitions() failure. Got %v, want: %v", string(got), string(want))
	}

	// You can add to partitions that do not exist
	targetKs = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
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
		t.Fatalf("AddSrvKeyspacePartitions() failed: %v", err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("SrvKeyspacePartitions() failure. Got %v, want: %v", string(got), string(want))
	}

	// it works in multiple cells

	if err = ts.AddSrvKeyspacePartitions(ctx, keyspace, shards, topodatapb.TabletType_REPLICA, nil); err != nil {
		t.Fatalf("AddSrvKeyspacePartitions() failed: %v", err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("AddSrvKeyspacePartitions() failure. Got %v, want: %v", string(got), string(want))
	}

	// Now let's get the srvKeyspace in cell2. Partition should have been added there too.
	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell2, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("GetSrvKeyspace() failure. Got %v, want: %v", string(got), string(want))
	}

}

func TestUpdateUpdateDisableQueryService(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell, cell2)

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
				ServedType: topodatapb.TabletType_MASTER,
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
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		t.Fatalf("CreateKeyspace() failed: %v", err)
	}

	targetKs := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
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
	if err != nil {
		t.Fatalf("LockKeyspace() failed: %v", err)
	}
	defer unlock(&err)

	if err := ts.UpdateDisableQueryService(ctx, keyspace, shards, topodatapb.TabletType_MASTER, []string{cell}, true /* disableQueryService */); err != nil {
		t.Fatalf("UpdateDisableQueryService() failed: %v", err)
	}

	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err := json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err := json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("UpdateDisableQueryService() failure. Got %v, want: %v", string(got), string(want))
	}

	// cell2 is untouched
	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell2, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(initial)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("UpdateDisableQueryService() failure. Got %v, want: %v", string(got), string(want))
	}

	// You can enable query service
	targetKs = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
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

	if err := ts.UpdateDisableQueryService(ctx, keyspace, shards, topodatapb.TabletType_MASTER, []string{cell}, false /* disableQueryService */); err != nil {
		t.Fatalf("UpdateDisableQueryService() failed: %v", err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("UpdateDisableQueryService() failure. Got %v, want: %v", string(got), string(want))
	}
}

func TestGetShardServingTypes(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell, cell2)

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
				ServedType: topodatapb.TabletType_MASTER,
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
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		t.Fatalf("CreateKeyspace() failed: %v", err)
	}

	shardInfo := topo.NewShardInfo(keyspace, "-80", &topodatapb.Shard{KeyRange: leftKeyRange[0]}, nil)

	got, err := ts.GetShardServingTypes(ctx, shardInfo)
	if err != nil {
		t.Fatalf("GetShardServingTypes() failed: %v", err)
	}

	want := []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_RDONLY}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingTypes() failure. Got %v, want: %v", got, want)
	}

	shardInfo = topo.NewShardInfo(keyspace, "80-", &topodatapb.Shard{KeyRange: rightKeyRange[0]}, nil)

	got, err = ts.GetShardServingTypes(ctx, shardInfo)
	if err != nil {
		t.Fatalf("GetShardServingTypes() failed: %v", err)
	}

	want = []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingTypes() failure. Got %v, want: %v", got, want)
	}

	keyRange, _ := key.ParseShardingSpec("-")

	shardInfo = topo.NewShardInfo(keyspace, "-", &topodatapb.Shard{KeyRange: keyRange[0]}, nil)

	got, err = ts.GetShardServingTypes(ctx, shardInfo)
	if err != nil {
		t.Fatalf("GetShardServingTypes() failed: %v", err)
	}

	want = []topodatapb.TabletType{}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingTypes() failure. Got %v, want: %v", got, want)
	}
}

func TestGetShardServingCells(t *testing.T) {
	cell := "cell1"
	cell2 := "cell2"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell, cell2)

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
				ServedType: topodatapb.TabletType_MASTER,
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
	if err != nil {
		t.Fatalf("GetShardServingTypes() failed: %v", err)
	}

	want := []string{}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingCells() failure. Got %v, want: %v", got, want)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, initial); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	ks := &topodatapb.Keyspace{}
	if err := ts.CreateKeyspace(ctx, keyspace, ks); err != nil {
		t.Fatalf("CreateKeyspace() failed: %v", err)
	}

	got, err = ts.GetShardServingCells(ctx, shardInfo)
	if err != nil {
		t.Fatalf("GetShardServingCells() failed: %v", err)
	}

	want = []string{cell}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetShardServingCells() failure. Got %v, want: %v", got, want)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	got, err = ts.GetShardServingCells(ctx, shardInfo)
	if err != nil {
		t.Fatalf("GetShardServingCells() failed: %v", err)
	}

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
	ctx := context.Background()
	ts := memorytopo.NewServer(cell, cell2)

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
				ServedType: topodatapb.TabletType_MASTER,
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
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	if err := ts.UpdateSrvKeyspace(ctx, cell2, keyspace, initial); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
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
		t.Fatalf("CreateKeyspace() failed: %v", err)
	}

	ctx, unlock, err := ts.LockKeyspace(ctx, keyspace, "Locking for tests")
	if err != nil {
		t.Fatalf("LockKeyspace() failed: %v", err)
	}
	defer unlock(&err)

	// You can migrate a single cell at a time

	err = ts.MigrateServedType(ctx, keyspace, destinationShards, sourceShards, topodatapb.TabletType_RDONLY, []string{cell})
	if err != nil {
		t.Fatalf("MigrateServedType() failed: %v", err)
	}

	targetKs := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
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
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err := json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err := json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("MigrateServedType() failure. Got %v, want: %v", string(got), string(want))
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell2, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(initial)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("MigrateServedType() failure. Got %v, want: %v", string(got), string(want))
	}

	// migrating all cells

	err = ts.MigrateServedType(ctx, keyspace, destinationShards, sourceShards, topodatapb.TabletType_RDONLY, nil)
	if err != nil {
		t.Fatalf("MigrateServedType() failed: %v", err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell2, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("MigrateServedType() failure. Got %v, want: %v", string(got), string(want))
	}

	// migrating master type cleans up shard tablet controls records

	targetKs = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
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

	err = ts.MigrateServedType(ctx, keyspace, destinationShards, sourceShards, topodatapb.TabletType_MASTER, nil)
	if err != nil {
		t.Fatalf("MigrateServedType() failed: %v", err)
	}

	srvKeyspace, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace() failed: %v", err)
	}

	got, err = json2.MarshalPB(srvKeyspace)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	want, err = json2.MarshalPB(targetKs)
	if err != nil {
		t.Fatalf("MarshalPB() failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("MigrateServedType() failure. Got %v, want: %v", string(got), string(want))
	}
}
