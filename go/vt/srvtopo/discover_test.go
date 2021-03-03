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

package srvtopo

import (
	"flag"
	"reflect"
	"sort"
	"testing"

	"context"

	"vitess.io/vitess/go/vt/topo/memorytopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// To sort []*querypb.Target for comparison.
type TargetArray []*querypb.Target

func (a TargetArray) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a TargetArray) Len() int      { return len(a) }
func (a TargetArray) Less(i, j int) bool {
	if a[i].Cell != a[j].Cell {
		return a[i].Cell < a[j].Cell
	}
	if a[i].Keyspace != a[j].Keyspace {
		return a[i].Keyspace < a[j].Keyspace
	}
	if a[i].Shard != a[j].Shard {
		return a[i].Shard < a[j].Shard
	}
	return a[i].TabletType < a[j].TabletType
}

func TestFindAllTargets(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	flag.Set("srv_topo_cache_refresh", "0s") // No caching values
	flag.Set("srv_topo_cache_ttl", "0s")     // No caching values
	rs := NewResilientServer(ts, "TestFindAllKeyspaceShards")

	// No keyspace / shards.
	ks, err := FindAllTargets(ctx, rs, "cell1", []topodatapb.TabletType{topodatapb.TabletType_MASTER})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(ks) > 0 {
		t.Errorf("why did I get anything? %v", ks)
	}

	// Add one.
	if err := ts.UpdateSrvKeyspace(ctx, "cell1", "test_keyspace", &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "test_shard0",
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("can't add srvKeyspace: %v", err)
	}

	// Get it.
	ks, err = FindAllTargets(ctx, rs, "cell1", []topodatapb.TabletType{topodatapb.TabletType_MASTER})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(ks, []*querypb.Target{
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace",
			Shard:      "test_shard0",
			TabletType: topodatapb.TabletType_MASTER,
		},
	}) {
		t.Errorf("got wrong value: %v", ks)
	}

	// Add another one.
	if err := ts.UpdateSrvKeyspace(ctx, "cell1", "test_keyspace2", &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "test_shard1",
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "test_shard2",
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("can't add srvKeyspace: %v", err)
	}

	// Get it for all types.
	ks, err = FindAllTargets(ctx, rs, "cell1", []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	sort.Sort(TargetArray(ks))
	if !reflect.DeepEqual(ks, []*querypb.Target{
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace",
			Shard:      "test_shard0",
			TabletType: topodatapb.TabletType_MASTER,
		},
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace2",
			Shard:      "test_shard1",
			TabletType: topodatapb.TabletType_MASTER,
		},
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace2",
			Shard:      "test_shard2",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}) {
		t.Errorf("got wrong value: %v", ks)
	}

	// Only get the REPLICA targets.
	ks, err = FindAllTargets(ctx, rs, "cell1", []topodatapb.TabletType{topodatapb.TabletType_REPLICA})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(ks, []*querypb.Target{
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace2",
			Shard:      "test_shard2",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}) {
		t.Errorf("got wrong value: %v", ks)
	}
}
