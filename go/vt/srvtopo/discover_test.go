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
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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

func TestFindAllTargetsAndKeyspaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")

	srvTopoCacheRefresh = 0
	srvTopoCacheTTL = 0
	defer func() {
		srvTopoCacheRefresh = 1 * time.Second
		srvTopoCacheTTL = 1 * time.Second

	}()
	rs := NewResilientServer(ctx, ts, "TestFindAllKeyspaceShards")

	// No keyspace / shards.
	targets, ksList, err := FindAllTargetsAndKeyspaces(ctx, rs, "cell1", []string{"test_keyspace"}, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY})
	assert.NoError(t, err)
	assert.Len(t, targets, 0)
	assert.EqualValues(t, []string{"test_keyspace"}, ksList)

	// Add one.
	assert.NoError(t, ts.UpdateSrvKeyspace(ctx, "cell1", "test_keyspace", &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "test_shard0",
					},
				},
			},
		},
	}))

	// Get it.
	targets, ksList, err = FindAllTargetsAndKeyspaces(ctx, rs, "cell1", []string{"test_keyspace"}, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY})
	assert.NoError(t, err)
	assert.EqualValues(t, []*querypb.Target{
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace",
			Shard:      "test_shard0",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
	}, targets)
	assert.EqualValues(t, []string{"test_keyspace"}, ksList)

	// Get any keyspace.
	targets, ksList, err = FindAllTargetsAndKeyspaces(ctx, rs, "cell1", nil, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY})
	assert.NoError(t, err)
	assert.EqualValues(t, []*querypb.Target{
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace",
			Shard:      "test_shard0",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
	}, targets)
	assert.EqualValues(t, []string{"test_keyspace"}, ksList)

	// Add another one.
	assert.NoError(t, ts.UpdateSrvKeyspace(ctx, "cell1", "test_keyspace2", &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
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
	}))

	// Get it for any keyspace, all types.
	targets, ksList, err = FindAllTargetsAndKeyspaces(ctx, rs, "cell1", nil, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA})
	assert.NoError(t, err)
	sort.Sort(TargetArray(targets))
	assert.EqualValues(t, []*querypb.Target{
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace",
			Shard:      "test_shard0",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace2",
			Shard:      "test_shard1",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace2",
			Shard:      "test_shard2",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, targets)
	sort.Strings(ksList)
	assert.EqualValues(t, []string{"test_keyspace", "test_keyspace2"}, ksList)

	// Only get 1 keyspace for all types.
	targets, ksList, err = FindAllTargetsAndKeyspaces(ctx, rs, "cell1", []string{"test_keyspace2"}, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA})
	assert.NoError(t, err)
	assert.EqualValues(t, []*querypb.Target{
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace2",
			Shard:      "test_shard1",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace2",
			Shard:      "test_shard2",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, targets)
	assert.EqualValues(t, []string{"test_keyspace2"}, ksList)

	// Only get the REPLICA targets for any keyspace.
	targets, ksList, err = FindAllTargetsAndKeyspaces(ctx, rs, "cell1", []string{}, []topodatapb.TabletType{topodatapb.TabletType_REPLICA})
	assert.NoError(t, err)
	assert.Equal(t, []*querypb.Target{
		{
			Cell:       "cell1",
			Keyspace:   "test_keyspace2",
			Shard:      "test_shard2",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, targets)
	sort.Strings(ksList)
	assert.EqualValues(t, []string{"test_keyspace", "test_keyspace2"}, ksList)

	// Get non-existent keyspace.
	targets, ksList, err = FindAllTargetsAndKeyspaces(ctx, rs, "cell1", []string{"doesnt-exist"}, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA})
	assert.NoError(t, err)
	assert.Len(t, targets, 0)
	assert.EqualValues(t, []string{"doesnt-exist"}, ksList)
}
