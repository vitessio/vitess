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

package discovery

import (
	"fmt"
	"sync"
	"testing"
	"time"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"
)

func TestCellTabletsWatcher(t *testing.T) {
	checkWatcher(t, true)
}

func TestShardReplicationWatcher(t *testing.T) {
	checkWatcher(t, false)
}

func checkWatcher(t *testing.T, cellTablets bool) {
	ft := newFakeTopo(cellTablets)
	fhc := NewFakeHealthCheck()
	t.Logf(`ft = FakeTopo(); fhc = FakeHealthCheck()`)
	var tw *TopologyWatcher
	if cellTablets {
		tw = NewCellTabletsWatcher(topo.Server{Impl: ft}, fhc, "aa", 10*time.Minute, 5)
		t.Logf(`tw = CellTabletsWatcher(topo.Server{ft}, fhc, "aa", 10ms, 5)`)
	} else {
		tw = NewShardReplicationWatcher(topo.Server{Impl: ft}, fhc, "aa", "keyspace", "shard", 10*time.Minute, 5)
		t.Logf(`tw = ShardReplicationWatcher(topo.Server{ft}, fhc, "aa", "keyspace", "shard", 10ms, 5)`)
	}

	// Wait for the initial topology load to finish. Otherwise we
	// have a background loadTablets() that's running, and it can
	// interact with our tests in weird ways.
	if err := tw.WaitForInitialTopology(); err != nil {
		t.Fatalf("initial WaitForInitialTopology failed")
	}

	// add a tablet to the topology
	ft.AddTablet("aa", 0, "host1", map[string]int32{"vt": 123})
	tw.loadTablets()
	t.Logf(`ft.AddTablet("aa", 0, "host1", {"vt": 123}); tw.loadTablets()`)
	want := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Uid: 0,
		},
		Hostname: "host1",
		PortMap:  map[string]int32{"vt": 123},
	}
	allTablets := fhc.GetAllTablets()
	key := TabletToMapKey(want)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 1 {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, want)
	}

	// same tablet, different port, should update (previous
	// one should go away, new one be added).
	ft.AddTablet("aa", 0, "host1", map[string]int32{"vt": 456})
	tw.loadTablets()
	t.Logf(`ft.AddTablet("aa", 0, "host1", {"vt": 456}); tw.loadTablets()`)
	want = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Uid: 0,
		},
		Hostname: "host1",
		PortMap:  map[string]int32{"vt": 456},
	}
	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(want)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 1 {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, want)
	}

	// Remove and re-add with a new uid. This should trigger a ReplaceTablet in loadTablets,
	// because the uid does not match.
	ft.RemoveTablet("aa", 0)
	ft.AddTablet("aa", 1, "host1", map[string]int32{"vt": 456})
	tw.loadTablets()
	t.Logf(`ft.ReplaceTablet("aa", 0, "host1", {"vt": 456}); tw.loadTablets()`)
	want = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Uid: 1,
		},
		Hostname: "host1",
		PortMap:  map[string]int32{"vt": 456},
	}
	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(want)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 1 {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, want)
	}

	tw.Stop()
}

type fakeTopo struct {
	faketopo.FakeTopo
	expectGetTabletsByCell bool

	// mu protects the tablets map.
	mu sync.RWMutex

	// tablets key is topoproto.TabletAliasString(tablet alias).
	tablets map[string]*topodatapb.Tablet
}

func newFakeTopo(expectGetTabletsByCell bool) *fakeTopo {
	return &fakeTopo{
		expectGetTabletsByCell: expectGetTabletsByCell,
		tablets:                make(map[string]*topodatapb.Tablet),
	}
}

func (ft *fakeTopo) AddTablet(cell string, uid uint32, host string, ports map[string]int32) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ta := topodatapb.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	tablet := &topodatapb.Tablet{
		Alias:    &ta,
		Hostname: host,
		PortMap:  make(map[string]int32),
	}
	for name, port := range ports {
		if name == "mysql" {
			topoproto.SetMysqlPort(tablet, port)
		} else {
			tablet.PortMap[name] = port
		}
	}
	ft.tablets[topoproto.TabletAliasString(&ta)] = tablet
}

func (ft *fakeTopo) RemoveTablet(cell string, uid uint32) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ta := topodatapb.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	delete(ft.tablets, topoproto.TabletAliasString(&ta))
}

func (ft *fakeTopo) GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	if !ft.expectGetTabletsByCell {
		return nil, fmt.Errorf("unexpected GetTabletsByCell")
	}
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	res := make([]*topodatapb.TabletAlias, 0, 1)
	for _, tablet := range ft.tablets {
		if tablet.Alias.Cell == cell {
			res = append(res, tablet.Alias)
		}
	}
	return res, nil
}

// GetShardReplication should return all the nodes in a shard,
// but instead we cheat for this test and just return all the
// tablets in the cell.
func (ft *fakeTopo) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	if ft.expectGetTabletsByCell {
		return nil, fmt.Errorf("unexpected GetShardReplication")
	}

	ft.mu.RLock()
	defer ft.mu.RUnlock()
	nodes := make([]*topodatapb.ShardReplication_Node, 0, 1)
	for _, tablet := range ft.tablets {
		if tablet.Alias.Cell == cell {
			nodes = append(nodes, &topodatapb.ShardReplication_Node{
				TabletAlias: tablet.Alias,
			})
		}
	}
	return topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
		Nodes: nodes,
	}, cell, keyspace, shard), nil
}

func (ft *fakeTopo) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	// Note we want to be correct here. The way we call this, we never
	// change the tablet list in between a call to list them,
	// and a call to get the record, so we could just blindly return it.
	// (It wasn't the case before we added the WaitForInitialTopology()
	// call in the test though!).
	tablet, ok := ft.tablets[topoproto.TabletAliasString(alias)]
	if !ok {
		return nil, 0, topo.ErrNoNode
	}
	return tablet, 0, nil
}

func TestFilterByShard(t *testing.T) {
	testcases := []struct {
		filters  []string
		keyspace string
		shard    string
		included bool
	}{
		// un-sharded keyspaces
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks1",
			shard:    "0",
			included: true,
		},
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks2",
			shard:    "0",
			included: false,
		},
		// custom sharding, different shard
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks1",
			shard:    "1",
			included: false,
		},
		// keyrange based sharding
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "0",
			included: false,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "-40",
			included: true,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "-80",
			included: true,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "80-",
			included: false,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "c0-",
			included: false,
		},
	}

	for _, tc := range testcases {
		fbs, err := NewFilterByShard(nil, tc.filters)
		if err != nil {
			t.Errorf("cannot create FilterByShard for filters %v: %v", tc.filters, err)
		}

		tablet := &topodatapb.Tablet{
			Keyspace: tc.keyspace,
			Shard:    tc.shard,
		}

		got := fbs.isIncluded(tablet)
		if got != tc.included {
			t.Errorf("isIncluded(%v,%v) for filters %v returned %v but expected %v", tc.keyspace, tc.shard, tc.filters, got, tc.included)
		}
	}
}
