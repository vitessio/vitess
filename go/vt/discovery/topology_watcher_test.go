package discovery

import (
	"fmt"
	"sync"
	"testing"
	"time"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
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

	tw.Stop()
}

func newFakeTopo(expectGetTabletsByCell bool) *fakeTopo {
	return &fakeTopo{
		expectGetTabletsByCell: expectGetTabletsByCell,
		tablets:                make(map[topodatapb.TabletAlias]*topodatapb.Tablet),
	}
}

type fakeTopo struct {
	faketopo.FakeTopo
	expectGetTabletsByCell bool
	mu                     sync.RWMutex
	tablets                map[topodatapb.TabletAlias]*topodatapb.Tablet
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
		tablet.PortMap[name] = port
	}
	ft.tablets[ta] = tablet
}

func (ft *fakeTopo) RemoveTablet(cell string, uid uint32) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ta := topodatapb.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	delete(ft.tablets, ta)
}

func (ft *fakeTopo) GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	if !ft.expectGetTabletsByCell {
		return nil, fmt.Errorf("unexpected GetTabletsByCell")
	}
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	res := make([]*topodatapb.TabletAlias, 0, 1)
	for alias, tablet := range ft.tablets {
		if tablet.Alias.Cell == cell {
			res = append(res, &alias)
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
	for alias, tablet := range ft.tablets {
		if tablet.Alias.Cell == cell {
			nodes = append(nodes, &topodatapb.ShardReplication_Node{
				TabletAlias: &alias,
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
	return ft.tablets[*alias], 0, nil
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
