// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"golang.org/x/net/context"
)

func TestFilterUnhealthy(t *testing.T) {
	cases := []struct {
		source *topo.EndPoints
		want   *topo.EndPoints
	}{
		{
			source: nil,
			want:   nil,
		},
		{
			source: &topo.EndPoints{},
			want:   &topo.EndPoints{Entries: nil},
		},
		{
			source: &topo.EndPoints{Entries: []topo.EndPoint{}},
			want:   &topo.EndPoints{Entries: []topo.EndPoint{}},
		},
		{
			// All are healthy and all should be returned.
			source: &topo.EndPoints{
				Entries: []topo.EndPoint{
					topo.EndPoint{
						Uid:    1,
						Health: nil,
					},
					topo.EndPoint{
						Uid:    2,
						Health: map[string]string{},
					},
					topo.EndPoint{
						Uid: 3,
						Health: map[string]string{
							"Random": "Value1",
						},
					},
					topo.EndPoint{
						Uid:    4,
						Health: nil,
					},
				},
			},
			want: &topo.EndPoints{
				Entries: []topo.EndPoint{
					topo.EndPoint{
						Uid:    1,
						Health: nil,
					},
					topo.EndPoint{
						Uid:    2,
						Health: map[string]string{},
					},
					topo.EndPoint{
						Uid: 3,
						Health: map[string]string{
							"Random": "Value1",
						},
					},
					topo.EndPoint{
						Uid:    4,
						Health: nil,
					},
				},
			},
		},
		{
			// 4 is unhealthy, it should be filtered out.
			source: &topo.EndPoints{
				Entries: []topo.EndPoint{
					topo.EndPoint{
						Uid:    1,
						Health: nil,
					},
					topo.EndPoint{
						Uid:    2,
						Health: map[string]string{},
					},
					topo.EndPoint{
						Uid: 3,
						Health: map[string]string{
							"Random": "Value2",
						},
					},
					topo.EndPoint{
						Uid: 4,
						Health: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid:    5,
						Health: nil,
					},
				},
			},
			want: &topo.EndPoints{
				Entries: []topo.EndPoint{
					topo.EndPoint{
						Uid:    1,
						Health: nil,
					},
					topo.EndPoint{
						Uid:    2,
						Health: map[string]string{},
					},
					topo.EndPoint{
						Uid: 3,
						Health: map[string]string{
							"Random": "Value2",
						},
					},
					topo.EndPoint{
						Uid:    5,
						Health: nil,
					},
				},
			},
		},
		{
			// Only unhealthy servers, return all of them.
			source: &topo.EndPoints{
				Entries: []topo.EndPoint{
					topo.EndPoint{
						Uid: 1,
						Health: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid: 2,
						Health: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid: 3,
						Health: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
				},
			},
			want: &topo.EndPoints{
				Entries: []topo.EndPoint{
					topo.EndPoint{
						Uid: 1,
						Health: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid: 2,
						Health: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid: 3,
						Health: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
				},
			},
		},
	}

	for _, c := range cases {
		if got := filterUnhealthyServers(c.source); !reflect.DeepEqual(got, c.want) {
			t.Errorf("filterUnhealthy(%+v)=%+v, want %+v", c.source, got, c.want)
		}
	}
}

// fakeTopo is used in testing ResilientSrvTopoServer logic.
// returns errors for everything, except the one keyspace.
type fakeTopo struct {
	faketopo.FakeTopo
	keyspace  string
	callCount int
}

func (ft *fakeTopo) GetSrvKeyspaceNames(cell string) ([]string, error) {
	return []string{ft.keyspace}, nil
}

func (ft *fakeTopo) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	ft.callCount++
	if keyspace == ft.keyspace {
		return &topo.SrvKeyspace{}, nil
	}
	return nil, fmt.Errorf("Unknown keyspace")
}

func (ft *fakeTopo) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	return nil, fmt.Errorf("No endpoints")
}

type fakeTopoRemoteMaster struct {
	fakeTopo
	cell       string
	remoteCell string
}

func (ft *fakeTopoRemoteMaster) GetSrvShard(cell, keyspace, shard string) (*topo.SrvShard, error) {
	return &topo.SrvShard{
		Name:       shard,
		MasterCell: ft.remoteCell,
	}, nil
}

func (ft *fakeTopoRemoteMaster) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	if cell != ft.cell && cell != ft.remoteCell {
		return nil, fmt.Errorf("GetEndPoints: invalid cell: %v", cell)
	}
	if cell == ft.cell || tabletType != topo.TYPE_MASTER {
		return &topo.EndPoints{
			Entries: []topo.EndPoint{
				topo.EndPoint{
					Uid:    0,
					Health: nil,
				},
			},
		}, nil
	}
	return &topo.EndPoints{
		Entries: []topo.EndPoint{
			topo.EndPoint{
				Uid:    1,
				Health: nil,
			},
		},
	}, nil
}

// TestRemoteMaster will test getting endpoints for remote master.
func TestRemoteMaster(t *testing.T) {
	ft := &fakeTopoRemoteMaster{cell: "cell1", remoteCell: "cell2"}
	rsts := NewResilientSrvTopoServer(ft, "TestRemoteMaster")
	rsts.enableRemoteMaster = true

	// remote cell for master
	ep, err := rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "1", topo.TYPE_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints got unexpected error: %v", err)
	}
	if ep.Entries[0].Uid != 1 {
		t.Fatalf("GetEndPoints got %v want 1", ep.Entries[0].Uid)
	}
	remoteQueryCount := rsts.counts.Counts()[remoteQueryCategory]
	if remoteQueryCount != 1 {
		t.Fatalf("Get remoteQueryCategory count got %v want 1", remoteQueryCount)
	}

	// no remote cell for non-master
	ep, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "0", topo.TYPE_REPLICA)
	if err == nil {
		t.Fatalf("GetEndPoints did not return an error")
	}

	// no remote cell for master
	rsts.enableRemoteMaster = false
	ep, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "2", topo.TYPE_MASTER)
	if err == nil {
		t.Fatalf("GetEndPoints did not return an error")
	}
	// use cached value from above
	ep, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "1", topo.TYPE_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints got unexpected error: %v", err)
	}
	ep, err = rsts.GetEndPoints(context.Background(), "cell1", "test_ks", "1", topo.TYPE_MASTER)
	if ep.Entries[0].Uid != 0 {
		t.Fatalf("GetEndPoints got %v want 0", ep.Entries[0].Uid)
	}
}

// TestCacheWithErrors will test we properly return cached errors.
func TestCacheWithErrors(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(ft, "TestCacheWithErrors")

	// ask for the known keyspace, that populates the cache
	_, err := rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}

	// now make the topo server fail, and ask again, should get cached
	// value, not even ask underlying guy
	ft.keyspace = "another_test_ks"
	_, err = rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}

	// now reduce TTL to nothing, so we won't use cache, and ask again
	rsts.cacheTTL = 0
	_, err = rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}
}

// TestCachedErrors will test we properly return cached errors.
func TestCachedErrors(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(ft, "TestCachedErrors")

	// ask for an unknown keyspace, should get an error
	_, err := rsts.GetSrvKeyspace(context.Background(), "", "unknown_ks")
	if err == nil {
		t.Fatalf("First GetSrvKeyspace didn't return an error")
	}
	if ft.callCount != 1 {
		t.Fatalf("GetSrvKeyspace didn't get called 1 but %v times", ft.callCount)
	}

	// ask again, should get an error and use cache
	_, err = rsts.GetSrvKeyspace(context.Background(), "", "unknown_ks")
	if err == nil {
		t.Fatalf("Second GetSrvKeyspace didn't return an error")
	}
	if ft.callCount != 1 {
		t.Fatalf("GetSrvKeyspace was called again: %v times", ft.callCount)
	}

	// ask again after expired cache, should get an error
	rsts.cacheTTL = 0
	_, err = rsts.GetSrvKeyspace(context.Background(), "", "unknown_ks")
	if err == nil {
		t.Fatalf("Third GetSrvKeyspace didn't return an error")
	}
	if ft.callCount != 2 {
		t.Fatalf("GetSrvKeyspace was not called again: %v times", ft.callCount)
	}
}
