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

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestFilterUnhealthy(t *testing.T) {
	cases := []struct {
		source *pb.EndPoints
		want   *pb.EndPoints
	}{
		{
			source: nil,
			want:   nil,
		},
		{
			source: &pb.EndPoints{},
			want:   &pb.EndPoints{Entries: nil},
		},
		{
			source: &pb.EndPoints{Entries: []*pb.EndPoint{}},
			want:   &pb.EndPoints{Entries: []*pb.EndPoint{}},
		},
		{
			// All are healthy and all should be returned.
			source: &pb.EndPoints{
				Entries: []*pb.EndPoint{
					&pb.EndPoint{
						Uid:       1,
						HealthMap: nil,
					},
					&pb.EndPoint{
						Uid:       2,
						HealthMap: map[string]string{},
					},
					&pb.EndPoint{
						Uid: 3,
						HealthMap: map[string]string{
							"Random": "Value1",
						},
					},
					&pb.EndPoint{
						Uid:       4,
						HealthMap: nil,
					},
				},
			},
			want: &pb.EndPoints{
				Entries: []*pb.EndPoint{
					&pb.EndPoint{
						Uid:       1,
						HealthMap: nil,
					},
					&pb.EndPoint{
						Uid:       2,
						HealthMap: map[string]string{},
					},
					&pb.EndPoint{
						Uid: 3,
						HealthMap: map[string]string{
							"Random": "Value1",
						},
					},
					&pb.EndPoint{
						Uid:       4,
						HealthMap: nil,
					},
				},
			},
		},
		{
			// 4 is unhealthy, it should be filtered out.
			source: &pb.EndPoints{
				Entries: []*pb.EndPoint{
					&pb.EndPoint{
						Uid:       1,
						HealthMap: nil,
					},
					&pb.EndPoint{
						Uid:       2,
						HealthMap: map[string]string{},
					},
					&pb.EndPoint{
						Uid: 3,
						HealthMap: map[string]string{
							"Random": "Value2",
						},
					},
					&pb.EndPoint{
						Uid: 4,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					&pb.EndPoint{
						Uid:       5,
						HealthMap: nil,
					},
				},
			},
			want: &pb.EndPoints{
				Entries: []*pb.EndPoint{
					&pb.EndPoint{
						Uid:       1,
						HealthMap: nil,
					},
					&pb.EndPoint{
						Uid:       2,
						HealthMap: map[string]string{},
					},
					&pb.EndPoint{
						Uid: 3,
						HealthMap: map[string]string{
							"Random": "Value2",
						},
					},
					&pb.EndPoint{
						Uid:       5,
						HealthMap: nil,
					},
				},
			},
		},
		{
			// Only unhealthy servers, return all of them.
			source: &pb.EndPoints{
				Entries: []*pb.EndPoint{
					&pb.EndPoint{
						Uid: 1,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					&pb.EndPoint{
						Uid: 2,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					&pb.EndPoint{
						Uid: 3,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
				},
			},
			want: &pb.EndPoints{
				Entries: []*pb.EndPoint{
					&pb.EndPoint{
						Uid: 1,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					&pb.EndPoint{
						Uid: 2,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					&pb.EndPoint{
						Uid: 3,
						HealthMap: map[string]string{
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

func (ft *fakeTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	return []string{ft.keyspace}, nil
}

func (ft *fakeTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topo.SrvKeyspace, error) {
	ft.callCount++
	if keyspace == ft.keyspace {
		return &topo.SrvKeyspace{}, nil
	}
	return nil, fmt.Errorf("Unknown keyspace")
}

func (ft *fakeTopo) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType) (*pb.EndPoints, int64, error) {
	return nil, -1, fmt.Errorf("No endpoints")
}

type fakeTopoRemoteMaster struct {
	fakeTopo
	cell       string
	remoteCell string
}

func (ft *fakeTopoRemoteMaster) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*pb.SrvShard, error) {
	return &pb.SrvShard{
		Name:       shard,
		MasterCell: ft.remoteCell,
	}, nil
}

func (ft *fakeTopoRemoteMaster) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType) (*pb.EndPoints, int64, error) {
	if cell != ft.cell && cell != ft.remoteCell {
		return nil, -1, fmt.Errorf("GetEndPoints: invalid cell: %v", cell)
	}
	if cell == ft.cell || tabletType != topo.TYPE_MASTER {
		return &pb.EndPoints{
			Entries: []*pb.EndPoint{
				&pb.EndPoint{
					Uid:       0,
					HealthMap: nil,
				},
			},
		}, -1, nil
	}
	return &pb.EndPoints{
		Entries: []*pb.EndPoint{
			&pb.EndPoint{
				Uid:       1,
				HealthMap: nil,
			},
		},
	}, -1, nil
}

// TestRemoteMaster will test getting endpoints for remote master.
func TestRemoteMaster(t *testing.T) {
	ft := &fakeTopoRemoteMaster{cell: "cell1", remoteCell: "cell2"}
	rsts := NewResilientSrvTopoServer(ft, "TestRemoteMaster")
	rsts.enableRemoteMaster = true

	// remote cell for master
	ep, _, err := rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "1", topo.TYPE_MASTER)
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
	ep, _, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "0", topo.TYPE_REPLICA)
	if err == nil {
		t.Fatalf("GetEndPoints did not return an error")
	}

	// no remote cell for master
	rsts.enableRemoteMaster = false
	ep, _, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "2", topo.TYPE_MASTER)
	if err == nil {
		t.Fatalf("GetEndPoints did not return an error")
	}
	// use cached value from above
	ep, _, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "1", topo.TYPE_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints got unexpected error: %v", err)
	}
	ep, _, err = rsts.GetEndPoints(context.Background(), "cell1", "test_ks", "1", topo.TYPE_MASTER)
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
