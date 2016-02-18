// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestFilterUnhealthy(t *testing.T) {
	cases := []struct {
		source *topodatapb.EndPoints
		want   *topodatapb.EndPoints
	}{
		{
			source: nil,
			want:   nil,
		},
		{
			source: &topodatapb.EndPoints{},
			want:   &topodatapb.EndPoints{Entries: nil},
		},
		{
			source: &topodatapb.EndPoints{Entries: []*topodatapb.EndPoint{}},
			want:   &topodatapb.EndPoints{Entries: []*topodatapb.EndPoint{}},
		},
		{
			// All are healthy and all should be returned.
			source: &topodatapb.EndPoints{
				Entries: []*topodatapb.EndPoint{
					{
						Uid:       1,
						HealthMap: nil,
					},
					{
						Uid:       2,
						HealthMap: map[string]string{},
					},
					{
						Uid: 3,
						HealthMap: map[string]string{
							"Random": "Value1",
						},
					},
					{
						Uid:       4,
						HealthMap: nil,
					},
				},
			},
			want: &topodatapb.EndPoints{
				Entries: []*topodatapb.EndPoint{
					{
						Uid:       1,
						HealthMap: nil,
					},
					{
						Uid:       2,
						HealthMap: map[string]string{},
					},
					{
						Uid: 3,
						HealthMap: map[string]string{
							"Random": "Value1",
						},
					},
					{
						Uid:       4,
						HealthMap: nil,
					},
				},
			},
		},
		{
			// 4 is unhealthy, it should be filtered out.
			source: &topodatapb.EndPoints{
				Entries: []*topodatapb.EndPoint{
					{
						Uid:       1,
						HealthMap: nil,
					},
					{
						Uid:       2,
						HealthMap: map[string]string{},
					},
					{
						Uid: 3,
						HealthMap: map[string]string{
							"Random": "Value2",
						},
					},
					{
						Uid: 4,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					{
						Uid:       5,
						HealthMap: nil,
					},
				},
			},
			want: &topodatapb.EndPoints{
				Entries: []*topodatapb.EndPoint{
					{
						Uid:       1,
						HealthMap: nil,
					},
					{
						Uid:       2,
						HealthMap: map[string]string{},
					},
					{
						Uid: 3,
						HealthMap: map[string]string{
							"Random": "Value2",
						},
					},
					{
						Uid:       5,
						HealthMap: nil,
					},
				},
			},
		},
		{
			// Only unhealthy servers, return all of them.
			source: &topodatapb.EndPoints{
				Entries: []*topodatapb.EndPoint{
					{
						Uid: 1,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					{
						Uid: 2,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					{
						Uid: 3,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
				},
			},
			want: &topodatapb.EndPoints{
				Entries: []*topodatapb.EndPoint{
					{
						Uid: 1,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					{
						Uid: 2,
						HealthMap: map[string]string{
							topo.ReplicationLag: topo.ReplicationLagHigh,
						},
					},
					{
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
	keyspace      string
	callCount     int
	notifications chan *topodatapb.SrvKeyspace
}

func (ft *fakeTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	return []string{ft.keyspace}, nil
}

func (ft *fakeTopo) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	if keyspace != ft.keyspace {
		return fmt.Errorf("Unknown keyspace")
	}
	ft.notifications <- srvKeyspace
	return nil
}

func (ft *fakeTopo) WatchSrvKeyspace(ctx context.Context, cell, keyspace string) (<-chan *topodatapb.SrvKeyspace, error) {
	ft.callCount++
	if keyspace == ft.keyspace {
		ft.notifications = make(chan *topodatapb.SrvKeyspace, 10)
		ft.notifications <- &topodatapb.SrvKeyspace{}
		return ft.notifications, nil
	}
	return nil, fmt.Errorf("Unknown keyspace")
}

func (ft *fakeTopo) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*topodatapb.SrvShard, error) {
	ft.callCount++
	if keyspace != ft.keyspace {
		return nil, fmt.Errorf("Unknown keyspace")
	}
	return &topodatapb.SrvShard{
		Name: shard,
	}, nil
}

func (ft *fakeTopo) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType) (*topodatapb.EndPoints, int64, error) {
	return nil, -1, fmt.Errorf("No endpoints")
}

type fakeTopoRemoteMaster struct {
	fakeTopo
	cell       string
	remoteCell string
}

func (ft *fakeTopoRemoteMaster) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*topodatapb.SrvShard, error) {
	return &topodatapb.SrvShard{
		Name:       shard,
		MasterCell: ft.remoteCell,
	}, nil
}

func (ft *fakeTopoRemoteMaster) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType) (*topodatapb.EndPoints, int64, error) {
	if cell != ft.cell && cell != ft.remoteCell {
		return nil, -1, fmt.Errorf("GetEndPoints: invalid cell: %v", cell)
	}
	if cell == ft.cell || tabletType != topodatapb.TabletType_MASTER {
		return &topodatapb.EndPoints{
			Entries: []*topodatapb.EndPoint{
				{
					Uid:       0,
					HealthMap: nil,
				},
			},
		}, -1, nil
	}
	return &topodatapb.EndPoints{
		Entries: []*topodatapb.EndPoint{
			{
				Uid:       1,
				HealthMap: nil,
			},
		},
	}, -1, nil
}

// TestRemoteMaster will test getting endpoints for remote master.
func TestRemoteMaster(t *testing.T) {
	ft := &fakeTopoRemoteMaster{cell: "cell1", remoteCell: "cell2"}
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestRemoteMaster")
	rsts.enableRemoteMaster = true

	// remote cell for master
	ep, _, err := rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "1", topodatapb.TabletType_MASTER)
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
	ep, _, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "0", topodatapb.TabletType_REPLICA)
	if err == nil {
		t.Fatalf("GetEndPoints did not return an error")
	}

	// no remote cell for master
	rsts.enableRemoteMaster = false
	ep, _, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "2", topodatapb.TabletType_MASTER)
	if err == nil {
		t.Fatalf("GetEndPoints did not return an error")
	}
	// use cached value from above
	ep, _, err = rsts.GetEndPoints(context.Background(), "cell3", "test_ks", "1", topodatapb.TabletType_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints got unexpected error: %v", err)
	}
	ep, _, err = rsts.GetEndPoints(context.Background(), "cell1", "test_ks", "1", topodatapb.TabletType_MASTER)
	if ep.Entries[0].Uid != 0 {
		t.Fatalf("GetEndPoints got %v want 0", ep.Entries[0].Uid)
	}
}

// TestGetSrvKeyspace will test we properly return updated SrvKeyspace.
func TestGetSrvKeyspace(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestGetSrvKeyspace")

	// ask for the known keyspace, that populates the cache
	_, err := rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}

	// update srvkeyspace with new value
	want := &topodatapb.SrvKeyspace{
		ShardingColumnName: "id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}
	ft.UpdateSrvKeyspace(context.Background(), "", "test_ks", want)

	// wait untl we get the right value
	var got *topodatapb.SrvKeyspace
	expiry := time.Now().Add(5 * time.Second)
	for {
		got, err = rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
		if err != nil {
			t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
		}
		if proto.Equal(want, got) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("GetSrvKeyspace() timeout = %+v, want %+v", got, want)
		}
		time.Sleep(time.Millisecond)
	}

	// now send an updated empty value, wait until we get the error
	ft.notifications <- nil
	expiry = time.Now().Add(5 * time.Second)
	for {
		got, err = rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
		if err != nil && strings.Contains(err.Error(), "no SrvKeyspace") {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("timeout waiting for no keyspace error")
		}
		time.Sleep(time.Millisecond)
	}

	// now send an updated real value, see it come through
	want = &topodatapb.SrvKeyspace{
		ShardingColumnName: "id2",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}
	ft.notifications <- want
	expiry = time.Now().Add(5 * time.Second)
	for {
		got, err = rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
		if err == nil && proto.Equal(want, got) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("timeout waiting for new keyspace value")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestCacheWithErrors will test we properly return cached errors.
func TestCacheWithErrors(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestCacheWithErrors")

	// ask for the known keyspace, that populates the cache
	_, err := rsts.GetSrvShard(context.Background(), "", "test_ks", "shard_0")
	if err != nil {
		t.Fatalf("GetSrvShard got unexpected error: %v", err)
	}

	// now make the topo server fail, and ask again, should get cached
	// value, not even ask underlying guy
	ft.keyspace = "another_test_ks"
	_, err = rsts.GetSrvShard(context.Background(), "", "test_ks", "shard_0")
	if err != nil {
		t.Fatalf("GetSrvShard got unexpected error: %v", err)
	}

	// now reduce TTL to nothing, so we won't use cache, and ask again
	rsts.cacheTTL = 0
	_, err = rsts.GetSrvShard(context.Background(), "", "test_ks", "shard_0")
	if err != nil {
		t.Fatalf("GetSrvShard got unexpected error: %v", err)
	}
}

// TestSrvKeyspaceCacheWithErrors will test we properly return cached errors for GetSrvKeyspace.
func TestSrvKeyspaceCacheWithErrors(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestSrvKeyspaceCacheWithErrors")

	// ask for the known keyspace, that populates the cache
	_, err := rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}

	// now make the topo server fail, and ask again, should get cached
	// value, not even ask underlying guy
	close(ft.notifications)
	_, err = rsts.GetSrvKeyspace(context.Background(), "", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}
}

// TestCachedErrors will test we properly return cached errors.
func TestCachedErrors(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestCachedErrors")

	// ask for an unknown keyspace, should get an error
	_, err := rsts.GetSrvShard(context.Background(), "", "unknown_ks", "shard_0")
	if err == nil {
		t.Fatalf("First GetSrvShard didn't return an error")
	}
	if ft.callCount != 1 {
		t.Fatalf("GetSrvShard didn't get called 1 but %v times", ft.callCount)
	}

	// ask again, should get an error and use cache
	_, err = rsts.GetSrvShard(context.Background(), "", "unknown_ks", "shard_0")
	if err == nil {
		t.Fatalf("Second GetSrvShard didn't return an error")
	}
	if ft.callCount != 1 {
		t.Fatalf("GetSrvShard was called again: %v times", ft.callCount)
	}

	// ask again after expired cache, should get an error
	rsts.cacheTTL = 0
	_, err = rsts.GetSrvShard(context.Background(), "", "unknown_ks", "shard_0")
	if err == nil {
		t.Fatalf("Third GetSrvShard didn't return an error")
	}
	if ft.callCount != 2 {
		t.Fatalf("GetSrvShard was not called again: %v times", ft.callCount)
	}
}

// TestSrvKeyspaceCachedErrors will test we properly return cached errors for SrvKeyspace.
func TestSrvKeyspaceCachedErrors(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestSrvKeyspaceCachedErrors")

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
	if ft.callCount != 2 {
		t.Fatalf("GetSrvKeyspace was not called again: %v times", ft.callCount)
	}
}
