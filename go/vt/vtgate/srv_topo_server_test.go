// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/topo"
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
							health.ReplicationLag: health.ReplicationLagHigh,
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
							health.ReplicationLag: health.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid: 2,
						Health: map[string]string{
							health.ReplicationLag: health.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid: 3,
						Health: map[string]string{
							health.ReplicationLag: health.ReplicationLagHigh,
						},
					},
				},
			},
			want: &topo.EndPoints{
				Entries: []topo.EndPoint{
					topo.EndPoint{
						Uid: 1,
						Health: map[string]string{
							health.ReplicationLag: health.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid: 2,
						Health: map[string]string{
							health.ReplicationLag: health.ReplicationLagHigh,
						},
					},
					topo.EndPoint{
						Uid: 3,
						Health: map[string]string{
							health.ReplicationLag: health.ReplicationLagHigh,
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

// TestCacheWithErrors will test we properly return cached errors.
func TestCacheWithErrors(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(ft, "TestCacheWithErrors")

	// ask for the known keyspace, that populates the cache
	_, err := rsts.GetSrvKeyspace("", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}

	// now make the topo server fail, and ask again, should get cached
	// value, not even ask underlying guy
	ft.keyspace = "another_test_ks"
	_, err = rsts.GetSrvKeyspace("", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}

	// now reduce TTL to nothing, so we won't use cache, and ask again
	rsts.cacheTTL = 0
	_, err = rsts.GetSrvKeyspace("", "test_ks")
	if err != nil {
		t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
	}
}

// TestCachedErrors will test we properly return cached errors.
func TestCachedErrors(t *testing.T) {
	ft := &fakeTopo{keyspace: "test_ks"}
	rsts := NewResilientSrvTopoServer(ft, "TestCachedErrors")

	// ask for an unknown keyspace, should get an error
	_, err := rsts.GetSrvKeyspace("", "unknown_ks")
	if err == nil {
		t.Fatalf("First GetSrvKeyspace didn't return an error")
	}
	if ft.callCount != 1 {
		t.Fatalf("GetSrvKeyspace didn't get called 1 but %u times", ft.callCount)
	}

	// ask again, should get an error and use cache
	_, err = rsts.GetSrvKeyspace("", "unknown_ks")
	if err == nil {
		t.Fatalf("Second GetSrvKeyspace didn't return an error")
	}
	if ft.callCount != 1 {
		t.Fatalf("GetSrvKeyspace was called again: %u times", ft.callCount)
	}

	// ask again after expired cache, should get an error
	rsts.cacheTTL = 0
	_, err = rsts.GetSrvKeyspace("", "unknown_ks")
	if err == nil {
		t.Fatalf("Third GetSrvKeyspace didn't return an error")
	}
	if ft.callCount != 2 {
		t.Fatalf("GetSrvKeyspace was not called again: %u times", ft.callCount)
	}
}
