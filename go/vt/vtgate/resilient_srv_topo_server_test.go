// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/status"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

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

	// make sure the HTML template works
	templ := template.New("").Funcs(status.StatusFuncs)
	templ, err = templ.Parse(TopoTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, rsts.CacheStatus()); err != nil {
		t.Fatalf("error executing template: %v", err)
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
