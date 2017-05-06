/*
Copyright 2017 Google Inc.

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
// It returns errors for everything, except for SrvKeyspace methods.
type fakeTopo struct {
	faketopo.FakeTopo
	watchCount int
	keyspaces  map[string]*fakeKeyspace
}

type fakeKeyspace struct {
	current *topodatapb.SrvKeyspace
	watch   chan *topo.WatchData
}

func newFakeTopo() *fakeTopo {
	return &fakeTopo{
		keyspaces: make(map[string]*fakeKeyspace),
	}
}

func (ft *fakeTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	if cell != "test_cell" {
		return nil, fmt.Errorf("wrong cell: %v", cell)
	}
	result := make([]string, 0, len(ft.keyspaces))
	for k := range ft.keyspaces {
		result = append(result, k)
	}
	return result, nil
}

func (ft *fakeTopo) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	if cell != "test_cell" {
		return fmt.Errorf("wrong cell: %v", cell)
	}

	fk, ok := ft.keyspaces[keyspace]
	if !ok {
		ft.keyspaces[keyspace] = &fakeKeyspace{
			current: srvKeyspace,
		}
		return nil
	}
	fk.current = srvKeyspace
	if fk.watch != nil {
		contents, err := proto.Marshal(srvKeyspace)
		if err != nil {
			return err
		}
		fk.watch <- &topo.WatchData{
			Contents: contents,
			Version:  nil,
			Err:      nil,
		}
	}
	return nil
}

func (ft *fakeTopo) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	if cell != "test_cell" {
		return fmt.Errorf("wrong cell: %v", cell)
	}

	fk, ok := ft.keyspaces[keyspace]
	if !ok {
		return topo.ErrNoNode
	}
	if fk.watch != nil {
		fk.watch <- &topo.WatchData{
			Contents: nil,
			Version:  nil,
			Err:      topo.ErrNoNode,
		}
		close(fk.watch)
		fk.watch = nil
	}
	delete(ft.keyspaces, keyspace)
	return nil
}

func (ft *fakeTopo) Watch(ctx context.Context, cell, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	if cell != "test_cell" {
		return &topo.WatchData{Err: fmt.Errorf("wrong cell: %v", cell)}, nil, nil
	}

	// We only handle SrvKeyspace: keyspaces/<keyspace>/SrvKeyspace
	parts := strings.Split(filePath, "/")
	if len(parts) != 3 || parts[0] != "keyspaces" || parts[2] != "SrvKeyspace" {
		return &topo.WatchData{Err: fmt.Errorf("unknown path: %v", filePath)}, nil, nil
	}
	keyspace := parts[1]

	ft.watchCount++

	fk, ok := ft.keyspaces[keyspace]
	if !ok {
		return &topo.WatchData{Err: topo.ErrNoNode}, nil, nil
	}
	contents, err := proto.Marshal(fk.current)
	if err != nil {
		return &topo.WatchData{Err: err}, nil, nil
	}
	if fk.watch != nil {
		return &topo.WatchData{Err: fmt.Errorf("only supports one concurrent watch")}, nil, nil
	}
	fk.watch = make(chan *topo.WatchData, 10)
	return &topo.WatchData{
		Contents: contents,
		Version:  nil,
		Err:      nil,
	}, fk.watch, func() {}
}

// TestGetSrvKeyspace will test we properly return updated SrvKeyspace.
func TestGetSrvKeyspace(t *testing.T) {
	ft := newFakeTopo()
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestGetSrvKeyspace")

	// Ask for a not-yet-created keyspace
	_, err := rsts.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
	if err != topo.ErrNoNode {
		t.Fatalf("GetSrvKeyspace(not created) got unexpected error: %v", err)
	}

	// Set SrvKeyspace with value
	want := &topodatapb.SrvKeyspace{
		ShardingColumnName: "id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}
	ft.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)

	// wait until we get the right value
	var got *topodatapb.SrvKeyspace
	expiry := time.Now().Add(5 * time.Second)
	for {
		got, err = rsts.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err != nil {
			t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
		}
		if proto.Equal(want, got) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("GetSrvKeyspace() timeout = %+v, want %+v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Now delete the SrvKeyspace, wait until we get the error.
	if err := ft.DeleteSrvKeyspace(context.Background(), "test_cell", "test_ks"); err != nil {
		t.Fatalf("DeleteSrvKeyspace() failed: %v", err)
	}
	expiry = time.Now().Add(5 * time.Second)
	for {
		got, err = rsts.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err == topo.ErrNoNode {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("timeout waiting for no keyspace error")
		}
		time.Sleep(time.Millisecond)
	}

	// Now send an updated real value, see it come through.
	want = &topodatapb.SrvKeyspace{
		ShardingColumnName: "id2",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}
	ft.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	expiry = time.Now().Add(5 * time.Second)
	for {
		got, err = rsts.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
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

// TestSrvKeyspaceCachedError will test we properly re-try to query
// the topo server upon failure.
func TestSrvKeyspaceCachedError(t *testing.T) {
	ft := newFakeTopo()
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestSrvKeyspaceCachedErrors")

	// ask for an unknown keyspace, should get an error
	_, err := rsts.GetSrvKeyspace(context.Background(), "test_cell", "unknown_ks")
	if err == nil {
		t.Fatalf("First GetSrvKeyspace didn't return an error")
	}
	if ft.watchCount != 1 {
		t.Fatalf("GetSrvKeyspace didn't get called 1 but %v times", ft.watchCount)
	}

	// ask again, should get an error and use cache
	_, err = rsts.GetSrvKeyspace(context.Background(), "test_cell", "unknown_ks")
	if err == nil {
		t.Fatalf("Second GetSrvKeyspace didn't return an error")
	}
	if ft.watchCount != 2 {
		t.Fatalf("GetSrvKeyspace was not called again: %v times", ft.watchCount)
	}
}

// TestGetSrvKeyspaceCreated will test we properly get the initial
// value if the SrvKeyspace already exists.
func TestGetSrvKeyspaceCreated(t *testing.T) {
	ft := newFakeTopo()
	rsts := NewResilientSrvTopoServer(topo.Server{Impl: ft}, "TestGetSrvKeyspaceCreated")

	// Set SrvKeyspace with value
	want := &topodatapb.SrvKeyspace{
		ShardingColumnName: "id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}
	ft.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)

	// wait until we get the right value
	expiry := time.Now().Add(5 * time.Second)
	for {
		got, err := rsts.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		switch err {
		case topo.ErrNoNode:
			// keep trying
		case nil:
			// we got a value, see if it's good
			if proto.Equal(want, got) {
				return
			}
		default:
			t.Fatalf("GetSrvKeyspace got unexpected error: %v", err)
		}
		if time.Now().After(expiry) {
			t.Fatalf("GetSrvKeyspace() timeout = %+v, want %+v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
