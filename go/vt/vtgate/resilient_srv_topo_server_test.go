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
	"html/template"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/status"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestGetSrvKeyspace will test we properly return updated SrvKeyspace.
func TestGetSrvKeyspace(t *testing.T) {
	ts := memorytopo.NewServer("test_cell")
	rsts := NewResilientSrvTopoServer(ts, "TestGetSrvKeyspace")

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
	ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)

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
	if err := ts.DeleteSrvKeyspace(context.Background(), "test_cell", "test_ks"); err != nil {
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
	ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
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
	ts := memorytopo.NewServer("test_cell")
	rsts := NewResilientSrvTopoServer(ts, "TestSrvKeyspaceCachedErrors")

	// Ask for an unknown keyspace, should get an error.
	ctx := context.Background()
	_, err := rsts.GetSrvKeyspace(ctx, "test_cell", "unknown_ks")
	if err == nil {
		t.Fatalf("First GetSrvKeyspace didn't return an error")
	}
	entry := rsts.getSrvKeyspaceEntry("test_cell", "unknown_ks")
	if err != entry.lastError {
		t.Errorf("Error wasn't saved properly")
	}
	if ctx != entry.lastErrorCtx {
		t.Errorf("Context wasn't saved properly")
	}

	// Ask again with a different context, should get an error and
	// save that context.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err2 := rsts.GetSrvKeyspace(ctx, "test_cell", "unknown_ks")
	if err2 == nil {
		t.Fatalf("Second GetSrvKeyspace didn't return an error")
	}
	if err2 != entry.lastError {
		t.Errorf("Error wasn't saved properly")
	}
	if ctx != entry.lastErrorCtx {
		t.Errorf("Context wasn't saved properly")
	}
}

// TestGetSrvKeyspaceCreated will test we properly get the initial
// value if the SrvKeyspace already exists.
func TestGetSrvKeyspaceCreated(t *testing.T) {
	ts := memorytopo.NewServer("test_cell")
	rsts := NewResilientSrvTopoServer(ts, "TestGetSrvKeyspaceCreated")

	// Set SrvKeyspace with value
	want := &topodatapb.SrvKeyspace{
		ShardingColumnName: "id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}
	ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)

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
