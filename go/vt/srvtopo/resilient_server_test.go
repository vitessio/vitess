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

package srvtopo

import (
	"bytes"
	"fmt"
	"html/template"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/status"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// TestGetSrvKeyspace will test we properly return updated SrvKeyspace.
func TestGetSrvKeyspace(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory("test_cell")
	rs := NewResilientServer(ts, "TestGetSrvKeyspace")
	ttl := time.Duration(500 * time.Millisecond)
	*srvTopoCacheTTL = ttl

	// Ask for a not-yet-created keyspace
	_, err := rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
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
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
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

	// make sure the HTML template works
	templ := template.New("").Funcs(status.StatusFuncs)
	templ, err = templ.Parse(TopoTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, rs.CacheStatus()); err != nil {
		t.Fatalf("error executing template: %v", err)
	}

	// Now delete the SrvKeyspace, wait until we get the error.
	if err := ts.DeleteSrvKeyspace(context.Background(), "test_cell", "test_ks"); err != nil {
		t.Fatalf("DeleteSrvKeyspace() failed: %v", err)
	}
	expiry = time.Now().Add(5 * time.Second)
	for {
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
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
	updateTime := time.Now()
	for {
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err == nil && proto.Equal(want, got) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("timeout waiting for new keyspace value")
		}
		time.Sleep(time.Millisecond)
	}

	// Now simulate a topo service error and see that the last value is
	// cached for at least half of the expected ttl.
	forceErr := fmt.Errorf("test topo error")
	factory.SetError(forceErr)

	expiry = time.Now().Add(ttl / 2)
	for {
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err != nil || !proto.Equal(want, got) {
			t.Errorf("expected keyspace to be cached for at least %s seconds, got error %v", time.Since(updateTime), err)
		}

		if time.Now().After(expiry) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Now wait for the TTL to expire and we should get the expected error
	expiry = time.Now().Add(1 * time.Second)
	for {
		_, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err != nil || err == forceErr {
			break
		}

		if time.Now().After(expiry) {
			t.Fatalf("timed out waiting for error to be returned")
		}
		time.Sleep(time.Millisecond)
	}

	// Clear the error away and check that we can now get the value
	factory.SetError(nil)

	got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
	if err != nil || !proto.Equal(want, got) {
		t.Errorf("expected value to be restored, got %v", err)
	}

	// Check that the watch now works to update the value
	want = &topodatapb.SrvKeyspace{
		ShardingColumnName: "id3",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}
	ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)
	expiry = time.Now().Add(5 * time.Second)
	for {
		got, err = rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
		if err == nil && proto.Equal(want, got) {
			break
		}
		if time.Now().After(expiry) {
			t.Fatalf("timeout waiting for new keyspace value")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestSrvKeyspaceCachedError will test we properly re-try to query
// the topo server upon failure.
func TestSrvKeyspaceCachedError(t *testing.T) {
	ts := memorytopo.NewServer("test_cell")
	rs := NewResilientServer(ts, "TestSrvKeyspaceCachedErrors")

	// Ask for an unknown keyspace, should get an error.
	ctx := context.Background()
	_, err := rs.GetSrvKeyspace(ctx, "test_cell", "unknown_ks")
	if err == nil {
		t.Fatalf("First GetSrvKeyspace didn't return an error")
	}
	entry := rs.getSrvKeyspaceEntry("test_cell", "unknown_ks")
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
	_, err2 := rs.GetSrvKeyspace(ctx, "test_cell", "unknown_ks")
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
	rs := NewResilientServer(ts, "TestGetSrvKeyspaceCreated")

	// Set SrvKeyspace with value.
	want := &topodatapb.SrvKeyspace{
		ShardingColumnName: "id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}
	ts.UpdateSrvKeyspace(context.Background(), "test_cell", "test_ks", want)

	// Wait until we get the right value.
	expiry := time.Now().Add(5 * time.Second)
	for {
		got, err := rs.GetSrvKeyspace(context.Background(), "test_cell", "test_ks")
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

func TestWatchSrvVSchema(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("test_cell")
	rs := NewResilientServer(ts, "TestWatchSrvVSchema")
	watchSrvVSchemaSleepTime = 10 * time.Millisecond

	// mu protects watchValue and watchErr.
	mu := sync.Mutex{}
	var watchValue *vschemapb.SrvVSchema
	var watchErr error
	rs.WatchSrvVSchema(ctx, "test_cell", func(v *vschemapb.SrvVSchema, e error) {
		mu.Lock()
		defer mu.Unlock()
		watchValue = v
		watchErr = e
	})
	get := func() (*vschemapb.SrvVSchema, error) {
		mu.Lock()
		defer mu.Unlock()
		return watchValue, watchErr
	}

	// WatchSrvVSchema won't return until it gets the initial value,
	// which is not there, so we should get watchErr=topo.ErrNoNode.
	if _, err := get(); err != topo.ErrNoNode {
		t.Fatalf("WatchSrvVSchema didn't return topo.ErrNoNode at first, but got: %v", err)
	}

	// Save a value, wait for it.
	newValue := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {},
		},
	}
	if err := ts.UpdateSrvVSchema(ctx, "test_cell", newValue); err != nil {
		t.Fatalf("UpdateSrvVSchema failed: %v", err)
	}
	start := time.Now()
	for {
		if v, err := get(); err == nil && proto.Equal(newValue, v) {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timed out waiting for new SrvVschema")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Update value, wait for it.
	updatedValue := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks2": {},
		},
	}
	if err := ts.UpdateSrvVSchema(ctx, "test_cell", updatedValue); err != nil {
		t.Fatalf("UpdateSrvVSchema failed: %v", err)
	}
	start = time.Now()
	for {
		if v, err := get(); err == nil && proto.Equal(updatedValue, v) {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timed out waiting for updated SrvVschema")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Delete the value, wait for topo.ErrNoNode
	if err := ts.DeleteSrvVSchema(ctx, "test_cell"); err != nil {
		t.Fatalf("DeleteSrvVSchema failed: %v", err)
	}
	start = time.Now()
	for {
		if _, err := get(); err == topo.ErrNoNode {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timed out waiting for deleted SrvVschema")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
