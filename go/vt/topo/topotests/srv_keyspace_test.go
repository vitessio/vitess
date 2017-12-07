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

package topotests

import (
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// waitForInitialSrvKeyspace waits for the initial SrvKeyspace to
// appear, and match the provided srvKeyspace.
func waitForInitialSrvKeyspace(t *testing.T, ts *topo.Server, cell, keyspace string) (current *topo.WatchSrvKeyspaceData, changes <-chan *topo.WatchSrvKeyspaceData, cancel topo.CancelFunc) {
	ctx := context.Background()
	start := time.Now()
	for {
		current, changes, cancel = ts.WatchSrvKeyspace(ctx, cell, keyspace)
		switch current.Err {
		case topo.ErrNoNode:
			// hasn't appeared yet
			if time.Now().Sub(start) > 10*time.Second {
				t.Fatalf("time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		case nil:
			return
		default:
			t.Fatalf("watch failed: %v", current.Err)
		}
	}
}

func TestWatchSrvKeyspaceNoNode(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// No SrvKeyspace -> ErrNoNode
	current, _, _ := ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if current.Err != topo.ErrNoNode {
		t.Errorf("Got invalid result from WatchSrvKeyspace(not there): %v", current.Err)
	}
}

func TestWatchSrvKeyspace(t *testing.T) {

	cell := "cell1"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// Create initial value
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, &topodatapb.SrvKeyspace{}); err != nil {
		t.Fatalf("Update(/keyspaces/ks1/SrvKeyspace) failed: %v", err)
	}

	// Starting the watch should now work, and return an empty
	// SrvKeyspace.
	wanted := &topodatapb.SrvKeyspace{}
	current, changes, cancel := waitForInitialSrvKeyspace(t, ts, cell, keyspace)
	if !proto.Equal(current.Value, wanted) {
		t.Fatalf("got bad data: %v expected: %v", current.Value, wanted)
	}

	// Update the value with good data, wait until we see it
	wanted.ShardingColumnName = "scn1"
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, wanted); err != nil {
		t.Fatalf("Update(/keyspaces/ks1/SrvKeyspace) failed: %v", err)
	}
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			t.Fatalf("watch channel unexpectedly got error: %v", wd.Err)
		}
		if proto.Equal(wd.Value, wanted) {
			break
		}
		if proto.Equal(wd.Value, &topodatapb.SrvKeyspace{}) {
			t.Log("got duplicate empty value, skipping.")
		}
		t.Fatalf("got bad data: %v expected: %v", wd.Value, wanted)
	}

	// Update the value with bad data, wait until error.
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		t.Fatalf("ConnForCell failed: %v", err)
	}
	if _, err := conn.Update(ctx, "/keyspaces/"+keyspace+"/SrvKeyspace", []byte("BAD PROTO DATA"), nil); err != nil {
		t.Fatalf("Update(/keyspaces/ks1/SrvKeyspace) failed: %v", err)
	}
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			if strings.Contains(wd.Err.Error(), "error unpacking SrvKeyspace object") {
				break
			}
			t.Fatalf("watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		if !proto.Equal(wd.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", wd.Value, wanted)
		}
		t.Log("got duplicate right value, skipping.")
	}

	// Cancel should still work here, although it does nothing.
	cancel()

	// Bad data in topo, setting the watch should now fail.
	current, changes, cancel = ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if current.Err == nil || !strings.Contains(current.Err.Error(), "error unpacking initial SrvKeyspace object") {
		t.Fatalf("expected an initial error setting watch on bad content, but got: %v", current.Err)
	}

	// Update content, wait until Watch works again
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, wanted); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}
	start := time.Now()
	for {
		current, changes, cancel = ts.WatchSrvKeyspace(ctx, cell, keyspace)
		if current.Err != nil {
			if strings.Contains(current.Err.Error(), "error unpacking initial SrvKeyspace object") {
				// hasn't changed yet
				if time.Now().Sub(start) > 10*time.Second {
					t.Fatalf("time out waiting for file to appear")
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatalf("got unexpected error while setting watch: %v", err)
		}
		if !proto.Equal(current.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", current.Value, wanted)
		}
		break
	}

	// Delete node, wait for error (skip any duplicate).
	if err := ts.DeleteSrvKeyspace(ctx, cell, keyspace); err != nil {
		t.Fatalf("DeleteSrvKeyspace() failed: %v", err)
	}
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err == topo.ErrNoNode {
			break
		}
		if wd.Err != nil {
			t.Fatalf("watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		if !proto.Equal(wd.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", wd.Value, wanted)
		}
		t.Log("got duplicate right value, skipping.")
	}
}

func TestWatchSrvKeyspaceCancel(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// No SrvKeyspace -> ErrNoNode
	current, changes, cancel := ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if current.Err != topo.ErrNoNode {
		t.Errorf("Got invalid result from WatchSrvKeyspace(not there): %v", current.Err)
	}

	// Create initial value
	wanted := &topodatapb.SrvKeyspace{
		ShardingColumnName: "scn2",
	}
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, wanted); err != nil {
		t.Fatalf("UpdateSrvKeyspace() failed: %v", err)
	}

	// Starting the watch should now work.
	current, changes, cancel = waitForInitialSrvKeyspace(t, ts, cell, keyspace)
	if !proto.Equal(current.Value, wanted) {
		t.Fatalf("got bad data: %v expected: %v", current.Value, wanted)
	}

	// Cancel watch, wait for error.
	cancel()
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err == topo.ErrInterrupted {
			break
		}
		if wd.Err != nil {
			t.Fatalf("watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		if !proto.Equal(wd.Value, wanted) {
			t.Fatalf("got bad data: %v expected: %v", wd.Value, wanted)
		}
		t.Log("got duplicate right value, skipping.")
	}

	// Cancel should still work here, although it does nothing.
	cancel()
}
