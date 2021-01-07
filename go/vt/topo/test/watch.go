/*
Copyright 2019 The Vitess Authors.

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

package test

import (
	"testing"
	"time"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// waitForInitialValue waits for the initial value of
// keyspaces/test_keyspace/SrvKeyspace to appear, and match the
// provided srvKeyspace.
func waitForInitialValue(t *testing.T, conn topo.Conn, srvKeyspace *topodatapb.SrvKeyspace) (changes <-chan *topo.WatchData, cancel func()) {
	var current *topo.WatchData
	ctx := context.Background()
	start := time.Now()
	for {
		current, changes, cancel = conn.Watch(ctx, "keyspaces/test_keyspace/SrvKeyspace")
		if topo.IsErrType(current.Err, topo.NoNode) {
			// hasn't appeared yet
			if time.Since(start) > 10*time.Second {
				t.Fatalf("time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if current.Err != nil {
			t.Fatalf("watch failed: %v", current.Err)
		}
		// we got a valid result
		break
	}
	got := &topodatapb.SrvKeyspace{}
	if err := proto.Unmarshal(current.Contents, got); err != nil {
		t.Fatalf("cannot proto-unmarshal data: %v", err)
	}
	if !proto.Equal(got, srvKeyspace) {
		t.Fatalf("got bad data: %v expected: %v", got, srvKeyspace)
	}

	return changes, cancel
}

// checkWatch runs the tests on the Watch part of the Conn API.
// We use a SrvKeyspace object.
func checkWatch(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	conn, err := ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}

	// start watching something that doesn't exist -> error
	current, changes, _ := conn.Watch(ctx, "keyspaces/test_keyspace/SrvKeyspace")
	if !topo.IsErrType(current.Err, topo.NoNode) {
		t.Errorf("watch on missing node didn't return ErrNoNode: %v %v", current, changes)
	}

	// create some data
	srvKeyspace := &topodatapb.SrvKeyspace{
		ShardingColumnName: "user_id",
	}
	if err := ts.UpdateSrvKeyspace(ctx, LocalCellName, "test_keyspace", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(1): %v", err)
	}

	// start watching again, it should work
	changes, cancel := waitForInitialValue(t, conn, srvKeyspace)
	defer cancel()

	// change the data
	srvKeyspace.ShardingColumnName = "new_user_id"
	if err := ts.UpdateSrvKeyspace(ctx, LocalCellName, "test_keyspace", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(2): %v", err)
	}

	// Make sure we get the watch data, maybe not as first notice,
	// but eventually. The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			t.Fatalf("watch interrupted: %v", wd.Err)
		}
		got := &topodatapb.SrvKeyspace{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}

		if got.ShardingColumnName == "user_id" {
			// extra first value, still good
			continue
		}
		if got.ShardingColumnName == "new_user_id" {
			// watch worked, good
			break
		}
		t.Fatalf("got unknown SrvKeyspace: %v", got)
	}

	// remove the SrvKeyspace
	if err := ts.DeleteSrvKeyspace(ctx, LocalCellName, "test_keyspace"); err != nil {
		t.Fatalf("DeleteSrvKeyspace: %v", err)
	}

	// Make sure we get the ErrNoNode notification eventually.
	// The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if topo.IsErrType(wd.Err, topo.NoNode) {
			// good
			break
		}
		if wd.Err != nil {
			t.Fatalf("bad error returned for deletion: %v", wd.Err)
		}
		// we got something, better be the right value
		got := &topodatapb.SrvKeyspace{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.ShardingColumnName == "new_user_id" {
			// good value
			continue
		}
		t.Fatalf("got unknown SrvKeyspace waiting for deletion: %v", got)
	}

	// now the channel should be closed
	if wd, ok := <-changes; ok {
		t.Fatalf("got unexpected event after error: %v", wd)
	}
}

// checkWatchInterrupt tests we can interrupt a watch.
func checkWatchInterrupt(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	conn, err := ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}

	// create some data
	srvKeyspace := &topodatapb.SrvKeyspace{
		ShardingColumnName: "user_id",
	}
	if err := ts.UpdateSrvKeyspace(ctx, LocalCellName, "test_keyspace", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(1): %v", err)
	}

	// Start watching, it should work.
	changes, cancel := waitForInitialValue(t, conn, srvKeyspace)

	// Now cancel the watch.
	cancel()

	// Make sure we get the topo.ErrInterrupted notification eventually.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if topo.IsErrType(wd.Err, topo.Interrupted) {
			// good
			break
		}
		if wd.Err != nil {
			t.Fatalf("bad error returned for cancellation: %v", wd.Err)
		}
		// we got something, better be the right value
		got := &topodatapb.SrvKeyspace{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.ShardingColumnName == "user_id" {
			// good value
			continue
		}
		t.Fatalf("got unknown SrvKeyspace waiting for deletion: %v", got)
	}

	// Now the channel should be closed.
	if wd, ok := <-changes; ok {
		t.Fatalf("got unexpected event after error: %v", wd)
	}

	// And calling cancel() again should just work.
	cancel()
}
