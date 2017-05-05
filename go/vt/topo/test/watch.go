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

package test

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// waitForInitialValue waits for the initial value of
// keyspaces/test_keyspace/SrvKeyspace to appear, and match the
// provided srvKeyspace.
func waitForInitialValue(t *testing.T, ts topo.Impl, cell string, srvKeyspace *topodatapb.SrvKeyspace) (changes <-chan *topo.WatchData, cancel func()) {
	var current *topo.WatchData
	ctx := context.Background()
	start := time.Now()
	for {
		current, changes, cancel = ts.Watch(ctx, cell, "keyspaces/test_keyspace/SrvKeyspace")
		if current.Err == topo.ErrNoNode {
			// hasn't appeared yet
			if time.Now().Sub(start) > 10*time.Second {
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

// checkWatch runs the tests on the Watch part of the Backend API.
// We can't just use the full API yet, so use SrvKeyspace for now.
func checkWatch(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	cell := getLocalCell(ctx, t, ts)

	// start watching something that doesn't exist -> error
	current, changes, cancel := ts.Watch(ctx, cell, "keyspaces/test_keyspace/SrvKeyspace")
	if current.Err != topo.ErrNoNode {
		t.Errorf("watch on missing node didn't return ErrNoNode: %v %v", current, changes)
	}

	// create some data
	srvKeyspace := &topodatapb.SrvKeyspace{
		ShardingColumnName: "user_id",
	}
	if err := ts.UpdateSrvKeyspace(ctx, cell, "test_keyspace", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(1): %v", err)
	}

	// start watching again, it should work
	changes, cancel = waitForInitialValue(t, ts, cell, srvKeyspace)
	defer cancel()

	// change the data
	srvKeyspace.ShardingColumnName = "new_user_id"
	if err := ts.UpdateSrvKeyspace(ctx, cell, "test_keyspace", srvKeyspace); err != nil {
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
	if err := ts.DeleteSrvKeyspace(ctx, cell, "test_keyspace"); err != nil {
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
		if wd.Err == topo.ErrNoNode {
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
func checkWatchInterrupt(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	cell := getLocalCell(ctx, t, ts)

	// create some data
	srvKeyspace := &topodatapb.SrvKeyspace{
		ShardingColumnName: "user_id",
	}
	if err := ts.UpdateSrvKeyspace(ctx, cell, "test_keyspace", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(1): %v", err)
	}

	// Start watching, it should work.
	changes, cancel := waitForInitialValue(t, ts, cell, srvKeyspace)

	// Now cancel the watch.
	cancel()

	// Make sure we get the topo.ErrInterrupted notification eventually.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err == topo.ErrInterrupted {
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
