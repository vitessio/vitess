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
	"context"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/key"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// waitForInitialValue waits for the initial value of
// keyspaces/test_keyspace/SrvKeyspace to appear, and match the
// provided srvKeyspace.
func waitForInitialValue(t *testing.T, conn topo.Conn, srvKeyspace *topodatapb.SrvKeyspace) (changes <-chan *topo.WatchData, cancel context.CancelFunc) {
	var current *topo.WatchData
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	var err error
	for {
		current, changes, err = conn.Watch(ctx, "keyspaces/test_keyspace/SrvKeyspace")
		if topo.IsErrType(err, topo.NoNode) {
			// hasn't appeared yet
			if time.Since(start) > 10*time.Second {
				cancel()
				t.Fatalf("time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			cancel()
			t.Fatalf("watch failed: %v", err)
		}
		// we got a valid result
		break
	}
	got := &topodatapb.SrvKeyspace{}
	if err := got.UnmarshalVT(current.Contents); err != nil {
		cancel()
		t.Fatalf("cannot proto-unmarshal data: %v", err)
	}
	if !proto.Equal(got, srvKeyspace) {
		cancel()
		t.Fatalf("got bad data: %v expected: %v", got, srvKeyspace)
	}

	return changes, cancel
}

// waitForInitialValue waits for the initial value of
// keyspaces/test_keyspace/SrvKeyspace to appear, and match the
// provided srvKeyspace.
func waitForInitialValueRecursive(t *testing.T, conn topo.Conn, srvKeyspace *topodatapb.SrvKeyspace) (changes <-chan *topo.WatchDataRecursive, cancel context.CancelFunc, err error) {
	var current []*topo.WatchDataRecursive
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	for {
		current, changes, err = conn.WatchRecursive(ctx, "keyspaces/test_keyspace")
		if topo.IsErrType(err, topo.NoNode) {
			// hasn't appeared yet
			if time.Since(start) > 10*time.Second {
				cancel()
				t.Fatalf("time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if topo.IsErrType(err, topo.NoImplementation) {
			// If this is not supported, skip the test
			cancel()
			return nil, nil, err
		}
		if err != nil {
			cancel()
			t.Fatalf("watch failed: %v", err)
		}
		// we got a valid result
		break
	}
	got := &topodatapb.SrvKeyspace{}
	if err := got.UnmarshalVT(current[0].Contents); err != nil {
		cancel()
		t.Fatalf("cannot proto-unmarshal data: %v", err)
	}
	if !proto.Equal(got, srvKeyspace) {
		cancel()
		t.Fatalf("got bad data: %v expected: %v", got, srvKeyspace)
	}

	return changes, cancel, nil
}

// checkWatch runs the tests on the Watch part of the Conn API.
// We use a SrvKeyspace object.
func checkWatch(t *testing.T, ctx context.Context, ts *topo.Server) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}

	// start watching something that doesn't exist -> error
	current, changes, err := conn.Watch(ctx, "keyspaces/test_keyspace/SrvKeyspace")
	if !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("watch on missing node didn't return ErrNoNode: %v %v", current, changes)
	}

	// create some data
	keyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(keyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(keyRange))
	}

	srvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "name",
						KeyRange: keyRange[0],
					},
				},
			},
		},
	}
	if err := ts.UpdateSrvKeyspace(ctx, LocalCellName, "test_keyspace", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(1): %v", err)
	}

	// start watching again, it should work
	changes, secondCancel := waitForInitialValue(t, conn, srvKeyspace)
	defer secondCancel()

	// change the data
	srvKeyspace.Partitions[0].ShardReferences[0].Name = "new_name"
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
		if err := got.UnmarshalVT(wd.Contents); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}

		if got.Partitions[0].ShardReferences[0].Name == "name" {
			// extra first value, still good
			continue
		}
		if got.Partitions[0].ShardReferences[0].Name == "new_name" {
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
		if err := got.UnmarshalVT(wd.Contents); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.Partitions[0].ShardReferences[0].Name == "new_name" {
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
func checkWatchInterrupt(t *testing.T, ctx context.Context, ts *topo.Server) {
	conn, err := ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}

	// create some data
	keyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(keyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(keyRange))
	}

	srvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "name",
						KeyRange: keyRange[0],
					},
				},
			},
		},
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
		if err := got.UnmarshalVT(wd.Contents); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.Partitions[0].ShardReferences[0].Name == "name" {
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

// checkWatchRecursive tests we can setup a recursive watch
func checkWatchRecursive(t *testing.T, ctx context.Context, ts *topo.Server) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}

	// create some data
	keyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(keyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(keyRange))
	}

	srvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name:     "name",
						KeyRange: keyRange[0],
					},
				},
			},
		},
	}
	if err := ts.UpdateSrvKeyspace(ctx, LocalCellName, "test_keyspace", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(1): %v", err)
	}

	// start watching again, it should work
	changes, secondCancel, err := waitForInitialValueRecursive(t, conn, srvKeyspace)
	if topo.IsErrType(err, topo.NoImplementation) {
		// Skip the rest if there's no implementation
		t.Logf("%T does not support WatchRecursive()", conn)
		return
	}
	defer secondCancel()

	// change the data
	srvKeyspace.Partitions[0].ShardReferences[0].Name = "new_name"
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
		if err := got.UnmarshalVT(wd.Contents); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}

		if got.Partitions[0].ShardReferences[0].Name == "name" {
			// extra first value, still good
			continue
		}
		if got.Partitions[0].ShardReferences[0].Name == "new_name" {
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
		if err := got.UnmarshalVT(wd.Contents); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.Partitions[0].ShardReferences[0].Name == "new_name" {
			// good value
			continue
		}
		t.Fatalf("got unknown SrvKeyspace waiting for deletion: %v", got)
	}

	// We now have to stop watching. This doesn't automatically
	// happen for recursive watches on a single file since others
	// can still be seen.
	secondCancel()

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
		if err := got.UnmarshalVT(wd.Contents); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.Partitions[0].ShardReferences[0].Name == "name" {
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
	secondCancel()
}
