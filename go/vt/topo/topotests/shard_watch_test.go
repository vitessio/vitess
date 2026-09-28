/*
Copyright 2019 The Vitess Authors

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

package topotests

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// waitForInitialShard waits for the initial Shard to appear.
func waitForInitialShard(t *testing.T, ts *topo.Server, keyspace, shard string) (current *topo.WatchShardData, changes <-chan *topo.WatchShardData, cancel context.CancelFunc) {
	ctx, cancel := context.WithCancel(t.Context())
	start := time.Now()
	var err error
	for {
		current, changes, err = ts.WatchShard(ctx, keyspace, shard)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// hasn't appeared yet
			require.LessOrEqualf(t, time.Since(start), 10*time.Second, "time out waiting for file to appear")
			time.Sleep(10 * time.Millisecond)
			continue
		case err == nil:
			return
		default:
			require.NoError(t, err)
		}
	}
}

func TestWatchShardNoNode(t *testing.T) {
	keyspace := "ks1"
	shard := "0"
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	// No Shard -> ErrNoNode
	_, _, err := ts.WatchShard(ctx, keyspace, shard)
	require.True(t, topo.IsErrType(err, topo.NoNode), "expected NoNode error, got: %v", err)
}

func TestWatchShard(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	shard := "0"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	// Create keyspace
	require.NoErrorf(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}), "CreateKeyspace %v failed", keyspace)

	// Create initial value
	if err := ts.CreateShard(ctx, keyspace, shard); err != nil {
		require.NoError(t, err)
	}

	// Starting the watch should now work, and return an empty
	// Shard.
	// Shards are always created with IsPrimaryServing true
	wanted := &topodatapb.Shard{IsPrimaryServing: true}
	current, changes, cancel := waitForInitialShard(t, ts, keyspace, shard)
	require.Truef(t, proto.Equal(current.Value, wanted), "got bad data: %v expected: %v", current.Value, wanted)

	// Update the value with good data, wait until we see it
	wanted.IsPrimaryServing = false
	if _, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.IsPrimaryServing = false
		return nil
	}); err != nil {
		require.NoError(t, err)
	}
	for {
		wd, ok := <-changes
		require.True(t, ok, "watch channel unexpectedly closed")
		require.NoErrorf(t, wd.Err, "watch channel unexpectedly got error")
		if proto.Equal(wd.Value, wanted) {
			break
		}
		if proto.Equal(wd.Value, &topodatapb.Shard{}) {
			t.Log("got duplicate empty value, skipping.")
		}
		require.Failf(t, "got bad data", "got bad data: %v expected: %v", wd.Value, wanted)
	}

	conn, err := ts.ConnForCell(ctx, "global")
	require.NoError(t, err)
	// Update the value with bad data, wait until error.
	if _, err := conn.Update(ctx, "/keyspaces/"+keyspace+"/shards/"+shard+"/Shard", []byte("BAD PROTO DATA"), nil); err != nil {
		require.NoError(t, err)
	}
	for {
		wd, ok := <-changes
		require.True(t, ok, "watch channel unexpectedly closed")
		if wd.Err != nil {
			if strings.Contains(wd.Err.Error(), "error unpacking Shard object") {
				break
			}
			require.Failf(t, "unexpected error", "watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		require.Truef(t, proto.Equal(wd.Value, wanted), "got bad data: %v expected: %v", wd.Value, wanted)
		t.Log("got duplicate right value, skipping.")
	}

	// Cancel should still work here, although it does nothing.
	cancel()

	// Bad data in topo, setting the watch should now fail.
	_, _, err = ts.WatchShard(ctx, keyspace, shard)
	require.ErrorContains(t, err, "error unpacking initial Shard object")

	data, err := wanted.MarshalVT()
	require.NoError(t, err)
	// Update content, wait until Watch works again
	if _, err := conn.Update(ctx, "/keyspaces/"+keyspace+"/shards/"+shard+"/Shard", data, nil); err != nil {
		require.NoError(t, err)
	}
	start := time.Now()
	for {
		current, changes, err = ts.WatchShard(ctx, keyspace, shard)
		if err != nil {
			if strings.Contains(err.Error(), "error unpacking initial Shard object") {
				// hasn't changed yet
				require.LessOrEqualf(t, time.Since(start), 10*time.Second, "time out waiting for file to appear")
				time.Sleep(10 * time.Millisecond)
				continue
			}
			require.NoError(t, err)
		}
		require.Truef(t, proto.Equal(current.Value, wanted), "got bad data: %v expected: %v", current.Value, wanted)
		break
	}

	// Delete node, wait for error (skip any duplicate).
	if err := ts.DeleteShard(ctx, keyspace, shard); err != nil {
		require.NoError(t, err)
	}
	for {
		wd, ok := <-changes
		require.True(t, ok, "watch channel unexpectedly closed")
		if topo.IsErrType(wd.Err, topo.NoNode) {
			break
		}
		if wd.Err != nil {
			require.Failf(t, "unexpected error", "watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		require.Truef(t, proto.Equal(wd.Value, wanted), "got bad data: %v expected: %v", wd.Value, wanted)
		t.Log("got duplicate right value, skipping.")
	}
}

func TestWatchShardCancel(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	shard := "0"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	// No Shard -> ErrNoNode
	_, _, err := ts.WatchShard(ctx, keyspace, shard)
	require.Truef(t, topo.IsErrType(err, topo.NoNode), "expected topo.NoNode error, got: %v", err)

	// Create keyspace
	require.NoErrorf(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}), "CreateKeyspace %v failed", keyspace)

	// Create initial value
	if err := ts.CreateShard(ctx, keyspace, shard); err != nil {
		require.NoError(t, err)
	}
	wanted := &topodatapb.Shard{
		IsPrimaryServing: false,
	}
	if _, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.IsPrimaryServing = false
		return nil
	}); err != nil {
		require.NoError(t, err)
	}

	// Starting the watch should now work.
	current, changes, cancel := waitForInitialShard(t, ts, keyspace, shard)
	require.Truef(t, proto.Equal(current.Value, wanted), "got bad data: %v expected: %v", current.Value, wanted)

	// Cancel watch, wait for error.
	cancel()
	for {
		wd, ok := <-changes
		require.True(t, ok, "watch channel unexpectedly closed")
		if topo.IsErrType(wd.Err, topo.Interrupted) {
			break
		}
		if wd.Err != nil {
			require.Failf(t, "unexpected error", "watch channel unexpectedly got unknown error: %v", wd.Err)
		}
		require.Truef(t, proto.Equal(wd.Value, wanted), "got bad data: %v expected: %v", wd.Value, wanted)
		t.Log("got duplicate right value, skipping.")
	}

	// Cancel should still work here, although it does nothing.
	cancel()
}
