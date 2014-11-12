// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools_test

import (
	"strings"
	"testing"
	"time"

	"code.google.com/p/go.net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"github.com/youtube/vitess/go/vt/zktopo"

	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	. "github.com/youtube/vitess/go/vt/topotools"
)

func TestRebuildShardRace(t *testing.T) {
	ctx := context.Background()
	cells := []string{"test_cell"}
	logger := logutil.NewMemoryLogger()
	timeout := 10 * time.Second
	interrupted := make(chan struct{})

	// Set up topology.
	ts := zktopo.NewTestServer(t, cells)
	f := faketopo.New(t, logger, ts, cells)
	defer f.TearDown()

	keyspace := faketopo.TestKeyspace
	shard := faketopo.TestShard
	master := f.AddTablet(1, "test_cell", topo.TYPE_MASTER, nil)
	f.AddTablet(2, "test_cell", topo.TYPE_REPLICA, master)

	// Do an initial rebuild.
	if _, err := RebuildShard(ctx, logger, f.Topo, keyspace, shard, cells, timeout, interrupted); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	// Check initial state.
	ep, err := ts.GetEndPoints(cells[0], keyspace, shard, topo.TYPE_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints: %v", err)
	}
	if got, want := len(ep.Entries), 1; got != want {
		t.Fatalf("len(Entries) = %v, want %v", got, want)
	}
	ep, err = ts.GetEndPoints(cells[0], keyspace, shard, topo.TYPE_REPLICA)
	if err != nil {
		t.Fatalf("GetEndPoints: %v", err)
	}
	if got, want := len(ep.Entries), 1; got != want {
		t.Fatalf("len(Entries) = %v, want %v", got, want)
	}

	// Install a hook that hands out locks out of order to simulate a race.
	trigger := make(chan struct{})
	stalled := make(chan struct{})
	done := make(chan struct{})
	wait := make(chan bool, 2)
	wait <- true  // first guy waits for trigger
	wait <- false // second guy doesn't wait
	ts.HookLockSrvShardForAction = func() {
		if <-wait {
			close(stalled)
			<-trigger
		}
	}

	// Make a change and start a rebuild that will stall when it tries to get
	// the SrvShard lock.
	masterInfo := f.GetTablet(1)
	masterInfo.Type = topo.TYPE_SPARE
	if err := topo.UpdateTablet(ctx, ts, masterInfo); err != nil {
		t.Fatalf("UpdateTablet: %v", err)
	}
	go func() {
		if _, err := RebuildShard(ctx, logger, f.Topo, keyspace, shard, cells, timeout, interrupted); err != nil {
			t.Fatalf("RebuildShard: %v", err)
		}
		close(done)
	}()

	// Wait for first rebuild to stall.
	<-stalled

	// While the first rebuild is stalled, make another change and start a rebuild
	// that doesn't stall.
	replicaInfo := f.GetTablet(2)
	replicaInfo.Type = topo.TYPE_SPARE
	if err := topo.UpdateTablet(ctx, ts, replicaInfo); err != nil {
		t.Fatalf("UpdateTablet: %v", err)
	}
	if _, err := RebuildShard(ctx, logger, f.Topo, keyspace, shard, cells, timeout, interrupted); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	// Now that the second rebuild is done, un-stall the first rebuild and wait
	// for it to finish.
	close(trigger)
	<-done

	// Check that the rebuild picked up both changes.
	if _, err := ts.GetEndPoints(cells[0], keyspace, shard, topo.TYPE_MASTER); err == nil || !strings.Contains(err.Error(), "node doesn't exist") {
		t.Errorf("first change wasn't picked up by second rebuild")
	}
	if _, err := ts.GetEndPoints(cells[0], keyspace, shard, topo.TYPE_REPLICA); err == nil || !strings.Contains(err.Error(), "node doesn't exist") {
		t.Errorf("second change was overwritten by first rebuild finishing late")
	}
}
