// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package helpers

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/fakezk"
	"launchpad.net/gozk/zookeeper"
)

type fakeServer struct {
	topo.Impl
	localCells []string
}

func (s fakeServer) GetKnownCells(ctx context.Context) ([]string, error) {
	return s.localCells, nil
}

func newFakeTeeServer(t *testing.T) topo.Impl {
	cells := []string{"test", "global"} // global has to be last

	zconn1 := fakezk.NewConn()
	zconn2 := fakezk.NewConn()

	for _, cell := range cells {
		if _, err := zk.CreateRecursive(zconn1, fmt.Sprintf("/zk/%v/vt", cell), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
			t.Fatalf("cannot init ZooKeeper: %v", err)
		}
		if _, err := zk.CreateRecursive(zconn2, fmt.Sprintf("/zk/%v/vt", cell), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
			t.Fatalf("cannot init ZooKeeper: %v", err)
		}
	}
	s1 := fakeServer{Impl: zktopo.NewServer(zconn1), localCells: cells[:len(cells)-1]}
	s2 := fakeServer{Impl: zktopo.NewServer(zconn2), localCells: cells[:len(cells)-1]}

	return NewTee(s1, s2, false)
}

func TestKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := newFakeTeeServer(t)
	test.CheckKeyspace(ctx, t, ts)
}

func TestShard(t *testing.T) {
	ctx := context.Background()
	ts := newFakeTeeServer(t)
	test.CheckShard(ctx, t, ts)
}

func TestTablet(t *testing.T) {
	ctx := context.Background()
	ts := newFakeTeeServer(t)
	test.CheckTablet(ctx, t, ts)
}

func TestServingGraph(t *testing.T) {
	ctx := context.Background()
	ts := newFakeTeeServer(t)
	test.CheckServingGraph(ctx, t, ts)
}

func TestWatchSrvKeyspace(t *testing.T) {
	zktopo.WatchSleepDuration = 2 * time.Millisecond
	ts := newFakeTeeServer(t)
	test.CheckWatchSrvKeyspace(context.Background(), t, ts)
}

func TestShardReplication(t *testing.T) {
	ctx := context.Background()
	ts := newFakeTeeServer(t)
	test.CheckShardReplication(ctx, t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ctx := context.Background()
	ts := newFakeTeeServer(t)
	test.CheckKeyspaceLock(ctx, t, ts)
}

func TestShardLock(t *testing.T) {
	ctx := context.Background()
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newFakeTeeServer(t)
	test.CheckShardLock(ctx, t, ts)
}
