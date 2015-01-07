// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package helpers

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/fakezk"
	"launchpad.net/gozk/zookeeper"
)

type fakeServer struct {
	topo.Server
	localCells []string
}

func (s fakeServer) GetKnownCells() ([]string, error) {
	return s.localCells, nil
}

func newFakeTeeServer(t *testing.T) topo.Server {
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
	s1 := fakeServer{Server: zktopo.NewServer(zconn1), localCells: cells[:len(cells)-1]}
	s2 := fakeServer{Server: zktopo.NewServer(zconn2), localCells: cells[:len(cells)-1]}

	return NewTee(s1, s2, false)
}

func TestKeyspace(t *testing.T) {
	ts := newFakeTeeServer(t)
	test.CheckKeyspace(t, ts)
}

func TestShard(t *testing.T) {
	ts := newFakeTeeServer(t)
	test.CheckShard(context.Background(), t, ts)
}

func TestTablet(t *testing.T) {
	ts := newFakeTeeServer(t)
	test.CheckTablet(context.Background(), t, ts)
}

func TestServingGraph(t *testing.T) {
	ts := newFakeTeeServer(t)
	test.CheckServingGraph(context.Background(), t, ts)
}

func TestShardReplication(t *testing.T) {
	ts := newFakeTeeServer(t)
	test.CheckShardReplication(t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ts := newFakeTeeServer(t)
	test.CheckKeyspaceLock(t, ts)
}

func TestShardLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newFakeTeeServer(t)
	test.CheckShardLock(t, ts)
}

func TestSrvShardLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newFakeTeeServer(t)
	test.CheckSrvShardLock(t, ts)
}
