// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package helpers

import (
	"fmt"
	"testing"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/fakezk"
)

type fakeServer struct {
	topo.Impl
	localCells []string
}

func (s fakeServer) GetKnownCells(ctx context.Context) ([]string, error) {
	return s.localCells, nil
}

func newFakeTeeServer(t *testing.T) topo.Impl {
	cells := []string{"test", topo.GlobalCell} // global has to be last

	zconn1 := fakezk.NewConn()
	zconn2 := fakezk.NewConn()

	for _, cell := range cells {
		if _, err := zk.CreateRecursive(zconn1, fmt.Sprintf("/zk/%v/vt", cell), nil, 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
			t.Fatalf("cannot init ZooKeeper: %v", err)
		}
		if _, err := zk.CreateRecursive(zconn2, fmt.Sprintf("/zk/%v/vt", cell), nil, 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
			t.Fatalf("cannot init ZooKeeper: %v", err)
		}
	}
	s1 := fakeServer{Impl: zktopo.NewServer(zconn1), localCells: cells[:len(cells)-1]}
	s2 := fakeServer{Impl: zktopo.NewServer(zconn2), localCells: cells[:len(cells)-1]}

	return NewTee(s1, s2, false)
}

func TestTeeTopo(t *testing.T) {
	test.TopoServerTestSuite(t, func() topo.Impl {
		return newFakeTeeServer(t)
	})
}
