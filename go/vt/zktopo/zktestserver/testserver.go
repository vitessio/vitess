package zktestserver

import (
	"fmt"
	"testing"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/fakezk"
)

// TestServer is a proxy for a real implementation of topo.Server that
// provides hooks for testing.
type TestServer struct {
	topo.Impl
	localCells []string
}

// newTestServer returns a new TestServer (with the required paths created)
func newTestServer(t *testing.T, cells []string) topo.Impl {
	zconn := fakezk.NewConn()

	// create the toplevel zk paths
	if _, err := zk.CreateRecursive(zconn, "/zk/global/vt", "", 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("cannot init ZooKeeper: %v", err)
	}
	for _, cell := range cells {
		if _, err := zk.CreateRecursive(zconn, fmt.Sprintf("/zk/%v/vt", cell), "", 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
			t.Fatalf("cannot init ZooKeeper: %v", err)
		}
	}
	return &TestServer{Impl: zktopo.NewServer(zconn), localCells: cells}
}

// New returns a new TestServer (with the required paths created)
func New(t *testing.T, cells []string) topo.Server {
	return topo.Server{Impl: newTestServer(t, cells)}
}

// GetKnownCells is part of topo.Server interface
func (s *TestServer) GetKnownCells(ctx context.Context) ([]string, error) {
	return s.localCells, nil
}
