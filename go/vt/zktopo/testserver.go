package zktopo

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/fakezk"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"
)

// TestServer is a proxy for a real implementation of topo.Server that
// provides hooks for testing.
type TestServer struct {
	topo.Server
	localCells []string

	HookLockSrvShardForAction func()
}

// NewTestServer returns a new TestServer (with the required paths created)
func NewTestServer(t *testing.T, cells []string) *TestServer {
	zconn := fakezk.NewConn()

	// create the toplevel zk paths
	if _, err := zk.CreateRecursive(zconn, "/zk/global/vt", "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("cannot init ZooKeeper: %v", err)
	}
	for _, cell := range cells {
		if _, err := zk.CreateRecursive(zconn, fmt.Sprintf("/zk/%v/vt", cell), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
			t.Fatalf("cannot init ZooKeeper: %v", err)
		}
	}
	return &TestServer{Server: NewServer(zconn), localCells: cells}
}

// GetKnownCells is part of topo.Server interface
func (s *TestServer) GetKnownCells(ctx context.Context) ([]string, error) {
	return s.localCells, nil
}

// LockSrvShardForAction should override the function defined by the underlying
// topo.Server.
func (s *TestServer) LockSrvShardForAction(ctx context.Context, cell, keyspace, shard, contents string) (string, error) {
	if s.HookLockSrvShardForAction != nil {
		s.HookLockSrvShardForAction()
	}
	return s.Server.LockSrvShardForAction(ctx, cell, keyspace, shard, contents)
}

// SaveVSchema has to be redefined here.
// Otherwise the test type assertion fails.
// TODO(sougou): Remove these two functions after they're
// migrated into topo.Server.
func (s *TestServer) SaveVSchema(ctx context.Context, vschema string) error {
	return s.Server.(topo.Schemafier).SaveVSchema(ctx, vschema)
}

// GetVSchema has to be redefined here.
// Otherwise the test type assertion fails.
func (s *TestServer) GetVSchema(ctx context.Context) (string, error) {
	return s.Server.(topo.Schemafier).GetVSchema(ctx)
}
