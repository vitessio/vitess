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

type TestServer struct {
	topo.Server
	localCells []string

	HookLockSrvShardForAction func()
}

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

func (s *TestServer) GetKnownCells() ([]string, error) {
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

// TODO(sougou): Remove these two functions after they're
// migrated into topo.Server.
// SaveVSchema has to be redefined here.
// Otherwise the test type assertion fails.
func (s *TestServer) SaveVSchema(vschema string) error {
	return s.Server.(topo.Schemafier).SaveVSchema(vschema)
}

// GetVSchema has to be redefined here.
// Otherwise the test type assertion fails.
func (s *TestServer) GetVSchema() (string, error) {
	return s.Server.(topo.Schemafier).GetVSchema()
}
