package zktopo

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/fakezk"
	"launchpad.net/gozk/zookeeper"
)

type fakeServer struct {
	topo.Server
	localCells []string
}

func newFakeServer(t *testing.T, zconn zk.Conn, cells []string) topo.Server {
	cells = append(cells, "global")
	for _, cell := range cells {
		if _, err := zk.CreateRecursive(zconn, fmt.Sprintf("/zk/%v/vt", cell), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
			t.Fatalf("cannot init ZooKeeper: %v", err)
		}
	}
	return fakeServer{Server: NewServer(zconn), localCells: cells[:len(cells)-1]}

}

func (s fakeServer) GetKnownCells() ([]string, error) {
	return s.localCells, nil
}

func prepareTS(t *testing.T) topo.Server {
	return newFakeServer(t, fakezk.NewConn(), []string{"test"})
}

func TestTopoServer(t *testing.T) {
	test.CheckAll(t, prepareTS)
}
