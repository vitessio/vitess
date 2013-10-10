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

func newFakeServer(t *testing.T) topo.Server {
	zconn := fakezk.NewConn()
	cells := []string{"test", "global"} // global has to be last

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

func TestKeyspace(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckKeyspace(t, ts)
}

func TestShard(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckShard(t, ts)
}

func TestTablet(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckTablet(t, ts)
}

func TestShardReplication(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckShardReplication(t, ts)
}

func TestServingGraph(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckServingGraph(t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckKeyspaceLock(t, ts)
}

func TestShardLock(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckShardLock(t, ts)
}

func TestPid(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckPid(t, ts)
}

func TestActions(t *testing.T) {
	ts := newFakeServer(t)
	test.CheckActions(t, ts)
}
