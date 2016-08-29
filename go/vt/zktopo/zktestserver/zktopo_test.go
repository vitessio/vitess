package zktestserver

import (
	"path"
	"testing"
	"time"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestZkTopo(t *testing.T) {
	zktopo.WatchSleepDuration = 2 * time.Millisecond
	test.TopoServerTestSuite(t, func() topo.Impl {
		return newTestServer(t, []string{"test"})
	})
}

// TestPurgeActions is a ZK specific unit test
func TestPurgeActions(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	actionPath := path.Join(zktopo.GlobalKeyspacesPath, "test_keyspace", "action")
	zkts := ts.(*TestServer).Impl.(*zktopo.Server)

	if _, err := zk.CreateRecursive(zkts.GetZConn(), actionPath+"/topurge", []byte("purgeme"), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("CreateRecursive(topurge): %v", err)
	}
	if _, err := zk.CreateRecursive(zkts.GetZConn(), actionPath+"/tokeep", []byte("keepme"), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("CreateRecursive(tokeep): %v", err)
	}

	if err := zkts.PurgeActions(actionPath, func(data []byte) bool {
		return string(data) == "purgeme"
	}); err != nil {
		t.Fatalf("PurgeActions(tokeep): %v", err)
	}

	actions, _, err := zkts.GetZConn().Children(actionPath)
	if err != nil || len(actions) != 1 || actions[0] != "tokeep" {
		t.Errorf("PurgeActions kept the wrong things: %v %v", err, actions)
	}
}

// TestPruneActionLogs is a ZK specific unit test
func TestPruneActionLogs(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	actionLogPath := path.Join(zktopo.GlobalKeyspacesPath, "test_keyspace", "actionlog")
	zkts := ts.(*TestServer).Impl.(*zktopo.Server)

	if _, err := zk.CreateRecursive(zkts.GetZConn(), actionLogPath+"/0", []byte("first"), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("CreateRecursive(stale): %v", err)
	}
	if _, err := zk.CreateRecursive(zkts.GetZConn(), actionLogPath+"/1", []byte("second"), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("CreateRecursive(fresh): %v", err)
	}

	if count, err := zkts.PruneActionLogs(actionLogPath, 1); err != nil || count != 1 {
		t.Fatalf("PruneActionLogs: %v %v", err, count)
	}

	actionLogs, _, err := zkts.GetZConn().Children(actionLogPath)
	if err != nil || len(actionLogs) != 1 || actionLogs[0] != "1" {
		t.Errorf("PruneActionLogs kept the wrong things: %v %v", err, actionLogs)
	}
}
