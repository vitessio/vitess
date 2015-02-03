package zktopo

import (
	"path"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

func TestKeyspace(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspace(t, ts)
}

func TestShard(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShard(context.Background(), t, ts)
}

func TestTablet(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckTablet(context.Background(), t, ts)
}

func TestShardReplication(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardReplication(t, ts)
}

func TestServingGraph(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckServingGraph(context.Background(), t, ts)
}

func TestWatchEndPoints(t *testing.T) {
	WatchSleepDuration = 2 * time.Millisecond
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckWatchEndPoints(context.Background(), t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspaceLock(t, ts)
}

func TestShardLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardLock(t, ts)
}

func TestSrvShardLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckSrvShardLock(t, ts)
}

func TestVSchema(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckVSchema(t, ts)
}

// TestPurgeActions is a ZK specific unit test
func TestPurgeActions(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()

	if err := ts.CreateKeyspace("test_keyspace", &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	actionPath := path.Join(globalKeyspacesPath, "test_keyspace", "action")
	zkts := ts.Server.(*Server)

	if _, err := zk.CreateRecursive(zkts.zconn, actionPath+"/topurge", "purgeme", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("CreateRecursive(topurge): %v", err)
	}
	if _, err := zk.CreateRecursive(zkts.zconn, actionPath+"/tokeep", "keepme", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("CreateRecursive(tokeep): %v", err)
	}

	if err := zkts.PurgeActions(actionPath, func(data string) bool {
		return data == "purgeme"
	}); err != nil {
		t.Fatalf("PurgeActions(tokeep): %v", err)
	}

	actions, _, err := zkts.zconn.Children(actionPath)
	if err != nil || len(actions) != 1 || actions[0] != "tokeep" {
		t.Errorf("PurgeActions kept the wrong things: %v %v", err, actions)
	}
}

// TestPruneActionLogs is a ZK specific unit test
func TestPruneActionLogs(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()

	if err := ts.CreateKeyspace("test_keyspace", &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	actionLogPath := path.Join(globalKeyspacesPath, "test_keyspace", "actionlog")
	zkts := ts.Server.(*Server)

	if _, err := zk.CreateRecursive(zkts.zconn, actionLogPath+"/0", "first", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("CreateRecursive(stale): %v", err)
	}
	if _, err := zk.CreateRecursive(zkts.zconn, actionLogPath+"/1", "second", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("CreateRecursive(fresh): %v", err)
	}

	if count, err := zkts.PruneActionLogs(actionLogPath, 1); err != nil || count != 1 {
		t.Fatalf("PruneActionLogs: %v %v", err, count)
	}

	actionLogs, _, err := zkts.zconn.Children(actionLogPath)
	if err != nil || len(actionLogs) != 1 || actionLogs[0] != "1" {
		t.Errorf("PruneActionLogs kept the wrong things: %v %v", err, actionLogs)
	}
}
