package zktopo

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/zkctl"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Run the topology test suite on zktopo.
func TestZkTopo(t *testing.T) {
	// Start a real single ZK daemon, and close it after all tests are done.
	zkd, serverAddr := zkctl.StartLocalZk(testfiles.GoVtZktopoZkID, testfiles.GoVtZktopoPort)
	defer zkd.Teardown()

	// Create a ZK_CLIENT_CONFIG file to use.
	fd, err := ioutil.TempFile("", "zkconf")
	if err != nil {
		t.Fatalf("ioutil.TempFile failed: %v", err)
	}
	configPath := fd.Name()
	fd.Close()
	defer os.Remove(configPath)
	if err := os.Setenv("ZK_CLIENT_CONFIG", configPath); err != nil {
		t.Fatalf("setenv failed: %v", err)
	}

	// This function will wipe all data before creating new directories.
	createServer := func(cells ...string) *Server {
		// Create the config map, all pointing to our server.
		configMap := map[string]string{"global": serverAddr}
		for _, cell := range cells {
			configMap[cell] = serverAddr
		}
		fd, err := os.OpenFile(configPath, os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}
		err = json.NewEncoder(fd).Encode(configMap)
		if err != nil {
			t.Fatalf("json.Encode failed: %v", err)
		}
		fd.Close()

		conncache := zk.NewConnCache()
		defer conncache.Close()
		zconn, err := conncache.ConnForPath("/zk/global/vt")
		if err != nil {
			t.Fatalf("ConnForPath failed: %v", err)
		}

		// Wipe the old directories, create new ones.
		if err := zk.DeleteRecursive(zconn, "/zk", -1); err != nil && err != zookeeper.ErrNoNode {
			t.Fatalf("zk.DeleteRecursive failed: %v", err)
		}
		if _, err := zk.CreateRecursive(zconn, "/zk/global/vt", nil, 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
			t.Fatalf("CreateRecursive(/zk/global/vt) failed: %v", err)
		}
		for _, cell := range cells {
			p := fmt.Sprintf("/zk/%v/vt", cell)
			if _, err := zk.CreateRecursive(zconn, p, nil, 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
				t.Fatalf("CreateRecursive(%v) failed: %v", p, err)
			}
		}

		return newServer()
	}

	test.TopoServerTestSuite(t, func() topo.Impl {
		return createServer("test")
	})

	impl := createServer("test")
	testPurgeActions(t, impl)
	impl.Close()

	impl = createServer("test")
	testPruneActionLogs(t, impl)
	impl.Close()
}

// testPurgeActions is a ZK specific unit test
func testPurgeActions(t *testing.T, impl *Server) {
	t.Log("=== testPurgeActions")
	ctx := context.Background()
	ts := topo.Server{Impl: impl}

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	actionPath := path.Join(GlobalKeyspacesPath, "test_keyspace", "action")

	if _, err := zk.CreateRecursive(impl.GetZConn(), actionPath+"/topurge", []byte("purgeme"), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("CreateRecursive(topurge): %v", err)
	}
	if _, err := zk.CreateRecursive(impl.GetZConn(), actionPath+"/tokeep", []byte("keepme"), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("CreateRecursive(tokeep): %v", err)
	}

	if err := impl.PurgeActions(actionPath, func(data []byte) bool {
		return string(data) == "purgeme"
	}); err != nil {
		t.Fatalf("PurgeActions(tokeep): %v", err)
	}

	actions, _, err := impl.GetZConn().Children(actionPath)
	if err != nil || len(actions) != 1 || actions[0] != "tokeep" {
		t.Errorf("PurgeActions kept the wrong things: %v %v", err, actions)
	}
}

// testPruneActionLogs is a ZK specific unit test
func testPruneActionLogs(t *testing.T, impl *Server) {
	t.Log("=== testPruneActionLogs")
	ctx := context.Background()
	ts := topo.Server{Impl: impl}

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	actionLogPath := path.Join(GlobalKeyspacesPath, "test_keyspace", "actionlog")

	if _, err := zk.CreateRecursive(impl.GetZConn(), actionLogPath+"/0", []byte("first"), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("CreateRecursive(stale): %v", err)
	}
	if _, err := zk.CreateRecursive(impl.GetZConn(), actionLogPath+"/1", []byte("second"), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		t.Fatalf("CreateRecursive(fresh): %v", err)
	}

	if count, err := impl.PruneActionLogs(actionLogPath, 1); err != nil || count != 1 {
		t.Fatalf("PruneActionLogs: %v %v", err, count)
	}

	actionLogs, _, err := impl.GetZConn().Children(actionLogPath)
	if err != nil || len(actionLogs) != 1 || actionLogs[0] != "1" {
		t.Errorf("PruneActionLogs kept the wrong things: %v %v", err, actionLogs)
	}
}
