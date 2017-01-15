// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/gitql/vitess/go/testfiles"
	"github.com/gitql/vitess/go/vt/topo"
	"github.com/gitql/vitess/go/vt/topo/test"

	topodatapb "github.com/gitql/vitess/go/vt/proto/topodata"
)

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T) (*exec.Cmd, string, string) {
	// Create a temporary directory.
	dataDir, err := ioutil.TempDir("", "etcd")
	if err != nil {
		t.Fatalf("cannot create tempdir: %v", err)
	}

	// Get our two ports to listen to.
	port := testfiles.GoVtEtcdtopoPort
	name := "vitess_unit_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", port)
	peerAddr := fmt.Sprintf("http://localhost:%v", port+1)
	initialCluster := fmt.Sprintf("%v=%v", name, peerAddr)

	cmd := exec.Command("etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)
	err = cmd.Start()
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}

	// Create a client to connect to the created etcd.
	c := newEtcdClient([]string{clientAddr})

	// Wait until we can list "/", or timeout.
	start := time.Now()
	for {
		if _, err := c.Get("/", false /* sort */, false /* recursive */); err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("Failed to start etcd daemon in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	return cmd, dataDir, clientAddr
}

func TestEtcdTopo(t *testing.T) {
	// Start a single etcd in the background, and configure ourselves
	// to read from it.
	cmd, dataDir, clientAddr := startEtcd(t)
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dataDir)
	}()
	globalAddrs = []string{clientAddr}

	// newServer wipes the existing etcd data, and creates a server
	// with the cells. Note all cells point to the same server.
	newServer := func(cells ...string) *Server {
		c := newEtcdClient([]string{clientAddr})
		if _, err := c.Delete("/vt", true /* recursive */); err != nil && convertError(err) != topo.ErrNoNode {
			t.Fatalf("DeleteDir(/vt) failed: %v", err)
		}

		// Multiple cells all point to the same server.
		for _, cell := range cells {
			if _, err := c.Set("/vt/cells/"+cell, clientAddr, 0); err != nil {
				t.Fatalf("Set(/vt/cells/%v) failed: %v", cell, err)
			}
		}

		return NewServer()
	}

	test.TopoServerTestSuite(t, func() topo.Impl {
		return newServer("test")
	})

	ts := newServer("test")
	testKeyspaceLock(t, ts)
	ts.Close()

	// Run explorer tests
	ts = newServer("cell1", "cell2", "cell3")
	testHandlePathRoot(t, ts)
	ts.Close()

	ts = newServer("cell1", "cell2", "cell3")
	testHandlePathKeyspace(t, ts)
	ts.Close()

	ts = newServer("cell1", "cell2", "cell3")
	testHandlePathShard(t, ts)
	ts.Close()

	ts = newServer("cell1", "cell2", "cell3")
	testHandlePathTablet(t, ts)
	ts.Close()
}

// test etcd-specific heartbeat (TTL).
func testKeyspaceLock(t *testing.T, ts *Server) {
	ctx := context.Background()

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	// Long TTL, unlock before timeout.
	*lockTTL = 1000 * time.Second
	actionPath, err := ts.LockKeyspaceForAction(ctx, "test_keyspace", "contents")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction failed: %v", err)
	}
	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", actionPath, "results"); err != nil {
		t.Fatalf("UnlockKeyspaceForAction failed: %v", err)
	}

	// Short TTL, make sure it doesn't expire.
	*lockTTL = 300 * time.Millisecond
	actionPath, err = ts.LockKeyspaceForAction(ctx, "test_keyspace", "contents")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction failed: %v", err)
	}
	time.Sleep(time.Second)
	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", actionPath, "results"); err != nil {
		t.Fatalf("UnlockKeyspaceForAction failed: %v", err)
	}

	// Long TTL, lose the lock.
	*lockTTL = 1000 * time.Second
	actionPath, err = ts.LockKeyspaceForAction(ctx, "test_keyspace", "contents")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction failed: %v", err)
	}
	if _, err := ts.getGlobal().Delete(path.Join(keyspaceDirPath("test_keyspace"), lockFilename), false); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", actionPath, "results"); err != topo.ErrNoNode {
		t.Fatalf("UnlockKeyspaceForAction = %v, want %v", err, topo.ErrNoNode)
	}
}
