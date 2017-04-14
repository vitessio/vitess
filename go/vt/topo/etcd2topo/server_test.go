// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcd2topo

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T) (*exec.Cmd, string, string) {
	// Create a temporary directory.
	dataDir, err := ioutil.TempDir("", "etcd")
	if err != nil {
		t.Fatalf("cannot create tempdir: %v", err)
	}

	// Get our two ports to listen to.
	port := testfiles.GoVtTopoEtcd2topoPort
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
	c, err := newCellClient(clientAddr, "/")
	if err != nil {
		t.Fatalf("newCellClient(%v) failed: %v", clientAddr, err)
	}

	// Wait until we can list "/", or timeout.
	ctx := context.Background()
	start := time.Now()
	for {
		if _, err := c.cli.Get(ctx, "/"); err == nil {
			break
		}
		if time.Now().Sub(start) > 10*time.Second {
			t.Fatalf("Failed to start etcd daemon in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	return cmd, dataDir, clientAddr
}

func TestEtcd2Topo(t *testing.T) {
	// Start a single etcd in the background.
	cmd, dataDir, clientAddr := startEtcd(t)
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dataDir)
	}()

	// This function will create a toplevel directory for a new test.
	testIndex := 0
	newServer := func() (*Server, string) {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		s, err := NewServer(clientAddr, path.Join(testRoot, "global"))
		if err != nil {
			t.Fatalf("NewServer() failed: %v", err)
		}

		return s, testRoot
	}

	// Run the TopoServerTestSuite tests.
	test.TopoServerTestSuite(t, func() topo.Impl {
		s, testRoot := newServer()

		// Create the CellInfo.
		ctx := context.Background()
		cell := "test"
		ci := &topodatapb.CellInfo{
			ServerAddress: clientAddr,
			Root:          path.Join(testRoot, cell),
		}
		data, err := proto.Marshal(ci)
		if err != nil {
			t.Fatalf("cannot proto.Marshal CellInfo: %v", err)
		}
		nodePath := path.Join(s.global.root, cellsPath, cell, topo.CellInfoFile)
		if _, err := s.global.cli.Put(ctx, nodePath, string(data)); err != nil {
			t.Fatalf("s.global.cli.Put(%v) failed: %v", nodePath, err)
		}

		return s
	})

	// Run etcd-specific tests.
	s, _ := newServer()
	testKeyspaceLock(t, s)
}

// testKeyspaceLock tests etcd-specific heartbeat (TTL).
// Note TTL granularity is in seconds, even though the API uses time.Duration.
// So we have to wait a long time in these tests.
func testKeyspaceLock(t *testing.T, ts *Server) {
	ctx := context.Background()
	defer ts.Close()

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	// Long TTL, unlock before lease runs out.
	*leaseTTL = 1000
	actionPath, err := ts.LockKeyspaceForAction(ctx, "test_keyspace", "contents")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction failed: %v", err)
	}
	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", actionPath, "results"); err != nil {
		t.Fatalf("UnlockKeyspaceForAction failed: %v", err)
	}

	// Short TTL, make sure it doesn't expire.
	*leaseTTL = 1
	actionPath, err = ts.LockKeyspaceForAction(ctx, "test_keyspace", "contents")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction failed: %v", err)
	}
	time.Sleep(2 * time.Second)
	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", actionPath, "results"); err != nil {
		t.Fatalf("UnlockKeyspaceForAction failed: %v", err)
	}
}
