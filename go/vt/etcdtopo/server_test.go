// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"path"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func newTestServer(t *testing.T, cells []string) *Server {
	s := &Server{
		_cells:    make(map[string]*cellClient),
		newClient: newTestClient,
	}

	// In tests, use cell name as the address.
	globalAddrs = flagutil.StringListValue([]string{topo.GlobalCell})
	c := s.getGlobal()

	// Add local cell "addresses" to the global cell.
	for _, cell := range cells {
		c.Set("/vt/cells/"+cell, cell, 0)
	}

	return s
}

func TestEtcdTopo(t *testing.T) {
	test.TopoServerTestSuite(t, func() topo.Impl {
		return newTestServer(t, []string{"test"})
	})
}

// Test etcd-specific heartbeat (TTL).
func TestKeyspaceLock(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()

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
	*lockTTL = time.Second
	actionPath, err = ts.LockKeyspaceForAction(ctx, "test_keyspace", "contents")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction failed: %v", err)
	}
	time.Sleep(2 * time.Second)
	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", actionPath, "results"); err != nil {
		t.Fatalf("UnlockKeyspaceForAction failed: %v", err)
	}

	// Short TTL, lose the lock.
	*lockTTL = time.Second
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

	// Short TTL, force expiry.
	*lockTTL = time.Second
	ignoreTTLRefresh = true
	actionPath, err = ts.LockKeyspaceForAction(ctx, "test_keyspace", "contents")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction failed: %v", err)
	}
	time.Sleep(2 * time.Second)
	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", actionPath, "results"); err != topo.ErrNoNode {
		t.Fatalf("UnlockKeyspaceForAction = %v, want %v", err, topo.ErrNoNode)
	}
	ignoreTTLRefresh = false
}
