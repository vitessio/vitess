// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"testing"

	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/vt/topo/test"
	"golang.org/x/net/context"
)

func newTestServer(t *testing.T, cells []string) *Server {
	s := &Server{
		_cells:    make(map[string]*cellClient),
		newClient: newTestClient,
	}

	// In tests, use cell name as the address.
	globalAddrs = flagutil.StringListValue([]string{"global"})
	c := s.getGlobal()

	// Add local cell "addresses" to the global cell.
	for _, cell := range cells {
		c.Set("/vt/cells/"+cell, cell, 0)
	}

	return s
}

func TestKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspace(ctx, t, ts)
}

func TestShard(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShard(ctx, t, ts)
}

func TestTablet(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckTablet(ctx, t, ts)
}

func TestShardReplication(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardReplication(ctx, t, ts)
}

func TestServingGraph(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckServingGraph(ctx, t, ts)
}

func TestWatchEndPoints(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckWatchEndPoints(ctx, t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspaceLock(ctx, t, ts)
}

func TestShardLock(t *testing.T) {
	ctx := context.Background()
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardLock(ctx, t, ts)
}

func TestSrvShardLock(t *testing.T) {
	ctx := context.Background()
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckSrvShardLock(ctx, t, ts)
}

func TestVSchema(t *testing.T) {
	ctx := context.Background()
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckVSchema(ctx, t, ts)
}
