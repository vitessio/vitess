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
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspace(t, ts)
}

func TestShard(t *testing.T) {
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShard(context.Background(), t, ts)
}

func TestTablet(t *testing.T) {
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckTablet(context.Background(), t, ts)
}

func TestShardReplication(t *testing.T) {
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardReplication(t, ts)
}

func TestServingGraph(t *testing.T) {
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckServingGraph(context.Background(), t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspaceLock(t, ts)
}

func TestShardLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardLock(t, ts)
}

func TestSrvShardLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckSrvShardLock(t, ts)
}

func TestVSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := newTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckVSchema(t, ts)
}
