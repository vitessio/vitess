// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package testlib contains utility methods to include in unit tests to
deal with topology common tasks, liek fake tablets and action loops.
*/
package testlib

import (
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/gorpctmserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

// This file contains utility methods for unit tests.
// We allow the creation of fake tablets, and running their event loop based
// on a FakeMysqlDaemon.

// FakeTablet keeps track of a fake tablet in memory. It has:
// - a Tablet record (used for creating the tablet, kept for user's information)
// - a FakeMysqlDaemon (used by the fake event loop)
// - a 'done' channel (used to terminate the fake event loop)
type FakeTablet struct {
	// Tablet and FakeMysqlDaemon are populated at NewFakeTablet time.
	Tablet          *topo.Tablet
	FakeMysqlDaemon *mysqlctl.FakeMysqlDaemon

	// Agent and Listener are created when we start the event loop for
	// the tablet, and closed / cleared when we stop it.
	Agent    *tabletmanager.ActionAgent
	Listener net.Listener
}

// TabletOption is an interface for changing tablet parameters.
// It's a way to pass multiple parameters to NewFakeTablet without
// making it too cumbersome.
type TabletOption func(tablet *topo.Tablet)

// TabletParent is the tablet option to set the parent alias
func TabletParent(parent topo.TabletAlias) TabletOption {
	return func(tablet *topo.Tablet) {
		tablet.Parent = parent
	}
}

// TabletKeyspaceShard is the option to set the tablet keyspace and shard
func TabletKeyspaceShard(t *testing.T, keyspace, shard string) TabletOption {
	return func(tablet *topo.Tablet) {
		tablet.Keyspace = keyspace
		var err error
		tablet.Shard, tablet.KeyRange, err = topo.ValidateShardName(shard)
		if err != nil {
			t.Fatalf("cannot ValidateShardName value %v", shard)
		}
	}
}

// CreateTestTablet creates the test tablet in the topology.  'uid'
// has to be between 0 and 99. All the tablet info will be derived
// from that. Look at the implementation if you need values.
// Use TabletOption implementations if you need to change values at creation.
func NewFakeTablet(t *testing.T, wr *wrangler.Wrangler, cell string, uid uint32, tabletType topo.TabletType, options ...TabletOption) *FakeTablet {
	if uid < 0 || uid > 99 {
		t.Fatalf("uid has to be between 0 and 99: %v", uid)
	}
	state := topo.STATE_READ_ONLY
	if tabletType == topo.TYPE_MASTER {
		state = topo.STATE_READ_WRITE
	}
	tablet := &topo.Tablet{
		Alias:    topo.TabletAlias{Cell: cell, Uid: uid},
		Hostname: fmt.Sprintf("%vhost", cell),
		Portmap: map[string]int{
			"vt":    8100 + int(uid),
			"mysql": 3300 + int(uid),
			"vts":   8200 + int(uid),
		},
		IPAddr:   fmt.Sprintf("%v.0.0.1", 100+uid),
		Keyspace: "test_keyspace",
		Shard:    "0",
		Type:     tabletType,
		State:    state,
	}
	for _, option := range options {
		option(tablet)
	}
	if err := wr.InitTablet(tablet, false, true, false); err != nil {
		t.Fatalf("cannot create tablet %v: %v", uid, err)
	}

	// create a FakeMysqlDaemon with the right information by default
	fakeMysqlDaemon := &mysqlctl.FakeMysqlDaemon{}
	if !tablet.Parent.IsZero() {
		fakeMysqlDaemon.MasterAddr = fmt.Sprintf("%v.0.0.1:%v", 100+tablet.Parent.Uid, 3300+int(tablet.Parent.Uid))
	}
	fakeMysqlDaemon.MysqlPort = 3300 + int(uid)

	return &FakeTablet{
		Tablet:          tablet,
		FakeMysqlDaemon: fakeMysqlDaemon,
	}
}

// StartActionLoop will start the action loop for a fake tablet,
// using ft.FakeMysqlDaemon as the backing mysqld.
func (ft *FakeTablet) StartActionLoop(t *testing.T, wr *wrangler.Wrangler) {
	if ft.Agent != nil {
		t.Fatalf("Agent for %v is already running", ft.Tablet.Alias)
	}

	// Listen on a random port
	var err error
	ft.Listener, err = net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	port := ft.Listener.Addr().(*net.TCPAddr).Port

	// create a test agent on that port
	ft.Agent = tabletmanager.NewTestActionAgent(wr.TopoServer(), ft.Tablet.Alias, port, ft.FakeMysqlDaemon)

	// create the RPC server
	server := rpcplus.NewServer()
	gorpctmserver.RegisterForTest(server, ft.Agent)

	// create the HTTP server, serve the server from it
	handler := http.NewServeMux()
	bsonrpc.ServeTestRPC(handler, server)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(ft.Listener)
}

// StopActionLoop will stop the Action Loop for the given FakeTablet
func (ft *FakeTablet) StopActionLoop(t *testing.T) {
	if ft.Agent == nil {
		t.Fatalf("Agent for %v is not running", ft.Tablet.Alias)
	}
	ft.Listener.Close()
	ft.Agent.Stop()
	ft.Agent = nil
	ft.Listener = nil
}
