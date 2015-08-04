// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package testlib contains utility methods to include in unit tests to
deal with topology common tasks, like fake tablets and action loops.
*/
package testlib

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/grpctmserver"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"

	// import the gRPC client implementation for tablet manager
	_ "github.com/youtube/vitess/go/vt/tabletmanager/grpctmclient"

	// import the gRPC client implementation for query service
	_ "github.com/youtube/vitess/go/vt/tabletserver/grpctabletconn"
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

	// The following fields are created when we start the event loop for
	// the tablet, and closed / cleared when we stop it.
	// The Listener and RPCServer are used by the gRPC server.
	Agent     *tabletmanager.ActionAgent
	Listener  net.Listener
	RPCServer *grpc.Server

	// These optional fields are used if the tablet also needs to
	// listen on the 'vt' port.
	StartHTTPServer bool
	HTTPListener    net.Listener
	HTTPServer      http.Server
}

// TabletOption is an interface for changing tablet parameters.
// It's a way to pass multiple parameters to NewFakeTablet without
// making it too cumbersome.
type TabletOption func(tablet *topo.Tablet)

// TabletKeyspaceShard is the option to set the tablet keyspace and shard
func TabletKeyspaceShard(t *testing.T, keyspace, shard string) TabletOption {
	return func(tablet *topo.Tablet) {
		tablet.Keyspace = keyspace
		shard, ks, err := topo.ValidateShardName(shard)
		if err != nil {
			t.Fatalf("cannot ValidateShardName value %v", shard)
		}
		tablet.Shard = shard
		tablet.KeyRange = key.ProtoToKeyRange(ks)
	}
}

// ForceInitTablet is the tablet option to set the 'force' flag during InitTablet
func ForceInitTablet() TabletOption {
	return func(tablet *topo.Tablet) {
		// set the force_init field into the portmap as a hack
		tablet.Portmap["force_init"] = 1
	}
}

// StartHTTPServer is the tablet option to start the HTTP server when
// starting a tablet.
func StartHTTPServer() TabletOption {
	return func(tablet *topo.Tablet) {
		// set the start_http_server field into the portmap as a hack
		tablet.Portmap["start_http_server"] = 1
	}
}

// NewFakeTablet creates the test tablet in the topology.  'uid'
// has to be between 0 and 99. All the tablet info will be derived
// from that. Look at the implementation if you need values.
// Use TabletOption implementations if you need to change values at creation.
func NewFakeTablet(t *testing.T, wr *wrangler.Wrangler, cell string, uid uint32, tabletType topo.TabletType, options ...TabletOption) *FakeTablet {
	if uid < 0 || uid > 99 {
		t.Fatalf("uid has to be between 0 and 99: %v", uid)
	}
	tablet := &topo.Tablet{
		Alias:    topo.TabletAlias{Cell: cell, Uid: uid},
		Hostname: fmt.Sprintf("%vhost", cell),
		Portmap: map[string]int{
			"vt":    8100 + int(uid),
			"mysql": 3300 + int(uid),
			"grpc":  8200 + int(uid),
		},
		IPAddr:   fmt.Sprintf("%v.0.0.1", 100+uid),
		Keyspace: "test_keyspace",
		Shard:    "0",
		Type:     tabletType,
	}
	for _, option := range options {
		option(tablet)
	}
	_, startHTTPServer := tablet.Portmap["start_http_server"]
	delete(tablet.Portmap, "start_http_server")
	_, force := tablet.Portmap["force_init"]
	delete(tablet.Portmap, "force_init")
	if err := wr.InitTablet(context.Background(), tablet, force, true, false); err != nil {
		t.Fatalf("cannot create tablet %v: %v", uid, err)
	}

	// create a FakeMysqlDaemon with the right information by default
	fakeMysqlDaemon := mysqlctl.NewFakeMysqlDaemon()
	fakeMysqlDaemon.MysqlPort = 3300 + int(uid)

	return &FakeTablet{
		Tablet:          tablet,
		FakeMysqlDaemon: fakeMysqlDaemon,
		StartHTTPServer: startHTTPServer,
	}
}

// StartActionLoop will start the action loop for a fake tablet,
// using ft.FakeMysqlDaemon as the backing mysqld.
func (ft *FakeTablet) StartActionLoop(t *testing.T, wr *wrangler.Wrangler) {
	if ft.Agent != nil {
		t.Fatalf("Agent for %v is already running", ft.Tablet.Alias)
	}

	// Listen on a random port for gRPC
	var err error
	ft.Listener, err = net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	gRPCPort := ft.Listener.Addr().(*net.TCPAddr).Port

	// if needed, listen on a random port for HTTP
	vtPort := ft.Tablet.Portmap["vt"]
	if ft.StartHTTPServer {
		ft.HTTPListener, err = net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("Cannot listen on http port: %v", err)
		}
		handler := http.NewServeMux()
		ft.HTTPServer = http.Server{
			Handler: handler,
		}
		go ft.HTTPServer.Serve(ft.HTTPListener)
		vtPort = ft.HTTPListener.Addr().(*net.TCPAddr).Port
	}

	// create a test agent on that port, and re-read the record
	// (it has new ports and IP)
	ft.Agent = tabletmanager.NewTestActionAgent(context.Background(), wr.TopoServer(), ft.Tablet.Alias, vtPort, gRPCPort, ft.FakeMysqlDaemon)
	ft.Tablet = ft.Agent.Tablet().Tablet

	// create the gRPC server
	ft.RPCServer = grpc.NewServer()
	grpctmserver.RegisterForTest(ft.RPCServer, ft.Agent)
	go ft.RPCServer.Serve(ft.Listener)

	// and wait for it to serve, so we don't start using it before it's
	// ready.
	timeout := 5 * time.Second
	step := 10 * time.Millisecond
	c := tmclient.NewTabletManagerClient()
	for timeout >= 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := c.Ping(ctx, ft.Agent.Tablet())
		cancel()
		if err == nil {
			break
		}
		time.Sleep(step)
		timeout -= step
	}
	if timeout < 0 {
		panic("StartActionLoop failed.")
	}
}

// StopActionLoop will stop the Action Loop for the given FakeTablet
func (ft *FakeTablet) StopActionLoop(t *testing.T) {
	if ft.Agent == nil {
		t.Fatalf("Agent for %v is not running", ft.Tablet.Alias)
	}
	if ft.StartHTTPServer {
		ft.HTTPListener.Close()
	}
	ft.Listener.Close()
	ft.Agent.Stop()
	ft.Agent = nil
	ft.Listener = nil
	ft.HTTPListener = nil
}

func init() {
	// enforce we will use the right protocol (gRPC) in all unit tests
	*tmclient.TabletManagerProtocol = "grpc"
	*tabletconn.TabletProtocol = "grpc"
}
