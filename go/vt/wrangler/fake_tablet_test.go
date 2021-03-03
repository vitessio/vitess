/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wrangler

import (
	"net"
	"net/http"
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/grpctmserver"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	// import the gRPC client implementation for tablet manager
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	// import the gRPC client implementation for query service
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

// This file was copied from testlib. All tests from testlib should be moved
// to the current directory. In order to move tests from there, we have to
// remove the circular dependency it causes (through vtctl dependence).
// The tests in this directory call wrangler functions directly. So, there's
// no circular dependency.

// This file contains utility methods for unit tests.
// We allow the creation of fake tablets, and running their event loop based
// on a FakeMysqlDaemon.

// fakeTablet keeps track of a fake tablet in memory. It has:
// - a Tablet record (used for creating the tablet, kept for user's information)
// - a FakeMysqlDaemon (used by the fake event loop)
// - a 'done' channel (used to terminate the fake event loop)
type fakeTablet struct {
	// Tablet and FakeMysqlDaemon are populated at NewFakeTablet time.
	// We also create the RPCServer, so users can register more services
	// before calling StartActionLoop().
	Tablet          *topodatapb.Tablet
	FakeMysqlDaemon *fakemysqldaemon.FakeMysqlDaemon
	RPCServer       *grpc.Server

	// The following fields are created when we start the event loop for
	// the tablet, and closed / cleared when we stop it.
	// The Listener is used by the gRPC server.
	TM       *tabletmanager.TabletManager
	Listener net.Listener

	// These optional fields are used if the tablet also needs to
	// listen on the 'vt' port.
	StartHTTPServer bool
	HTTPListener    net.Listener
	HTTPServer      *http.Server
}

// TabletOption is an interface for changing tablet parameters.
// It's a way to pass multiple parameters to NewFakeTablet without
// making it too cumbersome.
type TabletOption func(tablet *topodatapb.Tablet)

// TabletKeyspaceShard is the option to set the tablet keyspace and shard
func TabletKeyspaceShard(t *testing.T, keyspace, shard string) TabletOption {
	return func(tablet *topodatapb.Tablet) {
		tablet.Keyspace = keyspace
		shard, kr, err := topo.ValidateShardName(shard)
		if err != nil {
			t.Fatalf("cannot ValidateShardName value %v", shard)
		}
		tablet.Shard = shard
		tablet.KeyRange = kr
	}
}

// newFakeTablet creates the test tablet in the topology.  'uid'
// has to be between 0 and 99. All the tablet info will be derived
// from that. Look at the implementation if you need values.
// Use TabletOption implementations if you need to change values at creation.
// 'db' can be nil if the test doesn't use a database at all.
func newFakeTablet(t *testing.T, wr *Wrangler, cell string, uid uint32, tabletType topodatapb.TabletType, db *fakesqldb.DB, options ...TabletOption) *fakeTablet {
	if uid > 99 {
		t.Fatalf("uid has to be between 0 and 99: %v", uid)
	}
	mysqlPort := int32(3300 + uid)
	hostname, err := netutil.FullyQualifiedHostname()
	require.NoError(t, err)
	tablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: cell, Uid: uid},
		Hostname:      hostname,
		MysqlHostname: hostname,
		PortMap: map[string]int32{
			"vt":   int32(8100 + uid),
			"grpc": int32(8200 + uid),
		},
		Keyspace: "test_keyspace",
		Shard:    "0",
		Type:     tabletType,
	}
	tablet.MysqlPort = mysqlPort
	for _, option := range options {
		option(tablet)
	}
	if err := wr.InitTablet(context.Background(), tablet, false /* allowMasterOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		t.Fatalf("cannot create tablet %v: %v", uid, err)
	}

	// create a FakeMysqlDaemon with the right information by default
	fakeMysqlDaemon := fakemysqldaemon.NewFakeMysqlDaemon(db)
	fakeMysqlDaemon.MysqlPort.Set(mysqlPort)

	return &fakeTablet{
		Tablet:          tablet,
		FakeMysqlDaemon: fakeMysqlDaemon,
		RPCServer:       grpc.NewServer(),
	}
}

// StartActionLoop will start the action loop for a fake tablet,
// using ft.FakeMysqlDaemon as the backing mysqld.
func (ft *fakeTablet) StartActionLoop(t *testing.T, wr *Wrangler) {
	if ft.TM != nil {
		t.Fatalf("TM for %v is already running", ft.Tablet.Alias)
	}

	// Listen on a random port for gRPC.
	var err error
	ft.Listener, err = net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	gRPCPort := int32(ft.Listener.Addr().(*net.TCPAddr).Port)

	// If needed, listen on a random port for HTTP.
	vtPort := ft.Tablet.PortMap["vt"]
	if ft.StartHTTPServer {
		ft.HTTPListener, err = net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("Cannot listen on http port: %v", err)
		}
		handler := http.NewServeMux()
		ft.HTTPServer = &http.Server{
			Handler: handler,
		}
		go ft.HTTPServer.Serve(ft.HTTPListener)
		vtPort = int32(ft.HTTPListener.Addr().(*net.TCPAddr).Port)
	}
	ft.Tablet.PortMap["vt"] = vtPort
	ft.Tablet.PortMap["grpc"] = gRPCPort

	// Create a test tm on that port, and re-read the record
	// (it has new ports and IP).
	ft.TM = &tabletmanager.TabletManager{
		BatchCtx:            context.Background(),
		TopoServer:          wr.TopoServer(),
		MysqlDaemon:         ft.FakeMysqlDaemon,
		DBConfigs:           &dbconfigs.DBConfigs{},
		QueryServiceControl: tabletservermock.NewController(),
	}
	if err := ft.TM.Start(ft.Tablet, 0); err != nil {
		t.Fatal(err)
	}
	ft.Tablet = ft.TM.Tablet()

	// Register the gRPC server, and starts listening.
	grpctmserver.RegisterForTest(ft.RPCServer, ft.TM)
	go ft.RPCServer.Serve(ft.Listener)

	// And wait for it to serve, so we don't start using it before it's
	// ready.
	timeout := 5 * time.Second
	step := 10 * time.Millisecond
	c := tmclient.NewTabletManagerClient()
	for timeout >= 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := c.Ping(ctx, ft.TM.Tablet())
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
func (ft *fakeTablet) StopActionLoop(t *testing.T) {
	if ft.TM == nil {
		t.Fatalf("TM for %v is not running", ft.Tablet.Alias)
	}
	if ft.StartHTTPServer {
		ft.HTTPListener.Close()
	}
	ft.Listener.Close()
	ft.TM.Stop()
	ft.TM = nil
	ft.Listener = nil
	ft.HTTPListener = nil
}

// Target returns the keyspace/shard/type info of this tablet as Target.
func (ft *fakeTablet) Target() querypb.Target {
	return querypb.Target{
		Keyspace:   ft.Tablet.Keyspace,
		Shard:      ft.Tablet.Shard,
		TabletType: ft.Tablet.Type,
	}
}

func init() {
	// enforce we will use the right protocol (gRPC) in all unit tests
	*tmclient.TabletManagerProtocol = "grpc"
	*tabletconn.TabletProtocol = "grpc"
}
