// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctmserver

import (
	"net"
	"net/http"
	"testing"

	"code.google.com/p/go.net/context"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/tabletmanager/agentrpctest"
	"github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/topo"
)

// the test here creates a fake server implementation, a fake client
// implementation, and runs the test suite against the setup.
func TestGoRpcTMServer(t *testing.T) {
	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a Go Rpc server and listen on the port
	server := rpcplus.NewServer()
	server.Register(&TabletManager{agentrpctest.NewFakeRpcAgent(t)})

	// create the HTTP server, serve the server from it
	handler := http.NewServeMux()
	bsonrpc.ServeCustomRPC(handler, server, false)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(listener)

	// Create a Go Rpc client to talk to the fake tablet
	client := &gorpctmclient.GoRpcTabletManagerClient{}
	ti := topo.NewTabletInfo(&topo.Tablet{
		Alias: topo.TabletAlias{
			Cell: "test",
			Uid:  123,
		},
		Hostname: "localhost",
		Portmap: map[string]int{
			"vt": port,
		},
	}, 0)

	// and run the test suite
	agentrpctest.AgentRpcTestSuite(context.Background(), t, client, ti)
}
