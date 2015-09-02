// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctmserver

import (
	"net"
	"net/http"
	"testing"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/tabletmanager/agentrpctest"
	"github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/topo"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// the test here creates a fake server implementation, a fake client
// implementation, and runs the test suite against the setup.
func TestGoRPCTMServer(t *testing.T) {
	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	port := int32(listener.Addr().(*net.TCPAddr).Port)

	// Create a Go Rpc server and listen on the port
	server := rpcplus.NewServer()
	fakeAgent := agentrpctest.NewFakeRPCAgent(t)
	server.Register(&TabletManager{fakeAgent})

	// create the HTTP server, serve the server from it
	handler := http.NewServeMux()
	bsonrpc.ServeCustomRPC(handler, server)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(listener)

	// Create a Go Rpc client to talk to the fake tablet
	client := &gorpctmclient.GoRPCTabletManagerClient{}
	ti := topo.NewTabletInfo(&pb.Tablet{
		Alias: &pb.TabletAlias{
			Cell: "test",
			Uid:  123,
		},
		Hostname: "localhost",
		PortMap: map[string]int32{
			"vt": port,
		},
	}, 0)

	// and run the test suite
	agentrpctest.Run(t, client, ti, fakeAgent)
}
