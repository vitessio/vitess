// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctabletconn

import (
	"net"
	"net/http"
	"testing"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/tabletserver/gorpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconntest"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This test makes sure the go rpc service works
func TestGoRPCTabletConn(t *testing.T) {
	// fake service
	service := tabletconntest.CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a Go Rpc server and listen on the port
	server := rpcplus.NewServer()
	server.Register(gorpcqueryservice.New(service))

	// create the HTTP server, serve the server from it
	handler := http.NewServeMux()
	bsonrpc.ServeCustomRPC(handler, server)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(listener)

	// run the test suite
	tabletconntest.TestSuite(t, protocolName, &pb.EndPoint{
		Host: "localhost",
		PortMap: map[string]int32{
			"vt": int32(port),
		},
	}, service)

	// run the error test suite
	tabletconntest.TestErrorSuite(t, protocolName, &pb.EndPoint{
		Host: "localhost",
		PortMap: map[string]int32{
			"vt": int32(port),
		},
	}, service)
}
