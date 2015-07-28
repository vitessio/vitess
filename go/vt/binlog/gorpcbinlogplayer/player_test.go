// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcbinlogplayer

import (
	"net"
	"net/http"
	"testing"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayertest"
	"github.com/youtube/vitess/go/vt/binlog/gorpcbinlogstreamer"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// the test here creates a fake server implementation, a fake client
// implementation, and runs the test suite against the setup.
func TestGoRPCBinlogStreamer(t *testing.T) {
	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a Go Rpc server and listen on the port
	server := rpcplus.NewServer()
	fakeUpdateStream := binlogplayertest.NewFakeBinlogStreamer(t)
	server.Register(gorpcbinlogstreamer.New(fakeUpdateStream))

	// create the HTTP server, serve the server from it
	handler := http.NewServeMux()
	bsonrpc.ServeCustomRPC(handler, server, false)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(listener)

	// Create a Go Rpc client to talk to the fake tablet
	c := &client{}

	// and send it to the test suite
	binlogplayertest.Run(t, c, &pb.EndPoint{
		Host: host,
		PortMap: map[string]int32{
			"vt": int32(port),
		},
	}, fakeUpdateStream)
}
