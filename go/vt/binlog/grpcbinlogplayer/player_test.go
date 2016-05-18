// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcbinlogplayer

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/binlog/binlogplayertest"
	"github.com/youtube/vitess/go/vt/binlog/grpcbinlogstreamer"

	binlogservicepb "github.com/youtube/vitess/go/vt/proto/binlogservice"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// the test here creates a fake server implementation, a fake client
// implementation, and runs the test suite against the setup.
func TestGRPCBinlogStreamer(t *testing.T) {
	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	fakeUpdateStream := binlogplayertest.NewFakeBinlogStreamer(t)
	binlogservicepb.RegisterUpdateStreamServer(server, grpcbinlogstreamer.New(fakeUpdateStream))
	go server.Serve(listener)

	// Create a GRPC client to talk to the fake tablet
	c := &client{}

	// and send it to the test suite
	binlogplayertest.Run(t, c, &topodatapb.Tablet{
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}, fakeUpdateStream)
}
