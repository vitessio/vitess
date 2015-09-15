// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctabletconn

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
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
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcqueryservice.RegisterForTest(server, service)
	go server.Serve(listener)

	// run the test suite
	tabletconntest.TestSuite(t, protocolName, &pb.EndPoint{
		Host: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}, service)

	// run the error test suite
	tabletconntest.TestErrorSuite(t, protocolName, &pb.EndPoint{
		Host: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}, service)
}
