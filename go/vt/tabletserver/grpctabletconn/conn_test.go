// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctabletconn

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconntest"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	pbs "github.com/youtube/vitess/go/vt/proto/queryservice"
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
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	pbs.RegisterQueryServer(server, grpcqueryservice.New(service))
	go server.Serve(listener)

	// Create a gRPC client connecting to the server
	ctx := context.Background()
	client, err := DialTablet(ctx, topo.EndPoint{
		Host: "localhost",
		NamedPortMap: map[string]int{
			"vt": port,
		},
	}, tabletconntest.TestKeyspace, tabletconntest.TestShard, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	// run the test suite
	tabletconntest.TestSuite(t, client, service)

	// and clean up
	client.Close()
}
