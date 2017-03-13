// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctmserver

import (
	"net"
	"testing"

	"github.com/youtube/vitess/go/vt/vttablet/agentrpctest"
	"github.com/youtube/vitess/go/vt/vttablet/grpctmclient"
	"google.golang.org/grpc"

	tabletmanagerservicepb "github.com/youtube/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestGRPCTMServer creates a fake server implementation, a fake client
// implementation, and runs the test suite against the setup.
func TestGRPCTMServer(t *testing.T) {
	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := int32(listener.Addr().(*net.TCPAddr).Port)

	// Create a gRPC server and listen on the port.
	s := grpc.NewServer()
	fakeAgent := agentrpctest.NewFakeRPCAgent(t)
	tabletmanagerservicepb.RegisterTabletManagerServer(s, &server{agent: fakeAgent})
	go s.Serve(listener)

	// Create a gRPC client to talk to the fake tablet.
	client := grpctmclient.NewClient()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test",
			Uid:  123,
		},
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": port,
		},
	}

	// and run the test suite
	agentrpctest.Run(t, client, tablet, fakeAgent)
}
