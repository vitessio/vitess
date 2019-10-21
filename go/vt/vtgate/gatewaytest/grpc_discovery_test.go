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

package gatewaytest

import (
	"flag"
	"net"
	"testing"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/gateway"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"

	// We will use gRPC to connect, register the dialer
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestGRPCDiscovery tests the discovery gateway with a gRPC
// connection from the gateway to the fake tablet.
func TestGRPCDiscovery(t *testing.T) {
	flag.Set("tablet_protocol", "grpc")
	flag.Set("gateway_implementation", "discoverygateway")

	// Fake services for the tablet, topo server.
	service, ts, cell := CreateFakeServers(t)

	// Tablet: listen on a random port.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port
	defer listener.Close()

	// Tablet: create a gRPC server and listen on the port.
	server := grpc.NewServer()
	grpcqueryservice.Register(server, service)
	go server.Serve(listener)
	defer server.Stop()

	// VTGate: create the discovery healthcheck, and the gateway.
	// Wait for the right tablets to be present.
	hc := discovery.NewHealthCheck(10*time.Second, 2*time.Minute)
	rs := srvtopo.NewResilientServer(ts, "TestGRPCDiscovery")
	dg := gateway.GetCreator()(context.Background(), hc, rs, cell, 2)
	hc.AddTablet(&topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  43,
		},
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}, "test_tablet")
	err = gateway.WaitForTablets(dg, []topodatapb.TabletType{tabletconntest.TestTarget.TabletType})
	if err != nil {
		t.Fatalf("WaitForTablets failed: %v", err)
	}
	defer dg.Close(context.Background())

	// run the test suite.
	TestSuite(t, "discovery-grpc", dg, service)
}
