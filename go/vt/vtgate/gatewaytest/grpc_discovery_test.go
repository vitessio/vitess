/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gatewaytest

import (
	"flag"
	"fmt"
	"net"
	"testing"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
	"github.com/youtube/vitess/go/vt/vtgate/l2vtgate"
	"github.com/youtube/vitess/go/vt/vttablet/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletconntest"

	// We will use gRPC to connect, register the dialer
	_ "github.com/youtube/vitess/go/vt/vttablet/grpctabletconn"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	dg := gateway.GetCreator()(hc, ts, ts, cell, 2)
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

// TestL2VTGateDiscovery tests the l2vtgate gateway with a gRPC
// connection from the gateway to a l2vtgate in-process object.
func TestL2VTGateDiscovery(t *testing.T) {
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

	// L2VTGate: Create the discovery healthcheck, and the gateway.
	// Wait for the right tablets to be present.
	hc := discovery.NewHealthCheck(10*time.Second, 2*time.Minute)
	l2vtgate := l2vtgate.Init(hc, ts, ts, "", cell, 2, nil)
	hc.AddTablet(&topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  44,
		},
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}, "test_tablet")
	ctx := context.Background()
	err = l2vtgate.Gateway().WaitForTablets(ctx, []topodatapb.TabletType{tabletconntest.TestTarget.TabletType})
	if err != nil {
		t.Fatalf("WaitForAllServingTablets failed: %v", err)
	}

	// L2VTGate: listen on a random port.
	listener, err = net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()

	// L2VTGate: create a gRPC server and listen on the port.
	server = grpc.NewServer()
	grpcqueryservice.Register(server, l2vtgate)
	go server.Serve(listener)
	defer server.Stop()

	// VTGate: create the l2vtgate gateway
	flag.Set("gateway_implementation", "l2vtgategateway")
	flag.Set("l2vtgategateway_addrs", fmt.Sprintf("%v|%v|%v", listener.Addr().String(), tabletconntest.TestTarget.Keyspace, tabletconntest.TestTarget.Shard))
	lg := gateway.GetCreator()(nil, ts, nil, "", 2)
	defer lg.Close(ctx)

	// and run the test suite.
	TestSuite(t, "l2vtgate-grpc", lg, service)
}
