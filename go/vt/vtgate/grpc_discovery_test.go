package vtgate

import (
	"flag"
	"net"
	"testing"
	"time"

	"context"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/srvtopo"
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

	// Fake services for the tablet, topo server.
	service, ts, cell := CreateFakeServers(t)

	// Tablet: listen on a random port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
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
	hc := discovery.NewLegacyHealthCheck(10*time.Second, 2*time.Minute)
	rs := srvtopo.NewResilientServer(ts, "TestGRPCDiscovery")
	dg := NewDiscoveryGateway(context.Background(), hc, rs, cell, 2)
	hc.AddTablet(&topodatapb.Tablet{
		Alias:    tabletconntest.TestAlias,
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}, "test_tablet")
	err = WaitForTablets(dg, []topodatapb.TabletType{tabletconntest.TestTarget.TabletType})
	if err != nil {
		t.Fatalf("WaitForTablets failed: %v", err)
	}
	defer dg.Close(context.Background())

	// run the test suite.
	TestSuite(t, "discovery-grpc", dg, service)
}
