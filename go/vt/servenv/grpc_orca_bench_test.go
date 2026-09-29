/*
Copyright 2026 The Vitess Authors.

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

package servenv

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

type benchVStreamServer struct {
	vtgateservicepb.UnimplementedVitessServer
}

var benchVStreamResponse = &vtgatepb.VStreamResponse{Events: []*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_HEARTBEAT}}}

func (benchVStreamServer) VStream(_ *vtgatepb.VStreamRequest, stream grpc.ServerStreamingServer[vtgatepb.VStreamResponse]) error {
	for range 100 {
		if err := stream.Send(benchVStreamResponse); err != nil {
			return err
		}
	}
	return nil
}

func startBenchServer(b *testing.B, orcaEnabled bool) *grpc.ClientConn {
	b.Cleanup(withTempVar(&gRPCPort, 1))
	b.Cleanup(withTempVar(&gRPCEnableOrcaMetrics, orcaEnabled))
	b.Cleanup(withTempVar(&GRPCServerMetricsRecorder, nil))
	b.Cleanup(withTempVar(&GRPCServer, (*grpc.Server)(nil)))
	createGRPCServer()
	healthpb.RegisterHealthServer(GRPCServer, health.NewServer())
	vtgateservicepb.RegisterVitessServer(GRPCServer, benchVStreamServer{})
	lis := bufconn.Listen(1 << 20)
	go GRPCServer.Serve(lis)
	b.Cleanup(GRPCServer.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { conn.Close() })
	return conn
}

// Comparing orca=false with orca=true shows the per-RPC cost of counting
// messages for ORCA QPS.
func BenchmarkOrcaAllocsUnaryHealthCheck(b *testing.B) {
	for _, orca := range []bool{false, true} {
		b.Run(fmt.Sprintf("orca=%t", orca), func(b *testing.B) {
			client := healthpb.NewHealthClient(startBenchServer(b, orca))
			ctx := b.Context()
			req := &healthpb.HealthCheckRequest{}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := client.Check(ctx, req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkOrcaAllocsVStream100Messages(b *testing.B) {
	for _, orca := range []bool{false, true} {
		b.Run(fmt.Sprintf("orca=%t", orca), func(b *testing.B) {
			client := vtgateservicepb.NewVitessClient(startBenchServer(b, orca))
			ctx := b.Context()
			req := &vtgatepb.VStreamRequest{}
			b.ReportAllocs()
			for b.Loop() {
				stream, err := client.VStream(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
				if err := drainStream(stream.Recv); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
