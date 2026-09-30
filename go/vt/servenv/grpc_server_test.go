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

package servenv

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/orca"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/tlstest"
)

// TestGRPCServerOptionalTLSWarning checks what a server with optional TLS
// warns about at startup. With a client CA it has to say that plain-text
// connections are not authenticated, since --grpc-ca then only applies to the
// TLS connections, and which stats show the plain-text connections without
// claiming they prove that every client has moved; without a CA the plain
// warning is enough.
func TestGRPCServerOptionalTLSWarning(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	t.Cleanup(withTempVar(&gRPCCert, certs.ServerCert))
	t.Cleanup(withTempVar(&gRPCKey, certs.ServerKey))
	t.Cleanup(withTempVar(&gRPCEnableOptionalTLS, true))

	for name, tc := range map[string]struct {
		ca      string
		want    []string
		notWant []string
	}{
		"with a client CA": {
			ca: certs.ClientCA,
			want: []string{
				"Plain-text connections will be accepted and are not authenticated",
				"--grpc-ca only applies to TLS connections",
				"GrpcOptionalTlsOpenConnections",
				"GrpcOptionalTlsConnections",
				"offline or connects only now and then",
			},
		},
		"without a client CA": {
			ca:      "",
			want:    []string{"Optional TLS is active. Plain-text connections will be accepted"},
			notWant: []string{"not authenticated", "--grpc-ca"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(withTempVar(&gRPCCA, tc.ca))
			t.Cleanup(withTempVar(&gRPCPort, getFreePort()))
			t.Cleanup(withTempVar(&GRPCServer, (*grpc.Server)(nil)))
			var logBuf bytes.Buffer
			oldLogger := log.SwapLogger(slog.New(slog.NewTextHandler(&logBuf, nil)))
			t.Cleanup(func() { log.SwapLogger(oldLogger) })

			createGRPCServer()
			require.NotNil(t, GRPCServer)
			t.Cleanup(GRPCServer.Stop)

			logged := logBuf.String()
			for _, want := range tc.want {
				assert.Contains(t, logged, want)
			}
			for _, notWant := range tc.notWant {
				assert.NotContains(t, logged, notWant)
			}
		})
	}
}

func TestEmpty(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	if len(interceptors.Build()) > 0 {
		t.Fatalf("expected empty builder to report as empty")
	}
}

func TestSingleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake := &FakeInterceptor{}

	interceptors.Add(fake.StreamServerInterceptor, fake.UnaryServerInterceptor)

	if len(interceptors.streamInterceptors) != 1 {
		t.Fatalf("expected 1 server options to be available")
	}
	if len(interceptors.unaryInterceptors) != 1 {
		t.Fatalf("expected 1 server options to be available")
	}
}

func TestDoubleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake1 := &FakeInterceptor{name: "ettan"}
	fake2 := &FakeInterceptor{name: "tvaon"}

	interceptors.Add(fake1.StreamServerInterceptor, fake1.UnaryServerInterceptor)
	interceptors.Add(fake2.StreamServerInterceptor, fake2.UnaryServerInterceptor)

	if len(interceptors.streamInterceptors) != 2 {
		t.Fatalf("expected 1 server options to be available")
	}
	if len(interceptors.unaryInterceptors) != 2 {
		t.Fatalf("expected 1 server options to be available")
	}
}

func TestOrcaRecorder(t *testing.T) {
	recorder := orca.NewServerMetricsRecorder()

	recorder.SetCPUUtilization(0.25)
	recorder.SetMemoryUtilization(0.5)

	snap := recorder.ServerMetrics()

	if snap.CPUUtilization != 0.25 {
		t.Errorf("expected cpu 0.25, got %v", snap.CPUUtilization)
	}
	if snap.MemUtilization != 0.5 {
		t.Errorf("expected memory 0.5, got %v", snap.MemUtilization)
	}
}

func TestReportedOrca(t *testing.T) {
	// Set the port to enable gRPC server.
	withTempVar(&gRPCPort, getFreePort())
	withTempVar(&gRPCEnableOrcaMetrics, true)
	withTempVar(&GRPCServerMetricsRecorder, nil)

	createGRPCServer()
	if GRPCServerMetricsRecorder == nil {
		t.Errorf("GRPCServerMetricsRecorder should be initialized when gRPCEnableOrcaMetrics is false")
	}

	serveGRPC()
	serverMetrics := GRPCServerMetricsRecorder.ServerMetrics()
	cpuUsage := serverMetrics.CPUUtilization
	if cpuUsage < 0 {
		t.Errorf("CPU Utilization is not set %.2f", cpuUsage)
	}
	t.Logf("CPU Utilization is %.2f", cpuUsage)

	memUsage := serverMetrics.MemUtilization
	if memUsage < 0 {
		t.Errorf("Mem Utilization is not set %.2f", memUsage)
	}
	t.Logf("Memory utilization is %.2f", memUsage)
}

func getFreePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("could not get free port: %v", err))
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func withTempVar[T any](set *T, temp T) (restore func()) {
	original := *set
	*set = temp
	return func() {
		*set = original
	}
}

type FakeInterceptor struct {
	name       string
	streamSeen any
	unarySeen  any
}

func (fake *FakeInterceptor) StreamServerInterceptor(value any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fake.streamSeen = value
	return handler(value, stream)
}

func (fake *FakeInterceptor) UnaryServerInterceptor(ctx context.Context, value any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	fake.unarySeen = value
	return handler(ctx, value)
}
