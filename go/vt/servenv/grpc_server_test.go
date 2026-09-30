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
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/orca"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"

	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

func TestEmpty(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	require.Empty(t, interceptors.Build(), "expected empty builder to report as empty")
}

func TestSingleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake := &FakeInterceptor{}

	interceptors.Add(fake.StreamServerInterceptor, fake.UnaryServerInterceptor)

	require.Len(t, interceptors.streamInterceptors, 1, "expected 1 server options to be available")
	require.Len(t, interceptors.unaryInterceptors, 1, "expected 1 server options to be available")
}

func TestDoubleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake1 := &FakeInterceptor{name: "ettan"}
	fake2 := &FakeInterceptor{name: "tvaon"}

	interceptors.Add(fake1.StreamServerInterceptor, fake1.UnaryServerInterceptor)
	interceptors.Add(fake2.StreamServerInterceptor, fake2.UnaryServerInterceptor)

	require.Len(t, interceptors.streamInterceptors, 2, "expected 2 server options to be available")
	require.Len(t, interceptors.unaryInterceptors, 2, "expected 2 server options to be available")
}

func TestOrcaRecorder(t *testing.T) {
	recorder := orca.NewServerMetricsRecorder()

	recorder.SetCPUUtilization(0.25)
	recorder.SetMemoryUtilization(0.5)

	snap := recorder.ServerMetrics()

	assert.Equalf(t, 0.25, snap.CPUUtilization, "expected cpu 0.25, got %v", snap.CPUUtilization)
	assert.Equalf(t, 0.5, snap.MemUtilization, "expected memory 0.5, got %v", snap.MemUtilization)
}

func TestReportedOrca(t *testing.T) {
	// Set the port to enable gRPC server.
	t.Cleanup(withTempVar(&gRPCPort, getFreePort()))
	t.Cleanup(withTempVar(&gRPCEnableOrcaMetrics, true))
	t.Cleanup(withTempVar(&GRPCServerMetricsRecorder, nil))
	t.Cleanup(withTempVar(&GRPCServer, (*grpc.Server)(nil)))

	createGRPCServer()
	assert.NotNil(t, GRPCServerMetricsRecorder, "GRPCServerMetricsRecorder should be initialized when gRPCEnableOrcaMetrics is false")

	// Cleanups run last-in-first-out, so the updater goroutine is fully
	// stopped before the withTempVar cleanups above restore the globals.
	stopOrcaUpdater := serveGRPC()
	t.Cleanup(stopOrcaUpdater)
	// The method value binds the receiver now, so the cleanup stops this
	// test's server no matter when the GRPCServer global gets restored.
	t.Cleanup(GRPCServer.Stop)

	serverMetrics := GRPCServerMetricsRecorder.ServerMetrics()
	cpuUsage := serverMetrics.CPUUtilization
	assert.GreaterOrEqualf(t, cpuUsage, float64(0), "CPU Utilization is not set %.2f", cpuUsage)
	t.Logf("CPU Utilization is %.2f", cpuUsage)

	memUsage := serverMetrics.MemUtilization
	assert.GreaterOrEqualf(t, memUsage, float64(0), "Mem Utilization is not set %.2f", memUsage)
	t.Logf("Memory utilization is %.2f", memUsage)
}

// TestGRPCServerSkipsIngressStatsByDefault verifies that servenv gRPC servers
// do not record ingress bytes unless a binary opts in.
func TestGRPCServerSkipsIngressStatsByDefault(t *testing.T) {
	restore := withTempVar(&gRPCIngressStatsEnabled, false)
	defer restore()

	var ingressBytes uint64
	runIngressStatsTestRPC(t, &ingressBytes)

	assert.Zero(t, atomic.LoadUint64(&ingressBytes))
}

// TestEnableGRPCIngressStatsInstallsServerOption verifies that opt-in servers
// attach inbound gRPC payload bytes to RPC contexts.
func TestEnableGRPCIngressStatsInstallsServerOption(t *testing.T) {
	restore := withTempVar(&gRPCIngressStatsEnabled, false)
	defer restore()
	EnableGRPCIngressStats()

	var ingressBytes uint64
	runIngressStatsTestRPC(t, &ingressBytes)

	assert.Positive(t, atomic.LoadUint64(&ingressBytes))
}

type ingressStatsTestService interface {
	Check(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type ingressStatsTestServer struct {
	ingressBytes *uint64
}

func (s *ingressStatsTestServer) Check(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if ingressBytes, ok := GRPCIngressBytes(ctx); ok {
		atomic.StoreUint64(s.ingressBytes, ingressBytes)
	}
	return &emptypb.Empty{}, nil
}

var ingressStatsTestServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.IngressStats",
	HandlerType: (*ingressStatsTestService)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Check",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			req := new(emptypb.Empty)
			if err := dec(req); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return srv.(ingressStatsTestService).Check(ctx, req)
			}
			info := &grpc.UnaryServerInfo{
				Server:     srv,
				FullMethod: "/test.IngressStats/Check",
			}
			handler := func(ctx context.Context, req any) (any, error) {
				return srv.(ingressStatsTestService).Check(ctx, req.(*emptypb.Empty))
			}
			return interceptor(ctx, req, info, handler)
		},
	}},
}

func runIngressStatsTestRPC(t *testing.T, ingressBytes *uint64) {
	t.Helper()

	port := getFreePort()
	t.Cleanup(withTempVar(&gRPCPort, port))
	t.Cleanup(withTempVar(&gRPCBindAddress, "127.0.0.1"))
	t.Cleanup(withTempVar(&GRPCServer, (*grpc.Server)(nil)))

	createGRPCServer()
	require.NotNil(t, GRPCServer)
	GRPCServer.RegisterService(&ingressStatsTestServiceDesc, &ingressStatsTestServer{ingressBytes: ingressBytes})
	serveGRPC()
	t.Cleanup(GRPCServer.Stop)

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, conn.Invoke(context.Background(), "/test.IngressStats/Check", &emptypb.Empty{}, &emptypb.Empty{}))
}

// TestGRPCServerCredsReload checks that a gRPC server serves the
// certificate it reloaded to connections established after the reload.
func TestGRPCServerCredsReload(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, oldCerts)

	creds, reloader, err := newGRPCServerCreds(files, false)
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(creds))
	healthpb.RegisterHealthServer(server, health.NewServer())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go server.Serve(ln)
	t.Cleanup(server.Stop)

	// servedCert makes a call as a client of certs and returns the
	// certificate the server presented.
	servedCert := func(certs tlstest.ClientServerKeyPairs) []byte {
		t.Helper()
		config, err := vttls.ClientConfig(vttls.VerifyIdentity, certs.ClientCert, certs.ClientKey, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)
		conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(config)))
		require.NoError(t, err)
		defer conn.Close()
		var p peer.Peer
		_, err = healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{}, grpc.Peer(&p))
		require.NoError(t, err)
		info, ok := p.AuthInfo.(credentials.TLSInfo)
		require.True(t, ok)
		require.Equal(t, "h2", info.State.NegotiatedProtocol)
		return info.State.PeerCertificates[0].Raw
	}

	require.Equal(t, readCert(t, oldCerts.ServerCert).Raw, servedCert(oldCerts))

	installTLSFiles(t, files, newCerts)
	require.NoError(t, reloader.Reload(true))
	require.Equal(t, readCert(t, newCerts.ServerCert).Raw, servedCert(newCerts))
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
