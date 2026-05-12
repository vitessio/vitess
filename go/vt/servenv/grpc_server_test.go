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
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/orca"
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
	withTempVar(&gRPCPort, getFreePort())
	withTempVar(&gRPCEnableOrcaMetrics, true)
	withTempVar(&GRPCServerMetricsRecorder, nil)

	createGRPCServer()
	assert.NotNil(t, GRPCServerMetricsRecorder, "GRPCServerMetricsRecorder should be initialized when gRPCEnableOrcaMetrics is false")

	serveGRPC()
	serverMetrics := GRPCServerMetricsRecorder.ServerMetrics()
	cpuUsage := serverMetrics.CPUUtilization
	assert.GreaterOrEqualf(t, cpuUsage, float64(0), "CPU Utilization is not set %.2f", cpuUsage)
	t.Logf("CPU Utilization is %.2f", cpuUsage)

	memUsage := serverMetrics.MemUtilization
	assert.GreaterOrEqualf(t, memUsage, float64(0), "Mem Utilization is not set %.2f", memUsage)
	t.Logf("Memory utilization is %.2f", memUsage)
}

func TestNewGRPCServer(t *testing.T) {
	restoreEnableOrca := withTempVar(&gRPCEnableOrcaMetrics, true)
	restoreRecorder := withTempVar(&GRPCServerMetricsRecorder, nil)
	t.Cleanup(restoreEnableOrca)
	t.Cleanup(restoreRecorder)

	server := NewGRPCServer()
	t.Cleanup(server.Stop)

	if GRPCServerMetricsRecorder == nil {
		t.Fatalf("GRPCServerMetricsRecorder should be initialized when gRPCEnableOrcaMetrics is true")
	}
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
