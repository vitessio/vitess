/*
Copyright 2020 The Vitess Authors.

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

package grpcserver

import (
	"log/slog"
	"net"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/log/logtest"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestShutdownLogs runs in a child process, as it sends SIGTERM to itself and
// other tests leave servers listening for it.
func TestShutdownLogs(t *testing.T) {
	if os.Getenv("VTADMIN_GRPCSERVER_SHUTDOWN_LOGS") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestShutdownLogs$", "-test.v")
		cmd.Env = append(os.Environ(), "VTADMIN_GRPCSERVER_SHUTDOWN_LOGS=1")
		out, err := cmd.CombinedOutput()
		assert.NoError(t, err, "child process output:\n%s", out)
		return
	}

	logs := logtest.Capture(t, slog.LevelInfo)

	lis, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err)
	listenFunc = func(network, address string) (net.Listener, error) {
		return lis, nil
	}

	s := New("testservice", Options{Addr: "test-address", LameDuckDuration: time.Millisecond})
	errCh := make(chan error, 1)
	go func() { errCh <- s.ListenAndServe() }()

	record := logs.Wait(t, "server listening")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "testservice", record["server"])
	assert.Equal(t, "test-address", record["address"])

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	_, err = healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{Service: "no-such-service"})
	require.Error(t, err)

	record = logs.Record(t, "RPC error")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "/grpc.health.v1.Health/Check", record["rpc"])
	assert.Contains(t, record["error"], "unknown service")

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
	require.NoError(t, <-errCh)

	record = logs.Record(t, "graceful shutdown triggered")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "received signal: terminated", record["reason"])
	assert.NotContains(t, record, "error")

	record = logs.Record(t, "entering lame duck period")
	assert.Equal(t, "INFO", record["level"])
	assert.InDelta(t, float64(time.Millisecond), record["lameduck_period"], 0)

	// GracefulStop makes Serve return a nil error, which isn't logged.
	record = logs.Wait(t, "grpc server stopped")
	assert.Equal(t, "WARN", record["level"])
	assert.NotContains(t, record, "error")
}

func TestServer(t *testing.T) {
	lis, err := nettest.NewLocalListener("tcp")
	listenFunc = func(network, address string) (net.Listener, error) {
		return lis, err
	}

	defer lis.Close()

	s := New("testservice", Options{
		EnableTracing:   true,
		AllowReflection: true,
		CMuxReadTimeout: time.Second,
	})

	go func() { err := s.ListenAndServe(); assert.NoError(t, err) }()

	readyCh := make(chan bool)

	go func() {
		for !s.isServing() {
		}
		readyCh <- true
	}()

	serveStart := time.Now()
	select {
	case <-readyCh:
	case serveStop := <-time.After(time.Millisecond * 500):
		assert.Failf(t, "server did not start", "server did not start within %s", serveStop.Sub(serveStart))
		return
	}
	close(readyCh)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()) //nolint:staticcheck
	require.NoError(t, err)

	defer conn.Close()

	healthclient := healthpb.NewHealthClient(conn)
	resp, err := healthclient.Check(t.Context(), &healthpb.HealthCheckRequest{Service: "grpc.health.v1.Health"})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestLameduck(t *testing.T) {
	lis, err := nettest.NewLocalListener("tcp")
	listenFunc = func(network, address string) (net.Listener, error) {
		return lis, err
	}

	ldd := time.Millisecond * 50

	s := New("testservice", Options{LameDuckDuration: ldd})

	go func() { err := s.ListenAndServe(); assert.NoError(t, err) }()

	readyCh := make(chan bool)

	go func() {
		for !s.isServing() {
		}
		readyCh <- true
	}()

	serveStart := time.Now()
	select {
	case <-readyCh:
	case serveStop := <-time.After(time.Millisecond * 500):
		assert.Failf(t, "server did not start", "server did not start within %s", serveStop.Sub(serveStart))
		return
	}

	stoppedCh := make(chan bool)

	go func() {
		for s.isServing() {
		}
		stoppedCh <- true
	}()

	shutdownStart := time.Now()

	lis.Close()

	select {
	case <-stoppedCh:
	case <-time.After(ldd):
	}

	shutdownDuration := time.Since(shutdownStart)
	assert.LessOrEqual(t, int64(ldd), int64(shutdownDuration),
		"should have taken at least %s to shutdown, took only %s", ldd, shutdownDuration)
}

func TestError(t *testing.T) {
	listenFunc = func(network, address string) (net.Listener, error) { return nil, assert.AnError }
	s := New("testservice", Options{})
	errCh := make(chan error)

	// This has to happen in a goroutine. In normal operation, this function
	// blocks until externally signalled, and we don't want to hold up the
	// tests.
	go func() {
		errCh <- s.ListenAndServe()
	}()

	start := time.Now()
	select {
	case err := <-errCh:
		require.Error(t, err)
	case ti := <-time.After(time.Millisecond * 10):
		assert.Failf(t, "timed out waiting for error after", "%s", ti.Sub(start))
	}
}
