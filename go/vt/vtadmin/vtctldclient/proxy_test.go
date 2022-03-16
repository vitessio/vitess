/*
Copyright 2021 The Vitess Authors.

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

package vtctldclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
)

type fakeVtctld struct {
	vtctlservicepb.VtctlServer
}

func TestDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer listener.Close()

	vtctld := &fakeVtctld{}
	server := grpc.NewServer()
	vtctlservicepb.RegisterVtctlServer(server, vtctld)

	go server.Serve(listener)
	defer server.Stop()

	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: listener.Addr().String(),
	})

	proxy := New(&Config{
		Cluster: &vtadminpb.Cluster{
			Id:   "test",
			Name: "testcluster",
		},
		Discovery: disco,
	})

	// We don't have a vtctld host until we call Dial
	require.Empty(t, proxy.host)

	err = proxy.Dial(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, listener.Addr().String(), proxy.host)
}

// TestRedial tests that vtadmin-api is able to recover from a lost connection to
// a vtctld by rediscovering and redialing a new one.
func TestRedial(t *testing.T) {
	// Initialize vtctld #1
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener1.Close()

	vtctld1 := &fakeVtctld{}
	server1 := grpc.NewServer()

	go server1.Serve(listener1)
	defer server1.Stop()

	vtctlservicepb.RegisterVtctlServer(server1, vtctld1)

	// Initialize vtctld #2
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener2.Close()

	vtctld2 := &fakeVtctld{}
	server2 := grpc.NewServer()

	go server2.Serve(listener2)
	defer server2.Stop()

	vtctlservicepb.RegisterVtctlServer(server2, vtctld2)

	// Register both vtctlds with VTAdmin
	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: listener1.Addr().String(),
	}, &vtadminpb.Vtctld{
		Hostname: listener2.Addr().String(),
	})

	proxy := New(&Config{
		Cluster: &vtadminpb.Cluster{
			Id:   "test",
			Name: "testcluster",
		},
		Discovery:           disco,
		ConnectivityTimeout: 2 * time.Second,
	})

	// We don't have a vtctld host until we call Dial
	require.Empty(t, proxy.host)

	// Check for a successful connection to whichever vtctld we discover first.
	err = proxy.Dial(context.Background())
	assert.NoError(t, err)

	// vtadmin's fakediscovery package discovers vtctlds in random order. Rather
	// than force some cumbersome sequential logic, we can just do a switcheroo
	// here in the test to determine our "current" and (expected) "next" vtctlds.
	var currentVtctld *grpc.Server
	var nextAddr string

	switch proxy.host {
	case listener1.Addr().String():
		currentVtctld = server1
		nextAddr = listener2.Addr().String()

	case listener2.Addr().String():
		currentVtctld = server2
		nextAddr = listener1.Addr().String()
	default:
		t.Fatalf("invalid proxy host %s", proxy.host)
	}

	// Remove the shut down vtctld from VTAdmin's service discovery (clumsily).
	// Otherwise, when redialing, we may redial the vtctld that we just shut down.
	// FIXME make this nicer
	disco.Clear()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: nextAddr,
	})

	// Force an ungraceful shutdown of the gRPC server to which we're connected
	currentVtctld.Stop()

	// Wait for the client connection to shut down. If we redial too quickly,
	// we get into a race condition with gRPC's internal retry logic.
	// (Using WaitForReady here _does_ expose more function internals than is ideal for a unit test,
	// but it's far less flaky than using time.Sleep.)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err = proxy.VtctldClient.WaitForReady(ctx); err != nil {
			break
		}
	}

	// Finally, check that dial + establish a connection to the remaining vtctld.
	err = proxy.Dial(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, nextAddr, proxy.host)
}
