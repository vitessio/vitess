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
	listener, err := net.Listen("tcp", ":0")
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
