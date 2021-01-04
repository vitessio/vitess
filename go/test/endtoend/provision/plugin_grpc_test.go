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

package sequence

import (
	"context"
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"net"
	"os"
	"testing"
	"time"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/provision"
)

var (
	clusterForProvisionTest *cluster.LocalProcessCluster
	cell                    = "zone1"
	hostname                = "localhost"
	keyspace = "keyspace"
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {

		var lc net.ListenConfig
		listener, err := lc.Listen(context.Background(), "tcp", "localhost:")
		if err != nil {
			log.Error(err)
			return 1
		}

		defer listener.Close()

		go func() {
			log.Error(startProvisionerServer(listener))
		}()

		clusterForProvisionTest = cluster.NewCluster(cell, hostname)
		//FIXME: underscores or dashes
		clusterForProvisionTest.VtGateExtraArgs = []string {
			"-provision_create_keyspace_authorized_users",
			"%",
			"-provision_delete_keyspace_authorized_users",
			"%",
			"-provision_timeout",
			"30s",
			"-provision_type",
			"grpc",
			"-provision_grpc_endpoint",
			listener.Addr().String(),
			"-provision_grpc_dial_timeout",
			"1s",
			"-provision_grpc_per_retry_timeout",
			"1s",
			"-provision_grpc_max_retries",
			"1",
		}

		defer clusterForProvisionTest.Teardown()

		if err := clusterForProvisionTest.StartTopo(); err != nil {
			return 1
		}

		if err := clusterForProvisionTest.StartVtgate(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestProvisionKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)

	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: clusterForProvisionTest.Hostname,
		Port: clusterForProvisionTest.VtgateMySQLPort,
		ConnectTimeoutMs: 1000,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)

	createStatement := fmt.Sprintf("CREATE DATABASE %s;", keyspace)
	qr, err := conn.ExecuteFetch(createStatement, 10, true)
	require.Nil(t, err)

	assert.Equal(t, uint64(1), qr.RowsAffected, "returned: %v", qr.Rows)

	_, err = clusterForProvisionTest.VtctlclientProcess.ExecuteCommandWithOutput("GetKeyspace", keyspace)
	//If GetKeyspace doesn't return an error, the keyspace exists.
	require.Nil(t, err)

	dropStatement := fmt.Sprintf("DROP DATABASE %s;", keyspace)
	qr, err = conn.ExecuteFetch(dropStatement, 10, true)
	require.Nil(t, err)

	assert.Equal(t, uint64(1), qr.RowsAffected, "returned: %v", qr.Rows)

	_, err = clusterForProvisionTest.VtctlclientProcess.ExecuteCommandWithOutput("GetKeyspace", keyspace)
	//If GetKeyspace does return an error, we assume it's because the keyspace no longer exists, and not because of
	//a network error.
	assert.True(t, err != nil, "keyspace %s was not deleted", keyspace)
}

type testGrpcServer struct {}

func (_ testGrpcServer)RequestCreateKeyspace(ctx context.Context, rckr *provision.RequestCreateKeyspaceRequest) (*provision.ProvisionResponse, error) {
	//We're doing this in a go routine to simulate the fact that RequestCreateKeyspace does not block while the
	//the keyspace is being created. We want to exercise the topo polling logic, so we use a large wait time.
	go func() {
		<- time.After(10 * time.Second)
		err := clusterForProvisionTest.VtctlProcess.CreateKeyspace(rckr.Keyspace)
		if err != nil {
			log.Error(err)
		}
	}()
	return &provision.ProvisionResponse{}, nil
}

func (_ testGrpcServer)RequestDeleteKeyspace(ctx context.Context, rckr *provision.RequestDeleteKeyspaceRequest) (*provision.ProvisionResponse, error) {
	//We're doing this in a go routine to simulate the fact that RequestDeleteKeyspace does not block while the
	//the keyspace is being deleted. We want to exercise the topo polling logic, so we use a large wait time.
	go func() {
		<- time.After(10 * time.Second)
		err := clusterForProvisionTest.VtctlProcess.DeleteKeyspace(rckr.Keyspace)
		if err != nil {
			log.Error(err)
		}
	}()
	return &provision.ProvisionResponse{}, nil
}

func startProvisionerServer(listener net.Listener) error {
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	defer grpcServer.Stop()

	provision.RegisterProvisionServer(grpcServer, testGrpcServer{})
	return grpcServer.Serve(listener)
}