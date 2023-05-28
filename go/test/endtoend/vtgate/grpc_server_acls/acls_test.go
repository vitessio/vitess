/*
Copyright 2023 The Vitess Authors.

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

package grpc_server_acls

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"vitess.io/vitess/go/vt/callerid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

var (
	clusterInstance   *cluster.LocalProcessCluster
	vtgateGrpcAddress string
	hostname          = "localhost"
	keyspaceName      = "ks"
	cell              = "zone1"
	sqlSchema         = `
		create table test_table (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
	grpcServerAuthStaticJSON = `
		[
		  {
			"Username": "some_other_user",
			"Password": "test_password"
		  },
		  {
			"Username": "another_unrelated_user",
			"Password": "test_password"
		  }
		]
`
	tableACLJSON = `
		{
		  "table_groups": [
			{
			  "name": "default",
			  "table_names_or_prefixes": ["%"],
			  "readers": ["user_with_access"],
			  "writers": ["user_with_access"],
			  "admins": ["user_with_access"]
			}
		  ]
		}
`
)

func TestMain(m *testing.M) {

	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Directory for authn / authz config files
		authDirectory := path.Join(clusterInstance.TmpDirectory, "auth")
		if err := os.Mkdir(authDirectory, 0700); err != nil {
			return 1
		}

		// Create grpc_server_auth_static.json file
		grpcServerAuthStaticPath := path.Join(authDirectory, "grpc_server_auth_static.json")
		if err := createFile(grpcServerAuthStaticPath, grpcServerAuthStaticJSON); err != nil {
			return 1
		}

		// Create table_acl.json file
		tableACLPath := path.Join(authDirectory, "table_acl.json")
		if err := createFile(tableACLPath, tableACLJSON); err != nil {
			return 1
		}

		// Configure vtgate to use static auth
		clusterInstance.VtGateExtraArgs = []string{
			"--grpc_auth_mode", "static",
			"--grpc_auth_static_password_file", grpcServerAuthStaticPath,
			"--grpc_use_effective_callerid",
			"--grpc-use-static-authentication-callerid",
		}

		// Configure vttablet to use table ACL
		clusterInstance.VtTabletExtraArgs = []string{
			"--enforce-tableacl-config",
			"--queryserver-config-strict-table-acl",
			"--table-acl-config", tableACLPath,
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			clusterInstance.VtgateProcess = cluster.VtgateProcess{}
			return 1
		}
		vtgateGrpcAddress = fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateGrpcPort)

		return m.Run()
	}()
	os.Exit(exitcode)
}

// TestEffectiveCallerIDWithAccess verifies that an authenticated gRPC static user with an effectiveCallerID that has ACL access can execute queries
func TestEffectiveCallerIDWithAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := dialVTGate(ctx, t, "some_other_user", "test_password")
	if err != nil {
		t.Fatal(err)
	}
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	ctx = callerid.NewContext(ctx, callerid.NewEffectiveCallerID("user_with_access", "", ""), nil)
	_, err = session.Execute(ctx, query, nil)
	assert.NoError(t, err)
}

// TestEffectiveCallerIDWithNoAccess verifies that an authenticated gRPC static user without an effectiveCallerID that has ACL access cannot execute queries
func TestEffectiveCallerIDWithNoAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := dialVTGate(ctx, t, "another_unrelated_user", "test_password")
	if err != nil {
		t.Fatal(err)
	}
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	ctx = callerid.NewContext(ctx, callerid.NewEffectiveCallerID("user_no_access", "", ""), nil)
	_, err = session.Execute(ctx, query, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'test_table' (ACL check error)")
}

func dialVTGate(ctx context.Context, t *testing.T, username string, password string) (*vtgateconn.VTGateConn, error) {
	clientCreds := &grpcclient.StaticAuthClientCreds{Username: username, Password: password}
	creds := grpc.WithPerRPCCredentials(clientCreds)
	dialerFunc := grpcvtgateconn.Dial(creds)
	dialerName := t.Name()
	vtgateconn.RegisterDialer(dialerName, dialerFunc)
	return vtgateconn.DialProtocol(ctx, dialerName, vtgateGrpcAddress)
}

func createFile(path string, contents string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = f.WriteString(contents)
	if err != nil {
		return err
	}
	return f.Close()
}
