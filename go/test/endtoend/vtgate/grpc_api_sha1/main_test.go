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

package grpc_api_sha1

import (
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/utils"
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
	// SHA1 hash for "test_password": "16c2f1d4c5a5daad7b2bc06a2e0f8b5c9e4d3c9a"
	// This config mixes both SHA1 and plaintext passwords to test the hybrid plugin
	grpcServerAuthSha1JSON = `
		[
		  {
			"Username": "some_other_user",
			"SHA1HashedPassword": "16c2f1d4c5a5daad7b2bc06a2e0f8b5c9e4d3c9a"
		  },
		  {
			"Username": "another_unrelated_user",
			"Password": "test_password"
		  },
		  {
			"Username": "user_with_access",
			"SHA1HashedPassword": "16c2f1d4c5a5daad7b2bc06a2e0f8b5c9e4d3c9a"
		  },
		  {
			"Username": "user_no_access",
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

		// Create grpc_server_auth_sha1.json file
		grpcServerAuthSha1Path := path.Join(authDirectory, "grpc_server_auth_sha1.json")
		if err := createFile(grpcServerAuthSha1Path, grpcServerAuthSha1JSON); err != nil {
			return 1
		}

		// Create table_acl.json file
		tableACLPath := path.Join(authDirectory, "table_acl.json")
		if err := createFile(tableACLPath, tableACLJSON); err != nil {
			return 1
		}

		// Configure vtgate to use sha1 auth
		clusterInstance.VtGateExtraArgs = []string{
			utils.GetFlagVariantForTests("--grpc-auth-mode"), "sha1",
			utils.GetFlagVariantForTests("--grpc-auth-sha1-password-file"), grpcServerAuthSha1Path,
			utils.GetFlagVariantForTests("--grpc-use-effective-callerid"),
			utils.GetFlagVariantForTests("--grpc-use-sha1-authentication-callerid"),
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
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false, clusterInstance.Cell); err != nil {
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
