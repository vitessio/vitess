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

package grpc_api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vitesst"
)

var (
	keyspaceName = "ks"
	sqlSchema    = `
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
		  },
		  {
			"Username": "user_with_access",
			"Password": "test_password"
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

const (
	grpcServerAuthStaticPath = "/vt/files/grpc_server_auth_static.json"
	tableACLPath             = "/vt/files/table_acl.json"
)

func setupCluster(t *testing.T) *vitesst.Cluster {
	t.Helper()

	ctx := t.Context()
	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(1).
			WithSchema(sqlSchema),
		vitesst.WithVTGateArgs(
			"--grpc-auth-mode", "static",
			"--grpc-auth-static-password-file", grpcServerAuthStaticPath,
			"--grpc-use-effective-callerid",
			"--grpc-use-static-authentication-callerid",
		),
		vitesst.WithVTTabletArgs(
			"--enforce-tableacl-config",
			"--queryserver-config-strict-table-acl",
			"--table-acl-config", tableACLPath,
		),
		vitesst.WithVTGateFiles(vitesst.ContainerFile{
			Content:       []byte(grpcServerAuthStaticJSON),
			ContainerPath: grpcServerAuthStaticPath,
		}),
		vitesst.WithTabletFiles(vitesst.ContainerFile{
			Content:       []byte(tableACLJSON),
			ContainerPath: tableACLPath,
		}),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	return cluster
}
