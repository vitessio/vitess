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

package mysqlserver

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance       *vitesst.Cluster
	vtParams              mysql.ConnParams
	keyspaceName          = "test_keyspace"
	tableACLConfig        = "/vt/files/table_acl_config.json"
	mysqlAuthServerStatic = "/vt/files/mysql_auth_server_static.json"
	sqlSchema             = `create table vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		keyspace_id bigint(20) unsigned NOT NULL,
		data longblob,
		primary key (id)
		) Engine=InnoDB;
	create table vt_partition_test (
		c1 int NOT NULL,
		logdata BLOB NOT NULL,
		created DATETIME NOT NULL,
		PRIMARY KEY(c1, created)
		)
		PARTITION BY HASH( TO_DAYS(created) )
		PARTITIONS 10;
`
	createProcSQL = `
CREATE PROCEDURE testing()
BEGIN
	delete from vt_insert_test;
	delete from vt_partition_test;
END;
`
)

func setupCluster(t testing.TB) {
	t.Helper()

	// setting grpc max size
	if os.Getenv("grpc-max-message-size") == "" {
		os.Setenv("grpc-max-message-size", strconv.FormatInt(16*1024*1024, 10))
	}

	ctx := t.Context()

	ACLConfig := `{
			"table_groups": [
				{
					"table_names_or_prefixes": ["vt_insert_test", "vt_partition_test", "dual"],
					"readers": ["vtgate client 1"],
					"writers": ["vtgate client 1"],
					"admins": ["vtgate client 1"]
				}
			]
		}`

	SQLConfig := `{
			"testuser1": {
				"Password": "testpassword1",
				"UserData": "vtgate client 1"
			},
			"testuser2": {
				"Password": "testpassword2",
				"UserData": "vtgate client 2"
			}
		}`

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(keyspaceName).
			WithSchema(sqlSchema).
			WithReplicas(1),
		vitesst.WithVTGateArgs(
			"--vschema-ddl-authorized-users=%",
			"--mysql-server-query-timeout", "1s",
			"--mysql-auth-server-impl", "static",
			"--mysql-auth-server-static-file", mysqlAuthServerStatic,
			"--mysql-server-version", "8.0.16-7",
			"--warn-sharded-only=true",
		),
		vitesst.WithVTGateFiles(vitesst.ContainerFile{
			Content:       []byte(SQLConfig),
			ContainerPath: mysqlAuthServerStatic,
		}),
		vitesst.WithVTTabletArgs(
			"--table-acl-config", tableACLConfig,
			"--queryserver-config-strict-table-acl",
		),
		vitesst.WithTabletFiles(vitesst.ContainerFile{
			Content:       []byte(ACLConfig),
			ContainerPath: tableACLConfig,
		}),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cleanup(context.WithoutCancel(ctx)))
	})

	clusterInstance = cluster

	vtParams = cluster.VTParams(ctx, "")
	vtParams.Uname = "testuser1"
	vtParams.Pass = "testpassword1"

	primaryTablet := cluster.Keyspace(keyspaceName).Shard("-").Primary()
	_, err = primaryTablet.QueryTablet(ctx, createProcSQL)
	require.NoError(t, err)
}
