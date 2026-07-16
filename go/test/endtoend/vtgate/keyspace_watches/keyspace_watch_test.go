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

/*
Test the vtgate's ability to route while watching a subset of keyspaces.
*/

package keyspacewatches

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

const authServerStaticPath = "/vt/files/mysql_auth_server_static.json"

var (
	keyspaceUnshardedName = "ks1"
	sqlSchema             = `
	create table keyspaces_to_watch_test(
		id BIGINT NOT NULL,
		msg VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) Engine=InnoDB;`
	vschemaDDL      = "alter vschema create vindex test_vdx using hash"
	vschemaDDLError = "Error 1105 (HY000): cannot update VSchema as the topology server connection is read-only"
)

func createCluster(t *testing.T, ctx context.Context, extraVTGateArgs ...string) (*vitesst.Cluster, func(context.Context) error, error) {
	SQLConfig := `{
		"testuser1": {
			"Password": "testpassword1",
			"UserData": "vtgate client 1"
		}
	}`

	vtGateArgs := []string{
		"--mysql-auth-server-impl", "static",
		"--mysql-auth-server-static-file", authServerStaticPath,
		"--keyspaces-to-watch", keyspaceUnshardedName,
	}
	vtGateArgs = append(vtGateArgs, extraVTGateArgs...)

	clusterInstance, err := vitesst.NewCluster(t,
		vitesst.WithVTGateArgs(vtGateArgs...),
		vitesst.WithVTGateFiles(vitesst.ContainerFile{
			Content:       []byte(SQLConfig),
			ContainerPath: authServerStaticPath,
			Mode:          0o644,
		}),
		vitesst.WithKeyspace(keyspaceUnshardedName).
			WithReplicas(1).
			WithSchema(sqlSchema),
	)
	if err != nil {
		return nil, nil, err
	}
	cleanup, err := clusterInstance.Start(t, ctx)
	if err != nil {
		return nil, nil, err
	}
	return clusterInstance, cleanup, nil
}

func TestRoutingWithKeyspacesToWatch(t *testing.T) {
	ctx := t.Context()
	clusterInstance, cleanup, err := createCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	}()

	addr, err := clusterInstance.VTGate().MySQLAddr(ctx)
	require.NoError(t, err)

	dsn := fmt.Sprintf("testuser1:testpassword1@tcp(%s)/", addr)
	db, err := sql.Open("mysql", dsn)
	require.Nil(t, err)
	defer db.Close()

	// if this returns w/o failing the test we're good to go
	_, err = db.Exec("select * from keyspaces_to_watch_test")
	require.Nil(t, err)
}

func TestVSchemaDDLWithKeyspacesToWatch(t *testing.T) {
	ctx := t.Context()
	clusterInstance, cleanup, err := createCluster(t, ctx, "--vschema-ddl-authorized-users", "%")
	require.NoError(t, err)
	defer func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	}()

	addr, err := clusterInstance.VTGate().MySQLAddr(ctx)
	require.NoError(t, err)

	dsn := fmt.Sprintf("testuser1:testpassword1@tcp(%s)/", addr)
	db, err := sql.Open("mysql", dsn)
	require.Nil(t, err)
	defer db.Close()

	// The topo server must be read-only when using keyspaces-to-watch in order to prevent
	// potentially corrupting the VSchema based on this vtgates limited view of the world
	_, err = db.Exec(vschemaDDL)
	require.EqualError(t, err, vschemaDDLError)
}
