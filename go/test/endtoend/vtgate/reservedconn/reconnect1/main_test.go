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

package reservedconn

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
	sqlSchema       = `create table test(id bigint primary key)Engine=InnoDB;`

	vSchema = `
		{	
			"sharded":true,
			"vindexes": {
				"hash_index": {
					"type": "hash"
				}
			},	
			"tables": {
				"test":{
					"column_vindexes": [
						{
							"column": "id",
							"name": "hash_index"
						}
					]
				}
			}
		}
	`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true); err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"-lock_heartbeat_time", "2s", "-enable_system_settings=true"} // enable reserved connection.
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestServingChange(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	checkedExec(t, conn, "use @rdonly")
	checkedExec(t, conn, "set sql_mode = ''")

	// to see rdonly is available and
	// also this will create reserved connection on rdonly on -80 and 80- shards.
	_, err = exec(t, conn, "select * from test")
	for err != nil {
		_, err = exec(t, conn, "select * from test")
	}

	// changing rdonly tablet to spare (non serving).
	rdonlyTablet := clusterInstance.Keyspaces[0].Shards[0].Rdonly()
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonlyTablet.Alias, "spare")
	require.NoError(t, err)

	// this should fail as there is no rdonly present
	_, err = exec(t, conn, "select * from test")
	require.Error(t, err)

	// changing replica tablet to rdonly to make rdonly available for serving.
	replicaTablet := clusterInstance.Keyspaces[0].Shards[0].Replica()
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replicaTablet.Alias, "rdonly")
	require.NoError(t, err)

	// to see/make the new rdonly available
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Ping", replicaTablet.Alias)
	require.NoError(t, err)

	// this should pass now as there is rdonly present
	_, err = exec(t, conn, "select * from test")
	assert.NoError(t, err)
}

func exec(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}

func checkedExec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}
