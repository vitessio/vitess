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
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	vtParams              mysql.ConnParams
	keyspaceUnshardedName = "ks1"
	cell                  = "zone1"
	hostname              = "localhost"
	mysqlAuthServerStatic = "mysql_auth_server_static.json"
	sqlSchema             = `
	create table keyspaces_to_watch_test(
		id BIGINT NOT NULL,
		msg VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) Engine=InnoDB;`
	vschemaDDL      = "alter vschema create vindex test_vdx using hash"
	vschemaDDLError = fmt.Sprintf("Error 1105 (HY000): cannot perform Update on keyspaces/%s/VSchema as the topology server connection is read-only",
		keyspaceUnshardedName)
)

// createConfig creates a config file in TmpDir in vtdataroot and writes the given data.
func createConfig(clusterInstance *cluster.LocalProcessCluster, name, data string) error {
	// creating new file
	f, err := os.Create(clusterInstance.TmpDirectory + "/" + name)
	if err != nil {
		return err
	}

	if data == "" {
		return nil
	}

	// write the given data
	_, err = fmt.Fprint(f, data)
	return err
}

func createCluster(extraVTGateArgs []string) (*cluster.LocalProcessCluster, int) {
	clusterInstance := cluster.NewCluster(cell, hostname)

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return nil, 1
	}

	// create auth server config
	SQLConfig := `{
		"testuser1": {
			"Password": "testpassword1",
			"UserData": "vtgate client 1"
		}
	}`
	if err := createConfig(clusterInstance, mysqlAuthServerStatic, SQLConfig); err != nil {
		return nil, 1
	}

	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceUnshardedName,
		SchemaSQL: sqlSchema,
	}
	if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
		return nil, 1
	}

	vtGateArgs := []string{
		"--mysql_auth_server_static_file", clusterInstance.TmpDirectory + "/" + mysqlAuthServerStatic,
		"--keyspaces_to_watch", keyspaceUnshardedName,
	}

	if extraVTGateArgs != nil {
		vtGateArgs = append(vtGateArgs, extraVTGateArgs...)
	}
	clusterInstance.VtGateExtraArgs = vtGateArgs

	// Start vtgate
	if err := clusterInstance.StartVtgate(); err != nil {
		return nil, 1
	}
	vtParams = mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	rand.Seed(time.Now().UnixNano())
	return clusterInstance, 0
}

func TestRoutingWithKeyspacesToWatch(t *testing.T) {
	defer cluster.PanicHandler(t)

	clusterInstance, exitCode := createCluster(nil)
	defer clusterInstance.Teardown()

	if exitCode != 0 {
		os.Exit(exitCode)
	}

	dsn := fmt.Sprintf(
		"testuser1:testpassword1@tcp(%s:%v)/",
		clusterInstance.Hostname,
		clusterInstance.VtgateMySQLPort,
	)
	db, err := sql.Open("mysql", dsn)
	require.Nil(t, err)
	defer db.Close()

	// if this returns w/o failing the test we're good to go
	_, err = db.Exec("select * from keyspaces_to_watch_test")
	require.Nil(t, err)
}

func TestVSchemaDDLWithKeyspacesToWatch(t *testing.T) {
	defer cluster.PanicHandler(t)

	extraVTGateArgs := []string{
		"--vschema_ddl_authorized_users", "%",
	}
	clusterInstance, exitCode := createCluster(extraVTGateArgs)
	defer clusterInstance.Teardown()

	if exitCode != 0 {
		os.Exit(exitCode)
	}

	dsn := fmt.Sprintf(
		"testuser1:testpassword1@tcp(%s:%v)/",
		clusterInstance.Hostname,
		clusterInstance.VtgateMySQLPort,
	)
	db, err := sql.Open("mysql", dsn)
	require.Nil(t, err)
	defer db.Close()

	// The topo server must be read-only when using keyspaces_to_watch in order to prevent
	// potentially corrupting the VSchema based on this vtgates limited view of the world
	_, err = db.Exec(vschemaDDL)
	require.EqualError(t, err, vschemaDDLError)
}
