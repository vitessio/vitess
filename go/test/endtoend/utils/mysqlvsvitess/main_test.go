/*
Copyright 2022 The Vitess Authors.

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

package mysqlvsvitess

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "test"
	schemaSQL       = `create table t1(
		id1 bigint,
		id2 bigint,
		id3 bigint,
		primary key(id1)
	) Engine=InnoDB;`

	vschema = `
{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
	"t1": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        }
      ]
    }
  }
}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vschema,
		}
		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal", "--queryserver-config-schema-change-signal-interval", "0.1"}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--enable_system_settings=true")
		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		// create mysql instance and connection parameters
		conn, closer, err := utils.NewMySQL(clusterInstance, keyspaceName, schemaSQL)
		if err != nil {
			fmt.Println(err)
			return 1
		}
		defer closer()
		mysqlParams = conn

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestCreateMySQL(t *testing.T) {
	ctx := context.Background()
	mysqlConn, err := mysql.Connect(ctx, &mysqlParams)
	require.NoError(t, err)
	defer mysqlConn.Close()

	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer vtConn.Close()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2, id3) values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select * from t1;", `[[INT64(1) INT64(1) INT64(1)] [INT64(2) INT64(2) INT64(2)] [INT64(3) INT64(3) INT64(3)]]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select * from t1 order by id1 desc;", `[[INT64(3) INT64(3) INT64(3)] [INT64(2) INT64(2) INT64(2)] [INT64(1) INT64(1) INT64(1)]]`)
}
