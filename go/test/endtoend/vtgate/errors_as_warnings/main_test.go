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

package vtgate

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
	KeyspaceName    = "ks"
	Cell            = "test"
	SchemaSQL       = `create table t1(
	id1 bigint,
	id2 bigint,
	primary key(id1)
) Engine=InnoDB;`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "xxhash": {
      "type": "xxhash"
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "xxhash"
        }
      ]
    }
  }
}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		vtgateInstance := clusterInstance.NewVtgateInstance()
		vtgateInstance.TabletTypesToWait = "MASTER,REPLICA,RDONLY"
		if err := vtgateInstance.Setup(); err != nil {
			return 1
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestScatterErrsAsWarns(t *testing.T) {
	oltp, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer oltp.Close()

	olap, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer olap.Close()
	exec(t, olap, "set workload = olap")
	exec(t, olap, "use @replica")

	exec(t, oltp, "use @master")
	exec(t, oltp, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	exec(t, oltp, "set workload = oltp")
	exec(t, oltp, "use @replica")

	defer func() {
		exec(t, oltp, "use @master")
		exec(t, oltp, `delete from t1`)
	}()

	require.NoError(t, // stop one tablet from the first shard
		clusterInstance.Keyspaces[0].Shards[0].Replica().MysqlctlProcess.Stop())

	query := `select /*vt+ SCATTER_ERRORS_AS_WARNINGS */ id1 from t1`
	qr, err := oltp.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	assert.NotEmpty(t, qr.Rows)

	qr, err = olap.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	assert.NotEmpty(t, qr.Rows)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}
