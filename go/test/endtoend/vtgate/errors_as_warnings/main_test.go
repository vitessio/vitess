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

package vtgate

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"

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
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1
		}

		// Start vtgate
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

func TestScatterErrsAsWarns(t *testing.T) {
	oltp, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer oltp.Close()

	olap, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer olap.Close()

	checkedExec(t, oltp, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	defer func() {
		checkedExec(t, oltp, "use @master")
		checkedExec(t, oltp, `delete from t1`)
	}()

	// connection setup
	checkedExec(t, oltp, "use @replica")
	checkedExec(t, oltp, "set workload = oltp")
	checkedExec(t, olap, "use @replica")
	checkedExec(t, olap, "set workload = olap")

	// stop one tablet from the first shard
	require.NoError(t,
		clusterInstance.Keyspaces[0].Shards[0].Replica().MysqlctlProcess.Stop())

	query1 := `select /*vt+ SCATTER_ERRORS_AS_WARNINGS */ id1 from t1`
	query2 := `select /*vt+ SCATTER_ERRORS_AS_WARNINGS */ id1 from t1 order by id1`

	assertMatches(t, oltp, query1, `[[INT64(4)]]`)
	assertMatches(t, olap, query1, `[[INT64(4)]]`)
	assertMatches(t, oltp, query2, `[[INT64(4)]]`)
	assertMatches(t, olap, query2, `[[INT64(4)]]`)

	// change tablet type
	assert.NoError(t,
		clusterInstance.VtctlclientProcess.ExecuteCommand(
			"ChangeTabletType", clusterInstance.Keyspaces[0].Shards[0].Replica().Alias, "spare"))

	assertMatches(t, oltp, query1, `[[INT64(4)]]`)
	assertMatches(t, olap, query1, `[[INT64(4)]]`)
	assertMatches(t, oltp, query2, `[[INT64(4)]]`)
	assertMatches(t, olap, query2, `[[INT64(4)]]`)
}

func checkedExec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := checkedExec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}
