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
	"strings"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
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

	utils.Exec(t, oltp, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	defer func() {
		utils.Exec(t, oltp, "use @primary")
		utils.Exec(t, oltp, `delete from t1`)
	}()

	query1 := `select /*vt+ SCATTER_ERRORS_AS_WARNINGS */ id1 from t1`
	query2 := `select /*vt+ SCATTER_ERRORS_AS_WARNINGS */ id1 from t1 order by id1`
	showQ := "show warnings"

	// stop the mysql on one tablet, query will fail at vttablet level
	require.NoError(t,
		clusterInstance.Keyspaces[0].Shards[0].Replica().MysqlctlProcess.Stop())

	modes := []struct {
		conn *mysql.Conn
		m    string
	}{
		{m: "oltp", conn: oltp},
		{m: "olap", conn: olap},
	}

	for _, mode := range modes {
		t.Run(mode.m, func(t *testing.T) {
			// connection setup
			utils.Exec(t, mode.conn, "use @replica")
			utils.Exec(t, mode.conn, fmt.Sprintf("set workload = %s", mode.m))

			utils.AssertMatches(t, mode.conn, query1, `[[INT64(4)]]`)
			assertContainsOneOf(t, mode.conn, showQ, "no valid tablet", "no healthy tablet", "mysql.sock: connect: no such file or directory")
			utils.AssertMatches(t, mode.conn, query2, `[[INT64(4)]]`)
			assertContainsOneOf(t, mode.conn, showQ, "no valid tablet", "no healthy tablet", "mysql.sock: connect: no such file or directory")

			// invalid_field should throw error and not warning
			_, err = mode.conn.ExecuteFetch("SELECT /*vt+ PLANNER=Gen4 SCATTER_ERRORS_AS_WARNINGS */ invalid_field from t1;", 1, false)
			require.Error(t, err)
			serr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
			require.Equal(t, 1054, serr.Number(), serr.Error())
		})
	}
}

func assertContainsOneOf(t *testing.T, conn *mysql.Conn, query string, expected ...string) {
	t.Helper()
	qr := utils.Exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	for _, s := range expected {
		if strings.Contains(got, s) {
			return
		}
	}

	t.Errorf("%s\n did not match any of %v", got, expected)
}
