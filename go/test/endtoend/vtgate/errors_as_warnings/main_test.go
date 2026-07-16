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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	KeyspaceName = "ks"
	SchemaSQL    = `create table t1(
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

func setupCluster(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()

	ctx := t.Context()
	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(KeyspaceName).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	return cluster, cluster.VTParams(ctx, "")
}

func TestScatterErrsAsWarns(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)
	oltp, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer oltp.Close()

	olap, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer olap.Close()

	vitesst.Exec(t, oltp, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	defer func() {
		vitesst.Exec(t, oltp, "use @primary")
		vitesst.Exec(t, oltp, `delete from t1`)
	}()

	query1 := `select /*vt+ SCATTER_ERRORS_AS_WARNINGS */ id1 from t1`
	query2 := `select /*vt+ SCATTER_ERRORS_AS_WARNINGS */ id1 from t1 order by id1`
	showQ := "show warnings"

	// stop the mysql on one tablet, query will fail at vttablet level
	require.NoError(t,
		clusterInstance.Keyspace(KeyspaceName).Shards()[0].Replicas()[0].StopMySQL(t.Context()))

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
			vitesst.Exec(t, mode.conn, "use @replica")
			vitesst.Exec(t, mode.conn, "set workload = "+mode.m)

			expectedWarnings := []string{
				"operation not allowed in state NOT_SERVING",
				"operation not allowed in state SHUTTING_DOWN",
				"no valid tablet",
				"no healthy tablet",
				"mysql.sock: connect: no such file or directory",
			}
			vitesst.AssertMatches(t, mode.conn, query1, `[[INT64(4)]]`)
			assertContainsOneOf(t, mode.conn, showQ, expectedWarnings...)
			vitesst.AssertMatches(t, mode.conn, query2, `[[INT64(4)]]`)
			assertContainsOneOf(t, mode.conn, showQ, expectedWarnings...)

			// invalid_field should throw error and not warning
			_, err = mode.conn.ExecuteFetch("SELECT /*vt+ PLANNER=Gen4 SCATTER_ERRORS_AS_WARNINGS */ invalid_field from t1", 1, false)
			require.Error(t, err)
			serr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
			require.Equal(t, sqlerror.ERBadFieldError, serr.Number(), serr.Error())
		})
	}
}

func assertContainsOneOf(t *testing.T, conn *mysql.Conn, query string, expected ...string) {
	t.Helper()
	qr := vitesst.Exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	for _, s := range expected {
		if strings.Contains(got, s) {
			return
		}
	}

	assert.Failf(t, "no match", "%s\n did not match any of %v", got, expected)
}
