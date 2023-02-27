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

package vtgate

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

type consolidatorTestCase struct {
	tabletType           string
	tabletProcess        *cluster.VttabletProcess
	query                string
	expectConsolidations bool
}

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
  "sharded": false,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "t1": {}
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
		if err := clusterInstance.StartKeyspace(
			*keyspace,
			[]string{"-"},
			1, /*creates 1 replica tablet in addition to primary*/
			false,
		); err != nil {
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

		conn, err := mysql.Connect(context.Background(), &vtParams)
		if err != nil {
			return 1
		}
		defer conn.Close()

		// Insert some test data.
		_, err = conn.ExecuteFetch(`insert into t1(id1, id2) values (1, 1)`, 1000, true)
		if err != nil {
			return 1
		}
		defer func() {
			conn.ExecuteFetch(`use @primary`, 1000, true)
			conn.ExecuteFetch(`delete from t1`, 1000, true)
		}()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestConsolidatorEnabledByDefault(t *testing.T) {
	testConsolidator(t, []consolidatorTestCase{
		{
			"@primary",
			clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess,
			`select id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
		{
			"@replica",
			clusterInstance.Keyspaces[0].Shards[0].Replica().VttabletProcess,
			`select id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
	})
}

func TestConsolidatorEnabledWithDirective(t *testing.T) {
	testConsolidator(t, []consolidatorTestCase{
		{
			"@primary",
			clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess,
			`select /*vt+ CONSOLIDATOR=enabled */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
		{
			"@replica",
			clusterInstance.Keyspaces[0].Shards[0].Replica().VttabletProcess,
			`select /*vt+ CONSOLIDATOR=enabled */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
	})
}

func TestConsolidatorDisabledWithDirective(t *testing.T) {
	testConsolidator(t, []consolidatorTestCase{
		{
			"@primary",
			clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess,
			`select /*vt+ CONSOLIDATOR=disabled */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			false,
		},
		{
			"@replica",
			clusterInstance.Keyspaces[0].Shards[0].Replica().VttabletProcess,
			`select /*vt+ CONSOLIDATOR=disabled */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			false,
		},
	})
}

func TestConsolidatorEnabledReplicasWithDirective(t *testing.T) {
	testConsolidator(t, []consolidatorTestCase{
		{
			"@primary",
			clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess,
			`select /*vt+ CONSOLIDATOR=enabled_replicas */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			false,
		},
		{
			"@replica",
			clusterInstance.Keyspaces[0].Shards[0].Replica().VttabletProcess,
			`select /*vt+ CONSOLIDATOR=enabled_replicas */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
	})
}

func testConsolidator(t *testing.T, testCases []consolidatorTestCase) {
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s%s", testCase.query, testCase.tabletType), func(t *testing.T) {
			// Create a connection.
			conn1, err := mysql.Connect(context.Background(), &vtParams)
			require.NoError(t, err)
			utils.Exec(t, conn1, fmt.Sprintf("use %s", testCase.tabletType))
			defer conn1.Close()

			// Create another connection.
			conn2, err := mysql.Connect(context.Background(), &vtParams)
			require.NoError(t, err)
			utils.Exec(t, conn2, fmt.Sprintf("use %s", testCase.tabletType))
			defer conn2.Close()

			// Create a channel for query results.
			qrCh := make(chan *sqltypes.Result, 2)
			defer close(qrCh)

			execAsync := func(conn *mysql.Conn, query string, qrCh chan *sqltypes.Result) {
				go func() {
					qrCh <- utils.Exec(t, conn, query)
				}()
			}

			// Check initial consolidations.
			consolidations, err := testCase.tabletProcess.GetConsolidations()
			require.NoError(t, err, "Failed to get consolidations.")
			count := consolidations[testCase.query]

			// Send two identical async queries in quick succession.
			execAsync(conn1, testCase.query, qrCh)
			execAsync(conn2, testCase.query, qrCh)

			// Wait for results, verify they are the same.
			qr1 := <-qrCh
			qr2 := <-qrCh
			diff := cmp.Diff(fmt.Sprintf("%v", qr1.Rows), fmt.Sprintf("%v", qr2.Rows))
			require.Empty(t, diff, "Expected query results to be equal but they are different.")

			// Verify the query was (or was not) consolidated.
			consolidations, err = testCase.tabletProcess.GetConsolidations()
			require.NoError(t, err, "Failed to get consolidations.")
			if testCase.expectConsolidations {
				require.Greater(
					t,
					consolidations[testCase.query],
					count,
					"Expected query `%s` to be consolidated on %s tablet.",
					testCase.query,
					testCase.tabletType,
				)
			} else {
				require.Equal(
					t,
					count,
					consolidations[testCase.query],
					"Did not expect query `%s` to be consolidated on %s tablet.",
					testCase.query,
					testCase.tabletType,
				)
			}
		})
	}
}
