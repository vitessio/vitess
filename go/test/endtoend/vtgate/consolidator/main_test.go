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
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"
)

type consolidatorTestCase struct {
	tabletType           string
	tablet               *vitesst.Tablet
	query                string
	expectConsolidations bool
}

var (
	KeyspaceName = "ks"
	SchemaSQL    = `create table t1(
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

func setupCluster(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()

	ctx := t.Context()
	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(KeyspaceName).
			WithReplicas(1).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	vtParams := cluster.VTParams(ctx, "")
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})
	vitesst.Exec(t, conn, `insert into t1(id1, id2) values (1, 1)`)

	return cluster, vtParams
}

func TestConsolidatorEnabledByDefault(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)
	shard := clusterInstance.Keyspace(KeyspaceName).Shard("-")
	testConsolidator(t, vtParams, []consolidatorTestCase{
		{
			"@primary",
			shard.Primary(),
			`select id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
		{
			"@replica",
			shard.Replicas()[0],
			`select id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
	})
}

func TestConsolidatorEnabledWithDirective(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)
	shard := clusterInstance.Keyspace(KeyspaceName).Shard("-")
	testConsolidator(t, vtParams, []consolidatorTestCase{
		{
			"@primary",
			shard.Primary(),
			`select /*vt+ CONSOLIDATOR=enabled */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
		{
			"@replica",
			shard.Replicas()[0],
			`select /*vt+ CONSOLIDATOR=enabled */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
	})
}

func TestConsolidatorDisabledWithDirective(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)
	shard := clusterInstance.Keyspace(KeyspaceName).Shard("-")
	testConsolidator(t, vtParams, []consolidatorTestCase{
		{
			"@primary",
			shard.Primary(),
			`select /*vt+ CONSOLIDATOR=disabled */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			false,
		},
		{
			"@replica",
			shard.Replicas()[0],
			`select /*vt+ CONSOLIDATOR=disabled */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			false,
		},
	})
}

func TestConsolidatorEnabledReplicasWithDirective(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)
	shard := clusterInstance.Keyspace(KeyspaceName).Shard("-")
	testConsolidator(t, vtParams, []consolidatorTestCase{
		{
			"@primary",
			shard.Primary(),
			`select /*vt+ CONSOLIDATOR=enabled_replicas */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			false,
		},
		{
			"@replica",
			shard.Replicas()[0],
			`select /*vt+ CONSOLIDATOR=enabled_replicas */ id2 from t1 where sleep(2) = 0 order by id1 asc limit 1`,
			true,
		},
	})
}

func testConsolidator(t *testing.T, vtParams mysql.ConnParams, testCases []consolidatorTestCase) {
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s%s", testCase.query, testCase.tabletType), func(t *testing.T) {
			// Create a connection.
			conn1, err := mysql.Connect(t.Context(), &vtParams)
			require.NoError(t, err)
			vitesst.Exec(t, conn1, "use "+testCase.tabletType)
			defer conn1.Close()

			// Create another connection.
			conn2, err := mysql.Connect(t.Context(), &vtParams)
			require.NoError(t, err)
			vitesst.Exec(t, conn2, "use "+testCase.tabletType)
			defer conn2.Close()

			// Create a channel for query results.
			qrCh := make(chan *sqltypes.Result, 2)
			defer close(qrCh)

			execAsync := func(conn *mysql.Conn, query string, qrCh chan *sqltypes.Result) {
				go func() {
					qrCh <- vitesst.Exec(t, conn, query)
				}()
			}

			// Check initial consolidations.
			consolidations, err := getConsolidations(t.Context(), testCase.tablet)
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
			consolidations, err = getConsolidations(t.Context(), testCase.tablet)
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

// getConsolidations scrapes the tablet's /debug/consolidations endpoint and
// returns each consolidated query mapped to its consolidation count.
func getConsolidations(ctx context.Context, tablet *vitesst.Tablet) (map[string]int, error) {
	status, body, err := tablet.MakeAPICall(ctx, "/debug/consolidations")
	if err != nil {
		return nil, fmt.Errorf("failed to get consolidations: %v", err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("failed to get consolidations: status %d", status)
	}

	result := make(map[string]int)

	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		splits := strings.SplitN(line, ":", 2)
		if len(splits) != 2 {
			return nil, fmt.Errorf("failed to split consolidations line: %s", line)
		}
		// Discard "Length: [N]" lines.
		if splits[0] == "Length" {
			continue
		}
		countI, err := strconv.Atoi(splits[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse consolidations count: %v", err)
		}
		result[strings.TrimSpace(splits[1])] = countI
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read consolidations: %v", err)
	}

	return result, nil
}
