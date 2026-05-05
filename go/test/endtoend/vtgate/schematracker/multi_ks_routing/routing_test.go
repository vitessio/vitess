/*
Copyright 2026 The Vitess Authors.

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

package multiksrouting

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	cell            = "zone1"
	ksA             = "ks_a"
	ksB             = "ks_b"

	//go:embed schema_a.sql
	schemaA string

	//go:embed schema_b.sql
	schemaB string

	//go:embed vschema.json
	emptyVSchema string

	//go:embed routing_rules.json
	routingRules string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		if err := clusterInstance.StartTopo(); err != nil {
			fmt.Println(err)
			return 1
		}

		// Two unsharded keyspaces, each with a single table on the tablet but
		// an explicit empty vschema (the schema tracker is the only source of
		// column info). The vschema is set explicitly rather than left to a
		// default so the test always pins down the routing-rules-only +
		// schema-tracker scenario regardless of vtctld defaults.
		ka := &cluster.Keyspace{Name: ksA, SchemaSQL: schemaA, VSchema: emptyVSchema}
		if err := clusterInstance.StartUnshardedKeyspace(*ka, 0, false, cell); err != nil {
			fmt.Println(err)
			return 1
		}
		kb := &cluster.Keyspace{Name: ksB, SchemaSQL: schemaB, VSchema: emptyVSchema}
		if err := clusterInstance.StartUnshardedKeyspace(*kb, 0, false, cell); err != nil {
			fmt.Println(err)
			return 1
		}

		if err := clusterInstance.VtctldClientProcess.ApplyRoutingRules(routingRules); err != nil {
			fmt.Println(err)
			return 1
		}
		if err := clusterInstance.VtctldClientProcess.ExecuteCommand("RebuildVSchemaGraph"); err != nil {
			fmt.Println(err)
			return 1
		}

		// vtgate runs with --schema-change-signal=true by default; the schema
		// tracker is what populates columns for ks_a.table_a and ks_b.table_b.
		if err := clusterInstance.StartVtgate(); err != nil {
			fmt.Println(err)
			return 1
		}
		if err := clusterInstance.WaitForVTGateAndVTTablets(time.Minute); err != nil {
			fmt.Println(err)
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

// TestRoutedTableColumnsAreAuthoritativeForStarExpansion exercises the case
// where two unsharded keyspaces have empty `tables: {}` in vschema and rely
// entirely on routing rules + the schema tracker to make their tables
// queryable. Once the tracker has reported columns for each routed table,
// the planner has to treat those columns as authoritative so it can expand
// `t.*` for queries it executes at vtgate.
//
// We cover four query shapes:
//   - `t.*` against each routed table (single-Route push-down),
//   - a cross-keyspace JOIN with explicit columns (no `*` expansion),
//   - a cross-keyspace JOIN with `t.*` qualified to one side (forces
//     vtgate-side expansion of the qualified `*`).
//
// All four should plan and return the seeded rows.
func TestRoutedTableColumnsAreAuthoritativeForStarExpansion(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait until the schema tracker has reported columns for both routed
	// tables. We poll the published vschema rather than retrying the query
	// itself so a transient `table not found` doesn't get conflated with the
	// authoritativeness bug we're actually testing.
	waitForColumns := func(ks, tbl string, want int) {
		t.Helper()
		utils.WaitForVschemaCondition(t, clusterInstance.VtgateProcess, ks,
			func(t *testing.T, keyspace map[string]any) bool {
				tables, ok := keyspace["tables"].(map[string]any)
				if !ok {
					return false
				}
				table, ok := tables[tbl].(map[string]any)
				if !ok {
					return false
				}
				cols, ok := table["columns"].([]any)
				return ok && len(cols) >= want
			}, fmt.Sprintf("%s.%s columns not yet tracked", ks, tbl))
	}
	waitForColumns(ksA, "table_a", 3)
	waitForColumns(ksB, "table_b", 2)

	// Seed two matching rows on each side so the JOINs actually return data
	// and we can verify execution, not just planning.
	utils.Exec(t, conn, "use @primary")
	utils.Exec(t, conn, "insert into table_a (id, fk, name) values (1, 10, 'alice'), (2, 20, 'bob')")
	utils.Exec(t, conn, "insert into table_b (id, label) values (10, 'ten'), (20, 'twenty')")

	tcases := []struct {
		name   string
		query  string
		expect string
	}{{
		name:   "star against routed unsharded table on ks_a",
		query:  "select a.* from table_a a where a.id = 1",
		expect: `[[INT32(1) INT32(10) VARCHAR("alice")]]`,
	}, {
		name:   "star against routed unsharded table on ks_b",
		query:  "select b.* from table_b b where b.id = 10",
		expect: `[[INT32(10) VARCHAR("ten")]]`,
	}, {
		name:   "cross-keyspace join, explicit columns",
		query:  "select a.id, b.label from table_a a join table_b b on a.fk = b.id order by a.id",
		expect: `[[INT32(1) VARCHAR("ten")] [INT32(2) VARCHAR("twenty")]]`,
	}, {
		// The JOIN can't be pushed to a single MySQL, so the planner has to
		// expand `a.*` at vtgate using the columns the tracker reported for
		// the routing rule's target table.
		name:   "cross-keyspace join, star qualified to one side",
		query:  "select a.* from table_a a join table_b b on a.fk = b.id order by a.id",
		expect: `[[INT32(1) INT32(10) VARCHAR("alice")] [INT32(2) INT32(20) VARCHAR("bob")]]`,
	}}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			qr, err := utils.ExecAllowError(t, conn, tc.query)
			require.NoErrorf(t, err, "query failed: %s", tc.query)
			require.Equalf(t, tc.expect, fmt.Sprintf("%v", qr.Rows), "unexpected result for %s", tc.query)
		})
	}
}
