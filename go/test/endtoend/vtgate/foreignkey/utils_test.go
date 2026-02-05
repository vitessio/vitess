/*
Copyright 2023 The Vitess Authors.

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

package foreignkey

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var supportedOpps = []string{"*", "+", "-"}

// getTestName prepends the test with keyspace name.
func getTestName(testName string, keyspace string) string {
	return keyspace + " - " + testName
}

// isMultiColFkTable tells if the table is a multicol table or not.
func isMultiColFkTable(tableName string) bool {
	return strings.Contains(tableName, "multicol")
}

func (fz *fuzzer) generateExpression(length int, cols ...string) string {
	expr := fz.getColOrInt(cols...)
	if length == 1 {
		return expr
	}
	rhsExpr := fz.generateExpression(length-1, cols...)
	op := supportedOpps[rand.IntN(len(supportedOpps))]
	return fmt.Sprintf("%v %s (%v)", expr, op, rhsExpr)
}

// getColOrInt gets a column or an integer/NULL literal with equal probability.
func (fz *fuzzer) getColOrInt(cols ...string) string {
	if len(cols) == 0 || rand.IntN(2) == 0 {
		return convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
	}
	return cols[rand.IntN(len(cols))]
}

// convertIntValueToString converts the given value to a string
func convertIntValueToString(value int) string {
	if value == 0 {
		return "NULL"
	}
	return fmt.Sprintf("%d", value)
}

// waitForSchemaTrackingForFkTables waits for schema tracking to have run and seen the tables used
// for foreign key tests.
func waitForSchemaTrackingForFkTables(t testing.TB) {
	err := utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t1", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t18", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t11", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t1", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t18", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t11", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedUnmanagedKs, "fk_t11", "col")
	require.NoError(t, err)
}

// getReplicaTablets gets all the replica tablets.
func getReplicaTablets(keyspace string) []*cluster.Vttablet {
	var replicaTablets []*cluster.Vttablet
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != keyspace {
			continue
		}
		for _, shard := range ks.Shards {
			for _, vttablet := range shard.Vttablets {
				if vttablet.Type != "primary" {
					replicaTablets = append(replicaTablets, vttablet)
				}
			}
		}
	}
	return replicaTablets
}

// removeAllForeignKeyConstraints removes all the foreign key constraints from the given tablet.
func removeAllForeignKeyConstraints(t *testing.T, vttablet *cluster.Vttablet, keyspace string) {
	getAllFksQuery := `SELECT RefCons.table_name, RefCons.constraint_name FROM information_schema.referential_constraints RefCons;`
	res, err := utils.RunSQL(t, getAllFksQuery, vttablet, "")
	require.NoError(t, err)
	var queries []string
	queries = append(queries, "set global super_read_only=0")
	for _, row := range res.Rows {
		tableName := row[0].ToString()
		constraintName := row[1].ToString()
		removeFkQuery := fmt.Sprintf("ALTER TABLE %v DROP CONSTRAINT %v", tableName, constraintName)
		queries = append(queries, removeFkQuery)
	}
	queries = append(queries, "set global super_read_only=1")
	err = utils.RunSQLs(t, queries, vttablet, fmt.Sprintf("vt_%v", keyspace))
	require.NoError(t, err)
}

// checkReplicationHealthy verifies that the replication on the given vttablet is working as expected.
func checkReplicationHealthy(t *testing.T, vttablet *cluster.Vttablet) {
	rs, err := utils.RunSQL(t, "show replica status", vttablet, "")
	require.NoError(t, err)
	var ioThreadRunning, sqlThreadRunning string
	for idx, value := range rs.Rows[0] {
		fieldName := rs.Fields[idx].Name
		if fieldName == "Replica_IO_Running" {
			ioThreadRunning = value.ToString()
		}
		if fieldName == "Replica_SQL_Running" {
			sqlThreadRunning = value.ToString()
		}
	}
	require.Equal(t, "Yes", sqlThreadRunning, "SQL Thread isn't happy on %v, Replica status - %v", vttablet.Alias, rs.Rows)
	require.Equal(t, "Yes", ioThreadRunning, "IO Thread isn't happy on %v, Replica status - %v", vttablet.Alias, rs.Rows)
}

// compareVitessAndMySQLResults compares Vitess and MySQL results and reports if they don't report the same number of rows affected.
func compareVitessAndMySQLResults(t *testing.T, vtRes sql.Result, mysqlRes sql.Result) {
	if vtRes == nil && mysqlRes == nil {
		return
	}
	if vtRes == nil {
		t.Error("Vitess result is 'nil' while MySQL's is not.")
		return
	}
	if mysqlRes == nil {
		t.Error("MySQL result is 'nil' while Vitess' is not.")
		return
	}
	vtRa, err := vtRes.RowsAffected()
	require.NoError(t, err)
	mysqlRa, err := mysqlRes.RowsAffected()
	require.NoError(t, err)
	if mysqlRa != vtRa {
		t.Errorf("Vitess and MySQL don't agree on the rows affected. Vitess rows affected - %v, MySQL rows affected - %v", vtRa, mysqlRa)
	}
}

// compareVitessAndMySQLErrors compares Vitess and MySQL errors and reports if one errors and the other doesn't.
func compareVitessAndMySQLErrors(t *testing.T, vtErr, mysqlErr error) {
	if vtErr != nil && mysqlErr != nil || vtErr == nil && mysqlErr == nil {
		return
	}
	out := fmt.Sprintf("Vitess and MySQL are not erroring the same way.\nVitess error: %v\nMySQL error: %v", vtErr, mysqlErr)
	t.Error(out)
}

// ensureDatabaseState ensures that the database is either empty or not.
func ensureDatabaseState(t *testing.T, vtconn *mysql.Conn, empty bool) {
	results := collectFkTablesState(vtconn)
	isEmpty := true
	for _, res := range results {
		if len(res.Rows) > 0 {
			isEmpty = false
		}
	}
	require.Equal(t, isEmpty, empty)
}

// verifyDataIsCorrect verifies that the data in MySQL database matches the data in the Vitess database.
func verifyDataIsCorrect(t *testing.T, mcmp utils.MySQLCompare, concurrency int) {
	// For single concurrent thread, we run all the queries on both MySQL and Vitess, so we can verify correctness
	// by just checking if the data in MySQL and Vitess match.
	if concurrency == 1 {
		for _, table := range fkTables {
			query := fmt.Sprintf("SELECT * FROM %v ORDER BY id", table)
			mcmp.Exec(query)
		}
	} else {
		// For higher concurrency, we don't have MySQL data to verify everything is fine,
		// so we'll have to do something different.
		// We run LEFT JOIN queries on all the parent and child tables linked by foreign keys
		// to make sure that nothing is broken in the database.
		for _, reference := range fkReferences {
			query := fmt.Sprintf("select %v.id from %v left join %v on (%v.col = %v.col) where %v.col is null and %v.col is not null", reference.childTable, reference.childTable, reference.parentTable, reference.parentTable, reference.childTable, reference.parentTable, reference.childTable)
			if isMultiColFkTable(reference.childTable) {
				query = fmt.Sprintf("select %v.id from %v left join %v on (%v.cola = %v.cola and %v.colb = %v.colb) where %v.cola is null and %v.cola is not null and %v.colb is not null", reference.childTable, reference.childTable, reference.parentTable, reference.parentTable, reference.childTable, reference.parentTable, reference.childTable, reference.parentTable, reference.childTable, reference.childTable)
			}
			res, err := mcmp.VtConn.ExecuteFetch(query, 1000, false)
			require.NoError(t, err)
			require.Zerof(t, len(res.Rows), "Query %v gave non-empty results", query)
		}
	}
	// We also verify that the results in Primary and Replica table match as is.
	for _, keyspace := range clusterInstance.Keyspaces {
		for _, shard := range keyspace.Shards {
			var primaryTab, replicaTab *cluster.Vttablet
			for _, vttablet := range shard.Vttablets {
				if vttablet.Type == "primary" {
					primaryTab = vttablet
				} else {
					replicaTab = vttablet
				}
			}
			require.NotNil(t, primaryTab)
			require.NotNil(t, replicaTab)
			checkReplicationHealthy(t, replicaTab)
			cluster.WaitForReplicationPos(t, primaryTab, replicaTab, true, 1*time.Minute)
			primaryConn, err := utils.GetMySQLConn(primaryTab, fmt.Sprintf("vt_%v", keyspace.Name))
			require.NoError(t, err)
			replicaConn, err := utils.GetMySQLConn(replicaTab, fmt.Sprintf("vt_%v", keyspace.Name))
			require.NoError(t, err)
			primaryRes := collectFkTablesState(primaryConn)
			replicaRes := collectFkTablesState(replicaConn)
			verifyDataMatches(t, primaryRes, replicaRes)
		}
	}
}

// verifyDataMatches verifies that the two list of results are the same.
func verifyDataMatches(t testing.TB, resOne []*sqltypes.Result, resTwo []*sqltypes.Result) {
	require.EqualValues(t, len(resTwo), len(resOne), "Res 1 - %v, Res 2 - %v", resOne, resTwo)
	for idx, resultOne := range resOne {
		resultTwo := resTwo[idx]
		require.True(t, resultOne.Equal(resultTwo), "Data for %v doesn't match\nRows 1\n%v\nRows 2\n%v", fkTables[idx], resultOne.Rows, resultTwo.Rows)
	}
}

// collectFkTablesState collects the data stored in the foreign key tables for the given connection.
func collectFkTablesState(conn *mysql.Conn) []*sqltypes.Result {
	var tablesData []*sqltypes.Result
	for _, table := range fkTables {
		query := fmt.Sprintf("SELECT * FROM %v ORDER BY id", table)
		res, _ := conn.ExecuteFetch(query, 10000, true)
		tablesData = append(tablesData, res)
	}
	return tablesData
}

func validateReplication(t *testing.T) {
	for _, keyspace := range clusterInstance.Keyspaces {
		for _, shard := range keyspace.Shards {
			for _, vttablet := range shard.Vttablets {
				if vttablet.Type != "primary" {
					checkReplicationHealthy(t, vttablet)
				}
			}
		}
	}
}

// compareResultRows compares the rows of the two results provided.
func compareResultRows(resOne *sqltypes.Result, resTwo *sqltypes.Result) bool {
	return slices.EqualFunc(resOne.Rows, resTwo.Rows, func(a, b sqltypes.Row) bool {
		return sqltypes.RowEqual(a, b)
	})
}

// setupBenchmark sets up the benchmark by creating the set of queries that we want to run. It also ensures that the 3 modes (MySQL, Vitess Managed, Vitess Unmanaged) we verify all return the same results after the queries have been executed.
func setupBenchmark(b *testing.B, maxValForId int, maxValForCol int, insertShare int, deleteShare int, updateShare int, numQueries int) ([]string, *mysql.Conn, *mysql.Conn, *mysql.Conn) {
	// Clear out all the data to ensure we start with a clean slate.
	startBenchmark(b)
	// Create a fuzzer to generate and store a certain set of queries.
	fz := newFuzzer(1, maxValForId, maxValForCol, insertShare, deleteShare, updateShare, SQLQueries, nil)
	fz.noFkSetVar = true
	var queries []string
	for j := 0; j < numQueries; j++ {
		genQueries := fz.generateQuery()
		require.Len(b, genQueries, 1)
		queries = append(queries, genQueries[0])
	}

	// Connect to MySQL and run all the queries
	mysqlConn, err := mysql.Connect(context.Background(), &mysqlParams)
	require.NoError(b, err)
	// Connect to Vitess managed foreign keys keyspace
	vtConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(b, err)
	utils.Exec(b, vtConn, fmt.Sprintf("use `%v`", unshardedKs))
	// Connect to Vitess unmanaged foreign keys keyspace
	vtUnmanagedConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(b, err)
	utils.Exec(b, vtUnmanagedConn, fmt.Sprintf("use `%v`", unshardedUnmanagedKs))

	// First we make sure that running all the queries in both the Vitess modes and MySQL gives the same data.
	// So we run all the queries and then check that the data in all of them matches.
	runQueries(b, mysqlConn, queries)
	runQueries(b, vtConn, queries)
	runQueries(b, vtUnmanagedConn, queries)
	for _, table := range fkTables {
		query := fmt.Sprintf("SELECT * FROM %v ORDER BY id", table)
		resVitessManaged, _ := vtConn.ExecuteFetch(query, 10000, true)
		resMySQL, _ := mysqlConn.ExecuteFetch(query, 10000, true)
		resVitessUnmanaged, _ := vtUnmanagedConn.ExecuteFetch(query, 10000, true)
		require.True(b, compareResultRows(resVitessManaged, resMySQL), "Results for %v don't match\nVitess Managed\n%v\nMySQL\n%v", table, resVitessManaged, resMySQL)
		require.True(b, compareResultRows(resVitessUnmanaged, resMySQL), "Results for %v don't match\nVitess Unmanaged\n%v\nMySQL\n%v", table, resVitessUnmanaged, resMySQL)
	}
	return queries, mysqlConn, vtConn, vtUnmanagedConn
}

func runQueries(t testing.TB, conn *mysql.Conn, queries []string) {
	for _, query := range queries {
		_, _ = utils.ExecAllowError(t, conn, query)
	}
}
